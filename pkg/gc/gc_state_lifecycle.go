// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// A generation is a unique identity, including after its active lifetime ends.
// Its context owns the cancellation domain for shared loads and runtime workers;
// retirement closes done and cancels that work before waiting for the manager lock.
type gcStateGeneration struct {
	mu      syncutil.Mutex
	done    chan struct{}
	flights map[uint32]*gcStateLoadFlight

	// Lifecycle ownership is established before publishing this generation.
	ctx      context.Context
	cancel   context.CancelFunc
	workDone chan struct{}
	index    *enabledKeyspaceIndex
	warmup   *gcStateWarmup
}

func (g *gcStateGeneration) retire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.done:
		return
	default:
	}
	close(g.done)
	g.cancel()
	for id, flight := range g.flights {
		flight.err = errs.ErrNotLeader
		close(flight.done)
		delete(g.flights, id)
	}
}

// SetEtcdClient enables metadata discovery and initial GC warmup. NextGen
// configures it before the first leadership generation; other deployments leave
// it unset and retain passive cache loading.
func (m *GCStateManager) SetEtcdClient(client *clientv3.Client) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.etcdClient = client
}

// OnNodeBecomesLeader starts a unique leadership generation and returns its
// idempotent cleanup. A delayed cleanup only affects the generation it owns.
func (m *GCStateManager) OnNodeBecomesLeader() func() {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if previous := m.activeGeneration.Swap(nil); previous != nil {
		previous.retire()
		// Runtime completion never takes lifecycleMu. It is safe to retain
		// reset serialization while joining already-cancelled work, before
		// acquiring the manager lock.
		previous.waitRuntime()
	}
	m.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	generation := &gcStateGeneration{
		done:    make(chan struct{}),
		flights: make(map[uint32]*gcStateLoadFlight),
		ctx:     ctx,
		cancel:  cancel,
	}
	if m.etcdClient != nil {
		generation.workDone = make(chan struct{})
		generation.index = newEnabledKeyspaceIndex(m.etcdClient, keypath.KeyspaceMetaPrefix())
		generation.warmup = newGCStateWarmup(m, generation, generation.index)
	}
	failpoint.InjectCall("beforeLeaderGCStateCacheReset")
	m.gcStateCache.clearAll()
	m.barrierMetrics.clearMetrics()
	productionBarrierMetrics.current.Store(m.barrierMetrics)
	m.generation = generation
	m.activeGeneration.Store(generation)
	m.mu.Unlock()
	if generation.workDone != nil {
		go func() {
			defer close(generation.workDone)
			go generation.warmup.run(generation.ctx)
			generation.index.run(generation.ctx, enabledKeyspaceLoadHooks{
				onPage:            generation.warmup.onPage,
				onInitialSnapshot: generation.warmup.onInitialSnapshot,
			})
			<-generation.warmup.done
		}()
	}
	return func() { m.stopGCStateGeneration(generation) }
}

func (g *gcStateGeneration) waitRuntime() {
	if g.workDone != nil {
		<-g.workDone
	}
}

func (m *GCStateManager) stopGCStateGeneration(generation *gcStateGeneration) {
	if generation == nil {
		return
	}
	m.activeGeneration.CompareAndSwap(generation, nil)
	generation.retire()
	m.lifecycleMu.Lock()
	m.mu.Lock()
	if m.generation == generation {
		m.generation = nil
		m.gcStateCache.clearAll()
		m.barrierMetrics.clearMetrics()
		productionBarrierMetrics.current.CompareAndSwap(m.barrierMetrics, nil)
	}
	m.mu.Unlock()
	m.lifecycleMu.Unlock()
	// Runtime workers may need manager and warmup state locks to finish cancellation.
	generation.waitRuntime()
}

func (m *GCStateManager) nodeIsLeader() bool {
	return m.activeGeneration.Load() != nil
}
