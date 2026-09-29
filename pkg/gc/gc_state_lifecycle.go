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

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// A generation is a unique identity, including after its active lifetime ends.
// Contexts stay on the executing goroutines; retirement closes done and cancels
// their registered operations before waiting for the manager lock.
type gcStateGeneration struct {
	mu      syncutil.Mutex
	done    chan struct{}
	flights map[uint32]*gcStateLoadFlight
	cancels map[*gcStateLoadBatch]context.CancelFunc
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
	for id, flight := range g.flights {
		flight.err = errs.ErrNotLeader
		close(flight.done)
		delete(g.flights, id)
	}
	for _, cancel := range g.cancels {
		cancel()
	}
}

// OnNodeBecomesLeader starts a unique leadership generation and returns its
// idempotent cleanup. A delayed cleanup only affects the generation it owns.
func (m *GCStateManager) OnNodeBecomesLeader() func() {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if previous := m.activeGeneration.Swap(nil); previous != nil {
		previous.retire()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	generation := &gcStateGeneration{
		done:    make(chan struct{}),
		flights: make(map[uint32]*gcStateLoadFlight),
		cancels: make(map[*gcStateLoadBatch]context.CancelFunc),
	}
	failpoint.InjectCall("beforeLeaderGCStateCacheReset")
	m.gcStateCache.clearAll()
	m.barrierMetrics.clearMetrics()
	productionBarrierMetrics.current.Store(m.barrierMetrics)
	m.generation = generation
	m.activeGeneration.Store(generation)
	return func() { m.stopGCStateGeneration(generation) }
}

func (m *GCStateManager) stopGCStateGeneration(generation *gcStateGeneration) {
	if generation == nil {
		return
	}
	m.activeGeneration.CompareAndSwap(generation, nil)
	generation.retire()
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.generation != generation {
		return
	}
	m.generation = nil
	m.gcStateCache.clearAll()
	m.barrierMetrics.clearMetrics()
	productionBarrierMetrics.current.CompareAndSwap(m.barrierMetrics, nil)
}

func (m *GCStateManager) nodeIsLeader() bool {
	return m.activeGeneration.Load() != nil
}
