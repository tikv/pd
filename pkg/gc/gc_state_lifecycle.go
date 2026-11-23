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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
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
	ctx       context.Context
	cancel    context.CancelFunc
	workDone  chan struct{}
	index     *enabledKeyspaceIndex
	warmup    *gcStateWarmup
	migration *starterGCMigration
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
	m.nativeFollowerReads.Store(false)
	m.etcdClient = client
}

// PreparedGCLeader holds the verified migration mode for one leadership term.
// Preparation has no background workers or cache side effects.
type PreparedGCLeader struct {
	manager   *GCStateManager
	migration *starterGCMigration
}

// PrepareLeaderGeneration determines the GC mode before the cluster starts its workers.
func (m *GCStateManager) PrepareLeaderGeneration(ctx context.Context, leadership *election.Leadership) (*PreparedGCLeader, error) {
	prepared := &PreparedGCLeader{manager: m}
	if m.etcdClient != nil {
		m.nativeFollowerReads.Store(false)
		migration, err := readStarterGCMigration(ctx, m.etcdClient, leadership)
		if err != nil {
			return nil, err
		}
		prepared.migration = migration
	}
	return prepared, nil
}

func readStarterGCMigration(ctx context.Context, client *clientv3.Client, leadership *election.Leadership) (*starterGCMigration, error) {
	key := keypath.StarterGCMigrationPath()
	resp, err := etcdutil.EtcdKVGetWithContext(ctx, client, key)
	if err != nil {
		return nil, errors.Annotate(err, "read starter GC migration marker")
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	marker := resp.Kvs[0]
	if string(marker.Value) != "1" || marker.Version != 1 || marker.Lease != 0 ||
		marker.CreateRevision <= 0 || marker.CreateRevision != marker.ModRevision {
		return nil, errors.Errorf("invalid starter GC migration marker %s (version %d, create revision %d, mod revision %d, lease %d)",
			key, marker.Version, marker.CreateRevision, marker.ModRevision, marker.Lease)
	}

	// The same PD member can be elected again with the same serialized value.
	// Only its current campaign lease identifies the election revision that our
	// migration transactions are allowed to use.
	leaderKey, leaderValue := leadership.GetLeaderKey(), leadership.GetLeaderValue()
	if leaderKey == "" || leaderValue == "" {
		return nil, errs.ErrNotLeader
	}
	lease := leadership.GetLease()
	if lease.IsExpired() {
		return nil, errs.ErrNotLeader
	}
	leader, err := etcdutil.EtcdKVGetWithContext(ctx, client, leaderKey)
	if err != nil {
		return nil, errors.Annotate(err, "read PD leadership for starter GC migration")
	}
	clusterID := resp.Header.GetClusterId()
	if clusterID == 0 || leader.Header.GetClusterId() != clusterID {
		return nil, errors.New("inconsistent etcd cluster identity during starter GC migration initialization")
	}
	if len(leader.Kvs) != 1 || leader.Kvs[0].Lease != int64(lease.GetID()) ||
		string(leader.Kvs[0].Value) != leaderValue || leader.Kvs[0].ModRevision <= 0 {
		return nil, errs.ErrNotLeader
	}
	return newStarterGCMigration(marker.CreateRevision, clusterID, leaderKey, leader.Kvs[0].ModRevision), nil
}

// Start publishes this leader's verified generation and returns its idempotent cleanup.
func (p *PreparedGCLeader) Start() func() {
	return p.manager.startLeaderGeneration(p.migration)
}

func (m *GCStateManager) startLeaderGeneration(migration *starterGCMigration) func() {
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
	m.terminateAllGCStateWatchersLocked(errs.ErrNotLeader, watcherTerminationLeaderLost)
	ctx, cancel := context.WithCancel(context.Background())
	generation := &gcStateGeneration{
		done:      make(chan struct{}),
		flights:   make(map[uint32]*gcStateLoadFlight),
		ctx:       ctx,
		cancel:    cancel,
		migration: migration,
	}
	if m.etcdClient != nil {
		generation.workDone = make(chan struct{})
		generation.index = newEnabledKeyspaceIndex(m.etcdClient, keypath.KeyspaceMetaPrefix())
		generation.index.getLoadConcurrency = func() int { return m.getConfig().GCKeyspaceIndexLoadConcurrency }
		generation.index.onRemoved = func(ids []uint32) { m.publishGCStateRemovals(generation, ids) }
		generation.warmup = newGCStateWarmup(m, generation, generation.index)
	}
	failpoint.InjectCall("beforeLeaderGCStateCacheReset")
	m.gcStateCache.clearAll()
	m.barrierMetrics.clearMetrics()
	productionBarrierMetrics.current.Store(m.barrierMetrics)
	m.generation = generation
	m.activeGeneration.Store(generation)
	m.nativeFollowerReads.Store(migration == nil)
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

// InvalidateStarterMigrationLeader gates in-flight migration reads and cache
// publication before resigning can block on lease revocation. RaftCluster.Stop
// later retires the generation, joins its workers and clears the cache.
func (m *GCStateManager) InvalidateStarterMigrationLeader() {
	if generation := m.activeGeneration.Load(); generation != nil && generation.migration != nil {
		m.activeGeneration.CompareAndSwap(generation, nil)
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
		m.terminateAllGCStateWatchersLocked(errs.ErrNotLeader, watcherTerminationLeaderLost)
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

// gcServingGeneration allows passive loading only for managers that do not
// perform NextGen leader initialization. All other callers must observe the
// mode of a prepared, active generation before reading a transaction safe point.
func (m *GCStateManager) gcServingGeneration() (*gcStateGeneration, error) {
	generation := m.activeGeneration.Load()
	if generation == nil && m.etcdClient != nil {
		return nil, errs.ErrNotLeader
	}
	return generation, nil
}

// gcReadGeneration lets a read admitted by a native leader still read current
// storage after stepdown. The next marker preflight revokes this exception.
func (m *GCStateManager) gcReadGeneration(admitted *gcStateGeneration) (*gcStateGeneration, error) {
	generation, err := m.gcServingGeneration()
	if err != nil && admitted != nil && admitted.migration == nil && m.nativeFollowerReads.Load() {
		return nil, nil
	}
	return generation, err
}
