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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// OnNodeBecomesLeader starts a native generation for unit tests that simulate elections.
func (m *GCStateManager) OnNodeBecomesLeader() func() {
	return m.startLeaderGeneration(nil)
}

func TestStarterGCMigrationNativeReadCosts(t *testing.T) {
	var markerReads, pairReads atomic.Int64
	opt := newGCStateManagerForTestOptions{
		useEnabledKeyspaceIndex: true,
		etcdClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if r, ok := req.(*etcdserverpb.RangeRequest); ok && string(r.Key) == keypath.StarterGCMigrationPath() {
					markerReads.Add(1)
				}
				if gcWarmupRead(req) != nil {
					pairReads.Add(1)
				}
				return invoke(ctx, method, req, reply, cc, opts...)
			}))
		},
	}
	opt.generateKeyspacesByCount(1)
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, opt)
	t.Cleanup(clean)
	t.Cleanup(cancel)
	for term := range 2 {
		prepared, err := manager.PrepareLeaderGeneration(t.Context(), nil)
		require.NoError(t, err)
		stop := prepared.Start()
		t.Cleanup(stop)
		gcWarmupWait(t, manager.activeGeneration.Load().warmup.done)
		reads := pairReads.Load()
		// A cold load costs one batch read. Subsequent cache hits cost none.
		manager.gcStateCache.remove(1)
		for range 5 {
			_, err := manager.GetGCState(1, true)
			require.NoError(t, err)
		}
		require.Equal(t, reads+1, pairReads.Load())
		require.EqualValues(t, term+1, markerReads.Load())
		stop()
	}
}

func TestStarterGCMigrationLeaderInitialization(t *testing.T) {
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{useEnabledKeyspaceIndex: true})
	defer clean()
	defer cancel()
	m.stopGCStateGeneration(m.activeGeneration.Load())
	ctx := t.Context()
	client := m.etcdClient
	markerKey := keypath.StarterGCMigrationPath()

	// A successful read of a missing marker prepares a native generation.
	// Preparation alone cannot expose the cache or start warmup.
	_, err := m.gcServingGeneration()
	require.ErrorIs(t, err, errs.ErrNotLeader)
	canceled, cancelRead := context.WithCancel(ctx)
	cancelRead()
	_, err = m.PrepareLeaderGeneration(canceled, nil)
	require.Error(t, err)
	native, err := m.PrepareLeaderGeneration(ctx, nil)
	require.NoError(t, err)
	_, err = m.gcServingGeneration()
	require.ErrorIs(t, err, errs.ErrNotLeader)
	stop := native.Start()
	generation, err := m.gcServingGeneration()
	require.NoError(t, err)
	require.Nil(t, generation.migration)
	m.InvalidateStarterMigrationLeader()
	require.Same(t, generation, m.activeGeneration.Load())
	stop()
	_, err = m.gcServingGeneration()
	require.ErrorIs(t, err, errs.ErrNotLeader)
	_, err = m.gcReadGeneration(nil)
	require.ErrorIs(t, err, errs.ErrNotLeader)
	active, err := m.gcReadGeneration(generation)
	require.NoError(t, err)
	require.Nil(t, active)

	// A rewritten marker, even with the same value, has lost its boundary.
	_, err = client.Put(ctx, markerKey, "2")
	require.NoError(t, err)
	_, err = m.PrepareLeaderGeneration(ctx, nil)
	require.ErrorContains(t, err, "invalid starter GC migration marker")
	_, err = m.gcReadGeneration(generation)
	require.ErrorIs(t, err, errs.ErrNotLeader)
	_, err = client.Put(ctx, markerKey, "1")
	require.NoError(t, err)
	_, err = m.PrepareLeaderGeneration(ctx, nil)
	require.ErrorContains(t, err, "invalid starter GC migration marker")
	_, err = client.Delete(ctx, markerKey)
	require.NoError(t, err)

	const leaderValue = "same-member"
	leaderKey := keypath.ElectionPath(nil)
	_, err = client.Delete(ctx, leaderKey)
	require.NoError(t, err)
	first := election.NewLeadership(client, leaderKey, "gc", "gc")
	require.NoError(t, first.Campaign(60, leaderValue))
	defer first.Reset()
	_, err = client.Put(ctx, markerKey, "1", clientv3.WithLease(first.GetLease().GetID()))
	require.NoError(t, err)
	_, err = m.PrepareLeaderGeneration(ctx, first)
	require.ErrorContains(t, err, "invalid starter GC migration marker")
	_, err = client.Delete(ctx, markerKey)
	require.NoError(t, err)
	marker, err := client.Put(ctx, markerKey, "1")
	require.NoError(t, err)
	_, err = m.PrepareLeaderGeneration(ctx, nil)
	require.ErrorIs(t, err, errs.ErrNotLeader)

	previous, err := m.PrepareLeaderGeneration(ctx, first)
	require.NoError(t, err)
	leader, err := client.Get(ctx, leaderKey)
	require.NoError(t, err)
	require.Equal(t, marker.Header.Revision, previous.migration.revision)
	require.Equal(t, marker.Header.ClusterId, previous.migration.etcdClusterID)
	require.Equal(t, leaderKey, previous.migration.leaderKey)
	require.Equal(t, leader.Kvs[0].ModRevision, previous.migration.leaderRevision)

	// Another campaign with identical member data must fence this old term.
	_, err = client.Delete(ctx, leaderKey)
	require.NoError(t, err)
	next := election.NewLeadership(client, leaderKey, "gc", "gc")
	require.NoError(t, next.Campaign(60, leaderValue))
	defer next.Reset()
	_, err = m.PrepareLeaderGeneration(ctx, first)
	require.ErrorIs(t, err, errs.ErrNotLeader)
	current, err := m.PrepareLeaderGeneration(ctx, next)
	require.NoError(t, err)
	require.Greater(t, current.migration.leaderRevision, previous.migration.leaderRevision)
	require.Equal(t, previous.migration.revision, current.migration.revision)
	stop = current.Start()
	defer stop()
	generation, err = m.gcServingGeneration()
	require.NoError(t, err)
	require.Same(t, current.migration, generation.migration)

	// Resignation may block before RaftCluster.Stop runs. A request that
	// already passed the leader middleware must not return even a cached T.
	_, err = m.GetGCState(constant.NullKeyspaceID, true)
	require.NoError(t, err)
	_, cached := m.gcStateCache.load(constant.NullKeyspaceID)
	require.True(t, cached)
	m.InvalidateStarterMigrationLeader()
	_, err = m.GetGCState(constant.NullKeyspaceID, true)
	require.ErrorIs(t, err, errs.ErrNotLeader)
}
