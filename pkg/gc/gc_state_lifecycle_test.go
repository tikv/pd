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

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestStarterGCMigrationNativeReadCosts(t *testing.T) {
	var cutoffReads, pairReads atomic.Int64
	opt := newGCStateManagerForTestOptions{
		useEnabledKeyspaceIndex: true,
		etcdClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if r, ok := req.(*etcdserverpb.RangeRequest); ok && string(r.Key) == keypath.GCMigrationCutoffPath() {
					cutoffReads.Add(1)
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
	for range 2 {
		stop := manager.OnNodeBecomesLeader()
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
		stop()
	}
	require.EqualValues(t, 1, cutoffReads.Load())
}

func TestStarterGCMigrationStartup(t *testing.T) {
	_, provider, native, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{useEnabledKeyspaceIndex: true})
	defer clean()
	defer cancel()
	ctx, client := t.Context(), native.etcdClient
	native.stopGCStateGeneration(native.activeGeneration.Load())
	upgraded := NewGCStateManager(provider, *native.getConfig(), native.keyspaceManager, native.metaServiceGroupManager)
	defer upgraded.CloseBarrierMetrics()
	canceled, cancelRead := context.WithCancel(ctx)
	cancelRead()
	_, err := LoadMigrationCutoff(canceled, client)
	require.Error(t, err)
	key := keypath.GCMigrationCutoffPath()
	_, err = client.Put(ctx, key, "unknown")
	require.NoError(t, err)
	_, err = LoadMigrationCutoff(ctx, client)
	require.ErrorContains(t, err, "invalid starter GC migration cutoff")
	// Rewriting the cutoff key violates its create-once contract even with a valid value.
	_, err = client.Put(ctx, key, "1")
	require.NoError(t, err)
	_, err = LoadMigrationCutoff(ctx, client)
	require.ErrorContains(t, err, "invalid starter GC migration cutoff")
	_, err = client.Delete(ctx, key)
	require.NoError(t, err)
	lease, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = client.Put(ctx, key, "1", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	_, err = LoadMigrationCutoff(ctx, client)
	require.ErrorContains(t, err, "invalid starter GC migration cutoff")
	_, err = client.Delete(ctx, key)
	require.NoError(t, err)

	_, err = client.Put(ctx, keypath.TxnSafePointPath(2), "42")
	require.NoError(t, err)
	put, err := client.Put(ctx, key, "1")
	require.NoError(t, err)
	cutoff, err := LoadMigrationCutoff(ctx, client)
	require.NoError(t, err)
	require.Equal(t, put.Header.Revision, cutoff)
	upgraded.InitEtcdClient(client, cutoff)
	_, err = upgraded.GetGCState(2, true)
	require.ErrorIs(t, err, errs.ErrNotLeader)
	stop := upgraded.OnNodeBecomesLeader()
	defer stop()
	state, err := upgraded.GetGCState(2, true)
	require.NoError(t, err)
	require.EqualValues(t, 42, state.TxnSafePoint)
	_, revision := readStarterSafePoint(t, upgraded, 2)
	require.Greater(t, revision, put.Header.Revision)
	stop()
	_, err = upgraded.GetGCState(2, true)
	require.ErrorIs(t, err, errs.ErrNotLeader)
}
