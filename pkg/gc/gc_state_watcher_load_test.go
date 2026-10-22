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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestGCStateWatcherInitialLoadReportsRetiredGeneration(t *testing.T) {
	probeStarted := make(chan struct{})
	var blockProbe atomic.Bool
	m := newGCWarmupRuntimeTest(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if request, ok := req.(*etcdserverpb.RangeRequest); ok && blockProbe.Load() && string(request.Key) == keypath.KeyspaceMetaPrefix() {
			close(probeStarted)
			<-ctx.Done()
			return ctx.Err()
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	stop := m.OnNodeBecomesLeader()
	defer stop()
	generation := m.activeGeneration.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, generation.index.waitReady(ctx))
	blockProbe.Store(true)
	leaderLost := promtestutil.ToFloat64(gcStateWatcherTerminationLeaderLostCounter)
	initError := promtestutil.ToFloat64(gcStateWatcherTerminationInitErrorCounter)
	w, err := m.WatchGCStates(ctx, false)
	require.NoError(t, err)
	defer w.Close()
	gcWarmupWait(t, probeStarted)

	// Exercise the retirement phase before lifecycle cleanup acquires the
	// manager lock. The initial loader can finish inside this window.
	require.True(t, m.activeGeneration.CompareAndSwap(generation, nil))
	generation.retire()
	gcWarmupWait(t, w.Done())
	require.ErrorIs(t, w.Err(), errs.ErrNotLeader)
	require.Equal(t, leaderLost+1, promtestutil.ToFloat64(gcStateWatcherTerminationLeaderLostCounter))
	require.Equal(t, initError, promtestutil.ToFloat64(gcStateWatcherTerminationInitErrorCounter))
}

func TestGCStateWatcherCancellationDoesNotCancelWarmup(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var reads atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if txn := gcWarmupRead(req); txn != nil && string(txn.Success[0].GetRequestRange().Key) == keypath.TxnSafePointPath(constant.NullKeyspaceID) {
			if reads.Add(1) == 1 {
				close(entered)
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	require.NoError(t, m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		if err := wb.SetTxnSafePoint(constant.NullKeyspaceID, 90); err != nil {
			return err
		}
		return wb.SetGCSafePoint(constant.NullKeyspaceID, 70)
	}))
	live, err := m.WatchGCStates(context.Background(), true)
	require.NoError(t, err)
	t.Cleanup(live.Close)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	generation := m.activeGeneration.Load()
	warmup := newGCStateWarmup(m, generation, nil)
	warmup.onInitialSnapshot(nil)
	const registered = "github.com/tikv/pd/pkg/gc/watchGCStatesRegistered"
	require.NoError(t, failpoint.EnableCall(registered, func() {
		// Register before warmup holds the manager read lock across storage I/O,
		// then keep that load in flight until the initial watcher joins it.
		runGCWarmupTest(t, warmup)
		gcWarmupWait(t, entered)
	}))
	defer func() { require.NoError(t, failpoint.Disable(registered)) }()
	joined := make(chan struct{})
	var joinedOnce sync.Once
	const onJoined = "github.com/tikv/pd/pkg/gc/onGCStateLoadJoined"
	require.NoError(t, failpoint.EnableCall(onJoined, func(id uint32) {
		if id == constant.NullKeyspaceID {
			joinedOnce.Do(func() { close(joined) })
		}
	}))
	defer func() { require.NoError(t, failpoint.Disable(onJoined)) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := m.WatchGCStates(ctx, false)
	require.NoError(t, err)
	t.Cleanup(w.Close)
	gcWarmupWait(t, joined)
	cancel()
	_, err = w.RecvBatch(1)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, generation.ctx.Err())

	unblock()
	gcWarmupWait(t, warmup.done)
	entry, ok := m.gcStateCache.load(constant.NullKeyspaceID)
	require.True(t, ok)
	require.Equal(t, uint64(90), entry.TxnSafePoint)
	require.Equal(t, uint64(70), entry.GCSafePoint)
	require.Equal(t, int32(1), reads.Load(), "initial watch and warmup must share one storage read")
	require.Empty(t, live.liveCh, "warming the cache must not publish live updates")
}
