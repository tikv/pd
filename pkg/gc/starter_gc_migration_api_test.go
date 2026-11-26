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
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// Start without warmup so each API can be the very first consumer of a legacy T.
func newStarterGCAPIFixture(t *testing.T, count int) (manager *GCStateManager, destination, source *clientv3.Client, cutoff int64) {
	t.Helper()
	return newStarterGCFixture(t, count, nil, nil)
}

func newStarterGCFixture(t *testing.T, count int, beforeCutoff func(destination, source *clientv3.Client), clientConfig etcdutil.CreateEtcdClientOpt) (manager *GCStateManager, destination, source *clientv3.Client, cutoff int64) {
	t.Helper()
	re := require.New(t)
	ctx := context.Background()
	_, source, closeSource := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(closeSource)
	opt := newGCStateManagerForTestOptions{
		useEnabledKeyspaceIndex: true,
		disableWarmupWorkers:    true,
		etcdClientCfgModifier:   clientConfig,
		metaServiceGroups:       map[string]string{"group1": strings.Join(source.Endpoints(), ",")},
		beforeLeader: func(cli *clientv3.Client) {
			destination = cli
			for i := 1; i <= count; i++ {
				key := keypath.TxnSafePointPath(uint32(i))
				_, err := cli.Put(ctx, key, "10")
				re.NoError(err)
				_, err = source.Put(ctx, key, strconv.Itoa(100+i))
				re.NoError(err)
			}
			if beforeCutoff != nil {
				beforeCutoff(cli, source)
			}
			resp, err := cli.Put(ctx, keypath.GCMigrationCutoffPath(), "1")
			re.NoError(err)
			cutoff = resp.Header.Revision
		},
	}
	opt.generateKeyspacesByCount(count)
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, opt)
	t.Cleanup(clean)
	t.Cleanup(cancel)
	return manager, destination, source, cutoff
}

// Start a new generation with one worker after the foreground-only setup.
func startStarterWarmup(m *GCStateManager) func() {
	m.stopGCStateGeneration(m.activeGeneration.Load())
	m.cfg.GCStateCacheWarmupConcurrency = 1
	return m.OnNodeBecomesLeader()
}

func TestStarterGCMigrationFirstGCOperation(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	const count = 5
	manager, destination, source, cutoff := newStarterGCAPIFixture(t, count)
	now := time.Now()

	_, err := manager.SetGCBarrier(1, "cdc", 80, time.Hour, now)
	re.ErrorIs(err, errs.ErrGCBarrierTSBehindTxnSafePoint)

	_, gcSafePoint, err := manager.AdvanceGCSafePoint(2, 80)
	re.NoError(err)
	re.Equal(uint64(80), gcSafePoint)

	_, err = manager.AdvanceTxnSafePoint(3, 80, now)
	re.ErrorIs(err, errs.ErrDecreasingTxnSafePoint)

	pin, _, err := manager.CompatibleUpdateServiceGCSafePoint(4, "cdc", 0, 0, now)
	re.NoError(err)
	re.Equal(uint64(104), pin.SafePoint)

	state, _, err := manager.GetGCStateWithGlobalGCBarriers(5, false)
	re.NoError(err)
	re.Equal(uint64(105), state.TxnSafePoint)

	for i := 1; i <= count; i++ {
		resp, err := destination.Get(ctx, keypath.TxnSafePointPath(uint32(i)))
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		re.Greater(resp.Kvs[0].ModRevision, cutoff)
		re.Equal(strconv.Itoa(100+i), string(resp.Kvs[0].Value))
	}
	// A new GC leader must serve the initialized T without consulting the old source.
	key := keypath.TxnSafePointPath(1)
	_, err = source.Put(ctx, key, "invalid")
	re.NoError(err)
	t.Cleanup(manager.OnNodeBecomesLeader())
	state, err = manager.GetGCState(1, true)
	re.NoError(err)
	re.Equal(uint64(101), state.TxnSafePoint)
}

func TestStarterGCMigrationFailedScanPreservesCache(t *testing.T) {
	re := require.New(t)
	const failing, healthy uint32 = 1, 2
	m, _, _, _ := newStarterGCFixture(t, 2, func(_, source *clientv3.Client) {
		_, err := source.Put(t.Context(), keypath.TxnSafePointPath(failing), "malformed")
		re.NoError(err)
	}, nil)
	_, err := m.GetGCState(healthy, true)
	re.NoError(err)
	before, ok := m.gcStateCache.load(healthy)
	re.True(ok)

	// The scan reaches the failing keyspace before it visits the cached one.
	states, err := m.GetAllKeyspacesGCStates(t.Context(), true)
	re.ErrorContains(err, "invalid transaction safe point")
	re.NotContains(states, healthy)
	after, ok := m.gcStateCache.load(healthy)
	re.True(ok)
	re.Equal(before, after)
}

func TestStarterGCMigrationRejectsInvalidBarrierID(t *testing.T) {
	re := require.New(t)
	manager, destination, source, cutoff := newStarterGCAPIFixture(t, 1)
	group, _ := recordStarterGroups(t, manager, destination, source)
	txn, revision := readStarterSafePoint(t, manager, 1)
	re.Less(revision, cutoff)
	now := time.Now()

	_, err := manager.SetGCBarrier(1, "", 200, time.Hour, now)
	re.ErrorIs(err, errs.ErrInvalidArgument)
	_, err = manager.SetGCBarrier(1, keypath.GCWorkerServiceSafePointID, 200, time.Hour, now)
	re.ErrorIs(err, errs.ErrReservedGCBarrierID)
	for _, ttl := range []int64{1, 0} {
		_, _, err = manager.CompatibleUpdateServiceGCSafePoint(1, "", 200, ttl, now)
		re.ErrorIs(err, errs.ErrInvalidArgument)
	}
	// TTL and keyspace errors retain precedence over barrier ID validation.
	_, err = manager.SetGCBarrier(2, keypath.GCWorkerServiceSafePointID, 200, 0, now)
	re.ErrorIs(err, errs.ErrInvalidArgument)
	_, err = manager.SetGCBarrier(2, keypath.GCWorkerServiceSafePointID, 200, time.Hour, now)
	re.ErrorIs(err, errs.ErrKeyspaceNotFound)

	re.Empty(group.requests)
	afterTxn, afterRevision := readStarterSafePoint(t, manager, 1)
	re.Equal(txn, afterTxn)
	re.Equal(revision, afterRevision)
}

func TestStarterGCMigrationFirstWatch(t *testing.T) {
	re := require.New(t)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	manager, _, source, cutoff := newStarterGCAPIFixture(t, 1)
	// A keyspace allocated after the cutoff has no old T to inherit.
	id := uint32(2)
	_, err := manager.keyspaceManager.CreateKeyspaceByID(&keyspace.CreateKeyspaceByIDRequest{
		ID: &id, Name: "new-tenant", CreateTime: time.Now().Unix(),
		Config: map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
	})
	re.NoError(err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = source.Put(ctx, keypath.TxnSafePointPath(id), "invalid")
	re.NoError(err)

	watcher, err := manager.WatchGCStates(ctx, false)
	re.NoError(err)
	defer watcher.Close()
	initial := make(map[uint32]uint64)
	for {
		batch, err := watcher.RecvBatch(32)
		re.NoError(err)
		for _, change := range batch {
			state := mustUpsert(t, change)
			initial[state.KeyspaceID] = state.TxnSafePoint
		}
		_, gotOld := initial[1]
		_, gotNew := initial[id]
		if gotOld && gotNew {
			break
		}
	}
	re.Equal(uint64(101), initial[1])
	re.Zero(initial[id])
	ts, rev := readStarterSafePoint(t, manager, id)
	re.Zero(ts)
	re.Greater(rev, cutoff)
}

func TestStarterGCMigrationWarmupAndReenabledTenant(t *testing.T) {
	re := require.New(t)
	manager, _, _, cutoff := newStarterGCAPIFixture(t, 2)
	now := time.Now()
	_, err := manager.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_DISABLED, now.Unix())
	re.NoError(err)
	t.Cleanup(startStarterWarmup(manager))
	gcWarmupWait(t, manager.activeGeneration.Load().warmup.done)
	states, err := manager.GetAllKeyspacesGCStates(t.Context(), true)
	re.NoError(err)
	re.Equal(uint64(101), states[1].TxnSafePoint)
	re.NotContains(states, uint32(2))
	txn, rev := readStarterSafePoint(t, manager, 2)
	re.Equal(uint64(10), txn)
	re.LessOrEqual(rev, cutoff)

	// Re-enabling a preexisting tenant changes its metadata's ModRevision.
	// Its first legacy gc_worker request must still inherit the frozen T.
	_, err = manager.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_ENABLED, now.Unix())
	re.NoError(err)
	pin, changed, err := manager.CompatibleUpdateServiceGCSafePoint(2, keypath.GCWorkerServiceSafePointID, 102, math.MaxInt64, now)
	re.NoError(err)
	re.False(changed)
	re.Equal(uint64(102), pin.SafePoint)
	txn, rev = readStarterSafePoint(t, manager, 2)
	re.Equal(uint64(102), txn)
	re.Greater(rev, cutoff)
}
