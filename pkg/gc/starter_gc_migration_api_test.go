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

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// Start without warmup so each API can be the very first consumer of a legacy T.
func newStarterGCAPIFixture(t *testing.T, count int) (manager *GCStateManager, destination, source *clientv3.Client, markerRevision int64) {
	t.Helper()
	return newStarterGCFixture(t, count, nil, nil)
}

func newStarterGCFixture(t *testing.T, count int, beforeMarker func(destination, source *clientv3.Client), clientConfig etcdutil.CreateEtcdClientOpt) (manager *GCStateManager, destination, source *clientv3.Client, markerRevision int64) {
	t.Helper()
	re := require.New(t)
	ctx := context.Background()
	_, source, closeSource := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(closeSource)
	opt := newGCStateManagerForTestOptions{
		etcdClientCfgModifier: clientConfig,
		metaServiceGroups:     map[string]string{"group1": strings.Join(source.Endpoints(), ",")},
		beforeLeader: func(cli *clientv3.Client) {
			destination = cli
			for i := 1; i <= count; i++ {
				key := keypath.TxnSafePointPath(uint32(i))
				_, err := cli.Put(ctx, key, "10")
				re.NoError(err)
				_, err = source.Put(ctx, key, strconv.Itoa(100+i))
				re.NoError(err)
			}
			if beforeMarker != nil {
				beforeMarker(cli, source)
			}
			resp, err := cli.Put(ctx, keypath.StarterGCMigrationPath(), "1")
			re.NoError(err)
			markerRevision = resp.Header.Revision
		},
	}
	opt.generateKeyspacesByCount(count)
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, opt)
	t.Cleanup(clean)
	t.Cleanup(cancel)
	startStarterGCAPITestTerm(t, manager, destination, markerRevision)
	// Supply metadata reads after generation startup to keep this first term unindexed.
	manager.SetEtcdClient(destination)
	return manager, destination, source, markerRevision
}

func startStarterGCAPITestTerm(t *testing.T, manager *GCStateManager, cli *clientv3.Client, markerRevision int64) {
	t.Helper()
	key := keypath.ElectionPath(nil)
	term, err := cli.Get(context.Background(), key)
	require.NoError(t, err)
	require.Len(t, term.Kvs, 1)
	migration := newStarterGCMigration(markerRevision, term.Header.ClusterId, key, term.Kvs[0].ModRevision)
	t.Cleanup(manager.startLeaderGeneration(migration))
}

func TestStarterGCMigrationFirstGCOperation(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	manager, destination, source, revision := newStarterGCAPIFixture(t, 6)
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

	// Scope 6 has not had any GC request. Its T still constrains this cluster-wide pin.
	pin, updated, err := manager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, keypath.NativeBRServiceSafePointID, 105, math.MaxInt64, now)
	re.NoError(err)
	re.False(updated)
	re.Equal(uint64(106), pin.SafePoint)
	globals, err := manager.LoadAllGlobalGCBarriers()
	re.NoError(err)
	re.Empty(globals)

	for i := 1; i <= 6; i++ {
		resp, err := destination.Get(ctx, keypath.TxnSafePointPath(uint32(i)))
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		re.Greater(resp.Kvs[0].ModRevision, revision)
		re.Equal(strconv.Itoa(100+i), string(resp.Kvs[0].Value))
	}
	// A new GC leader must serve the initialized T without consulting the old source.
	key := keypath.TxnSafePointPath(1)
	_, err = source.Put(ctx, key, "invalid")
	re.NoError(err)
	_, err = destination.Put(ctx, keypath.ElectionPath(nil), "member1")
	re.NoError(err)
	startStarterGCAPITestTerm(t, manager, destination, revision)
	state, err = manager.GetGCState(1, true)
	re.NoError(err)
	re.Equal(uint64(101), state.TxnSafePoint)
}

func TestStarterGCMigrationFirstWatch(t *testing.T) {
	re := require.New(t)
	manager, _, source, _ := newStarterGCAPIFixture(t, 1)
	// A keyspace allocated after the marker has no old T to inherit.
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
}

func TestStarterGCMigrationWarmupAndReenabledTenant(t *testing.T) {
	re := require.New(t)
	manager, dest, _, revision := newStarterGCAPIFixture(t, 2)
	now := time.Now()
	_, err := manager.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_DISABLED, now.Unix())
	re.NoError(err)
	startStarterGCAPITestTerm(t, manager, dest, revision)
	gcWarmupWait(t, manager.activeGeneration.Load().warmup.done)
	states, err := manager.GetAllKeyspacesGCStates(t.Context(), true)
	re.NoError(err)
	re.Equal(uint64(101), states[1].TxnSafePoint)
	re.NotContains(states, uint32(2))
	txn, rev, err := manager.gcMetaStorage.LoadTxnSafePointWithRevision(t.Context(), 2)
	re.NoError(err)
	re.Equal(uint64(10), txn)
	re.LessOrEqual(rev, revision)

	// Re-enabling a preexisting tenant changes its metadata's ModRevision.
	// Its first legacy gc_worker request must still inherit the frozen T.
	_, err = manager.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_ENABLED, now.Unix())
	re.NoError(err)
	pin, changed, err := manager.CompatibleUpdateServiceGCSafePoint(2, keypath.GCWorkerServiceSafePointID, 102, math.MaxInt64, now)
	re.NoError(err)
	re.False(changed)
	re.Equal(uint64(102), pin.SafePoint)
	txn, rev, err = manager.gcMetaStorage.LoadTxnSafePointWithRevision(t.Context(), 2)
	re.NoError(err)
	re.Equal(uint64(102), txn)
	re.Greater(rev, revision)
}
