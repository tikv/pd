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
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	pingcaperrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
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

func TestStarterGCWatchSkipsInvalidKeyspaces(t *testing.T) {
	re := require.New(t)
	const (
		badLegacyT uint32 = iota + 1
		emptyLocalT
		badLocalT
		badLocalG
		gAboveCandidates
		gAboveVerifiedT
		invalidGroup
		unknownGroup
		healthy
	)
	m, dest, source, _ := newStarterGCAPIFixture(t, int(healthy))
	put := func(client *clientv3.Client, key, value string) {
		t.Helper()
		_, err := client.Put(t.Context(), key, value)
		re.NoError(err)
	}
	put(source, keypath.TxnSafePointPath(badLegacyT), "invalid")
	put(dest, keypath.TxnSafePointPath(emptyLocalT), "")
	put(dest, keypath.TxnSafePointPath(badLocalT), "invalid")
	put(dest, keypath.GCSafePointPath(badLocalG), "invalid")
	put(dest, keypath.GCSafePointPath(gAboveCandidates), `{"safe_point":150}`)
	// Writing T after the cutoff exercises validation of an already verified keyspace.
	put(dest, keypath.TxnSafePointPath(gAboveVerifiedT), "110")
	put(dest, keypath.GCSafePointPath(gAboveVerifiedT), `{"safe_point":150}`)
	setStarterKeyspaceConfig(t, dest, invalidGroup, keyspace.MetaServiceGroupIDKey, "bad/group")
	setStarterKeyspaceConfig(t, dest, unknownGroup, keyspace.MetaServiceGroupIDKey, "missing")

	// Each failed keyspace still reports the underlying error to its own readers.
	failures := map[uint32]struct {
		message string
		cause   error
	}{
		badLegacyT:       {`invalid transaction safe point for keyspace 1 in meta-service group "group1"`, strconv.ErrSyntax},
		emptyLocalT:      {"empty transaction safe point", nil},
		badLocalT:        {"invalid syntax", nil},
		badLocalG:        {"invalid character", errs.ErrJSONUnmarshal},
		gAboveCandidates: {"exceeds selected transaction safe point", nil},
		gAboveVerifiedT:  {"exceeds transaction safe point", nil},
		invalidGroup:     {"invalid meta-service group", nil},
		unknownGroup:     {"unknown meta-service group", errs.ErrUnknownMetaServiceGroup},
	}
	for id, failure := range failures {
		_, err := m.GetGCState(id, true)
		re.ErrorContains(err, failure.message, "keyspace %d", id)
		if failure.cause != nil {
			re.ErrorIs(err, failure.cause)
		}
	}
	requireStarterGCWatchUpdates(t, m, healthy, slices.Collect(maps.Keys(failures))...)
}

func requireStarterGCWatchUpdates(t *testing.T, m *GCStateManager, healthy uint32, skipped ...uint32) {
	t.Helper()
	re := require.New(t)
	_, _, err := m.AdvanceGCSafePoint(healthy, 80)
	re.NoError(err)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	watcher, err := m.WatchGCStates(ctx, false)
	re.NoError(err)
	defer watcher.Close()
	requireStarterGCWatchState(t, watcher, healthy, 80, skipped...)
	_, _, err = m.AdvanceGCSafePoint(healthy, 90)
	re.NoError(err)
	requireStarterGCWatchState(t, watcher, healthy, 90, skipped...)
}

func requireStarterGCWatchState(t *testing.T, watcher *GCStateWatcher, id uint32, wantG uint64, skipped ...uint32) {
	t.Helper()
	re := require.New(t)
	for {
		changes, err := watcher.RecvBatch(1)
		re.NoError(err)
		state := mustUpsert(t, changes[0])
		re.NotContains(skipped, state.KeyspaceID)
		if state.KeyspaceID == id {
			re.Equal(uint64(100+id), state.TxnSafePoint)
			re.Equal(wantG, state.GCSafePoint)
			return
		}
	}
}

func TestStarterGCWarmupSkipsInvalidKeyspace(t *testing.T) {
	re := require.New(t)
	const failing, healthy uint32 = 1, 2
	m, _, source, _ := newStarterGCAPIFixture(t, 2)
	_, err := source.Put(t.Context(), keypath.TxnSafePointPath(failing), "invalid")
	re.NoError(err)
	t.Cleanup(startStarterWarmup(m))
	gcWarmupWait(t, m.activeGeneration.Load().warmup.done)

	_, ok := m.gcStateCache.load(failing)
	re.False(ok)
	cached, ok := m.gcStateCache.load(healthy)
	re.True(ok)
	re.Equal(uint64(102), cached.TxnSafePoint)
	_, err = m.GetGCState(failing, true)
	var parseErr *strconv.NumError
	re.ErrorAs(err, &parseErr)
	re.Same(parseErr, pingcaperrors.Cause(err))

	_, err = source.Put(t.Context(), keypath.TxnSafePointPath(failing), "101")
	re.NoError(err)
	state, err := m.GetGCState(failing, true)
	re.NoError(err)
	re.Equal(uint64(101), state.TxnSafePoint)
}

func TestStarterGCWatchAndWarmupKeepUnifiedScopeErrors(t *testing.T) {
	re := require.New(t)
	m, dest, _, _ := newStarterGCAPIFixture(t, 1)
	key := keypath.TxnSafePointPath(constant.NullKeyspaceID)
	_, err := dest.Put(t.Context(), key, "invalid")
	re.NoError(err)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	watcher, err := m.WatchGCStates(ctx, false)
	re.NoError(err)
	defer watcher.Close()
	_, err = watcher.RecvBatch(1)
	re.ErrorContains(err, "invalid syntax")

	t.Cleanup(startStarterWarmup(m))
	// One worker visits the unified scope first. Once tenant 1 is cached the
	// first read of the malformed unified record has already failed.
	re.Eventually(func() bool {
		_, ok := m.gcStateCache.load(1)
		return ok
	}, 5*time.Second, 10*time.Millisecond)
	_, err = dest.Put(t.Context(), key, "120")
	re.NoError(err)
	gcWarmupWait(t, m.activeGeneration.Load().warmup.done)
	cached, ok := m.gcStateCache.load(constant.NullKeyspaceID)
	re.True(ok)
	re.Equal(uint64(120), cached.TxnSafePoint)
}

func TestStarterGCWatchAndWarmupSkipUnavailableSource(t *testing.T) {
	re := require.New(t)
	const pending, migrated uint32 = 1, 2
	m, dest, source, cutoff := newStarterGCAPIFixture(t, 2)
	_, _, err := m.AdvanceGCSafePoint(migrated, 80)
	re.NoError(err)
	group, _ := recordStarterGroups(t, m, dest, source)
	var unavailable atomic.Bool
	unavailable.Store(true)
	group.beforeRead = func() error {
		if unavailable.Load() {
			return context.DeadlineExceeded
		}
		return nil
	}

	// Start the leader with a cold cache while only the external source is down.
	t.Cleanup(startStarterWarmup(m))
	gcWarmupWait(t, m.activeGeneration.Load().warmup.finished)
	_, ok := m.gcStateCache.load(pending)
	re.False(ok)
	cached, ok := m.gcStateCache.load(migrated)
	re.True(ok)
	re.Equal(uint64(102), cached.TxnSafePoint)
	re.Equal(uint64(80), cached.GCSafePoint)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	watcher, err := m.WatchGCStates(ctx, false)
	re.NoError(err)
	defer watcher.Close()
	requireStarterGCWatchState(t, watcher, migrated, 80, pending)
	_, _, err = m.AdvanceGCSafePoint(migrated, 90)
	re.NoError(err)
	requireStarterGCWatchState(t, watcher, migrated, 90, pending)

	_, err = m.GetGCState(pending, true)
	re.ErrorIs(err, context.DeadlineExceeded)
	re.ErrorContains(err, `read meta-service group "group1"`)
	re.NoError(ctx.Err())
	value, revision := readStarterSafePoint(t, m, pending)
	re.Equal(uint64(10), value)
	re.LessOrEqual(revision, cutoff)

	unavailable.Store(false)
	state, err := m.GetGCState(pending, true)
	re.NoError(err)
	re.Equal(uint64(101), state.TxnSafePoint)
	value, revision = readStarterSafePoint(t, m, pending)
	re.Equal(uint64(101), value)
	re.Greater(revision, cutoff)
	_, _, err = m.AdvanceGCSafePoint(pending, 70)
	re.NoError(err)
	requireStarterGCWatchState(t, watcher, pending, 70)
}

func TestStarterGCWatchAndWarmupSkipKeyspacesWithInvalidGroupConfig(t *testing.T) {
	re := require.New(t)
	const pending, migrated uint32 = 1, 2
	m, dest, _, cutoff := newStarterGCAPIFixture(t, 2)
	_, err := m.GetGCState(migrated, true)
	re.NoError(err)
	_, err = dest.Put(t.Context(), keypath.ConfigPath(), "{")
	re.NoError(err)
	_, err = m.GetGCState(pending, true)
	re.ErrorIs(err, errs.ErrJSONUnmarshal)
	requireStarterGCWatchUpdates(t, m, migrated, pending)

	// A new generation clears the cache. Only the migrated tenant can reload locally.
	t.Cleanup(startStarterWarmup(m))
	gcWarmupWait(t, m.activeGeneration.Load().warmup.done)
	_, ok := m.gcStateCache.load(pending)
	re.False(ok)
	cached, ok := m.gcStateCache.load(migrated)
	re.True(ok)
	re.Equal(uint64(102), cached.TxnSafePoint)
	ts, rev := readStarterSafePoint(t, m, pending)
	re.Equal(uint64(10), ts)
	re.LessOrEqual(rev, cutoff)
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
