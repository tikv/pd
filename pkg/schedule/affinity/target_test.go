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

package affinity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
	pdtestutil "github.com/tikv/pd/pkg/utils/testutil"
)

var targetTestRange = pdtestutil.TxnKeyspaceRange(nonOverlappingRange.StartKey, nonOverlappingRange.EndKey)

type targetTest struct {
	*require.Assertions
	*Manager
	store storage.Storage
	calls []GroupTarget
	reply func(context.Context, []GroupTarget) ([]string, error)
}

func newTargetTest(t *testing.T, ids ...string) *targetTest {
	return newTargetTestWithStorage(t, t.Context(), storage.NewStorageWithMemoryBackend(), ids...)
}

func newTargetTestWithStorage(t *testing.T, ctx context.Context, store storage.Storage, ids ...string) *targetTest {
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
	}
	labels, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	require.NoError(t, err)
	manager, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), labels)
	require.NoError(t, err)
	test := &targetTest{Assertions: require.New(t), Manager: manager, store: store}
	ranges := make([]GroupKeyRanges, len(ids))
	for i, id := range ids {
		ranges[i].GroupID = id
	}
	if len(ranges) > 0 {
		test.NoError(test.CreateAffinityGroups(ranges))
	}
	return test
}

func (test *targetTest) target(id string, leader uint64, voters ...uint64) GroupTarget {
	return GroupTarget{Expected: test.GetAffinityGroupState(id).Group, LeaderStoreID: leader, VoterStoreIDs: voters}
}

func (test *targetTest) stored(id string) *Group {
	value, err := test.store.LoadAffinityGroup(id)
	test.NoError(err)
	group := &Group{}
	test.NoError(json.Unmarshal([]byte(value), group))
	return group
}

func (test *targetTest) write(want []string, targets ...GroupTarget) {
	written, err := test.UpdateAffinityGroupTargets(test.ctx, targets)
	test.NoError(err)
	test.Equal(want, written)
}

func (test *targetTest) useWriter(reply func(context.Context, []GroupTarget) ([]string, error)) {
	test.reply = reply
	test.SetTargetWriter(test)
}

func (test *targetTest) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	test.calls = append(test.calls, targets...)
	return test.reply(ctx, targets)
}

func (test *targetTest) observe(id string, voters ...uint64) {
	test.ObserveAvailableRegion(targetRegion(voters...), test.GetAffinityGroupState(id))
	synctest.Wait()
}

func targetRegion(voters ...uint64) *core.RegionInfo {
	return generateRegionForTest(10, voters, targetTestRange)
}

func TestUpdateAffinityGroupTargetsValidation(t *testing.T) {
	test := newTargetTest(t, "g", "h")
	valid := test.target("g", 1, 1, 2)
	negativeRevision, invalidID := valid, valid
	negativeRevision.ExpectedModRevision = -1
	invalidID.Expected.ID = "bad id"
	for reason, targets := range map[string][]GroupTarget{
		"no targets provided":             nil,
		"duplicate group":                 {valid, test.target("g", 2, 1, 2)},
		"leader store ID and voter store": {test.target("g", 0)},
		"revision must be non-negative":   {negativeRevision},
		"bad id":                          {test.target("h", 1, 1, 2), invalidID},
	} {
		_, err := test.UpdateAffinityGroupTargets(t.Context(), targets)
		test.ErrorContains(err, reason)
	}
	test.Zero(test.stored("g").LeaderStoreID)
	test.Zero(test.stored("h").LeaderStoreID, "validation must precede all writes")
	test.applyStoreConditions(map[uint64]storeCondition{4: storeDown})
	valid.Expected.CreateTimestamp++
	success := test.target("h", 2, 3, 2, 1)
	for _, skipped := range []GroupTarget{
		{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		valid,
		test.target("g", 1, 1, 4),
	} {
		test.write([]string{"h"}, skipped, success)
	}
	test.Zero(test.stored("g").LeaderStoreID)
	test.Equal([]uint64{1, 2, 3}, test.GetAffinityGroupState(success.Expected.ID).VoterStoreIDs)
}

func TestUpdateAffinityGroupTargetsContextCancellation(t *testing.T) {
	t.Run("waiting for metaMutex", func(t *testing.T) {
		test := newTargetTest(t, "g")
		targets := []GroupTarget{test.target("g", 1, 1, 2)}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		transactions := 0
		test.interceptTxns(func(txnCtx context.Context, apply func(kv.Txn) error) error {
			transactions++
			return test.store.RunInTxn(txnCtx, apply)
		})
		started, done := make(chan struct{}), make(chan struct{})
		var written []string
		var err error
		test.metaMutex.Lock()
		go func() {
			defer close(done)
			close(started)
			written, err = test.UpdateAffinityGroupTargets(ctx, targets)
		}()
		<-started
		// Give the update time to reach the mutex before canceling the request.
		time.Sleep(20 * time.Millisecond)
		cancel()
		test.metaMutex.Unlock()
		<-done
		test.ErrorIs(err, context.Canceled)
		test.Empty(written)
		test.Zero(transactions)
	})

	synctest.Test(t, func(t *testing.T) {
		managerCtx, stopManager := context.WithCancel(t.Context())
		defer stopManager()
		test := newTargetTestWithStorage(t, managerCtx, storage.NewStorageWithMemoryBackend(), "first", "second")
		targets := []GroupTarget{test.target("first", 1, 1, 2), test.target("second", 1, 1, 2)}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		transactions := 0
		test.interceptTxns(func(txnCtx context.Context, apply func(kv.Txn) error) error {
			transactions++
			err := test.store.RunInTxn(txnCtx, apply)
			cancel()
			return err
		})
		written, err := test.UpdateAffinityGroupTargets(ctx, targets)
		test.ErrorIs(err, context.Canceled)
		test.Equal([]string{"first"}, written)
		test.Equal(1, transactions)
		test.Equal(targets[0].result(), test.stored("first"))
		test.Zero(test.stored("second").LeaderStoreID)

		test.interceptTxns(func(txnCtx context.Context, _ func(kv.Txn) error) error {
			stopManager()
			<-txnCtx.Done()
			return txnCtx.Err()
		})
		var writerErr error
		test.useWriter(func(writerCtx context.Context, targets []GroupTarget) ([]string, error) {
			requestCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			written, err = test.UpdateAffinityGroupTargets(requestCtx, targets)
			writerErr = writerCtx.Err()
			return written, err
		})
		test.observe("second", 1, 2)
		test.ErrorIs(err, context.Canceled, "the Manager cancels the in-flight transaction")
		test.ErrorIs(writerErr, context.Canceled)
		test.Empty(written)
		synctest.Sleep(time.Second)
		test.observe("second", 1, 2)
		test.Equal(targets[1:], test.calls, "a stopped Manager sends no further requests")
	})
}

func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, source := range []string{"watcher", "conflict", "written"} {
		t.Run(source, func(t *testing.T) {
			test := newTargetTest(t)
			old := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			test.SyncGroupFromEtcdWithRevision(&old, 1)
			test.NoError(test.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{targetTestRange}}}, nil))
			test.RestoreAffinityGroup("g")
			_, cached := test.GetAndCacheRegionAffinityGroupState(targetRegion(1, 2, 3))
			test.True(cached)

			recreated := old
			recreated.CreateTimestamp++
			test.NoError(test.store.SaveAffinityGroup(test.store, recreated.ID, &recreated))
			target := test.target("g", 2, 1, 2, 3)
			switch source {
			case "watcher":
				test.SyncGroupFromEtcdWithRevision(&recreated, 2)
				test.SyncGroupFromEtcdWithRevision(&old, 1)
			case "conflict":
				test.write([]string{}, target)
			case "written":
				target.Expected = recreated
				test.write([]string{"g"}, target)
				recreated.LeaderStoreID = 2
			}
			state := test.GetAffinityGroupState("g")
			test.Equal(recreated, state.Group)
			test.Zero(state.RegionCount)
			test.Equal(1, state.RangeCount)
		})
	}
}

func TestFirstWinStoreConditions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewStorageWithMemoryBackend()
		loaded := &Group{ID: "loaded", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
		require.NoError(t, store.SaveAffinityGroup(store, loaded.ID, loaded))
		test := newTargetTestWithStorage(t, t.Context(), store, "empty")
		state := test.GetAffinityGroupState
		test.useWriter(test.UpdateAffinityGroupTargets)
		test.False(test.storeConditionsKnown)
		test.False(state("loaded").AffinitySchedulingAllowed)
		test.observe("loaded", 2, 1, 3)
		test.Empty(test.calls, "a loaded target waits for the first store check")
		test.Equal(loaded, test.stored("loaded"))

		empty := test.target("empty", 2, 1, 2, 3)
		test.observe("empty", 2, 1, 3)
		test.Equal([]GroupTarget{empty}, test.calls)
		test.Equal(empty.result(), test.stored("empty"))
		test.Equal(groupDegraded, test.groups["empty"].GetAvailability())
		test.checkGroupsAvailability()
		test.True(state("loaded").AffinitySchedulingAllowed)
		test.True(state("empty").AffinitySchedulingAllowed)

		test.ExpireAffinityGroup("loaded")
		test.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
		loaded.LeaderStoreID = 2
		test.SyncGroupFromEtcdWithRevision(loaded, 2)
		degraded := state(loaded.ID)
		test.Equal(*loaded, degraded.Group)
		test.False(degraded.RegularSchedulingAllowed)
		test.False(degraded.AffinitySchedulingAllowed)
		deadline := newDegradedExpiredAtFromNow()
		test.Equal(deadline, test.groups["loaded"].degradedExpiredAt)
		time.Sleep(time.Second)
		test.SyncGroupFromEtcdWithRevision(loaded, 3)
		test.Equal(degraded, state(loaded.ID))
		test.Equal(deadline, test.groups["loaded"].degradedExpiredAt)
	})
}

func TestFirstWinSingleFlightAndGlobalBackoff(t *testing.T) {
	for name, replyErr := range map[string]error{
		"failed":    errors.New("PD unavailable"),
		"not ready": fmt.Errorf("PD leader is not found: %w", ErrTargetWriterNotReady),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t, "g", "h", "i", "j")
				failures := testutil.ToFloat64(targetWriteFailureCounter)
				release := make(chan struct{})
				defer close(release)
				test.useWriter(func(_ context.Context, targets []GroupTarget) ([]string, error) {
					if len(test.calls) <= 3 {
						<-release
					}
					if len(targets) == 2 {
						return nil, replyErr
					}
					return []string{targets[0].Expected.ID}, nil
				})
				g, h := test.target("g", 1, 1, 2), test.target("h", 1, 1, 2)
				i, j := test.target("i", 1, 1, 2), test.target("j", 1, 1, 2)
				test.observe("g", 1, 2)
				test.observe("h", 1, 2)
				test.observe("i", 1, 2)
				test.Equal([]GroupTarget{g}, test.calls, "only one request can be in flight")
				release <- struct{}{}
				synctest.Wait()
				test.Equal([]GroupTarget{g, h, i}, test.calls, "queued groups are sent together")
				test.observe("j", 1, 2)
				release <- struct{}{}
				synctest.Wait()
				test.Len(test.calls, 3, "failure drops pending observations")
				test.observe("j", 1, 2)
				test.Len(test.calls, 3, "failure delays all groups")
				if !errors.Is(replyErr, ErrTargetWriterNotReady) {
					failures += 2
				}
				test.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))

				time.Sleep(time.Second)
				test.observe("h", 1, 2)
				test.Zero(test.GetAffinityGroupState("h").LeaderStoreID, "HTTP success waits for the watcher")
				test.observe("j", 1, 2)
				test.Equal([]GroupTarget{g, h, i, h, j}, test.calls, "success does not delay other groups")
			})
		})
	}
}

func TestFirstWinSkippedGroupDoesNotBlockPatrol(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		skipped := make([]string, 0, 1024)
		for index := range 1024 {
			skipped = append(skipped, fmt.Sprintf("a%04d", index))
		}
		first := skipped[0]
		pd := newTargetTest(t, append(skipped, "b", "c")...)
		pd.applyStoreConditions(map[uint64]storeCondition{1: storeDown})
		test := newTargetTestWithStorage(t, t.Context(), pd.store)
		failures := testutil.ToFloat64(targetWriteFailureCounter)
		release := make(chan struct{})
		defer close(release)
		var written []string
		test.useWriter(func(ctx context.Context, targets []GroupTarget) ([]string, error) {
			test.LessOrEqual(len(targets), 128)
			if targets[0].Expected.ID == first {
				<-release
			}
			ids, err := pd.UpdateAffinityGroupTargets(ctx, targets)
			written = append(written, ids...)
			return ids, err
		})

		b := test.target("b", 2, 2, 3)
		c := test.target("c", 3, 3, 4)
		want := make([]GroupTarget, 0, len(skipped)+2)
		for _, id := range skipped {
			want = append(want, test.target(id, 1, 1, 2))
		}
		want = append(want, b, c)
		region := targetRegion(1, 2)
		firstPatrol := time.Now()
		for patrol := range 3 {
			start := firstPatrol.Add(time.Duration(patrol) * time.Second)
			before := len(test.calls)
			test.observe(first, 1, 2)
			for _, id := range skipped[1:] {
				test.ObserveAvailableRegion(region, test.GetAffinityGroupState(id))
			}
			test.observe("b", 1, 2)
			test.observe("b", 2, 3)
			test.observe("c", 3, 4)
			test.observe(first, 1, 2) // Queue a repeat of the in-flight request.
			test.Equal(want[:1], test.calls[before:])
			release <- struct{}{}
			synctest.Wait()

			test.Equal(start, time.Now(), "the queued groups drain without waiting for a cooldown")
			test.Equal([]string{"b", "c"}, written[patrol*2:])
			test.Equal(want, test.calls[before:])
			test.Zero(pd.stored(first).LeaderStoreID)
			test.Equal(b.result(), pd.stored("b"))
			test.Equal(c.result(), pd.stored("c"))

			time.Sleep(time.Second - time.Millisecond)
			for _, target := range []GroupTarget{want[0], b, c} {
				test.observe(target.Expected.ID, target.VoterStoreIDs...)
			}
			test.Len(test.calls, before+len(want), "reobserved groups are still in cooldown")
			time.Sleep(time.Millisecond)
		}
		test.Equal(failures+float64(3*len(skipped)), testutil.ToFloat64(targetWriteFailureCounter))
	})
}

func TestFirstWinLateReply(t *testing.T) {
	for _, name := range []string{"new revision", "expired", "recreated in the same second", "store down"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t)
				group := Group{ID: "g", CreateTimestamp: 1}
				if name == "new revision" {
					group.LeaderStoreID, group.VoterStoreIDs = 2, []uint64{1, 2}
				}
				test.SyncGroupFromEtcdWithRevision(&group, 1)
				switch name {
				case "new revision":
					test.applyStoreConditions(map[uint64]storeCondition{2: storeDown})
				case "store down":
					test.checkGroupsAvailability()
				}
				state := test.GetAffinityGroupState
				before := state("g")
				var updated *GroupState
				test.useWriter(func(context.Context, []GroupTarget) ([]string, error) {
					switch name {
					case "new revision":
						test.SyncGroupFromEtcdWithRevision(&group, 2)
					case "expired":
						test.ExpireAffinityGroup("g")
					case "recreated in the same second":
						test.SyncGroupDeleteFromEtcd("g")
						test.SyncGroupFromEtcdWithRevision(&group, 1)
					case "store down":
						test.applyStoreConditions(map[uint64]storeCondition{1: storeDown})
					}
					updated = state("g")
					return nil, ErrTargetWriterUnsupported
				})
				test.observe("g", 1, 3)
				switch name {
				case "new revision", "store down":
					test.Equal(before, updated, "a revision-only update or a store outage for an unset target leaves the group unchanged")
				case "recreated in the same second":
					test.NotSame(before.groupInfoPtr, updated.groupInfoPtr)
					test.Equal(before.Group, updated.Group)
					test.Equal(before.affinityVer, updated.affinityVer)
				}
				test.Equal(updated, state("g"), "a late 404 must preserve the newer state")
				test.observe("g", 3, 4)
				test.True(state("g").HasUnpersistedTarget())
				test.Len(test.calls, 1, "a fresh observation applies during the 404 cooldown")
			})
		})
	}
}

func TestFirstWinReconcilesLocalTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pd := newTargetTest(t, "g", "h")
		test := newTargetTestWithStorage(t, t.Context(), pd.store)
		test.checkGroupsAvailability()
		failures := testutil.ToFloat64(targetWriteFailureCounter)
		state := test.GetAffinityGroupState
		test.useWriter(func(ctx context.Context, targets []GroupTarget) ([]string, error) {
			if len(test.calls) == 1 {
				test.ObserveAvailableRegion(targetRegion(2, 3), state("h"))
			}
			if len(test.calls) <= 2 {
				return nil, fmt.Errorf("404 Not Found: %w", ErrTargetWriterUnsupported)
			}
			return pd.UpdateAffinityGroupTargets(ctx, targets)
		})
		target := test.target("g", 1, 1, 2)
		test.observe("g", 1, 2)
		local := state("g")
		test.Equal(*target.result(), local.Group)
		test.True(local.HasUnpersistedTarget())
		test.True(local.AffinitySchedulingAllowed)
		test.Zero(pd.stored("g").LeaderStoreID)
		test.True(state("h").HasUnpersistedTarget(), "the queued observation also falls back to a local target")
		test.observe("g", 4, 1)
		test.Len(test.calls, 1, "the cooldown allows local targets without sending HTTP requests")
		test.Equal(local, state("g"))
		time.Sleep(time.Second)
		test.observe("g", 1, 2)
		test.Equal([]GroupTarget{target, target}, test.calls)
		test.Equal(local, state("g"), "another 404 keeps the active target")
		test.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))

		time.Sleep(time.Second)
		test.observe("g", 1, 2)
		persisted := pd.stored("g")
		test.Equal(target.result(), persisted)
		test.Equal(local, state("g"), "success waits for confirmation")
		test.SyncGroupFromEtcdWithRevision(persisted, 2)
		confirmed := state("g")
		test.False(confirmed.HasUnpersistedTarget())
		test.Equal(local.affinityVer, confirmed.affinityVer)
		test.DegradeAffinityGroup("g")
		test.observe("g", 1, 2)
		test.True(state("g").AffinitySchedulingAllowed)
		test.Len(test.calls, 3, "a matching observation restores the persisted target without writing")
		test.observe("h", 2, 3)
		test.Equal(uint64(2), pd.stored("h").LeaderStoreID, "a successful probe clears the cooldown")

		test.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
		test.True(state("h").HasUnpersistedTarget())
		test.Equal(groupDegraded, test.groups["h"].GetAvailability())
		time.Sleep(time.Second)
		test.SyncGroupFromEtcdWithRevision(pd.stored("h"), 3)
		test.False(state("h").HasUnpersistedTarget())
		test.Equal(groupDegraded, test.groups["h"].GetAvailability())
		test.Equal(newDegradedExpiredAtFromNow(), test.groups["h"].degradedExpiredAt)
	})
}

type targetTxnStorage struct {
	endpoint.AffinityStorage
	run func(context.Context, func(kv.Txn) error) error
}

func (store targetTxnStorage) RunInTxn(ctx context.Context, apply func(kv.Txn) error) error {
	return store.run(ctx, apply)
}

func (test *targetTest) interceptTxns(run func(context.Context, func(kv.Txn) error) error) {
	test.storage = targetTxnStorage{test.store, run}
}
