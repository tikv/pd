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
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
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
	store   storage.Storage
	callsMu sync.Mutex
	calls   []GroupTarget
	reply   func(context.Context, []GroupTarget) ([]string, error)
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
	test.callsMu.Lock()
	test.calls = append(test.calls, targets...)
	test.callsMu.Unlock()
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

func TestUpdateAffinityGroupTargetsBusyLeader(t *testing.T) {
	test := newTargetTest(t, "replayed", "changed")
	replay := test.target("replayed", 1, 1, 2)
	test.write([]string{"replayed", "changed"}, replay, test.target("changed", 2, 1, 2))
	change := test.target("changed", 1, 1, 2)
	test.applyStoreConditions(map[uint64]storeCondition{1: storeBusy})
	test.ExpireAffinityGroup("replayed")
	expired := test.GetAffinityGroupState("replayed")

	test.write([]string{"replayed"}, replay, change)
	test.Equal(expired, test.GetAffinityGroupState("replayed"))
	test.Equal(&change.Expected, test.stored("changed"))
}

func TestUpdateAffinityGroupTargetsReplayPreservesConcurrentExpiration(t *testing.T) {
	test := newTargetTest(t, "g")
	test.checkGroupsAvailability()
	target := test.target("g", 1, 1, 2)
	test.write([]string{"g"}, target)
	before := test.GetAffinityGroupState("g")
	var expired *GroupState
	test.interceptTxns(func(ctx context.Context, apply func(kv.Txn) error) error {
		err := test.store.RunInTxn(ctx, apply)
		var ok bool
		expired, ok = test.ExpireAffinityGroupIf(before)
		test.True(ok)
		return err
	})

	test.write([]string{"g"}, target)
	test.Equal(groupExpired, test.groups["g"].GetAvailability())
	test.Equal(expired, test.GetAffinityGroupState("g"))
}

func TestUpdateAffinityGroupTargetsContextCancellation(t *testing.T) {
	t.Run("canceled after locking metadata", func(t *testing.T) {
		test := newTargetTest(t, "g")
		targets := []GroupTarget{test.target("g", 1, 1, 2)}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		transactions := 0
		test.interceptTxns(func(txnCtx context.Context, apply func(kv.Txn) error) error {
			transactions++
			return test.store.RunInTxn(txnCtx, apply)
		})
		const hook = "github.com/tikv/pd/pkg/schedule/affinity/afterLockGroupTarget"
		test.NoError(failpoint.EnableCall(hook, func() { cancel() }))
		defer func() { test.NoError(failpoint.Disable(hook)) }()
		written, err := test.UpdateAffinityGroupTargets(ctx, targets)
		test.Zero(transactions)
		test.ErrorIs(err, context.Canceled)
		test.Empty(written)
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
		degraded.modRevision = 3
		test.Equal(degraded, state(loaded.ID))
		test.Equal(deadline, test.groups["loaded"].degradedExpiredAt)
		test.ExpireAffinityGroup("loaded")
		expired := state(loaded.ID)
		test.SyncGroupFromEtcdWithRevision(loaded, 4)
		expired.modRevision = 4
		test.Equal(groupExpired, test.groups["loaded"].GetAvailability())
		test.Equal(expired, state(loaded.ID))
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
				case "new revision":
					test.Greater(updated.modRevision, before.modRevision)
					test.Equal(before.affinityVer, updated.affinityVer)
					test.Equal(before.Group, updated.Group)
				case "store down":
					test.Equal(before, updated, "a store outage for an unset target leaves the group unchanged")
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

func TestObserveAvailableRegionRejectsStaleRevision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		test := newTargetTest(t, "g")
		before, err := test.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2})
		test.NoError(err)
		test.applyStoreConditions(map[uint64]storeCondition{2: storeDown})
		test.SyncGroupFromEtcdWithRevision(&before.Group, 1)
		observed := test.GetAffinityGroupState("g")
		// Deliver the watch event for an admin PUT that kept the same peers.
		test.SyncGroupFromEtcdWithRevision(&before.Group, 2)
		current := test.GetAffinityGroupState("g")
		test.Equal(observed.affinityVer, current.affinityVer)
		test.Equal(observed.Group, current.Group)
		test.Equal(int64(2), current.modRevision)
		test.useWriter(func(context.Context, []GroupTarget) ([]string, error) {
			return nil, ErrTargetWriterUnsupported
		})

		region := targetRegion(1, 3)
		test.ObserveAvailableRegion(region, observed)
		synctest.Wait()
		test.Empty(test.calls)
		test.Equal(current, test.GetAffinityGroupState("g"))

		test.observe("g", 1, 3)
		test.Len(test.calls, 1)
		test.Equal(current.modRevision, test.calls[0].ExpectedModRevision)
		test.True(test.GetAffinityGroupState("g").HasUnpersistedTarget())
	})
}

func TestFirstWinRetriesDifferentQueuedTargetAfterCooldown(t *testing.T) {
	for _, scenario := range []string{"duplicate only", "different peers", "stale revision", "slow other group", "observed during cooldown"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pd := newTargetTest(t, "g", "h")
				pd.applyStoreConditions(map[uint64]storeCondition{1: storeDown})
				test := newTargetTestWithStorage(t, t.Context(), pd.store)
				release := make(chan struct{})
				defer close(release)
				var lastSendAt time.Time
				test.useWriter(func(ctx context.Context, targets []GroupTarget) ([]string, error) {
					lastSendAt = time.Now()
					if len(test.calls) == 1 || (scenario == "slow other group" && targets[0].Expected.ID == "h") {
						<-release
					}
					return pd.UpdateAffinityGroupTargets(ctx, targets)
				})
				first, second := test.target("g", 1, 1, 2), test.target("g", 2, 2, 3)
				other := test.target("h", 3, 3, 4)
				test.observe("g", 1, 2)
				if scenario != "duplicate only" && scenario != "observed during cooldown" {
					test.observe("g", 2, 3)
				}
				if scenario == "slow other group" {
					test.observe("h", 3, 4)
				}
				test.observe("g", 1, 2)
				test.Equal([]GroupTarget{first}, test.calls)
				release <- struct{}{}
				synctest.Wait()
				test.Zero(pd.stored("g").LeaderStoreID)

				if scenario == "slow other group" {
					test.Equal([]GroupTarget{first, other}, test.calls)
					time.Sleep(1100 * time.Millisecond)
					test.observe("g", 1, 2)
					test.Zero(pd.stored("h").LeaderStoreID, "h is still in flight after g's cooldown")
					release <- struct{}{}
					synctest.Wait()
					test.Equal([]GroupTarget{first, other, second}, test.calls)
					test.Equal(other.result(), pd.stored("h"))
					test.Equal(second.result(), pd.stored("g"))
					return
				}
				if scenario == "observed during cooldown" {
					readyAt := time.Now().Add(time.Second)
					time.Sleep(100 * time.Millisecond)
					test.observe("g", 2, 3)
					test.observe("g", 1, 2)
					time.Sleep(time.Until(readyAt))
					synctest.Wait()
					test.Equal([]GroupTarget{first, second}, test.calls)
					test.Equal(readyAt, lastSendAt, "g sends the queued target when its cooldown ends")
					test.Equal(second.result(), pd.stored("g"))
					return
				}

				// A different group can wake the sender during g's cooldown.
				time.Sleep(time.Second - time.Millisecond)
				test.observe("h", 3, 4)
				test.callsMu.Lock()
				calls := slices.Clone(test.calls)
				test.callsMu.Unlock()
				test.Equal([]GroupTarget{first, other}, calls)
				test.Equal(other.result(), pd.stored("h"))
				if scenario == "stale revision" {
					test.SyncGroupFromEtcdWithRevision(&second.Expected, 1)
				}
				time.Sleep(time.Millisecond)
				synctest.Wait()
				if scenario == "different peers" {
					test.Equal([]GroupTarget{first, other, second}, test.calls)
					test.Equal(second.result(), pd.stored("g"))
				} else {
					test.Equal([]GroupTarget{first, other}, test.calls)
					test.Zero(pd.stored("g").LeaderStoreID)
					if scenario == "duplicate only" {
						pd.applyStoreConditions(nil)
						test.observe("g", 1, 2)
						test.Equal([]GroupTarget{first, other, first}, test.calls)
						test.Equal(first.result(), pd.stored("g"))
					}
				}
			})
		})
	}
}

type targetAttempt struct {
	at      time.Time
	targets []GroupTarget
}

func (test *targetTest) recordTargetAttempts(reply func(context.Context, []GroupTarget) ([]string, error)) <-chan targetAttempt {
	attempts := make(chan targetAttempt, maxPendingTargetsPerGroup+2)
	test.useWriter(func(ctx context.Context, targets []GroupTarget) ([]string, error) {
		at := time.Now()
		written, err := reply(ctx, targets)
		attempts <- targetAttempt{at: at, targets: targets}
		return written, err
	})
	return attempts
}

func (test *targetTest) expectTargetAttempt(attempts <-chan targetAttempt, at time.Time, target GroupTarget) {
	select {
	case attempt := <-attempts:
		test.Equal(at, attempt.at)
		test.Equal([]GroupTarget{target}, attempt.targets)
	case <-time.After(time.Millisecond):
		test.FailNow("expected a target attempt at the cooldown deadline")
	}
}

func TestFirstWinTriesDistinctCandidatesInPatrolOrder(t *testing.T) {
	for name, scenario := range map[string]struct {
		patrolOrder []uint64
		sendOrder   []uint64
	}{
		"healthy candidate second": {patrolOrder: []uint64{1, 2, 3}, sendOrder: []uint64{1, 2}},
		"healthy candidate third":  {patrolOrder: []uint64{1, 3, 2}, sendOrder: []uint64{1, 3, 2}},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pd := newTargetTest(t, "g")
				pd.applyStoreConditions(map[uint64]storeCondition{1: storeBusy, 3: storeBusy})
				test := newTargetTestWithStorage(t, t.Context(), pd.store)
				attempts := test.recordTargetAttempts(pd.UpdateAffinityGroupTargets)
				healthy := test.target("g", 2, 1, 2, 3)

				keys := []string{"a", "b", "c", "d"}
				regions := make([]*core.RegionInfo, 0, len(scenario.patrolOrder))
				for i, leader := range scenario.patrolOrder {
					voters := []uint64{1, 2, 3}
					voters[0], voters[leader-1] = voters[leader-1], voters[0]
					keyRange := pdtestutil.TxnKeyspaceRange([]byte(keys[i]), []byte(keys[i+1]))
					regions = append(regions, generateRegionForTest(uint64(i+1), voters, keyRange))
				}
				want := make([]GroupTarget, 0, len(scenario.sendOrder))
				for _, leader := range scenario.sendOrder {
					want = append(want, test.target("g", leader, 1, 2, 3))
				}
				patrol := func() {
					for _, region := range regions {
						test.ObserveAvailableRegion(region, test.GetAffinityGroupState("g"))
						synctest.Wait()
					}
				}

				start := time.Now()
				patrol()
				test.expectTargetAttempt(attempts, start, want[0])
				for i := 1; i < len(want); i++ {
					test.Zero(pd.stored("g").LeaderStoreID)
					time.Sleep(time.Second - time.Millisecond)
					patrol()
					test.Empty(attempts, "patrol observations cannot bypass cooldown")
					time.Sleep(time.Millisecond)
					test.expectTargetAttempt(attempts, start.Add(time.Duration(i)*time.Second), want[i])
					synctest.Wait()
				}
				persisted := pd.stored("g")
				test.Equal(healthy.result(), persisted)
				test.SyncGroupFromEtcdWithRevision(persisted, 1)
				synctest.Sleep(time.Second + time.Millisecond)
				test.Empty(attempts, "the watch update invalidates remaining candidates")
			})
		})
	}
}

func TestFirstWinPendingTargetLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		test := newTargetTest(t, "g")
		// Establish store conditions before queuing targets so the first periodic
		// scan does not change the group's affinity version while the queue drains.
		test.checkGroupsAvailability()
		attempts := test.recordTargetAttempts(func(context.Context, []GroupTarget) ([]string, error) { return nil, nil })
		start := time.Now()
		test.observe("g", 1, 2)
		test.expectTargetAttempt(attempts, start, test.target("g", 1, 1, 2))
		// Submit one more candidate than fits while the first candidate cools down.
		stores := test.storeSetInformer.(*core.StoresInfo)
		for i := range maxPendingTargetsPerGroup + 1 {
			leader := uint64(i + 2)
			stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: leader, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
			test.observe("g", leader, 1)
		}
		test.Empty(attempts)
		for i := range maxPendingTargetsPerGroup {
			leader := uint64(i + 2)
			time.Sleep(time.Second)
			test.expectTargetAttempt(attempts, start.Add(time.Duration(i+1)*time.Second), test.target("g", leader, 1, leader))
		}
		synctest.Sleep(time.Second + time.Millisecond)
		test.Empty(attempts, "the overflow candidate was dropped")
	})
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
				test.ObserveAvailableRegion(targetRegion(3, 4), state("h"))
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
		test.Equal(uint64(2), state("h").LeaderStoreID, "fallback uses the first healthy queued candidate")
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

func TestFirstWinReplacesRejectedLocalTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pd := newTargetTest(t, "g")
		test := newTargetTestWithStorage(t, t.Context(), pd.store)
		test.checkGroupsAvailability()
		test.useWriter(func(ctx context.Context, targets []GroupTarget) ([]string, error) {
			if len(test.calls) == 1 {
				return nil, ErrTargetWriterUnsupported
			}
			return pd.UpdateAffinityGroupTargets(ctx, targets)
		})

		initial := pd.stored("g")
		test.observe("g", 1, 2)
		local := test.GetAffinityGroupState("g")
		test.True(local.HasUnpersistedTarget())
		test.Equal(uint64(1), local.LeaderStoreID)

		pd.applyStoreConditions(map[uint64]storeCondition{1: storeBusy})
		time.Sleep(time.Second)
		test.observe("g", 1, 2)
		test.Len(test.calls, 2)
		reverted := test.GetAffinityGroupState("g")
		test.False(reverted.HasUnpersistedTarget())
		test.Equal(*initial, reverted.Group)
		test.Greater(reverted.affinityVer, local.affinityVer)

		time.Sleep(time.Second)
		test.observe("g", 2, 3)
		test.Len(test.calls, 3)
		test.Equal(test.calls[2].result(), pd.stored("g"))
		test.Equal(uint64(2), pd.stored("g").LeaderStoreID)
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
