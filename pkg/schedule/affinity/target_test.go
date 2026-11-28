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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

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
	reply   func(context.Context, []GroupTarget) (GroupTargetsResponse, error)
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
	test.Equal(want, written.WrittenGroupIDs)
}

func (test *targetTest) useWriter(reply func(context.Context, []GroupTarget) (GroupTargetsResponse, error)) {
	test.reply = reply
	test.SetTargetWriter(test)
}

func (test *targetTest) WriteGroupTargets(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
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

func TestTargetPolicyCAS(t *testing.T) {
	test := newTargetTest(t, "g")
	target := test.target("g", 1, 1, 2, 3)
	_, err := test.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
	test.NoError(err)
	test.write([]string{}, target)
	test.Zero(test.stored("g").LeaderStoreID)
	test.Equal(BalancePolicyFixed, test.GetAffinityGroupState("g").BalancePolicy)
}

func TestEmptyTargetRevisionMismatchIsNotPolicyMismatch(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		recreated bool
		policy    BalancePolicy
	}{
		{name: "same group and policy"},
		{name: "recreated with the same policy", recreated: true},
		{name: "recreated with a different policy", recreated: true, policy: BalancePolicyAuto},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			test := newTargetTest(t, "g")
			target := test.target("g", 1, 1, 2)
			target.ExpectedModRevision = 1
			stored := test.stored("g")
			if scenario.recreated {
				stored.CreateTimestamp++
			}
			stored.BalancePolicy = scenario.policy
			test.NoError(test.store.RunInTxn(t.Context(), func(txn kv.Txn) error {
				return test.store.SaveAffinityGroup(txn, "g", stored)
			}))
			test.storage = &revisionedTargetStorage{AffinityStorage: test.store, revision: 2}

			reply, err := test.UpdateAffinityGroupTargets(t.Context(), []GroupTarget{target})
			test.NoError(err)
			test.Empty(reply.WrittenGroupIDs)
			test.Empty(reply.PolicyMismatchGroupIDs)
			test.Equal(stored, test.stored("g"))
		})
	}
}

func TestClassicFirstWinKeepsCommittedPolicy(t *testing.T) {
	for _, hasTarget := range []bool{false, true} {
		t.Run(fmt.Sprintf("has-target-%t", hasTarget), func(t *testing.T) {
			test := newTargetTest(t, "g")
			test.applyStoreConditions(nil)
			if hasTarget {
				_, err := test.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
				test.NoError(err)
			}
			wrapped := &lostReplyStorage{AffinityStorage: test.store, lose: true}
			test.storage = wrapped
			_, err := test.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
			test.Error(err)
			wrapped.lose = false
			test.Empty(test.GetAffinityGroupState("g").BalancePolicy)
			committed := *test.stored("g")
			test.Equal(BalancePolicyFixed, committed.BalancePolicy)
			test.ExpireAffinityGroup("g")
			test.ObserveAvailableRegion(targetRegion(2, 1, 3), test.GetAffinityGroupState("g"))
			test.Equal(committed, *test.stored("g"))
			test.Equal(committed, test.GetAffinityGroupState("g").Group)
			test.ObserveAvailableRegion(targetRegion(2, 1, 3), test.GetAffinityGroupState("g"))
			state := test.GetAffinityGroupState("g")
			test.Equal(BalancePolicyFixed, state.BalancePolicy)
			test.Equal(uint64(2), state.LeaderStoreID)
			test.Equal(state.Group, *test.stored("g"))
		})
	}
}

func TestClassicFirstWinBeforeStoreScan(t *testing.T) {
	test := newTargetTest(t, "g")
	test.NoError(test.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{targetTestRange}}}, nil))
	test.False(test.storeConditionsKnown)
	region := targetRegion(1, 2, 3)
	before, isAffinity := test.GetAndCacheRegionAffinityGroupState(region)
	test.NotNil(before)
	test.False(isAffinity)

	test.ObserveAvailableRegion(region, before)
	state, isAffinity := test.GetAndCacheRegionAffinityGroupState(region)
	test.True(isAffinity)
	test.Greater(state.affinityVer, before.affinityVer)
	test.True(state.AffinitySchedulingAllowed)
	test.Equal(uint64(1), state.LeaderStoreID)
	test.Equal(state.Group, *test.stored("g"))
}

func TestClassicLeaderSwitchPreservesTargetBeforeStoreScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		previous := newTargetTest(t, "loaded", "empty")
		_, err := previous.UpdateAffinityGroupPolicy("loaded", BalancePolicyAuto)
		previous.NoError(err)
		_, err = previous.UpdateAffinityGroupPeers("loaded", 1, []uint64{1, 2, 3})
		previous.NoError(err)
		persisted := previous.stored("loaded")

		test := newTargetTestWithStorage(t, t.Context(), previous.store)
		state := test.GetAffinityGroupState
		test.False(test.storeConditionsKnown)
		test.Equal(*persisted, state("loaded").Group)
		test.False(state("loaded").AffinitySchedulingAllowed)
		test.False(state("loaded").RegularSchedulingAllowed)

		test.observe("loaded", 3, 1, 2)
		test.Equal(persisted, test.stored("loaded"))
		test.Equal(*persisted, state("loaded").Group)
		test.False(state("loaded").AffinitySchedulingAllowed)

		stores := test.storeSetInformer.(*core.StoresInfo)
		stores.PutStore(stores.GetStore(1).Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Minute))))
		test.observe("loaded", 1, 2, 3)
		test.False(state("loaded").AffinitySchedulingAllowed)
		test.Equal(persisted, test.stored("loaded"))

		stores.PutStore(stores.GetStore(1).Clone(core.SetLastHeartbeatTS(time.Now())))
		test.observe("loaded", 1, 2, 3)
		test.True(state("loaded").AffinitySchedulingAllowed)
		test.Equal(*persisted, state("loaded").Group)
		test.Equal(persisted, test.stored("loaded"))

		test.False(state("empty").hasTarget())
		test.observe("empty", 3, 1, 2)
		test.True(state("empty").AffinitySchedulingAllowed)
		test.Equal(uint64(3), state("empty").LeaderStoreID)
		test.Equal(state("empty").Group, *test.stored("empty"))
		test.False(test.storeConditionsKnown)

		test.ExpireAffinityGroup("empty")
		expired := state("empty")
		test.True(expired.RegularSchedulingAllowed)
		test.False(expired.AffinitySchedulingAllowed)
		test.ObserveAvailableRegion(targetRegion(1, 2, 4), expired)
		relearned := state("empty")
		test.True(relearned.AffinitySchedulingAllowed)
		test.Equal(uint64(1), relearned.LeaderStoreID)
		test.Equal([]uint64{1, 2, 4}, relearned.VoterStoreIDs)
		test.Equal(relearned.Group, *test.stored("empty"))
		test.False(test.storeConditionsKnown)

		test.DegradeAffinityGroup("loaded")
		staleObserved := state("loaded")
		test.False(staleObserved.AffinitySchedulingAllowed)
		test.checkGroupsAvailability()
		test.True(test.storeConditionsKnown)
		afterScan := state("loaded")
		test.True(afterScan.AffinitySchedulingAllowed)
		test.Greater(afterScan.affinityVer, staleObserved.affinityVer)
		test.ObserveAvailableRegion(targetRegion(3, 1, 2), staleObserved)
		test.Equal(*persisted, state("loaded").Group)
		test.Equal(persisted, test.stored("loaded"))

		test.DegradeAffinityGroup("loaded")
		test.interceptTxns(func(ctx context.Context, apply func(kv.Txn) error) error {
			// Publish a store scan after patrol has validated its observation.
			stores.PutStore(stores.GetStore(4).Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Minute))))
			test.checkGroupsAvailability()
			return test.store.RunInTxn(ctx, apply)
		})
		test.observe("loaded", 3, 1, 2)
		test.True(state("loaded").AffinitySchedulingAllowed)
		test.Equal(*persisted, state("loaded").Group)
		test.Equal(persisted, test.stored("loaded"))
	})
}

func TestClassicFirstWinReplayPreservesExpirationWithEvictedLeader(t *testing.T) {
	test := newTargetTest(t, "g")
	test.checkGroupsAvailability()
	test.True(test.storeConditionsKnown)
	_, err := test.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	test.NoError(err)
	test.ExpireAffinityGroup("g")
	expired := test.GetAffinityGroupState("g")
	test.Equal(groupExpired, getGroupForTest(test.Assertions, test.Manager, "g").GetAvailability())

	transactions := 0
	test.interceptTxns(func(ctx context.Context, apply func(kv.Txn) error) error {
		transactions++
		err := test.store.RunInTxn(ctx, apply)
		test.applyStoreConditions(map[uint64]storeCondition{1: storeLeaderEvicted})
		return err
	})

	test.ObserveAvailableRegion(targetRegion(1, 2, 3), expired)
	test.Equal(1, transactions)
	test.Equal(groupExpired, getGroupForTest(test.Assertions, test.Manager, "g").GetAvailability())
	test.Equal(expired, test.GetAffinityGroupState("g"))
}

func TestMCSFirstWinKeepsFixedTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		test := newTargetTest(t, "g")
		test.applyStoreConditions(nil)
		_, err := test.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2})
		test.NoError(err)
		test.DegradeAffinityGroup("g")
		observed := test.GetAffinityGroupState("g")
		_, err = test.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
		test.NoError(err)
		test.reply = func(_ context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			return GroupTargetsResponse{WrittenGroupIDs: []string{targets[0].Expected.ID}}, nil
		}
		test.SetTargetWriter(test)
		test.ObserveAvailableRegion(targetRegion(2, 1), observed)
		synctest.Wait()
		test.Empty(test.calls)
		test.Equal(uint64(1), test.GetAffinityGroupState("g").LeaderStoreID)
	})
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

func TestUpdateAffinityGroupTargetsSkipsDeletedStore(t *testing.T) {
	test := newTargetTest(t, "g", "h")
	test.checkGroupsAvailability()
	test.True(test.storeConditionsKnown)
	stores := test.storeSetInformer.(*core.StoresInfo)
	stores.DeleteStore(stores.GetStore(2))

	deleted := test.target("g", 1, 1, 2)
	healthy := test.target("h", 1, 1, 3)
	test.write([]string{"h"}, deleted, healthy)
	test.Equal(&deleted.Expected, test.stored("g"))
	test.Equal(healthy.result(), test.stored("h"))
}

func TestUpdateAffinityGroupTargetsReplayWithDeletedStore(t *testing.T) {
	test := newTargetTest(t, "g", "h")
	test.checkGroupsAvailability()
	test.True(test.storeConditionsKnown)
	replay := test.target("g", 1, 1, 2)
	test.write([]string{"g"}, replay)
	test.True(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	test.ExpireAffinityGroup("g")
	stores := test.storeSetInformer.(*core.StoresInfo)
	stores.DeleteStore(stores.GetStore(2))

	healthy := test.target("h", 1, 1, 3)
	test.write([]string{"g", "h"}, replay, healthy)
	test.False(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	test.Equal(healthy.result(), test.stored("h"))
}

func TestAdminTargetUsesStoreConditionsAtCommit(t *testing.T) {
	test := newTargetTest(t, "g")
	test.checkGroupsAvailability()
	test.True(test.storeConditionsKnown)
	test.interceptTxns(func(ctx context.Context, apply func(kv.Txn) error) error {
		err := test.store.RunInTxn(ctx, apply)
		test.applyStoreConditions(map[uint64]storeCondition{2: storeRemoving})
		return err
	})

	state, err := test.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	test.NoError(err)
	test.Equal(groupExpired, test.groups["g"].GetAvailability())
	test.False(state.AffinitySchedulingAllowed)
	test.applyStoreConditions(map[uint64]storeCondition{2: storeRemoving})
	test.False(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
}

func TestAdminTargetReturnsRecreatedGroup(t *testing.T) {
	test := newTargetTest(t, "g")
	old := test.GetAffinityGroupState("g")
	recreated := *test.stored("g")
	recreated.CreateTimestamp++
	test.NoError(test.store.SaveAffinityGroup(test.store, "g", &recreated))

	state, err := test.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	test.NoError(err)
	test.NotSame(old.groupInfoPtr, state.groupInfoPtr)
	test.Equal(recreated.CreateTimestamp, state.CreateTimestamp)
	test.Equal(test.stored("g"), &state.Group)
	test.Equal(test.GetAffinityGroupState("g"), state)
}

func TestFirstGroupRejectsRemovingStoreBeforeScan(t *testing.T) {
	test := newTargetTest(t)
	stores := test.storeSetInformer.(*core.StoresInfo)
	stores.PutStore(stores.GetStore(2).Clone(core.SetNodeState(metapb.NodeState_Removing)))
	test.checkGroupsAvailability()
	test.NoError(test.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	test.False(test.storeConditionsKnown)

	_, err := test.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2})
	test.ErrorContains(err, "store 2 is removing")
	test.write([]string{}, test.target("g", 1, 1, 2))
	test.Zero(test.stored("g").LeaderStoreID)
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
		test.Empty(written.WrittenGroupIDs)
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
		test.Equal([]string{"first"}, written.WrittenGroupIDs)
		test.Equal(1, transactions)
		test.Equal(targets[0].result(), test.stored("first"))
		test.Zero(test.stored("second").LeaderStoreID)

		test.interceptTxns(func(txnCtx context.Context, _ func(kv.Txn) error) error {
			stopManager()
			<-txnCtx.Done()
			return txnCtx.Err()
		})
		var writerErr error
		test.useWriter(func(writerCtx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			requestCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			written, err = test.UpdateAffinityGroupTargets(requestCtx, targets)
			writerErr = writerCtx.Err()
			return written, err
		})
		test.observe("second", 1, 2)
		test.ErrorIs(err, context.Canceled, "the Manager cancels the in-flight transaction")
		test.ErrorIs(writerErr, context.Canceled)
		test.Empty(written.WrittenGroupIDs)
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

func TestRecreatedGroupKeepsRangeConflict(t *testing.T) {
	for _, source := range []string{"watcher", "policy"} {
		t.Run(source, func(t *testing.T) {
			test := newTargetTest(t, "g", "h")
			test.write([]string{"g"}, test.target("g", 1, 1, 2, 3))
			test.checkGroupsAvailability()
			test.True(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
			for _, id := range []string{"g", "h"} {
				data, err := json.Marshal(MakeLabelRule(GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{targetTestRange}}))
				test.NoError(err)
				rule, err := labeler.NewLabelRuleFromJSON(data)
				test.NoError(err)
				test.NoError(test.SyncKeyRangesFromEtcd(rule))
			}
			test.ReconcileRangeConflicts()
			before := test.GetAffinityGroupState("g")
			test.True(before.rangeConflict)
			recreated := before.Group
			recreated.CreateTimestamp++
			if source == "watcher" {
				test.SyncGroupFromEtcdWithRevision(&recreated, 2)
			} else {
				test.NoError(test.store.RunInTxn(t.Context(), func(txn kv.Txn) error {
					return test.store.SaveAffinityGroup(txn, "g", &recreated)
				}))
				state, err := test.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
				test.NoError(err)
				recreated.BalancePolicy = BalancePolicyFixed
				test.Equal(recreated, state.Group)
				test.True(state.rangeConflict)
			}
			after := test.GetAffinityGroupState("g")
			test.NotSame(before.groupInfoPtr, after.groupInfoPtr)
			test.Equal(recreated, after.Group)
			test.True(after.rangeConflict)
			test.Equal(1, after.RangeCount)
			test.True(after.RegularSchedulingAllowed)
			test.False(after.AffinitySchedulingAllowed)

			test.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID("h"))
			test.ReconcileRangeConflicts()
			restored := test.GetAffinityGroupState("g")
			test.False(restored.rangeConflict)
			test.False(restored.RegularSchedulingAllowed)
			test.True(restored.AffinitySchedulingAllowed)
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
		test.False(state("empty").AffinitySchedulingAllowed, "the confirmed write waits for the watcher")

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
		"failed":    errors.New("404 Not Found"),
		"not ready": fmt.Errorf("PD leader is not found: %w", ErrTargetWriterNotReady),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t, "g", "h", "i", "j")
				failures := testutil.ToFloat64(targetWriteFailureCounter)
				release := make(chan struct{})
				defer close(release)
				test.useWriter(func(_ context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
					if len(test.calls) <= 3 {
						<-release
					}
					if len(targets) == 2 {
						return GroupTargetsResponse{}, replyErr
					}
					return GroupTargetsResponse{WrittenGroupIDs: []string{targets[0].Expected.ID}}, nil
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
				for _, id := range []string{"h", "i", "j"} {
					test.Zero(test.GetAffinityGroupState(id).LeaderStoreID, "failed and queued candidates must not install targets")
				}
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
		test.useWriter(func(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			test.LessOrEqual(len(targets), 128)
			if targets[0].Expected.ID == first {
				<-release
			}
			ids, err := pd.UpdateAffinityGroupTargets(ctx, targets)
			written = append(written, ids.WrittenGroupIDs...)
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
			expected := want
			if patrol > 0 {
				// b and c are confirmed and still waiting for the watcher.
				expected = want[:len(skipped)]
			}
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
			test.Equal([]string{"b", "c"}, written)
			test.Equal(expected, test.calls[before:])
			test.Zero(pd.stored(first).LeaderStoreID)
			test.Equal(b.result(), pd.stored("b"))
			test.Equal(c.result(), pd.stored("c"))

			time.Sleep(time.Second - time.Millisecond)
			for _, target := range []GroupTarget{want[0], b, c} {
				test.observe(target.Expected.ID, target.VoterStoreIDs...)
			}
			test.Len(test.calls, before+len(expected), "reobserved groups are cooling down or awaiting the watcher")
			time.Sleep(time.Millisecond)
		}
		test.Equal(failures+float64(3*len(skipped)), testutil.ToFloat64(targetWriteFailureCounter))
	})
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
		test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
			return GroupTargetsResponse{WrittenGroupIDs: []string{"g"}, WrittenRevisions: map[string]int64{"g": 3}}, nil
		})

		region := targetRegion(1, 3)
		test.ObserveAvailableRegion(region, observed)
		synctest.Wait()
		test.Empty(test.calls)
		test.Equal(current, test.GetAffinityGroupState("g"))

		test.observe("g", 1, 3)
		test.Len(test.calls, 1)
		test.Equal(current.modRevision, test.calls[0].ExpectedModRevision)
		test.Equal(current.Group, test.GetAffinityGroupState("g").Group, "the reply leaves the target unchanged until the watch event")
		test.SyncGroupFromEtcdWithRevision(test.calls[0].result(), 3)
		test.Equal(*test.calls[0].result(), test.GetAffinityGroupState("g").Group)
	})
}

func TestFirstWinQueuedRelearnPrecedesLocalRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pd := newTargetTest(t, "g", "h")
		pd.write([]string{"g"}, pd.target("g", 1, 1, 2))
		test := newTargetTestWithStorage(t, t.Context(), pd.store)
		test.checkGroupsAvailability()
		release := make(chan struct{})
		defer close(release)
		test.useWriter(func(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			if targets[0].Expected.ID == "h" {
				<-release
			}
			return pd.UpdateAffinityGroupTargets(ctx, targets)
		})

		blocking := test.target("h", 1, 1, 2)
		relearn := test.target("g", 2, 2, 3)
		test.observe("h", 1, 2)
		test.ExpireAffinityGroup("g")
		test.observe("g", 2, 3)
		test.observe("g", 1, 2)
		test.False(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
		test.Equal([]GroupTarget{blocking}, test.calls)
		release <- struct{}{}
		synctest.Wait()

		test.Equal([]GroupTarget{blocking, relearn}, test.calls)
		test.Equal(relearn.result(), pd.stored("g"))
	})
}

func TestFirstWinRecoversStoredTargetAfterWriteAttempt(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		attemptID string
		written   []string
		replyErr  error
		recovered bool
	}{
		{name: "unrelated failure", attemptID: "h", replyErr: errors.New("PD unavailable"), recovered: true},
		{name: "skipped", attemptID: "g", recovered: true},
		{name: "written", attemptID: "g", written: []string{"g"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t, "g", "h")
				test.write([]string{"g"}, test.target("g", 1, 1, 2))
				test.checkGroupsAvailability()
				test.ExpireAffinityGroup("g")
				test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
					return GroupTargetsResponse{WrittenGroupIDs: scenario.written}, scenario.replyErr
				})

				test.observe(scenario.attemptID, 2, 3)
				test.Len(test.calls, 1)
				test.observe("g", 1, 2)
				test.Equal(scenario.recovered, test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
				test.Len(test.calls, 1, "local recovery does not write to PD")
			})
		})
	}
}

func TestFirstWinRetainsStoredTargetInPatrolOrder(t *testing.T) {
	for _, scenario := range []struct {
		name         string
		secondBefore bool
		secondAfter  bool
		dropQueued   bool
		fullQueue    bool
		replyErr     error
		stayExpired  bool
	}{
		{name: "skipped"},
		{name: "stored before second", secondAfter: true},
		{name: "stored after second", secondBefore: true},
		{name: "writer not ready", secondBefore: true, replyErr: fmt.Errorf("PD leader is not found: %w", ErrTargetWriterNotReady)},
		{name: "HTTP 500", secondBefore: true, replyErr: errors.New("500 Internal Server Error"), stayExpired: true},
		{name: "unavailable queued candidate", dropQueued: true},
		{name: "full queue", fullQueue: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t, "g", "h")
				initial := test.target("g", 1, 1, 2)
				stored := initial.result()
				test.SyncGroupFromEtcdWithRevision(stored, 1)
				test.checkGroupsAvailability()
				test.ExpireAffinityGroup("g")
				release := make(chan struct{})
				defer close(release)
				test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
					if len(test.calls) == 1 {
						<-release
					}
					return GroupTargetsResponse{}, scenario.replyErr
				})

				first, second := test.target("g", 2, 2, 3), test.target("g", 3, 3, 4)
				first.ExpectedModRevision, second.ExpectedModRevision = 1, 1
				want := []GroupTarget{first}
				if scenario.dropQueued {
					want = []GroupTarget{test.target("h", 1, 1, 2)}
					test.observe("h", 1, 2)
				}
				test.observe("g", 2, 3)
				if scenario.secondBefore {
					test.observe("g", 3, 4)
				}
				unavailable := map[uint64]storeCondition{3: storeDown}
				if scenario.fullQueue {
					stores := test.storeSetInformer.(*core.StoresInfo)
					for i := range maxPendingTargetsPerGroup {
						id := uint64(i + 5)
						stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving},
							core.SetLastHeartbeatTS(time.Now())))
						test.observe("g", id, 1)
						unavailable[id] = storeDown
					}
				}
				test.observe("g", 1, 2)
				if scenario.secondAfter {
					test.observe("g", 3, 4)
				}
				if scenario.dropQueued || scenario.fullQueue {
					test.applyStoreConditions(unavailable)
				}
				test.False(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
				test.Equal(want, test.calls)
				release <- struct{}{}
				synctest.Wait()

				if scenario.secondBefore && scenario.replyErr == nil {
					test.False(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
					test.callsMu.Lock()
					calls := slices.Clone(test.calls)
					test.callsMu.Unlock()
					test.Equal(want, calls, "the earlier candidate must wait for the cooldown")
					time.Sleep(time.Second)
					synctest.Wait()
					want = append(want, second)
				}
				recovered := test.GetAffinityGroupState("g")
				test.Equal(!scenario.stayExpired, recovered.AffinitySchedulingAllowed)
				test.Equal(*stored, recovered.Group)
				time.Sleep(time.Second)
				synctest.Wait()
				test.Equal(want, test.calls)
			})
		})
	}
}

func TestFirstWinStoreRecoveryWaitsForReconciliation(t *testing.T) {
	t.Run("same peers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			test := newTargetTest(t, "g")
			test.write([]string{"g"}, test.target("g", 1, 1, 2))
			test.checkGroupsAvailability()
			test.applyStoreConditions(map[uint64]storeCondition{1: storeBusy})
			stored := test.stored("g")
			test.Lock()
			test.groups["g"].lastSentWritten = true
			test.Unlock()

			test.checkGroupsAvailability()
			test.Equal(groupDegraded, test.groups["g"].GetAvailability())
			test.SyncGroupFromEtcdWithRevision(stored, 2)
			test.Equal(groupAvailable, test.groups["g"].GetAvailability())
		})
	})

	synctest.Test(t, func(t *testing.T) {
		pd := newTargetTest(t, "g")
		pd.write([]string{"g"}, pd.target("g", 1, 1, 2))
		test := newTargetTestWithStorage(t, t.Context(), pd.store)
		test.checkGroupsAvailability()
		test.applyStoreConditions(map[uint64]storeCondition{1: storeBusy})
		test.Equal(groupDegraded, test.groups["g"].GetAvailability())
		test.useWriter(pd.UpdateAffinityGroupTargets)

		test.observe("g", 2, 1)
		persisted := pd.stored("g")
		test.Equal(uint64(2), persisted.LeaderStoreID)
		test.checkGroupsAvailability()
		test.Equal(groupDegraded, test.groups["g"].GetAvailability())
		test.Equal(uint64(1), test.GetAffinityGroupState("g").LeaderStoreID)

		test.SyncGroupFromEtcdWithRevision(persisted, 2)
		test.True(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
		test.Equal(uint64(2), test.GetAffinityGroupState("g").LeaderStoreID)
	})
}

func TestRemoteTargetConfirmationRevision(t *testing.T) {
	for _, path := range []string{"first-win", "balance"} {
		for _, event := range []string{"after reply", "put before reply", "delete before reply", "newer delete before reply"} {
			t.Run(path+"/"+event, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					test := newTargetTest(t)
					const id, confirmed = balanceTestGroupID, int64(13)
					initial := Group{ID: id, CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
					test.SyncGroupFromEtcdWithRevision(&initial, 10)
					test.checkGroupsAvailability()
					if path == "first-win" {
						test.ExpireAffinityGroup(id)
					}
					intermediate := initial
					intermediate.LeaderStoreID = 3
					test.useWriter(func(_ context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
						test.Len(targets, 1)
						test.Equal(int64(10), targets[0].ExpectedModRevision)
						switch event {
						case "put before reply":
							test.SyncGroupFromEtcdWithRevision(&intermediate, 11)
							test.True(test.GetAffinityGroupState(id).AffinitySchedulingAllowed)
						case "delete before reply":
							test.SyncGroupDeleteFromEtcdWithRevision(id, 11)
							test.Contains(test.GroupIDsAwaitingTargetRevision(), id)
						case "newer delete before reply":
							test.SyncGroupDeleteFromEtcdWithRevision(id, 14)
						}
						return GroupTargetsResponse{WrittenGroupIDs: []string{id}, WrittenRevisions: map[string]int64{id: confirmed}}, nil
					})
					if path == "first-win" {
						test.observe(id, 2, 1, 3)
					} else {
						written, err := test.CommitBalanceTargets(t.Context(), test.balanceSnapshot(), []*Group{test.groupTarget(id, 2, 1, 2, 3)})
						test.NoError(err)
						test.Equal([]string{id}, written)
					}
					test.Len(test.calls, 1)
					if event == "newer delete before reply" {
						test.Nil(test.GetAffinityGroupState(id))
						test.Empty(test.GroupIDsAwaitingTargetRevision())
						return
					}
					before := test.GetAffinityGroupState(id)
					test.SyncGroupFromEtcdWithRevision(&intermediate, 12)
					test.SyncGroupDeleteFromEtcdWithRevision(id, 12)
					test.Equal(before, test.GetAffinityGroupState(id))
					if before != nil {
						test.False(before.AffinitySchedulingAllowed)
						time.Sleep(time.Second)
						test.observe(id, 1, 2, 3)
						test.Len(test.calls, 1, "PD already confirmed the target")
					}
					test.SyncGroupFromEtcdWithRevision(test.calls[0].result(), confirmed)
					test.checkGroupsAvailability()
					current := test.GetAffinityGroupState(id)
					test.Equal(confirmed, current.modRevision)
					test.Equal(uint64(2), current.LeaderStoreID)
					test.True(current.AffinitySchedulingAllowed)
					test.SyncGroupDeleteFromEtcdWithRevision(id, confirmed+1)
					test.Nil(test.GetAffinityGroupState(id))
					test.Empty(test.GroupIDsAwaitingTargetRevision())
				})
			})
		}
	}
}

func TestFirstWinConfirmationWaitsForReconciliation(t *testing.T) {
	for _, mode := range []string{"new revision", "changed peers without revision", "replacement", "local update"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t, "g")
				initial := test.target("g", 1, 1, 2)
				stored := initial.result()
				test.SyncGroupFromEtcdWithRevision(stored, 1)
				test.checkGroupsAvailability()
				test.ExpireAffinityGroup("g")
				test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
					if len(test.calls) == 1 {
						return GroupTargetsResponse{WrittenGroupIDs: []string{"g"}}, nil
					}
					return GroupTargetsResponse{}, nil
				})
				test.observe("g", 2, 3)
				synctest.Sleep(time.Second + time.Millisecond)
				test.SyncGroupFromEtcd(stored)
				test.SyncGroupFromEtcdWithRevision(stored, 1)
				test.ExpireAffinityGroup("g") // Availability can change without reconciling metadata.

				test.observe("g", 3, 4)
				test.observe("g", 1, 2)
				test.False(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
				test.Len(test.calls, 1, "PD already confirmed a write for this group revision")

				persisted := test.calls[0].result()
				switch mode {
				case "new revision":
					test.SyncGroupFromEtcdWithRevision(persisted, 2)
				case "changed peers without revision":
					test.SyncGroupFromEtcd(persisted)
				case "replacement":
					persisted.CreateTimestamp++
					test.SyncGroupFromEtcdWithRevision(persisted, 2)
				case "local update":
					_, err := test.UpdateAffinityGroupPeers("g", persisted.LeaderStoreID, persisted.VoterStoreIDs)
					test.NoError(err)
				}
				test.ExpireAffinityGroup("g")
				test.observe("g", 2, 3)
				test.True(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
				test.Len(test.calls, 1, "the reconciled target recovers locally")
			})
		})
	}
}

func TestFirstWinDoesNotRestoreTargetWithPurgedVoter(t *testing.T) {
	for _, mode := range []string{"same target replay", "changed target"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t, "g")
				initial := test.target("g", 1, 1, 2)
				stored := initial.result()
				test.SyncGroupFromEtcdWithRevision(stored, 1)
				test.checkGroupsAvailability()
				stores := test.storeSetInformer.(*core.StoresInfo)
				stores.PutStore(stores.GetStore(2).Clone(core.SetStoreState(metapb.StoreState_Tombstone)))
				test.checkGroupsAvailability()
				test.Equal(groupExpired, test.groups["g"].GetAvailability())
				test.Equal(storeRemoved, test.unavailableStores[2])
				stores.DeleteStore(stores.GetStore(2))
				test.checkGroupsAvailability()
				test.NotContains(test.unavailableStores, uint64(2))

				if mode == "changed target" {
					stored.LeaderStoreID = 2
				}
				test.SyncGroupFromEtcdWithRevision(stored, 2)
				reverted := test.GetAffinityGroupState("g")
				test.Equal(*stored, reverted.Group)
				test.False(reverted.AffinitySchedulingAllowed)
				test.True(reverted.RegularSchedulingAllowed)
				test.Equal(groupExpired, test.groups["g"].GetAvailability())
			})
		})
	}
}

func TestFirstWinSkipsNewlyUnavailableQueuedTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pd := newTargetTest(t, "g", "h")
		test := newTargetTestWithStorage(t, t.Context(), pd.store)
		test.checkGroupsAvailability()
		release := make(chan struct{})
		defer close(release)
		test.useWriter(func(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			if targets[0].Expected.ID == "h" {
				<-release
			}
			return pd.UpdateAffinityGroupTargets(ctx, targets)
		})

		blocking := test.target("h", 2, 2, 3)
		healthy := test.target("g", 2, 2, 3)
		test.observe("h", 2, 3)
		test.observe("g", 1, 1, 2)
		test.observe("g", 2, 3)
		version := test.GetAffinityGroupState("g").affinityVer
		test.applyStoreConditions(map[uint64]storeCondition{1: storeDown})
		test.Equal(version, test.GetAffinityGroupState("g").affinityVer)
		release <- struct{}{}
		synctest.Wait()

		test.Equal([]GroupTarget{blocking, healthy}, test.calls)
		test.Equal(healthy.result(), pd.stored("g"))
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
				test.useWriter(func(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
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

func (test *targetTest) recordTargetAttempts(reply func(context.Context, []GroupTarget) (GroupTargetsResponse, error)) <-chan targetAttempt {
	attempts := make(chan targetAttempt, maxPendingTargetsPerGroup+2)
	test.useWriter(func(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
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
		attempts := test.recordTargetAttempts(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) { return GroupTargetsResponse{}, nil })
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

// Memory transactions do not track revisions, so supply the revision at each
// storage update to exercise the same checks as an etcd transaction.
type revisionedTargetStorage struct {
	endpoint.AffinityStorage
	revision int64
}

func (store *revisionedTargetStorage) LoadAffinityGroupInTxn(txn kv.Txn, id string) (string, int64, error) {
	value, _, err := store.AffinityStorage.LoadAffinityGroupInTxn(txn, id)
	return value, store.revision, err
}

func TestPolicyMismatchKeepsPendingGroupIndexed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pd := newTargetTest(t, "g", "blocker", "skipped")
		store := &revisionedTargetStorage{AffinityStorage: pd.store, revision: 1}
		pd.storage = store
		pd.applyStoreConditions(map[uint64]storeCondition{4: storeDown})
		test := newTargetTestWithStorage(t, t.Context(), pd.store)
		test.checkGroupsAvailability()
		for _, id := range []string{"g", "blocker", "skipped"} {
			test.SyncGroupFromEtcdWithRevision(pd.stored(id), 1)
		}

		release := make(chan struct{})
		defer close(release)
		var batches [][]GroupTarget
		var replies []GroupTargetsResponse
		var writeErrors []error
		test.useWriter(func(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			batches = append(batches, targets)
			if len(batches) <= 2 {
				<-release
			}
			reply, err := pd.UpdateAffinityGroupTargets(ctx, targets)
			replies = append(replies, reply)
			writeErrors = append(writeErrors, err)
			return reply, err
		})

		test.observe("blocker", 1, 2)
		test.observe("g", 1, 2)
		test.observe("skipped", 4, 3)
		release <- struct{}{}
		synctest.Wait()
		test.Len(batches, 2)
		test.Len(batches[1], 2)
		test.Equal("g", batches[1][0].Expected.ID)
		test.observe("g", 2, 3)

		_, err := pd.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
		test.NoError(err)
		store.revision++
		stored := pd.stored("g")
		// The ordinary skip in this batch gives the watch and new observations
		// a chance to run after the mismatch is handled and before the next pop.
		logCore, _ := observer.New(zapcore.WarnLevel)
		restore := log.ReplaceGlobals(zap.New(logCore, zap.Hooks(func(entry zapcore.Entry) error {
			if entry.Message == "skipped affinity group target writes" {
				test.SyncGroupFromEtcdWithRevision(stored, 2)
				state := test.GetAffinityGroupState("g")
				test.ObserveAvailableRegion(targetRegion(3, 2), state)
				test.ObserveAvailableRegion(targetRegion(2, 1), state)
			}
			return nil
		})), nil)
		defer restore()
		release <- struct{}{}
		synctest.Wait()

		test.Len(writeErrors, 3)
		test.NoError(writeErrors[2])
		test.Equal([]string{"g"}, replies[1].PolicyMismatchGroupIDs)
		test.Len(batches[2], 1)
		test.Equal(int64(2), batches[2][0].ExpectedModRevision)
		test.Equal([]string{"g"}, replies[2].WrittenGroupIDs)
		test.Equal(uint64(3), pd.stored("g").LeaderStoreID)
	})
}
