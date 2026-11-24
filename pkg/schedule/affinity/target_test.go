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
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newTargetTestManagerWithStorage(t *testing.T, ctx context.Context, store storage.Storage) *Manager {
	re := require.New(t)
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving},
			core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	m, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	return m
}

type targetTest struct {
	*require.Assertions
	*Manager
	store storage.Storage
	calls []GroupTarget
}

func newTargetTest(t *testing.T, ids ...string) *targetTest {
	return newTargetTestWithStorage(t, t.Context(), storage.NewStorageWithMemoryBackend(), ids...)
}

func newTargetTestWithStorage(t *testing.T, ctx context.Context, store storage.Storage, ids ...string) *targetTest {
	m := newTargetTestManagerWithStorage(t, ctx, store)
	test := &targetTest{Assertions: require.New(t), Manager: m, store: store}
	if len(ids) == 0 {
		return test
	}
	ranges := make([]GroupKeyRanges, len(ids))
	for index, id := range ids {
		ranges[index].GroupID = id
	}
	test.NoError(m.CreateAffinityGroups(ranges))
	return test
}

func (test *targetTest) state(id string) *GroupState {
	return test.GetAffinityGroupState(id)
}

func (test *targetTest) target(id string, leader uint64, voters ...uint64) GroupTarget {
	return GroupTarget{Expected: test.state(id).Group, LeaderStoreID: leader, VoterStoreIDs: voters}
}

func (test *targetTest) stored(id string) *Group {
	value, err := test.store.LoadAffinityGroup(id)
	test.NoError(err)
	group := &Group{}
	test.NoError(json.Unmarshal([]byte(value), group))
	return group
}

func (test *targetTest) save(group *Group) {
	test.NoError(test.store.RunInTxn(test.ctx, func(txn kv.Txn) error {
		return test.store.SaveAffinityGroup(txn, group.ID, group)
	}))
}

func (test *targetTest) write(want []string, targets ...GroupTarget) {
	written, err := test.UpdateAffinityGroupTargets(targets)
	test.NoError(err)
	if want == nil {
		test.Empty(written)
	} else {
		test.Equal(want, written)
	}
}

// setWriter records attempts for tests that synchronize with synctest.Wait.
func (test *targetTest) setWriter(reply func(GroupTarget) ([]string, error)) {
	test.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
		test.calls = append(test.calls, targets...)
		return reply(targets[0])
	}))
}

func unsupportedTarget(GroupTarget) ([]string, error) {
	return nil, ErrTargetWriterUnsupported
}

func (test *targetTest) observe(id string, region *core.RegionInfo) {
	test.ObserveAvailableRegion(region, test.state(id))
	synctest.Wait()
}

func (test *targetTest) mapRange(id string) {
	test.NoError(test.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))
}

func runTargetCase(t *testing.T, name string, run func(*testing.T)) {
	t.Run(name, func(t *testing.T) {
		synctest.Test(t, run)
	})
}

func targetRegion(voters ...uint64) *core.RegionInfo {
	return generateRegionForTest(10, voters, nonOverlappingRange)
}

func TestUpdateAffinityGroupTargetsValidation(t *testing.T) {
	test := newTargetTest(t, "g", "h")
	valid := test.target("g", 1, 1, 2)
	negativeRevision := valid
	negativeRevision.ExpectedModRevision = -1
	for name, targets := range map[string][]GroupTarget{
		"empty":             nil,
		"duplicate":         {valid, test.target("g", 2, 1, 2)},
		"leader not voter":  {test.target("g", 3, 1, 2)},
		"duplicate voters":  {test.target("g", 1, 1, 1, 2)},
		"missing leader":    {test.target("g", 0, 1, 2)},
		"unknown store":     {test.target("g", 9, 9)},
		"negative revision": {negativeRevision},
		"invalid second group": {test.target("h", 1, 1, 2),
			{Expected: Group{ID: "bad id"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := test.UpdateAffinityGroupTargets(targets)
			require.Error(t, err)
		})
	}
	for _, id := range []string{"g", "h"} {
		test.Zero(test.stored(id).LeaderStoreID, "validation must precede all writes")
		test.Zero(test.state(id).LeaderStoreID)
	}

	test.applyStoreConditions(map[uint64]storeCondition{4: storeDown})
	stale := valid
	stale.Expected.CreateTimestamp++
	for _, skipped := range []GroupTarget{
		{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}},
		stale,
		test.target("g", 1, 1, 4),
	} {
		test.write([]string{"h"}, skipped, test.target("h", 2, 3, 2, 1))
	}
	test.Zero(test.stored("g").LeaderStoreID)
	test.Equal([]uint64{1, 2, 3}, test.state("h").VoterStoreIDs)
}

func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, source := range []string{"watcher", "conflict", "written"} {
		t.Run(source, func(t *testing.T) {
			test := newTargetTest(t)
			old := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			test.SyncGroupFromEtcdWithRevision(&old, 1)
			test.mapRange("g")
			test.RestoreAffinityGroup("g")
			_, cached := test.GetAndCacheRegionAffinityGroupState(targetRegion(1, 2, 3))
			test.True(cached)

			recreated := old
			recreated.CreateTimestamp++
			test.save(&recreated)
			target := test.target("g", 2, 1, 2, 3)
			switch source {
			case "watcher":
				test.SyncGroupFromEtcdWithRevision(&recreated, 2)
			case "conflict":
				test.write(nil, target)
			case "written":
				target.Expected = recreated
				test.write([]string{"g"}, target)
				recreated.LeaderStoreID = 2
			}
			state := test.state("g")
			test.Equal(recreated, state.Group)
			test.Zero(state.RegionCount)
			test.Equal(1, state.RangeCount)
		})
	}
}

func TestFirstWinStoreConditions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		test := newTargetTest(t, "loaded", "empty")
		loaded := test.target("loaded", 1, 1, 2, 3)
		test.save(loaded.result())
		test.Manager = newTargetTestManagerWithStorage(t, t.Context(), test.store)
		test.False(test.storeConditionsKnown)
		test.False(test.state("loaded").AffinitySchedulingAllowed)
		test.setWriter(func(target GroupTarget) ([]string, error) {
			return test.UpdateAffinityGroupTargets([]GroupTarget{target})
		})
		region := targetRegion(2, 1, 3)
		test.observe("loaded", region)
		test.Empty(test.calls, "a loaded target waits for the first store check")
		test.Equal(*loaded.result(), *test.stored("loaded"))

		empty := test.target("empty", 2, 1, 2, 3)
		test.observe("empty", region)
		test.Equal([]GroupTarget{empty}, test.calls)
		test.Equal(*empty.result(), *test.stored("empty"))
		degraded := test.state("empty")
		test.Equal(*empty.result(), degraded.Group)
		test.Equal(groupDegraded, test.groups["empty"].GetAvailability())
		test.write([]string{"empty"}, empty)
		test.Equal(degraded, test.state("empty"))
		test.checkGroupsAvailability()
		test.True(test.state("loaded").AffinitySchedulingAllowed)
		test.True(test.state("empty").AffinitySchedulingAllowed)

		test.DegradeAffinityGroup("empty")
		version := test.state("empty").affinityVer
		test.write([]string{"empty"}, empty)
		test.Equal(version+1, test.state("empty").affinityVer)
		test.True(test.state("empty").AffinitySchedulingAllowed)

		for _, voter := range []uint64{4, 3} {
			test.ExpireAffinityGroup("loaded")
			test.applyStoreConditions(map[uint64]storeCondition{3: storeRemoving})
			loaded.VoterStoreIDs = []uint64{1, 2, voter}
			test.SyncGroupFromEtcd(loaded.result())
			test.Equal(voter != 3, test.state("loaded").AffinitySchedulingAllowed)
			test.Equal(voter == 3, test.state("loaded").RegularSchedulingAllowed)
		}
		loaded.Expected.CreateTimestamp++
		loaded.VoterStoreIDs = []uint64{1, 2, 4}
		test.SyncGroupFromEtcd(loaded.result())
		test.True(test.state("loaded").AffinitySchedulingAllowed)
	})
}

func TestSyncTargetDegradedWindow(t *testing.T) {
	for _, tc := range []struct {
		name      string
		condition storeCondition
		local     bool
		conflict  bool
	}{
		{name: "watcher updates expired target", condition: storeBusy},
		{name: "CAS reconciles expired target", condition: storeBusy, conflict: true},
		{name: "watcher confirms expired local target", condition: storeDown, local: true},
		{name: "watcher confirms degraded local target", condition: storeDisconnected, local: true},
	} {
		runTargetCase(t, tc.name, func(t *testing.T) {
			test := newTargetTest(t, "g")
			test.checkGroupsAvailability()
			target := test.target("g", 1, 1, 2)
			test.setWriter(unsupportedTarget)
			if tc.local {
				test.observe("g", targetRegion(1, 2))
				test.assertActive(target.result(), true)
				time.Sleep(time.Second)
			} else {
				test.SyncGroupFromEtcdWithRevision(target.result(), 1)
				target.LeaderStoreID = 2
			}

			test.applyStoreConditions(map[uint64]storeCondition{2: tc.condition})
			if tc.condition == storeDown {
				test.applyStoreConditions(nil)
			}
			if !tc.local {
				test.ExpireAffinityGroup("g")
			}
			if tc.condition == storeDisconnected {
				test.Equal(groupDegraded, test.groups["g"].GetAvailability())
			} else {
				test.Equal(groupExpired, test.groups["g"].GetAvailability())
			}
			stored := target.result()
			test.save(stored)
			if tc.conflict {
				test.write(nil, test.target("g", 1, 1, 3))
			} else {
				test.SyncGroupFromEtcdWithRevision(stored, 2)
			}

			state := test.state("g")
			test.Equal(*stored, state.Group)
			test.False(state.HasUnpersistedTarget())
			test.False(state.RegularSchedulingAllowed)
			test.Equal(tc.condition == storeDown, state.AffinitySchedulingAllowed)
			deadline := newDegradedExpiredAtFromNow()
			test.Equal(deadline, test.groups["g"].degradedExpiredAt)
			calls := len(test.calls)
			test.observe("g", targetRegion(1, 3))
			test.Len(test.calls, calls)
			test.Equal(stored, test.stored("g"))
			time.Sleep(time.Second)
			test.SyncGroupFromEtcdWithRevision(stored, 3)
			test.Equal(state, test.state("g"))
			test.Equal(deadline, test.groups["g"].degradedExpiredAt)
			if tc.conflict {
				written, err := test.updateGroupTarget(target)
				test.NoError(err)
				test.True(written)
				test.Equal(deadline, test.groups["g"].degradedExpiredAt)
			}
		})
	}
}

func TestStoredGroupFields(t *testing.T) {
	base := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	for _, tc := range []struct {
		group Group
		match bool
	}{
		{group: Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{2, 1}}, match: true},
		{group: Group{ID: "h", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}},
		{group: Group{ID: "g", CreateTimestamp: 2, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}},
		{group: Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}},
		{group: Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 3}}},
	} {
		require.Equal(t, tc.match, sameStoredGroup(&base, &tc.group), "%+v", tc.group)
	}
}

type targetWriterFunc func(context.Context, []GroupTarget) ([]string, error)

func (f targetWriterFunc) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	return f(ctx, targets)
}

func TestFirstWinSingleFlightAndGlobalBackoff(t *testing.T) {
	for _, tc := range []struct {
		name    string
		written []string
		err     error
		counted bool
	}{
		{name: "failed", err: errors.New("PD unavailable"), counted: true},
		{name: "not written", written: []string{"another-group"}, counted: true},
		{name: "not ready", err: errors.Annotate(ErrTargetWriterNotReady, "PD leader is not found")},
	} {
		runTargetCase(t, tc.name, func(t *testing.T) {
			test := newTargetTest(t, "g", "h")
			region := targetRegion(1, 2)
			failures := testutil.ToFloat64(targetWriteFailureCounter)
			release := make(chan struct{})
			defer close(release)
			test.setWriter(func(target GroupTarget) ([]string, error) {
				if len(test.calls) != 1 {
					return []string{target.Expected.ID}, nil
				}
				<-release
				return tc.written, tc.err
			})
			g, h := test.target("g", 1, 1, 2), test.target("h", 1, 1, 2)
			test.observe("g", region)
			for range 10 {
				test.ObserveAvailableRegion(region, test.state("g"))
				test.ObserveAvailableRegion(region, test.state("h"))
			}
			synctest.Wait()
			test.Equal([]GroupTarget{g}, test.calls, "busy observations are discarded")
			release <- struct{}{}
			synctest.Wait()
			test.observe("h", region)
			test.Len(test.calls, 1, "failure delays all groups")
			if tc.counted {
				failures++
			}
			test.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))

			time.Sleep(time.Second)
			test.observe("h", region)
			test.Equal([]GroupTarget{g, h}, test.calls)
			test.Zero(test.state("h").LeaderStoreID, "HTTP success waits for the watcher")
			test.Zero(test.stored("h").LeaderStoreID)
			test.applyStoreConditions(map[uint64]storeCondition{2: storeRemoving})
			test.SyncGroupFromEtcd(h.result())
			test.False(test.state("h").AffinitySchedulingAllowed)
			test.True(test.state("h").RegularSchedulingAllowed)
			test.applyStoreConditions(nil)
			test.Zero(test.state("g").LeaderStoreID, "discarded observations are never replayed")
			test.observe("g", region)
			test.Equal([]GroupTarget{g, h, g}, test.calls, "success has no cooldown")
			test.SyncGroupFromEtcd(g.result())
			test.DegradeAffinityGroup("g")
			test.observe("g", region)
			test.Len(test.calls, 3, "a matching Region confirms the target without writing")
			test.True(test.state("g").AffinitySchedulingAllowed)
		})
	}
}

func TestFirstWinCancelsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		test := newTargetTestWithStorage(t, ctx, storage.NewStorageWithMemoryBackend(), "g")
		stopped := false
		test.SetTargetWriter(targetWriterFunc(func(ctx context.Context, targets []GroupTarget) ([]string, error) {
			test.calls = append(test.calls, targets...)
			<-ctx.Done()
			stopped = true
			return nil, ctx.Err()
		}))
		region := targetRegion(1, 2)
		test.observe("g", region)
		test.Len(test.calls, 1)
		cancel()
		synctest.Wait()
		test.True(stopped)
		test.observe("g", region)
		test.Len(test.calls, 1)
	})
}

func TestFirstWinLateReply(t *testing.T) {
	for _, tc := range []struct {
		name      string
		condition storeCondition
	}{
		{name: "success behind watcher"},
		{name: "404 behind admin update"},
		{name: "expires during initialization"},
		{name: "expires during retry"},
		{name: "recreated in the same second"},
		{name: "same peers with healthy stores"},
		{name: "same peers with store down"},
		{name: "disconnected", condition: storeDisconnected},
		{name: "low space", condition: storeLowSpace},
		{name: "evicted leader", condition: storeLeaderEvicted},
		{name: "down", condition: storeDown},
	} {
		runTargetCase(t, tc.name, func(t *testing.T) {
			test := newTargetTest(t)
			group := Group{ID: "g", CreateTimestamp: 1}
			test.SyncGroupFromEtcd(&group)
			region := targetRegion(1, 2)
			if tc.condition != storeAvailable {
				test.checkGroupsAvailability()
			}
			switch tc.name {
			case "same peers with healthy stores", "same peers with store down":
				group.LeaderStoreID, group.VoterStoreIDs = 2, []uint64{1, 2}
				test.SyncGroupFromEtcdWithRevision(&group, 1)
				test.applyStoreConditions(map[uint64]storeCondition{2: storeDown})
				region = targetRegion(1, 3)
			case "expires during initialization", "expires during retry":
				region = targetRegion(1, 2, 3)
			}
			release := make(chan struct{})
			defer close(release)
			attempt := 1
			if tc.name == "expires during retry" {
				attempt++
			}
			test.setWriter(func(GroupTarget) ([]string, error) {
				if len(test.calls) == attempt {
					<-release
				}
				if tc.name == "success behind watcher" {
					return []string{"g"}, nil
				}
				return nil, ErrTargetWriterUnsupported
			})
			if attempt == 2 {
				test.observe("g", region)
				test.Len(test.calls, 1)
				test.assertActive(test.calls[0].result(), true)
				time.Sleep(time.Second)
			}
			before := test.state("g")
			test.observe("g", region)
			test.Len(test.calls, attempt)
			test.True(test.targetWriteInFlight)

			switch tc.name {
			case "success behind watcher", "404 behind admin update":
				group.LeaderStoreID, group.VoterStoreIDs = 2, []uint64{1, 2}
				test.SyncGroupFromEtcd(&group)
				test.Equal(group, test.state("g").Group)
				test.False(test.state("g").HasUnpersistedTarget())
			case "expires during initialization", "expires during retry":
				test.ExpireAffinityGroup("g")
				test.True(test.state("g").RegularSchedulingAllowed)
				test.False(test.state("g").AffinitySchedulingAllowed)
			case "recreated in the same second":
				test.SyncGroupDeleteFromEtcd("g")
				test.SyncGroupFromEtcd(&group)
				test.NotSame(before.groupInfoPtr, test.state("g").groupInfoPtr)
				test.Equal(before.Group, test.state("g").Group)
				test.Equal(before.affinityVer, test.state("g").affinityVer)
			case "same peers with healthy stores", "same peers with store down":
				healthy := tc.name == "same peers with healthy stores"
				if healthy {
					test.applyStoreConditions(nil)
				}
				test.SyncGroupFromEtcdWithRevision(&group, 2)
				test.Equal(int64(2), test.groupModRevisions["g"])
				if healthy {
					test.assertActive(&group, false)
					test.Greater(test.state("g").affinityVer, before.affinityVer)
				} else {
					test.Equal(before, test.state("g"))
				}
			default:
				test.applyStoreConditions(map[uint64]storeCondition{1: tc.condition})
				test.Equal(before, test.state("g"), "an unset group has no voters to invalidate")
			}
			want := test.state("g")
			release <- struct{}{}
			synctest.Wait()
			test.Equal(want, test.state("g"), "the late reply must preserve the newer state")

			switch tc.name {
			case "expires during initialization", "expires during retry", "recreated in the same second":
				follower := uint64(1)
				if tc.name == "recreated in the same second" {
					test.ObserveAvailableRegion(region, before)
					test.Equal(want, test.state("g"))
					follower = 3
				}
				voters := []uint64{2, follower}
				slices.Sort(voters)
				replacement := test.target("g", 2, voters...)
				test.observe("g", targetRegion(2, follower))
				test.assertActive(replacement.result(), true)
				test.Len(test.calls, attempt, "a fresh observation applies during the 404 cooldown")
			}
		})
	}
}

func TestFirstWinReconcilesLocalTarget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		condition storeCondition
		upgrade   bool
	}{
		{name: "initialize", upgrade: true},
		{name: "replace leader", condition: storeLeaderEvicted, upgrade: true},
		{name: "replace voters", condition: storeDown, upgrade: true},
		{name: "admin update", condition: storeLeaderEvicted},
		{name: "admin reassertion", condition: storeLeaderEvicted},
		{name: "recreation", condition: storeLeaderEvicted},
		{name: "matching observation", condition: storeLeaderEvicted},
		{name: "admin before retry"},
	} {
		runTargetCase(t, tc.name, func(t *testing.T) {
			pd, test := newTargetTest(t, "g", "h"), newTargetTest(t)
			voters := []uint64{1, 2}
			revision := int64(0)
			if tc.condition != storeAvailable {
				setTargetPeers(pd, "g", 2, 1, 2)
				test.applyStoreConditions(map[uint64]storeCondition{2: tc.condition})
			}
			if tc.condition == storeDown {
				voters = []uint64{1, 3}
			}
			if tc.name == "admin reassertion" {
				revision = 1
			}
			stored := pd.stored("g")
			test.SyncGroupFromEtcdWithRevision(stored, revision)
			test.mapRange("g")
			test.False(test.state("g").AffinitySchedulingAllowed)
			test.Equal(tc.condition != storeLeaderEvicted, test.state("g").RegularSchedulingAllowed)

			failures := testutil.ToFloat64(targetWriteFailureCounter)
			test.setWriter(func(GroupTarget) ([]string, error) {
				return nil, errors.Annotate(ErrTargetWriterUnsupported, "404 Not Found")
			})
			region := targetRegion(voters...)
			target := GroupTarget{Expected: *stored, ExpectedModRevision: revision, LeaderStoreID: 1, VoterStoreIDs: voters}
			test.observe("g", region)
			test.Equal([]GroupTarget{target}, test.calls)
			test.assertActive(target.result(), true)
			test.Equal(stored, pd.stored("g"))
			test.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))
			_, cached := test.GetAndCacheRegionAffinityGroupState(region)
			test.True(cached)
			local := test.state("g")
			test.Equal(PhaseStable, local.Phase)
			test.Equal(1, local.AffinityRegionCount)
			test.SyncGroupFromEtcdWithRevision(stored, revision)
			test.Equal(local, test.state("g"), "reloading the same revision preserves the replacement")

			want := *stored
			switch {
			case tc.upgrade:
				testUpgradeLocalTargets(test, pd, target, region, tc.condition == storeAvailable)
				want = *target.result()
				test.Equal(local.affinityVer, test.state("g").affinityVer)
				testDuplicateTarget(test, &want, region)
				test.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))
			case tc.name == "matching observation":
				test.applyStoreConditions(map[uint64]storeCondition{1: storeLeaderEvicted})
				test.observe("g", targetRegion(2, 1))
			case tc.name == "admin reassertion":
				test.applyStoreConditions(nil)
				test.SyncGroupFromEtcdWithRevision(&want, 2)
				_, cached = test.GetRegionAffinityGroupState(region)
				test.False(cached, "the cache follows the reasserted peers")
				reasserted := test.state("g")
				test.Greater(reasserted.affinityVer, local.affinityVer)
				test.Zero(reasserted.AffinityRegionCount)
				time.Sleep(time.Second)
				test.ObserveAvailableRegion(region, local)
				test.observe("g", region)
				test.SyncGroupFromEtcdWithRevision(target.result(), 1)
				test.Equal(reasserted, test.state("g"), "older revisions cannot change reasserted peers")
			case tc.name == "recreation":
				test.applyStoreConditions(nil)
				want.CreateTimestamp++
				test.SyncGroupFromEtcdWithRevision(&want, 2)
				test.NotSame(local.groupInfoPtr, test.state("g").groupInfoPtr)
			default:
				if tc.name == "admin before retry" {
					setTargetPeers(pd, "g", 4, 2, 4)
					test.applyStoreConditions(nil)
					testRetryLocalTarget(test, pd, target, region, nil)
				} else {
					setTargetPeers(pd, "g", 3, 1, 3)
				}
				want = *pd.stored("g")
				test.SyncGroupFromEtcdWithRevision(&want, 2)
			}
			test.assertActive(&want, false)
			if !tc.upgrade && tc.name != "admin before retry" {
				test.Len(test.calls, 1)
			}
		})
	}
}

func testUpgradeLocalTargets(test, pd *targetTest, target GroupTarget, region *core.RegionInfo, initialize bool) {
	local := test.state("g")
	test.observe("g", targetRegion(4, 1))
	test.Equal(local, test.state("g"))
	time.Sleep(time.Second)
	test.observe("g", region)
	test.Equal([]GroupTarget{target, target}, test.calls)
	test.Equal(local, test.state("g"), "another 404 keeps the active target and cache")
	if initialize {
		testCooldownLocalTargets(test, pd)
	}
	testRetryLocalTarget(test, pd, target, region, []string{"g"})
	persisted := pd.stored("g")
	test.Equal(target.result(), persisted)
	test.SyncGroupFromEtcdWithRevision(persisted, 2)
	if initialize {
		region := targetRegion(3, 2)
		second := GroupTarget{Expected: *pd.stored("h"), LeaderStoreID: 3, VoterStoreIDs: []uint64{2, 3}}
		test.observe("h", region)
		test.Equal([]GroupTarget{target, target, target, second}, test.calls, "a successful probe lets another group persist immediately")
		test.Equal(second.result(), pd.stored("h"))
		test.SyncGroupFromEtcd(second.result())
		test.assertActive(second.result(), false)
		test.observe("h", region)
		test.Len(test.calls, 4)
	}
}

func setTargetPeers(test *targetTest, id string, leader uint64, voters ...uint64) {
	_, err := test.UpdateAffinityGroupPeers(id, leader, voters)
	test.NoError(err)
}

func (test *targetTest) assertActive(group *Group, unpersisted bool) {
	state := test.state(group.ID)
	test.Equal(*group, state.Group)
	test.True(state.AffinitySchedulingAllowed)
	test.False(state.RegularSchedulingAllowed)
	test.Equal(unpersisted, state.HasUnpersistedTarget())
}

func testRetryLocalTarget(test, pd *targetTest, target GroupTarget, region *core.RegionInfo, want []string) {
	var written []string
	var err error
	test.setWriter(func(target GroupTarget) ([]string, error) {
		written, err = pd.UpdateAffinityGroupTargets([]GroupTarget{target})
		return written, err
	})
	requests := append(slices.Clone(test.calls), target)
	time.Sleep(time.Second)
	test.observe("g", region)
	test.Equal(requests, test.calls)
	test.NoError(err)
	test.ElementsMatch(want, written)
}

func testCooldownLocalTargets(test, pd *targetTest) {
	created := pd.stored("h")
	test.SyncGroupFromEtcd(created)
	observeLocal := func(leader, follower uint64) {
		test.observe("h", targetRegion(leader, follower))
		voters := []uint64{leader, follower}
		slices.Sort(voters)
		target := GroupTarget{Expected: *created, LeaderStoreID: leader, VoterStoreIDs: voters}
		test.assertActive(target.result(), true)
		test.Len(test.calls, 2, "the 404 cooldown suppresses requests")
	}
	observeLocal(2, 1)
	test.Zero(pd.stored("h").LeaderStoreID)
	test.applyStoreConditions(map[uint64]storeCondition{2: storeLeaderEvicted})
	test.observe("h", targetRegion(3, 1))
	test.Equal([]uint64{1, 2}, test.state("h").VoterStoreIDs, "degraded targets keep their voters")
	observeLocal(1, 2)
	test.ExpireAffinityGroup("h")
	observeLocal(3, 2)
}

func testDuplicateTarget(test *targetTest, stored *Group, region *core.RegionInfo) {
	stable := test.state("g")
	test.False(stable.HasUnpersistedTarget())
	test.Equal(PhaseStable, stable.Phase)
	deadline := test.groups["g"].degradedExpiredAt
	time.Sleep(time.Second)
	test.SyncGroupFromEtcdWithRevision(stored, 4)
	test.True(test.groups["g"].IsAvailable())
	test.Equal(deadline, test.groups["g"].degradedExpiredAt)
	stale := *stored
	stale.LeaderStoreID = 2
	test.SyncGroupFromEtcdWithRevision(&stale, 3)
	test.Equal(stable, test.state("g"), "duplicate PUT preserves the stable group and advances its revision")
	before := len(test.calls)
	test.observe("g", region)
	test.Len(test.calls, before)
}

type targetReplyStorage struct {
	endpoint.AffinityStorage
	beforeCommit func()
	loseReply    bool
}

func (s *targetReplyStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	err := s.AffinityStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			s.beforeCommit()
		}
		return nil
	})
	if err == nil && s.loseReply {
		return errors.New("committed reply lost")
	}
	return err
}

func TestTargetCASAndLostReplyWithEtcd(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	test := newTargetTestWithStorage(t, t.Context(), store, "g", "h")
	other := newTargetTestManagerWithStorage(t, t.Context(), store)
	adminWrite := func(leader uint64) {
		_, err := other.UpdateAffinityGroupPeers("g", leader, []uint64{1, 2, 3})
		test.NoError(err)
	}
	read := func() *mvccpb.KeyValue {
		resp, err := client.Get(t.Context(), keypath.AffinityGroupPath("g"))
		test.NoError(err)
		test.Len(resp.Kvs, 1)
		return resp.Kvs[0]
	}
	stale := test.target("g", 1, 1, 2, 3)
	test.applyStoreConditions(nil)
	before := test.state("h").affinityVer
	commits := 0
	wrapped := &targetReplyStorage{AffinityStorage: store, beforeCommit: func() {
		commits++
		if commits == 2 {
			adminWrite(2)
		}
	}}
	test.storage = wrapped
	written, err := test.UpdateAffinityGroupTargets([]GroupTarget{test.target("h", 1, 1, 2, 3), stale})
	test.Error(err, "another Manager wrote after the read, before commit")
	test.Equal([]string{"h"}, written)
	test.Equal(uint64(1), test.stored("h").LeaderStoreID, "earlier commits survive a later error")
	test.Equal(before+1, test.state("h").affinityVer)
	test.Equal(uint64(2), test.stored("g").LeaderStoreID)
	test.Zero(test.state("g").LeaderStoreID)

	wrapped.beforeCommit = nil
	test.applyStoreConditions(map[uint64]storeCondition{2: storeLeaderEvicted})
	test.write(nil, stale)
	test.Equal(uint64(2), test.state("g").LeaderStoreID)
	test.False(test.state("g").AffinitySchedulingAllowed)
	test.applyStoreConditions(nil)

	target := test.target("g", 3, 1, 2, 3)
	original := read()
	target.ExpectedModRevision = original.ModRevision
	adminWrite(1)
	adminWrite(2)
	restored := read()
	test.Equal(original.Value, restored.Value)
	test.Greater(restored.ModRevision, original.ModRevision)
	test.write(nil, target)
	test.Equal(restored.ModRevision, read().ModRevision, "a stale revision must reject ABA even when the group matches")

	target.ExpectedModRevision = restored.ModRevision
	wrapped.loseReply = true
	_, err = test.UpdateAffinityGroupTargets([]GroupTarget{target})
	test.ErrorContains(err, "committed reply lost")
	test.Equal(uint64(2), test.state("g").LeaderStoreID)
	test.Equal(uint64(3), test.stored("g").LeaderStoreID)
	wrapped.loseReply = false
	beforeRetry := read()
	target.Expected.VoterStoreIDs = []uint64{3, 2, 1}
	test.write([]string{"g"}, target)
	test.Equal(*test.stored("g"), test.state("g").Group)
	test.Equal(beforeRetry.ModRevision, read().ModRevision, "idempotent retries do not put again")
}
