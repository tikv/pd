// Copyright 2025 TiKV Project Authors.
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

package checker

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	scheconfig "github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	"github.com/tikv/pd/server/config"
)

type targetWriterFunc func(context.Context, []affinity.GroupTarget) (affinity.GroupTargetsResponse, error)

func (f targetWriterFunc) WriteGroupTargets(ctx context.Context, targets []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
	return f(ctx, targets)
}

func newAffinityTestOptions() *config.PersistOptions {
	opt := mockconfig.NewTestOptions()
	cfg := opt.GetScheduleConfig().Clone()
	cfg.AffinityScheduleLimit = 4 // Set affinity schedule limit to enable affinity scheduling
	opt.SetScheduleConfig(cfg)
	return opt
}

func newTestAffinityChecker(ctx context.Context, cluster sche.CheckerCluster, conf scheconfig.CheckerConfigProvider) *AffinityChecker {
	checker := NewAffinityChecker(ctx, cluster, conf)
	// Set startTime to 2 minutes ago to bypass startup TTL check
	checker.startTime = time.Now().Add(-2 * time.Minute)
	return checker
}

// createAffinityGroupForTest creates an affinity group with the given ranges and peers.
func createAffinityGroupForTest(manager *affinity.Manager, group *affinity.Group, ranges ...keyutil.KeyRange) error {
	keyRanges := []affinity.GroupKeyRanges{{GroupID: group.ID, KeyRanges: ranges}}

	if err := manager.CreateAffinityGroups(keyRanges); err != nil {
		return err
	}
	if group.LeaderStoreID != 0 || len(group.VoterStoreIDs) > 0 {
		_, err := manager.UpdateAffinityGroupPeers(group.ID, group.LeaderStoreID, group.VoterStoreIDs)
		return err
	}
	return nil
}

// deleteAffinityGroupForTest is a test helper that deletes an affinity group.
func deleteAffinityGroupForTest(manager *affinity.Manager, groupID string, force bool) error {
	return manager.DeleteAffinityGroups([]string{groupID}, force)
}

func addAffinityTestRegion(tc *mockcluster.Cluster, regionID, leaderStoreID uint64, followers ...uint64) {
	region := tc.AddLeaderRegion(regionID, leaderStoreID, followers...)
	if kerneltype.IsNextGen() {
		tc.PutRegion(region.Clone(withAffinityTestRange(string(region.GetStartKey()), string(region.GetEndKey()))))
	}
}

func affinityTestRange(start, end string) keyutil.KeyRange {
	return testutil.TxnKeyspaceRange([]byte(start), []byte(end))
}

func withAffinityTestRange(start, end string) core.RegionCreateOption {
	r := affinityTestRange(start, end)
	return func(region *core.RegionInfo) {
		core.WithStartKey(r.StartKey)(region)
		core.WithEndKey(r.EndKey)(region)
	}
}

func TestAffinityCheckerInvalidCache(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group
	err := createAffinityGroupForTest(affinityManager, &affinity.Group{ID: "test_group"}, affinityTestRange("", ""))
	re.NoError(err)

	addAffinityTestRegion(tc, 100, 1, 2, 3)
	addAffinityTestRegion(tc, 101, 4, 2, 3)
	region1 := tc.GetRegion(100)
	region2 := tc.GetRegion(101)
	regionMerge := region1.Clone(core.WithEndKey(region2.GetEndKey()), core.WithIncVersion(), core.WithIncConfVer())

	// Check Region 1 first
	checker.Check(region1)
	group := affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(group)
	re.Equal(1, group.RegionCount)
	re.Equal(1, group.AffinityRegionCount)

	// Merge Region 1 and 2 before Region 2 is checked
	tc.PutRegion(regionMerge)

	// In this case, checking Region 2 will not increase the counter.
	checker.Check(region2)
	group = affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(group)
	re.Equal(1, group.RegionCount)
	re.Equal(1, group.AffinityRegionCount)
}

func TestAffinityCheckerTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	addAffinityTestRegion(tc, 1, 1, 2, 3) // Leader on store 1

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group with leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Check should create transfer leader operator
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
	re.Equal(operator.OpAffinity, ops[0].SchedulerKind()) // Not OpLeader
}

func TestAffinityCheckerMovePeer(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	addAffinityTestRegion(tc, 1, 1, 2, 4) // Peers on 1, 2, 4

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group expecting peers on 1, 2, 3
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Check should create move peer operator (from 4 to 3)
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
	re.Equal(operator.OpAffinity, ops[0].SchedulerKind()) // Not OpRegion
}

func TestAffinityCheckerPaused(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	addAffinityTestRegion(tc, 1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group with leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Pause the checker (pause for 60 seconds)
	checker.PauseOrResume(60)

	// Check should return nil when paused
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)

	// Resume the checker (pause for 0 seconds)
	checker.PauseOrResume(0)

	// Now should create operator
	ops = checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

// TestAffinityCheckerGroupState tests the full flow:
// Manager detects unhealthy store -> invalidates group -> checker skips operators -> store recovers -> checker creates operators
func TestAffinityCheckerGroupState(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	addAffinityTestRegion(tc, 100, 4, 2, 3)
	addAffinityTestRegion(tc, 200, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	affinityChecker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group with expected leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Verify group is in effect initially
	groupInfo := affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(groupInfo)
	re.True(groupInfo.AffinitySchedulingAllowed)
	re.Equal(affinity.PhasePreparing, groupInfo.Phase)

	// For cases where the Region and Group voter store IDs do not fully overlap.
	// Checker should create operator for move peer (4 -> 1) and leader transfer (4 -> 2)
	ops := affinityChecker.Check(tc.GetRegion(100))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	// Simulate health check invalidating the group
	affinityManager.DegradeAffinityGroup("test_group")
	// Checker should NOT create operator when group is degraded
	ops = affinityChecker.Check(tc.GetRegion(100))
	re.Nil(ops, "Checker should not create operator for degraded group")
	// Simulate health check restoring the group
	affinityManager.RestoreAffinityGroup("test_group")
	// Checker should create operator again after group is restored
	ops = affinityChecker.Check(tc.GetRegion(100))
	re.NotNil(ops, "Checker should create operator for restored group")
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())

	// For cases where the Region and Group voter store IDs exactly match.
	ops = affinityChecker.Check(tc.GetRegion(200))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	// Simulate health check invalidating the group
	affinityManager.DegradeAffinityGroup("test_group")
	// If the voter store IDs remain unchanged, a valid Region will cause the Group to restore.
	ops = affinityChecker.Check(tc.GetRegion(200))
	re.True(affinityManager.GetAffinityGroupState("test_group").AffinitySchedulingAllowed)
	re.Nil(ops, "Checker should not create operator for same peers")
}

func TestAffinityCheckerPersistsUnpersistedTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		opt := newAffinityTestOptions()
		cluster := mockcluster.NewCluster(t.Context(), opt)
		for id := uint64(1); id <= 3; id++ {
			cluster.AddRegionStore(id, 10)
		}
		keyRange := testutil.TxnKeyspaceRange([]byte("a"), []byte("z"))
		cluster.AddLeaderRegionWithRange(100, string(keyRange.StartKey), string(keyRange.EndKey), 1, 2, 3)
		region := cluster.GetRegion(100)
		manager := cluster.GetAffinityManager()
		re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{keyRange}}}))
		created := manager.GetAffinityGroupState("g")
		var requests []affinity.GroupTarget
		manager.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
			requests = append(requests, targets...)
			if len(requests) == 1 {
				return affinity.GroupTargetsResponse{}, affinity.ErrTargetWriterUnsupported
			}
			return affinity.GroupTargetsResponse{WrittenGroupIDs: []string{"g"}}, nil
		}))
		manager.ObserveAvailableRegion(region, created)
		synctest.Wait()
		local := manager.GetAffinityGroupState("g")
		re.True(local.HasUnpersistedTarget())
		re.True(local.AffinitySchedulingAllowed)
		re.Len(requests, 1)

		synctest.Sleep(time.Second)
		re.Empty(newTestAffinityChecker(t.Context(), cluster, opt).Check(region))
		synctest.Wait()
		want := affinity.GroupTarget{Expected: created.Group, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
		re.Equal([]affinity.GroupTarget{want, want}, requests)
	})
}

// TestAffinityAvailabilityCheckWithOfflineStore tests that groups are invalidated when stores go offline.
func TestAffinityAvailabilityCheckWithOfflineStore(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"))
	}()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	addAffinityTestRegion(tc, 1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Verify group is in effect
	groupInfo := affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(groupInfo)
	re.True(groupInfo.AffinitySchedulingAllowed)
	re.Equal(affinity.PhasePreparing, groupInfo.Phase)

	// Set store 2 offline (this triggers IsRemoving())
	tc.SetStoreOffline(2)

	// Wait for health check to run
	testutil.Eventually(re, func() bool {
		groupInfo = affinityManager.GetAffinityGroupState("test_group")
		return groupInfo != nil && !groupInfo.AffinitySchedulingAllowed
	})

	// Group should be invalidated because store 2 is removing
	re.NotNil(groupInfo)
	re.False(groupInfo.AffinitySchedulingAllowed, "Group should be invalidated when store is removing")
	re.Equal(affinity.PhasePending, groupInfo.Phase)
}

// TestAffinityAvailabilityCheckWithUnhealthyStores tests behavior when stores go unhealthy.
func TestAffinityAvailabilityCheckWithUnhealthyStores(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()

		re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval", "return(true)"))
		defer func() {
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"))
		}()

		opt := newAffinityTestOptions()
		tc := mockcluster.NewCluster(ctx, opt)

		tc.AddRegionStore(1, 10)
		tc.AddRegionStore(2, 10)
		tc.AddRegionStore(3, 10)
		addAffinityTestRegion(tc, 1, 1, 2, 3)

		affinityManager := tc.GetAffinityManager()

		group := &affinity.Group{
			ID:            "test_group",
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 2, 3},
		}
		err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
		re.NoError(err)

		// Set stores 2 and 3 disconnect
		tc.SetStoreUp(1)
		tc.SetStoreDisconnect(2)
		tc.SetStoreDisconnect(3)

		// Wait for health check
		var groupInfo *affinity.GroupState
		testutil.Eventually(re, func() bool {
			groupInfo = affinityManager.GetAffinityGroupState("test_group")
			return groupInfo != nil && !groupInfo.AffinitySchedulingAllowed
		})

		// Group should be invalidated
		re.NotNil(groupInfo)
		re.False(groupInfo.AffinitySchedulingAllowed, "Group should be invalidated when stores are disconnected")
		re.Equal(affinity.PhasePending, groupInfo.Phase)

		// Recover store 2 and 3
		tc.SetStoreUp(1)
		tc.SetStoreUp(2)
		tc.SetStoreUp(3)

		// Wait for health check to mark group as available again
		testutil.Eventually(re, func() bool {
			groupInfo = affinityManager.GetAffinityGroupState("test_group")
			// Check that the group is no longer invalidated (Phase should not be Pending)
			return groupInfo != nil && groupInfo.AffinitySchedulingAllowed
		})

		// Now group should be restored
		re.NotNil(groupInfo)
		re.True(groupInfo.AffinitySchedulingAllowed, "Group should be restored when all stores are healthy")
		re.Equal(affinity.PhasePreparing, groupInfo.Phase)

		// Set stores 3 down
		tc.SetStoreUp(1)
		tc.SetStoreUp(2)
		tc.SetStoreDown(3)

		// Wait for health check
		testutil.Eventually(re, func() bool {
			groupInfo = affinityManager.GetAffinityGroupState("test_group")
			return groupInfo != nil && !groupInfo.AffinitySchedulingAllowed
		})

		// Group should still be invalidated
		re.NotNil(groupInfo)
		re.False(groupInfo.AffinitySchedulingAllowed, "Group should remain invalidated while any store is unhealthy")
		re.Equal(affinity.PhasePending, groupInfo.Phase)

		// Recover store 3
		tc.SetStoreUp(1)
		tc.SetStoreUp(2)
		tc.SetStoreUp(3)

		// Wait for health check
		synctest.Sleep(2 * time.Second)

		// Group should still be invalidated
		groupInfo = affinityManager.GetAffinityGroupState("test_group")
		re.NotNil(groupInfo)
		re.True(groupInfo.RegularSchedulingAllowed, "Group should remain invalidated while any store is unhealthy")
		re.Equal(affinity.PhasePending, groupInfo.Phase)

		// Manually observe region to enable affinity scheduling
		testutil.Eventually(re, func() bool {
			region := tc.GetRegion(1)
			groupInfo = affinityManager.GetAffinityGroupState("test_group")
			affinityManager.ObserveAvailableRegion(region, groupInfo)
			_, isAffinity := affinityManager.GetAndCacheRegionAffinityGroupState(region)
			return isAffinity
		})

		// Now group should be restored
		groupInfo = affinityManager.GetAffinityGroupState("test_group")
		re.NotNil(groupInfo)
		re.True(groupInfo.AffinitySchedulingAllowed, "Group should be restored when all stores are healthy")
		re.Equal(affinity.PhaseStable, groupInfo.Phase)
	})
}

// TestAffinityCheckerMultipleGroups tests checker with multiple affinity groups.
func TestAffinityCheckerMultipleGroups(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Region 1 belongs to group1
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	// Region 2 belongs to group2
	addAffinityTestRegion(tc, 2, 2, 3, 4)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create two different affinity groups
	group1 := &affinity.Group{
		ID:            "group1",
		LeaderStoreID: 2, // Need to transfer leader from 1 to 2
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	group2 := &affinity.Group{
		ID:            "group2",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{2, 3, 4},
	}
	err := createAffinityGroupForTest(affinityManager, group1, affinityTestRange("", "m"))
	re.NoError(err)
	err = createAffinityGroupForTest(affinityManager, group2, affinityTestRange("m", ""))
	re.NoError(err)

	// Manually set region metadata to have the right key ranges
	region1 := tc.GetRegion(1).Clone(
		withAffinityTestRange("", "m"),
	)
	region2 := tc.GetRegion(2).Clone(
		withAffinityTestRange("m", ""),
	)
	tc.PutRegion(region1)
	tc.PutRegion(region2)

	// Check region 1 should create transfer leader operator
	ops1 := checker.Check(tc.GetRegion(1))
	re.NotNil(ops1)
	re.Len(ops1, 1)
	re.Equal("affinity-move-region", ops1[0].Desc())

	// Check region 2 should return nil (already aligned)
	ops2 := checker.Check(tc.GetRegion(2))
	re.Nil(ops2)
}

// TestAffinityCheckerRegionWithoutGroup tests that checker ignores regions not in any group.
func TestAffinityCheckerRegionWithoutGroup(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	addAffinityTestRegion(tc, 1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)
	re.False(checker.hasAffinityGroups())

	// Create an affinity group with a key range that doesn't include region 1
	// Region 1 has default key range, so use ["z", "") to exclude it
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("z", ""))
	re.NoError(err)
	re.True(checker.hasAffinityGroups())

	// Check should return nil because region 1 is not in the group's key range
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityCheckerConcurrentGroupDeletion tests checker behavior during group deletion.
func TestAffinityCheckerConcurrentGroupDeletion(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	addAffinityTestRegion(tc, 1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Verify checker can create operator
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)

	// Delete the group (force=true to ensure deletion)
	err = deleteAffinityGroupForTest(affinityManager, "test_group", true)
	re.NoError(err)

	// Check should now return nil (group no longer exists)
	ops = checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityMergeCheckBasic tests basic merge functionality for affinity regions.
func TestAffinityMergeCheckBasic(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20) // Small size to trigger merge
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions in the same group
	addAffinityTestRegion(tc, 1, 1, 2, 3) // Small region
	addAffinityTestRegion(tc, 2, 1, 2, 3) // Adjacent small region
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	// Set regions to be small
	region1 = region1.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))
	region2 = region2.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))

	// Make them adjacent
	region1 = region1.Clone(withAffinityTestRange("a", "b"))
	region2 = region2.Clone(withAffinityTestRange("b", "c"))

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should create merge operator
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.NotNil(ops)
	re.Len(ops, 2) // Merge operation creates 2 operators
	re.Contains(ops[0].Desc(), "merge")
	re.Zero(ops[0].Kind() & operator.OpMerge) // Not merge operator
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
	re.Equal(operator.OpAffinity, ops[0].SchedulerKind())
	re.Equal(operator.OpAffinity, ops[1].Kind()&operator.OpAffinity)
	re.Equal(operator.OpAffinity, ops[1].SchedulerKind())

	// The source check can hold the old target while the neighbor has aligned to the new one.
	_, err = affinityManager.UpdateAffinityGroupPeers(group.ID, 2, group.VoterStoreIDs)
	re.NoError(err)
	region2 = region2.Clone(core.WithLeader(region2.GetStorePeer(2)))
	tc.PutRegion(region2)
	_, aligned := affinityManager.GetRegionAffinityGroupState(region2)
	re.True(aligned)
	re.Empty(checker.mergeCheck(region1, groupState))
}

// TestAffinityCheckerMergePath ensures affinity merge path is triggered from Check when region is already in affinity.
func TestAffinityCheckerMergePath(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(5),
		core.SetApproximateKeys(5),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(5),
		core.SetApproximateKeys(5),
		withAffinityTestRange("b", "c"),
	)
	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Regions match affinity config, so Check should go to MergeCheck path.
	ops := checker.Check(region1)
	re.NotNil(ops)
	re.Len(ops, 2)
	re.Contains(ops[0].Desc(), "merge")
}

// TestAffinityMergeCheckNoTarget tests merge when no valid target exists.
func TestAffinityMergeCheckNoTarget(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create a small region with no adjacent regions
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	region1 := tc.GetRegion(1)
	region1 = region1.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))
	tc.PutRegion(region1)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil (no adjacent regions)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops)
}

// TestAffinityMergeCheckDifferentGroups tests that regions in different groups don't merge.
func TestAffinityMergeCheckDifferentGroups(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	region1 = region1.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 = region2.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create two different affinity groups
	group1 := &affinity.Group{
		ID:            "group1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	group2 := &affinity.Group{
		ID:            "group2",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group1, affinityTestRange("a", "b"))
	re.NoError(err)
	err = createAffinityGroupForTest(affinityManager, group2, affinityTestRange("b", "c"))
	re.NoError(err)

	// MergeCheck should return nil (different groups)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops)
}

// TestAffinityMergeCheckRegionTooLarge tests that large regions don't merge.
func TestAffinityMergeCheckRegionTooLarge(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(60)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create one small and one large adjacent region
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	region1 = region1.Clone(
		core.SetApproximateSize(70), // Too large to merge under 60 MB limit
		core.SetApproximateKeys(700000),
		withAffinityTestRange("a", "b"),
	)
	region2 = region2.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil (region1 is too large)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops)
}

// TestAffinityMergeCheckAdjacentNotAffinity tests that non-affinity adjacent regions don't merge.
func TestAffinityMergeCheckAdjacentNotAffinity(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	addAffinityTestRegion(tc, 1, 1, 2, 3) // This one is affinity-compliant
	addAffinityTestRegion(tc, 2, 2, 1, 3) // This one has wrong leader (leader on store 2 instead of 1)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	region1 = region1.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 = region2.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1, // Expect leader on store 1
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil (region2 is not affinity-compliant due to wrong leader)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops)
}

// TestAffinityMergeCheckNotAffinityRegion tests that non-affinity regions don't merge.
func TestAffinityMergeCheckNotAffinityRegion(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create a small region with wrong leader (not matching affinity requirement)
	addAffinityTestRegion(tc, 1, 2, 1, 3) // Leader on store 2, but group expects leader on store 1
	region1 := tc.GetRegion(1)
	region1 = region1.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))
	tc.PutRegion(region1)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1, // Expect leader on store 1
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil (region doesn't satisfy affinity requirements)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops)
}

// TestAffinityMergeCheckUnhealthyRegion tests that unhealthy regions don't merge.
func TestAffinityMergeCheckUnhealthyRegion(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create a small region with down peer
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	region1 := tc.GetRegion(1)

	// Get peer on store 2 to mark as down
	var peerOnStore2 *pdpb.PeerStats
	for _, peer := range region1.GetMeta().Peers {
		if peer.GetStoreId() == 2 {
			peerOnStore2 = &pdpb.PeerStats{Peer: peer}
			break
		}
	}

	region1 = region1.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithDownPeers([]*pdpb.PeerStats{peerOnStore2}), // Mark peer as down
	)
	tc.PutRegion(region1)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil (region is unhealthy)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops)
}

// TestAffinityMergeCheckBothDirections tests that merge can happen in both directions when one-way merge is disabled.
func TestAffinityMergeCheckBothDirections(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	// One-way merge is disabled by default
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create three small adjacent regions
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	addAffinityTestRegion(tc, 3, 1, 2, 3)

	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)
	region3 := tc.GetRegion(3).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("c", "d"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)
	tc.PutRegion(region3)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck on region2 can merge with either prev (region1) or next (region3)
	// When one-way merge is disabled, it should prefer next but can also merge with prev
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region2)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region2, groupState)
	re.NotNil(ops) // Should merge with one of the adjacent regions
}

// TestAffinityMergeCheckTargetTooBig tests that merging regions whose combined size exceeds the max limit is disallowed.
func TestAffinityMergeCheckTargetTooBig(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(60)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two adjacent regions whose total size exceeds the limit
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(45), // Source fits under 60 MB limit
		core.SetApproximateKeys(450000),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(25), // Total: 70 > 60
		core.SetApproximateKeys(250000),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil because the combined size (70) exceeds the limit (60)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Merged size exceeds MaxAffinityMergeRegionSize")
}

// TestAffinityMergeCheckRespectsAffinityLimit verifies that affinity merge uses its own limit
// even if the global max merge size is larger.
func TestAffinityMergeCheckRespectsAffinityLimit(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20) // Smaller than the default MaxMergeRegionSize (54)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(15),
		core.SetApproximateKeys(150000),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(6), // Total size: 21, allowed by global limit 54
		core.SetApproximateKeys(60000),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Merge should be blocked by the stricter affinity merge limit")
}

func TestAffinityMergeCheckDisabledByZeroConfig(t *testing.T) {
	ctx := t.Context()

	cases := []struct {
		name              string
		affinityMergeSize uint64
		globalMergeSize   uint64
	}{
		{name: "affinity-size-zero", affinityMergeSize: 0, globalMergeSize: 54},
		{name: "global-size-zero", affinityMergeSize: 20, globalMergeSize: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)

			opt := newAffinityTestOptions()
			opt.SetMaxAffinityMergeRegionSize(tc.affinityMergeSize)
			opt.SetMaxMergeRegionSize(tc.globalMergeSize)
			cluster := mockcluster.NewCluster(ctx, opt)
			cluster.AddRegionStore(1, 100)
			cluster.AddRegionStore(2, 100)
			cluster.AddRegionStore(3, 100)

			addAffinityTestRegion(cluster, 1, 1, 2, 3)
			addAffinityTestRegion(cluster, 2, 1, 2, 3)
			region1 := cluster.GetRegion(1).Clone(
				core.SetApproximateSize(10),
				core.SetApproximateKeys(10),
				withAffinityTestRange("a", "b"),
			)
			region2 := cluster.GetRegion(2).Clone(
				core.SetApproximateSize(5),
				core.SetApproximateKeys(5),
				withAffinityTestRange("b", "c"),
			)
			cluster.PutRegion(region1)
			cluster.PutRegion(region2)

			affinityManager := cluster.GetAffinityManager()
			checker := NewAffinityChecker(ctx, cluster, opt)
			group := &affinity.Group{
				ID:            "test_group",
				LeaderStoreID: 1,
				VoterStoreIDs: []uint64{1, 2, 3},
			}
			err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
			re.NoError(err)

			groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
			re.NotNil(groupState)
			ops := checker.mergeCheck(region1, groupState)
			re.Nil(ops)
		})
	}
}

// TestAffinityMergeCheckAdjacentUnhealthy tests that merging is blocked if the adjacent region is unhealthy.
func TestAffinityMergeCheckAdjacentUnhealthy(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two adjacent regions
	addAffinityTestRegion(tc, 1, 1, 2, 3) // Region 1 (Source)
	addAffinityTestRegion(tc, 2, 1, 2, 3) // Region 2 (Target, Unhealthy)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	// Get peer on store 2 of region 2 to mark as down
	var peerOnStore2 *pdpb.PeerStats
	for _, peer := range region2.GetMeta().Peers {
		if peer.GetStoreId() == 2 {
			peerOnStore2 = &pdpb.PeerStats{Peer: peer}
			break
		}
	}
	region2 = region2.Clone(
		core.WithDownPeers([]*pdpb.PeerStats{peerOnStore2}), // Mark target peer as down
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil (Adjacent region is unhealthy)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Should not merge into an unhealthy adjacent region")
}

func TestAffinityCheckerTargetLayout(t *testing.T) {
	for _, tc := range []struct {
		name   string
		peers  []uint64
		leader uint64
		voters []uint64
		kind   operator.OpKind
	}{
		{"aligned", []uint64{1, 2, 3}, 1, []uint64{1, 2, 3}, 0},
		{"unordered voters", []uint64{1, 2, 3}, 1, []uint64{3, 1, 2}, 0},
		{"leader only", []uint64{1, 2, 3}, 3, []uint64{1, 2, 3}, operator.OpLeader},
		{"voters only", []uint64{1, 2, 4}, 1, []uint64{1, 2, 3}, operator.OpRegion},
		{"leader without peer", []uint64{1, 2, 4}, 3, []uint64{1, 2, 3}, operator.OpLeader | operator.OpRegion},
		{"partial overlap", []uint64{1, 2, 3}, 4, []uint64{1, 4, 5}, operator.OpLeader | operator.OpRegion},
		{"disjoint voters", []uint64{1, 2, 4}, 5, []uint64{3, 5, 6}, operator.OpLeader | operator.OpRegion},
		{"one replica", []uint64{1}, 2, []uint64{2}, operator.OpLeader | operator.OpRegion},
		{"five replicas", []uint64{1, 2, 3, 4, 5}, 8, []uint64{6, 7, 8, 9, 10}, operator.OpLeader | operator.OpRegion},
		{"different replica counts", []uint64{1, 2, 3}, 8, []uint64{6, 7, 8, 9, 10}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAffinityBalanceTest(t, 10)
			f.SetMaxReplicasWithLabel(true, len(tc.peers))
			addAffinityTestRegion(f.Cluster, 1, tc.peers[0], tc.peers[1:]...)
			target := &affinity.Group{ID: "test_group", LeaderStoreID: tc.leader, VoterStoreIDs: tc.voters}
			require.NoError(t, createAffinityGroupForTest(f.affinityManager, target, affinityTestRange("", "")))
			region := f.GetRegion(1)
			group, _ := f.affinityManager.GetRegionAffinityGroupState(region)
			require.NotNil(t, group)
			require.Equal(t, target.ID, group.ID)

			ops := f.Check(region)
			if tc.kind == 0 {
				require.Empty(t, ops)
				return
			}
			require.Len(t, ops, 1)
			require.Equal(t, "affinity-move-region", ops[0].Desc())
			require.Equal(t, operator.OpAffinity|tc.kind, ops[0].Kind())
		})
	}
}

// TestAffinityCheckerRegionNoLeader tests region without leader.
func TestAffinityCheckerRegionNoLeader(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	// Create a region and manually remove the leader
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	region := tc.GetRegion(1)
	// Clone with no leader
	region = region.Clone(core.WithLeader(nil))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Should return nil because region has no leader
	ops := checker.Check(region)
	re.Nil(ops)
}

// TestAffinityCheckerUnhealthyRegion tests that regions with down peers are skipped.
func TestAffinityCheckerUnhealthyRegion(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	addAffinityTestRegion(tc, 1, 1, 2, 3)
	region := tc.GetRegion(1)
	var peerOnStore2 *pdpb.PeerStats
	for _, peer := range region.GetMeta().Peers {
		if peer.GetStoreId() == 2 {
			peerOnStore2 = &pdpb.PeerStats{Peer: peer}
			break
		}
	}
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{peerOnStore2}))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	ops := checker.Check(region)
	re.Nil(ops, "Unhealthy region should be skipped")
}

// TestAffinityCheckerPreserveLearners tests that existing learner peers are preserved.
func TestAffinityCheckerPreserveLearners(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3) // Enable placement rules with 3 replicas
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)

	// Add placement rule for learner
	err := tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "learner",
		Role:    placement.Learner,
		Count:   1,
	})
	re.NoError(err)

	// Create region with voters on [1, 2, 3] and learner on [4]
	// Leader on store 1
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	region := tc.GetRegion(1)

	// Add a learner peer on store 4
	learnerPeer := &metapb.Peer{
		Id:      4, // Add peer ID
		StoreId: 4,
		Role:    metapb.PeerRole_Learner,
	}
	newPeers := append(region.GetMeta().GetPeers(), learnerPeer)
	region = region.Clone(core.SetPeers(newPeers))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Affinity group expects voters on [1, 2, 3] with leader on 2
	// This should only transfer leader, not touch the learner on store 4
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	ops := checker.Check(region)
	re.NotNil(ops)
	re.Len(ops, 1)

	// Verify the operator preserves the learner
	op := ops[0]
	re.Equal("affinity-move-region", op.Desc())
	// The operator should have steps that preserve the learner on store 4
	re.Positive(op.Len())

	// Verify that learner on store 4 is preserved (not removed)
	verifyLearnersPreserved(re, op, []uint64{4})
}

// verifyLearnersPreserved checks that the operator does not remove or modify any learner peers.
// It uses type assertions to check specific step types rather than string matching for better robustness.
func verifyLearnersPreserved(re *require.Assertions, op *operator.Operator, learnerStores []uint64) {
	learnerStoreSet := make(map[uint64]bool)
	for _, store := range learnerStores {
		learnerStoreSet[store] = true
	}

	// Check that no step removes or modifies any of the learner stores
	for i := range op.Len() {
		step := op.Step(i)
		re.False(func() bool {
			switch s := step.(type) {
			case operator.RemovePeer:
				// Check if removing a learner peer
				return learnerStoreSet[s.FromStore]
			case operator.PromoteLearner:
				// Check if promoting a learner to voter (this changes the learner)
				return learnerStoreSet[s.ToStore]
			case operator.ChangePeerV2Enter:
				// Check PromoteLearners in joint consensus enter
				for _, pl := range s.PromoteLearners {
					if learnerStoreSet[pl.ToStore] {
						return true
					}
				}
			case operator.ChangePeerV2Leave:
				// Check PromoteLearners in joint consensus leave
				for _, pl := range s.PromoteLearners {
					if learnerStoreSet[pl.ToStore] {
						return true
					}
				}
			}
			return false
		}())
	}
}

// TestAffinityCheckerPreserveLearnersWithPeerChange tests learner preservation when peers change.
func TestAffinityCheckerPreserveLearnersWithPeerChange(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3) // Enable placement rules with 3 replicas
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)

	// Add placement rule for learner
	err := tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "learner",
		Role:    placement.Learner,
		Count:   1,
	})
	re.NoError(err)

	// Create region with voters on [1, 2, 4] and learner on [5]
	// Leader on store 1
	addAffinityTestRegion(tc, 1, 1, 2, 4)
	region := tc.GetRegion(1)

	// Add a learner peer on store 5
	learnerPeer := &metapb.Peer{
		Id:      5, // Add peer ID
		StoreId: 5,
		Role:    metapb.PeerRole_Learner,
	}
	newPeers := append(region.GetMeta().GetPeers(), learnerPeer)
	region = region.Clone(core.SetPeers(newPeers))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Affinity group expects voters on [1, 2, 3] with leader on 1
	// This should replace voter 4 with 3, and preserve learner on store 5
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	ops := checker.Check(region)
	re.NotNil(ops)
	re.Len(ops, 1)

	// Verify the operator preserves the learner while changing voters
	op := ops[0]
	re.Equal("affinity-move-region", op.Desc())
	re.Positive(op.Len())

	// Verify that learner on store 5 is preserved (not removed)
	verifyLearnersPreserved(re, op, []uint64{5})
}

// TestAffinityCheckerMultipleLearners tests preserving multiple learner peers.
func TestAffinityCheckerMultipleLearners(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3) // Enable placement rules with 3 replicas
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)
	tc.AddRegionStore(6, 10)

	// Add placement rule for 2 learners
	err := tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "learner",
		Role:    placement.Learner,
		Count:   2,
	})
	re.NoError(err)

	// Create region with voters on [1, 2, 3] and learners on [4, 5]
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	region := tc.GetRegion(1)

	// Add two learner peers
	learner1 := &metapb.Peer{
		Id:      4, // Add peer ID
		StoreId: 4,
		Role:    metapb.PeerRole_Learner,
	}
	learner2 := &metapb.Peer{
		Id:      5, // Add peer ID
		StoreId: 5,
		Role:    metapb.PeerRole_Learner,
	}
	newPeers := append(region.GetMeta().GetPeers(), learner1, learner2)
	region = region.Clone(core.SetPeers(newPeers))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Affinity group expects voters on [1, 2, 6] with leader on 2
	// This should replace voter 3 with 6, transfer leader to 2, and preserve both learners on [4, 5]
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 6},
	}
	err = createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	ops := checker.Check(region)
	re.NotNil(ops)
	re.Len(ops, 1)

	op := ops[0]
	re.Equal("affinity-move-region", op.Desc())
	re.Positive(op.Len())

	// Verify that both learners on stores 4 and 5 are preserved (not removed)
	verifyLearnersPreserved(re, op, []uint64{4, 5})
}

// TestAffinityCheckerGroupScheduleDisallowed verifies group state that forbids affinity scheduling is respected.
func TestAffinityCheckerGroupScheduleDisallowed(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"))
	}()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	addAffinityTestRegion(tc, 1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	tc.SetStoreEvictLeader(2, true)

	// Wait for availability checker to mark the group as not schedulable.
	var groupState *affinity.GroupState
	testutil.Eventually(re, func() bool {
		groupState = affinityManager.GetAffinityGroupState("test_group")
		return groupState != nil && !groupState.AffinitySchedulingAllowed
	})

	re.NotNil(groupState)
	re.False(groupState.AffinitySchedulingAllowed)
	re.Equal(affinity.PhasePending, groupState.Phase)

	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops, "Affinity scheduling should be blocked when group is not allowed")
}

// TestAffinityCheckerNormalizeGroupWhenPlacementRuleMismatch verifies that when the affinity group peers
// don't satisfy placement rules, the checker keeps the group schedulable by normalizing it to the valid Region peers.
func TestAffinityCheckerNormalizeGroupWhenPlacementRuleMismatch(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3) // enable placement rules

	// Add stores with zone labels. Only stores 1 and 4 satisfy zone=z1.
	tc.AddLabelsStore(1, 10, map[string]string{"zone": "z1"})
	tc.AddLabelsStore(2, 10, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(3, 10, map[string]string{"zone": "z3"})
	tc.AddLabelsStore(4, 10, map[string]string{"zone": "z1"})

	// Region currently on stores [1,2,3]; not in affinity state for the group below.
	addAffinityTestRegion(tc, 1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Affinity group expects 4 voters while placement rules (and the region) only allow 3.
	// The checker should normalize the group to the valid Region peers and keep it schedulable.
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3, 4},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops, "Group should not schedule when expected peers violate placement rules; it should refetch state instead")

	groupState := affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(groupState)
	re.True(groupState.AffinitySchedulingAllowed)
	re.Equal(affinity.PhaseStable, groupState.Phase)
	re.Equal([]uint64{1, 2, 3}, groupState.VoterStoreIDs)
}

func TestAffinityCheckerLeaderTransferRestrictions(t *testing.T) {
	for _, condition := range []string{"grant-leader", "evict-leader", "reject-leader"} {
		t.Run(condition, func(t *testing.T) {
			f := newAffinityBalanceTest(t, 3)
			id := f.seed("100", 1, 1)[0]
			_, err := f.affinityManager.UpdateAffinityGroupPeers(id, 2, []uint64{1, 2, 3})
			require.NoError(t, err)
			region := f.GetRegion(1)
			require.Len(t, f.Check(region), 1)
			switch condition {
			case "grant-leader":
				require.NoError(t, f.PauseLeaderTransfer(1, constant.Out))
			case "evict-leader":
				f.SetStoreEvictLeader(2, true)
			case "reject-leader":
				f.SetLabelProperty("reject-leader", "reject", "leader")
				f.SetStoreLabel(2, map[string]string{"reject": "leader"})
			}
			require.Empty(t, f.Check(region))
			require.Equal(t, uint64(2), f.affinityManager.GetAffinityGroupState(id).LeaderStoreID)
			if condition == "grant-leader" {
				f.ResumeLeaderTransfer(1, constant.Out)
				require.Len(t, f.Check(region), 1)
			}
		})
	}
}

// TestAffinityCheckerRegionHasBetterLocation tests how the checker handles a Region where isRegionPlacementRuleSatisfiedWithBestLocation returns false.
func TestAffinityCheckerRegionHasBetterLocation(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetLocationLabels([]string{"region", "zone", "host"})
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddLabelsStore(1, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h1"})
	tc.AddLabelsStore(3, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h2"})
	tc.AddLabelsStore(4, 0, map[string]string{"region": "r1", "zone": "z2", "host": "h1"})
	tc.AddLabelsStore(5, 0, map[string]string{"region": "r1", "zone": "z3", "host": "h1"})
	tc.AddLabelsStore(6, 0, map[string]string{"region": "r1", "zone": "z3", "host": "h2"})

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	// Create affinity group without best location
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// No scheduling is generated because the source Region is not at the best location.
	addAffinityTestRegion(tc, 100, 1, 3, 4)
	region := tc.GetRegion(100)
	re.False(checker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
	ops := checker.Check(region)
	re.Nil(ops)
	groupState := affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(groupState)
	re.Equal([]uint64{1, 2, 3}, groupState.VoterStoreIDs)

	// Because the Group’s currently specified Peers are not at the best location,
	// they are cleared and replaced with the source Region’s Peers.
	// In this case, no scheduling is generated, but the Peers are updated.
	addAffinityTestRegion(tc, 200, 1, 4, 5)
	region = tc.GetRegion(200)
	re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(region, false))
	ops = checker.Check(region)
	re.Nil(ops)
	groupState = affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(groupState)
	re.Equal([]uint64{1, 4, 5}, groupState.VoterStoreIDs)

	// When both the source Region and the target Region are at the best location, scheduling is generated.
	addAffinityTestRegion(tc, 300, 1, 4, 6)
	region = tc.GetRegion(300)
	re.True(checker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
	ops = checker.Check(region)
	re.NotNil(ops)
	re.Len(ops, 1)

	// No scheduling is generated when Placement Rules are disabled.
	tc.SetEnablePlacementRules(false)
	ops = checker.Check(region)
	re.Nil(ops)

	// If an IsolationLevel is configured, no scheduling is generated when the required isolation level cannot be met.
	tc.SetEnablePlacementRules(true)
	rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	re.NotNil(rule)
	rule.IsolationLevel = "region"
	re.NoError(tc.GetRuleManager().SetRule(rule))
	ops = checker.Check(region)
	re.Nil(ops)
}

func TestAffinityBestLocationFilteredByTemporarySourceState(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetLocationLabels([]string{"region", "zone", "host"})
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddLabelsStore(1, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h1"})
	tc.AddLabelsStore(3, 0, map[string]string{"region": "r1", "zone": "z1", "host": "h2"})
	tc.AddLabelsStore(4, 0, map[string]string{"region": "r1", "zone": "z2", "host": "h1"})
	tc.AddLabelsStore(5, 0, map[string]string{"region": "r1", "zone": "z3", "host": "h1"})
	tc.AddLabelsStore(6, 0, map[string]string{"region": "r1", "zone": "z3", "host": "h2"})

	checker := newTestAffinityChecker(ctx, tc, opt)
	tc.AddLeaderRegion(1, 1, 3, 4)
	region := tc.GetRegion(1)

	fit := checker.ruleManager.FitRegion(tc, region)
	re.NotNil(fit)
	re.True(fit.IsSatisfied())
	re.NotEmpty(fit.RuleFits)
	strategy := checker.strategy(region, fit.RuleFits[0].Rule, false)
	oldStoreID, newStoreID, filterByTempState := strategy.getBetterLocation(tc, region, fit, fit.RuleFits[0])
	re.NotZero(oldStoreID)
	re.NotZero(newStoreID)
	re.False(filterByTempState)

	tc.SetStoreBusy(1, true)
	tc.SetStoreBusy(3, true)
	oldStoreID, newStoreID, filterByTempState = strategy.getBetterLocation(tc, region, fit, fit.RuleFits[0])
	re.Zero(oldStoreID)
	re.Zero(newStoreID)
	re.True(filterByTempState)
	re.False(checker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
}

// TestAffinityStrictCheckDivergesFromIsRegionReplicated documents the intended
// semantic split: affinity scheduling may reject a region that is already
// placement-rule replicated when a strictly better location still exists.
func TestAffinityStrictCheckDivergesFromIsRegionReplicated(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := mockconfig.NewTestOptions()
	opt.SetLocationLabels([]string{"zone", "host"})
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetEnablePlacementRules(true)
	tc.SetMaxReplicas(3)

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h3"})

	checker := newTestAffinityChecker(ctx, tc, opt)

	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)

	re.True(filter.IsRegionReplicated(tc, region))
	re.False(checker.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
}

// TestAffinityMergeCheckPeerStoreMismatch tests that merge is rejected when peer stores don't match.
func TestAffinityMergeCheckPeerStoreMismatch(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)
	tc.AddRegionStore(4, 100)

	// Create two small adjacent regions with different peer stores
	addAffinityTestRegion(tc, 1, 1, 2, 3) // Region 1: stores [1, 2, 3]
	addAffinityTestRegion(tc, 2, 1, 2, 4) // Region 2: stores [1, 2, 4] - different from region 1
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil because peer stores don't match
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Should not merge when peer stores don't match")
}

// TestAffinityMergeCheckAdjacentAbnormalReplica tests that merge is rejected when adjacent region has abnormal replica count.
func TestAffinityMergeCheckAdjacentAbnormalReplica(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3) // Require 3 replicas
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two adjacent regions - region 2 has only 2 replicas (abnormal)
	addAffinityTestRegion(tc, 1, 1, 2, 3) // Region 1: 3 replicas (normal)
	addAffinityTestRegion(tc, 2, 1, 2)    // Region 2: 2 replicas (abnormal)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil because adjacent region has abnormal replica count
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Should not merge when adjacent region has abnormal replica count")
}

// TestAffinityMergeCheckPlacementSplitKeys tests that merge is rejected when placement rules require split.
func TestAffinityMergeCheckPlacementSplitKeys(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	// Add a placement rule that requires split at key "b" (between the two regions)
	err := tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test_rule",
		Role:        placement.Voter,
		Count:       3,
		StartKeyHex: hex.EncodeToString(testutil.TxnKeyspaceKey([]byte("b"))),
		EndKeyHex:   hex.EncodeToString(testutil.TxnKeyspaceKey([]byte("c"))),
	})
	re.NoError(err)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil because placement rules require split at key "b"
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Should not merge when placement rules require split")
}

// TestAffinityMergeCheckLabelerSplitKeys tests that merge is rejected when region labeler requires split.
func TestAffinityMergeCheckLabelerSplitKeys(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	addAffinityTestRegion(tc, 1, 1, 2, 3)
	addAffinityTestRegion(tc, 2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("a", "b"),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		withAffinityTestRange("b", "c"),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	// Add a region label rule that requires split at key "b"
	regionLabeler := tc.GetRegionLabeler()
	err := regionLabeler.SetLabelRule(&labeler.LabelRule{
		ID:       "test_rule",
		Labels:   []labeler.RegionLabel{{Key: "test", Value: "value"}},
		RuleType: labeler.KeyRange,
		Data: labeler.MakeKeyRanges(
			hex.EncodeToString(testutil.TxnKeyspaceKey([]byte("b"))),
			hex.EncodeToString(testutil.TxnKeyspaceKey([]byte("c"))),
		),
	})
	re.NoError(err)

	affinityManager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// MergeCheck should return nil because region labeler requires split at key "b"
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Should not merge when region labeler requires split")
}

func TestCloneRegionWithPeerStores(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 1)
	tc.AddRegionStore(3, 1)

	// Current: voters on [1, 2, 3], leader on 1
	tc.AddLeaderRegion(100, 1, 2, 3)
	region := tc.GetRegion(100)

	// failure: voters on [1, 2], leader on 1
	re.Nil(cloneRegionWithReplacePeerStores(region, 1, 1, 2))

	// failure: voters on [1, 2, 3, 4], leader on 1
	re.Nil(cloneRegionWithReplacePeerStores(region, 1, 1, 2, 3, 4))

	// failure: voters on [1, 2, 3], leader on 4
	re.Nil(cloneRegionWithReplacePeerStores(region, 4, 1, 2, 3))

	// success: voters on [3, 2, 1], leader on 3
	targetRegion := cloneRegionWithReplacePeerStores(region, 3, 3, 2, 1)
	re.NotNil(targetRegion)
	re.Equal(uint64(3), targetRegion.GetLeader().GetStoreId())
	storeIDsEq(re, []uint64{3, 2, 1}, targetRegion.GetVoters())

	// success: voters on [4, 1, 2], leader on 2
	targetRegion = cloneRegionWithReplacePeerStores(region, 2, 4, 1, 2)
	re.NotNil(targetRegion)
	re.Equal(uint64(2), targetRegion.GetLeader().GetStoreId())
	storeIDsEq(re, []uint64{4, 1, 2}, targetRegion.GetVoters())

	// success: voters on [4, 5, 6], leader on 4
	targetRegion = cloneRegionWithReplacePeerStores(region, 4, 4, 5, 6)
	re.NotNil(targetRegion)
	re.Equal(uint64(4), targetRegion.GetLeader().GetStoreId())
	storeIDsEq(re, []uint64{4, 5, 6}, targetRegion.GetVoters())
}

func storeIDsEq(re *require.Assertions, expectedStoreIDs []uint64, peers []*metapb.Peer) {
	storeIDs := make([]uint64, len(expectedStoreIDs))
	for i, peer := range peers {
		storeIDs[i] = peer.GetStoreId()
	}
	slices.Sort(storeIDs)
	slices.Sort(expectedStoreIDs)
	re.True(slices.Equal(expectedStoreIDs, storeIDs))
}

// TestAffinityMergeCheckWithOperatorController tests the merge cache callback mechanism:
// This test verifies that when merge operators complete, the RecordMergeOpSuccess callback
// properly caches merged region IDs to prevent cascading merges.
//
// Note: We cannot simulate actual operator completion in unit tests because merge operators
// require TiKV to execute the merge. Instead, we test the callback directly to verify the
// cache mechanism works correctly.
func TestAffinityMergeCheckWithOperatorController(t *testing.T) {
	re := require.New(t)

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(30)
	tc := mockcluster.NewCluster(t.Context(), opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create affinity checker and set up callback
	affinityChecker := newTestAffinityChecker(t.Context(), tc, opt)

	// Create THREE small adjacent regions
	regions := make([]*core.RegionInfo, 3)
	keys := []string{"a", "b", "c", "d"}
	for i := range 3 {
		regionID := uint64(i + 1)
		addAffinityTestRegion(tc, regionID, 1, 2, 3)
		regions[i] = tc.GetRegion(regionID).Clone(
			core.SetApproximateSize(10),
			core.SetApproximateKeys(10),
			withAffinityTestRange(keys[i], keys[i+1]),
		)
		tc.PutRegion(regions[i])
	}
	region1, region2, region3 := regions[0], regions[1], regions[2]

	// Create affinity group
	affinityManager := tc.GetAffinityManager()
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, affinityTestRange("", ""))
	re.NoError(err)

	// Phase 1: Create merge operators for region1 + region2
	ops1 := affinityChecker.Check(region1)
	re.NotNil(ops1, "Should create merge operators for small adjacent regions")
	re.Len(ops1, 2, "Merge operation creates 2 operators")

	// Simulate merge completion: region1 and region2 merge into region1 (ID=1)
	mergedRegion12 := region1.Clone(
		core.WithEndKey(region2.GetEndKey()), // Now ends at "c"
		core.WithIncVersion(),
		core.SetApproximateSize(20),
		core.SetApproximateKeys(20),
	)
	tc.PutRegion(mergedRegion12)

	// BEFORE callback: verify cache is empty and mergedRegion12 CAN merge with region3
	re.False(affinityChecker.recentMergeCache.Exists(region1.GetID()),
		"Region1 should NOT be in cache before callback")
	re.False(affinityChecker.recentMergeCache.Exists(region2.GetID()),
		"Region2 should NOT be in cache before callback")
	ops2 := affinityChecker.Check(mergedRegion12)
	re.NotNil(ops2, "Should create merge operators BEFORE callback fires")
	re.Len(ops2, 2, "Should create operators to merge with region3")

	// Phase 2: Manually invoke the callback to simulate first merge completion (region1+region2)
	for _, op := range ops1 {
		affinityChecker.RecordOpSuccess(op)
	}
	re.True(affinityChecker.recentMergeCache.Exists(region1.GetID()),
		"Region1 should be cached after callback")
	re.True(affinityChecker.recentMergeCache.Exists(region2.GetID()),
		"Region2 should be cached after callback")

	// Phase 3: AFTER callback: verify subsequent merge is skipped due to cache
	ops := affinityChecker.Check(mergedRegion12)
	re.Nil(ops, "Should skip merge because region1 is in cache")
	ops = affinityChecker.Check(region3)
	re.Nil(ops, "Should skip merge because adjacent region (mergedRegion12) is in cache")
}

func TestAffinityFeedbackConflictingRegions(t *testing.T) {
	f := newAffinityBalanceTest(t, 7, "a", "d", "b", "c", "b", "c", "d")
	for id := uint64(1); id <= 7; id++ {
		f.UpdateRegionCount(id, 10+int(id)*10)
	}
	f.setVoters(2)
	f.addLearnerRule(1)
	f.AddLeaderRegion(1, 1, 2)
	f.AddLeaderRegion(2, 1, 2)
	regions := []*core.RegionInfo{learnerAt(f.GetRegion(1), 5), learnerAt(f.GetRegion(2), 6)}
	ids := []uint64{1, 2, 3, 4, 5, 6, 7}
	check := f.balanceTargetCheck(t.Context(), ids, regions)
	loc := [][]int32{{0}, {3}, {1}, {2}, {1}, {2}, {3}}
	in := &balanceplan.Input{Stores: 7, BaseVoters: []int{0, 5, 0, 0, 0, 0, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Role: balanceplan.RoleVoter, Count: 2, Stores: []int{0, 1, 2, 3, 6}, Location: loc}}, Voters: []int{0, 1}, Leader: 0, Forced: []int{0}, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 7), Exclude: []int{4, 5}, CheckTarget: check}}}
	legal := 0
	for _, s := range []int{1, 2, 3, 6} {
		f := check([]int{0, s}, 0, balanceplan.DefaultWorkBudget)
		if f.Status == balanceplan.TargetSatisfied {
			legal++
		}
	}
	require.Equal(t, 2, legal)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Contains(t, r.Voters[0], 6)
	for i, v := range r.Voters {
		require.Equal(t, balanceplan.TargetSatisfied, check(v, r.Leaders[i], balanceplan.DefaultWorkBudget).Status)
	}
}

func TestAffinityFeedbackRuleReassignment(t *testing.T) {
	f := newAffinityBalanceTest(t, 6, "x", "a", "a", "b", "d", "d")
	for id := uint64(1); id <= 6; id++ {
		f.UpdateRegionCount(id, int(id)*10)
	}
	for i, ids := range [][]string{{"1"}, {"2", "3", "4", "6"}, {"2", "3", "5"}} {
		count, role := 1, placement.Voter
		if i == 0 {
			role = placement.Leader
		}
		if i == 1 {
			count = 2
		}
		require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: role, Count: count, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	// The real fitter can assign a voter to a different rule than the solver.
	f.AddLeaderRegion(1, 5, 1, 2, 4)
	region := f.GetRegion(1).Clone(core.WithReplaceLeaderStore(1))
	ids := []uint64{1, 2, 3, 4, 5, 6}
	loc := [][]int32{{0}, {1}, {1}, {2}, {3}, {3}}
	rules := []*balanceplan.Rule{{Role: balanceplan.RoleLeader, Count: 1, Stores: []int{0}, Location: loc}, {Role: balanceplan.RoleVoter, Count: 2, Stores: []int{1, 2, 3, 5}, Location: loc}, {Role: balanceplan.RoleVoter, Count: 1, Stores: []int{1, 2, 4}, Location: loc}}
	check := f.balanceTargetCheck(t.Context(), ids, []*core.RegionInfo{region})
	in := &balanceplan.Input{Stores: 6, BaseVoters: []int{0, 0, 0, 0, 5, 0}, Groups: []balanceplan.Group{{Rules: rules, Voters: []int{0, 1, 3, 4}, RuleOf: []int{0, 1, 1, 2}, Leader: 0, LeaderPinned: true, CanLead: []bool{true, false, false, false, false, false}}}}
	raw := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.Equal(t, balanceplan.TargetRejected, check(raw.Voters[0], raw.Leaders[0], balanceplan.DefaultWorkBudget).Status)
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 1, 3, 5}, 0, balanceplan.DefaultWorkBudget).Status)
	in.Groups[0].CheckTarget = check
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, balanceplan.TargetSatisfied, check(r.Voters[0], r.Leaders[0], balanceplan.DefaultWorkBudget).Status)
}

func TestAffinityFeedbackForcedSource(t *testing.T) {
	f := newAffinityBalanceTest(t, 5, "a", "a", "b", "b", "b")
	for i, ids := range [][]string{{"1", "3"}, {"2", "4", "5"}} {
		require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: placement.Voter, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	f.AddLeaderRegion(1, 1, 4)
	region := f.GetRegion(1)
	check := f.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5}, []*core.RegionInfo{region})
	loc := [][]int32{{0}, {0}, {1}, {1}, {1}}
	in := &balanceplan.Input{Stores: 5, BaseVoters: []int{0, 0, 0, 5, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Count: 1, Stores: []int{0, 2}, Location: loc}, {Count: 1, Stores: []int{1, 3, 4}, Location: loc}}, Voters: []int{0, 3}, RuleOf: []int{0, 1}, Leader: 0, Forced: []int{0}, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 5), CheckTarget: check}}}
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 4}, 0, balanceplan.DefaultWorkBudget).Status)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Contains(t, r.Voters[0], 0)
	require.Equal(t, 0, r.Leaders[0])
	require.Equal(t, balanceplan.TargetSatisfied, check(r.Voters[0], r.Leaders[0], balanceplan.DefaultWorkBudget).Status)
}

func TestAffinityFeedbackForcedSingleRule(t *testing.T) {
	f := newAffinityBalanceTest(t, 6, "a", "b", "c", "c", "a", "b")
	f.UpdateRegionCount(1, 100)
	f.setVoters(2)
	f.addLearnerRule(2)
	f.AddLeaderRegion(1, 1, 3)
	region := f.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}), core.WithAddPeer(&metapb.Peer{Id: 101, StoreId: 6, Role: metapb.PeerRole_Learner}))
	check := f.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5, 6}, []*core.RegionInfo{region})
	// The PD removal tie break chooses the pinned leader in [0,1]. Moving 1 also fixes it.
	in := &balanceplan.Input{Stores: 6, BaseVoters: []int{0, 0, 5, 0, 0, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Count: 2, Stores: []int{0, 1, 2, 3}, Location: [][]int32{{0}, {1}, {2}, {2}, {0}, {1}}}}, Voters: []int{0, 2}, Leader: 0, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 6), Exclude: []int{4, 5}, CheckTarget: check}}}
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 3}, 0, balanceplan.DefaultWorkBudget).Status)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, []int{0, 3}, r.Voters[0])
}

func TestAffinityFeedbackRegionRejectionDominatesTemporary(t *testing.T) {
	f := newAffinityBalanceTest(t, 7, "a", "a", "b", "c", "b", "c", "d")
	f.UpdateRegionCount(2, 100)
	f.setVoters(2)
	f.addLearnerRule(1)
	f.AddLeaderRegion(1, 1, 2)
	f.AddLeaderRegion(2, 1, 2)
	r1 := learnerAt(f.GetRegion(1), 5)
	r2 := learnerAt(f.GetRegion(2), 6)
	for _, id := range []uint64{4, 6, 7} {
		f.SetStoreBusy(id, true)
	}
	require.Equal(t, affinityPlacementTemporary, f.regionPlacementFeedback(r1, f.ruleManager.FitRegionWithoutCache(f.Cluster, r1)).status)
	require.Equal(t, affinityPlacementRejected, f.regionPlacementFeedback(r2, f.ruleManager.FitRegionWithoutCache(f.Cluster, r2)).status)
	for _, regions := range [][]*core.RegionInfo{{r1, r2}, {r2, r1}} {
		f := f.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5, 6, 7}, regions)([]int{0, 1}, 0, balanceplan.DefaultWorkBudget)
		require.Equal(t, balanceplan.TargetRejected, f.Status)
	}
}

func TestAffinityFeedbackStopsBetweenRegions(t *testing.T) {
	f := newAffinityBalanceTest(t, 3, "a", "b", "c")
	f.AddLeaderRegion(1, 1, 2, 3)
	regions := slices.Repeat([]*core.RegionInfo{f.GetRegion(1)}, 100)
	check := f.balanceTargetCheck(t.Context(), []uint64{1, 2, 3}, regions)
	limited := check([]int{0, 1, 2}, 0, 1)
	full := check([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Equal(t, balanceplan.TargetRejected, limited.Status)
	require.Equal(t, balanceplan.TargetSatisfied, full.Status)
	require.Equal(t, limited.Work*100, full.Work)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled := f.balanceTargetCheck(ctx, []uint64{1, 2, 3}, regions)([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Zero(t, canceled.Work)
}

func TestAffinityPlacementChecksEveryCompetingSource(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "a", "c")
	for i, ids := range [][]string{{"1", "2", "4"}, {"3"}} {
		re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i),
			Index: i + 1, Override: i == 0, Role: placement.Voter, Count: 2 - i, LocationLabels: []string{"zone"},
			LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	f.AddLeaderRegion(1, 1, 2, 3)
	region := f.GetRegion(1)
	for _, high := range []uint64{1, 2} {
		f.UpdateRegionCount(1, 10)
		f.UpdateRegionCount(2, 10)
		f.UpdateRegionCount(high, 100)
		fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
		re.True(fit.IsSatisfied())
		if high == 2 {
			strategy := f.strategy(region, fit.RuleFits[0].Rule, false)
			old, candidates, temporary := strategy.getBetterLocationCandidates(f.Cluster, region, fit, fit.RuleFits[0])
			re.Equal(uint64(2), old)
			re.Nil(candidates, "the sampled source alone misses the improvement")
			re.False(temporary)
			re.True(f.isRegionPlacementRuleSatisfiedWithBestLocation(region, true), "live Regions follow the RuleChecker repair predicate")
		}
		for _, detailed := range []bool{false, true} {
			feedback := f.checkRegionPlacement(region, fit, detailed)
			re.Equal(affinityPlacementRejected, feedback.status, "RegionScore must not change placement validity")
			if detailed {
				re.Contains(feedback.replacements, bestLocationReplacement{old: 1, candidates: []uint64{4}})
			}
		}
	}
}

func TestAffinityHardIsolationDominatesTemporary(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3, "a", "a", "b")
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count, rule.IsolationLevel = 2, "zone"
	re.NoError(f.ruleManager.SetRule(rule))
	f.AddLeaderRegion(1, 1, 2)
	f.SetStoreBusy(3, true)
	region := f.GetRegion(1)
	fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
	strategy := f.strategy(region, rule, false)
	_, candidates, temporary := strategy.getBetterLocationCandidates(f.Cluster, region, fit, fit.RuleFits[0])
	re.Nil(candidates)
	re.True(temporary)
	re.Equal(affinityPlacementRejected, f.checkRegionPlacement(region, fit, false).status)
}

func TestAffinityBalanceWitnessRulesStayUnsupported(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "c", "d")
	id := balanceTestGroupID("_tidb_pt_100_p1")
	f.group(id, 1, 1, []uint64{1, 2, 3}, "a", "b")
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.IsWitness, rule.Count = true, 1
	re.NoError(f.ruleManager.SetRule(rule))
	plan := f.balancePlanInput(f.snapshot(), nil, nil)
	re.Empty(plan.input.Groups)
	re.Equal(1, plan.groupCounts[balanceGroupUnsupported])
}

func TestAffinityPlacementUnlabeledCoLocationRule(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 4, "z1", "z1", "z2", "z3")
	re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "r1", Index: 1,
		Override: true, Role: placement.Voter, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"z1"}}}}))
	re.NoError(f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "r2", Index: 2,
		Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}}))
	f.AddLeaderRegion(1, 1, 2, 3)
	region := f.GetRegion(1)
	for _, high := range []uint64{2, 3} {
		f.UpdateRegionCount(2, 10)
		f.UpdateRegionCount(3, 10)
		f.UpdateRegionCount(high, 100)
		fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
		re.True(fit.IsSatisfied())
		re.Empty(fit.RuleFits[0].Rule.LocationLabels)
		for _, detailed := range []bool{false, true} {
			feedback := f.checkRegionPlacement(region, fit, detailed)
			re.Equal(affinityPlacementRejected, feedback.status, "an unlabeled co-located rule must not hide a better source")
			if detailed {
				re.Contains(feedback.replacements, bestLocationReplacement{old: 2, candidates: []uint64{4}})
			}
		}
	}
}

func TestBalanceTargetCheckCanceledBeforeRegionFits(t *testing.T) {
	f := newAffinityBalanceTest(t, 3)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A nil Region would panic if preparation tried to fit it after cancellation.
	check := f.balanceTargetCheck(ctx, []uint64{1, 2, 3}, []*core.RegionInfo{nil})
	feedback := check([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Equal(t, balanceplan.TargetRejected, feedback.Status)
	require.Zero(t, feedback.Work)
}

func TestAffinityPlacementSingleRuleAndLearnerScanOnce(t *testing.T) {
	for _, learner := range []bool{false, true} {
		f := newAffinityBalanceTest(t, 4, "z1", "z2", "z3", "z4")
		f.AddLeaderRegion(1, 1, 2, 3)
		region := f.GetRegion(1)
		if learner {
			f.SetStoreLabel(4, map[string]string{"engine": "tiflash"})
			require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner",
				Role: placement.Learner, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}}))
			region = region.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 4, Role: metapb.PeerRole_Learner}))
		}
		fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
		feedback := f.checkRegionPlacement(region, fit, false)
		require.Equal(t, affinityPlacementReady, feedback.status)
		require.Equal(t, 1, feedback.scans)
	}
}

// Unschedulable target stores cannot receive affinity operators.
func TestAffinityOperatorTargetStores(t *testing.T) {
	for _, tc := range []struct {
		name   string
		leader bool
	}{
		{"removing", false}, {"down", false}, {"special use", false}, {"low space", false},
		{"busy", false}, {"store limit", false}, {"store limit and snapshots", false}, {"removing", true}, {"busy", true},
		{"reject-leader", true}, {"evict-leader", true},
	} {
		t.Run(fmt.Sprintf("%s/leader=%t", tc.name, tc.leader), func(t *testing.T) {
			re := require.New(t)
			f := newAffinityBalanceTest(t, 4)
			ids := f.seed("100", 1, 1)
			// Move a voter from store 3 to store 4, or the leader to store 2.
			leader, voters, store := uint64(1), []uint64{1, 2, 4}, uint64(4)
			if tc.leader {
				leader, voters, store = 2, []uint64{1, 2, 3}, 2
			}
			_, err := f.affinityManager.UpdateAffinityGroupPeers(ids[0], leader, voters)
			re.NoError(err)
			region, group := f.GetRegion(1), f.affinityManager.GetAffinityGroupState(ids[0])
			re.NotNil(f.targetOperator(region, group))
			counter := affinityCheckerTargetStoreRejectedCounter
			if f.blockStore(store, tc.name) == balanceTemporarilyBlocked {
				counter = affinityCheckerTargetStoreBlockedCounter
			}
			before := promtestutil.ToFloat64(counter)
			if tc.name == "store limit" {
				re.NotNil(f.targetOperator(region, group))
				re.Equal(before, promtestutil.ToFloat64(counter))
				return
			}
			re.Nil(f.targetOperator(region, group))
			re.Equal(1.0, promtestutil.ToFloat64(counter)-before)
		})
	}
}

func TestAffinityCheckConvergesDifferentLeadersOnSameVoters(t *testing.T) {
	re := require.New(t)
	enableFastAffinityAvailabilityCheck(t)
	f := newAffinityBalanceTest(t, 4, "a", "b", "a", "c")
	f.setRules(
		&placement.Rule{ID: "primary", Index: 1, Override: true, Role: placement.Voter, Count: 2,
			LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"1", "2", "4"}}}},
		&placement.Rule{ID: "secondary", Index: 2, Role: placement.Voter, Count: 1,
			LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"3"}}}},
	)
	f.UpdateRegionCount(2, 100)
	id := balanceTestGroupID("_tidb_pt_100_p1")
	f.group(id, 1, 1, []uint64{1, 2, 3}, "a", "b", "c")
	f.waitForStoreScan(id)
	f.PutRegion(f.GetRegion(2).Clone(core.WithLeader(f.GetRegion(2).GetStoreVoter(2))))
	group := f.affinityManager.GetAffinityGroupState(id)
	_, status := f.validateBalanceCurrentRegion(group, f.GetRegion(1))
	re.Equal(balanceAllowed, status)
	ops := f.Check(f.GetRegion(2))
	re.Len(ops, 1)
	step, ok := ops[0].Step(0).(operator.TransferLeader)
	re.True(ok)
	re.Equal(uint64(2), step.FromStore)
	re.Equal(uint64(1), step.ToStore)
	f.applyBalanceMove(ops[0], 1)
	for _, regionID := range []uint64{1, 2} {
		_, aligned := f.affinityManager.GetAndCacheRegionAffinityGroupState(f.GetRegion(regionID))
		re.True(aligned)
	}
	re.True(group.MatchesTarget(f.affinityManager.GetAffinityGroupState(id)))
}

func TestAffinityPlacementScansTiedSourcesAcrossZones(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 6)
	for i, location := range [][2]string{{"a", "1"}, {"a", "1"}, {"b", "1"}, {"b", "1"}, {"a", "2"}, {"b", "2"}} {
		f.SetStoreLabel(uint64(i+1), map[string]string{"zone": location[0], "rack": location[1]})
	}
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 4
	rule.LocationLabels = []string{"zone", "rack"}
	f.setRules(rule)
	f.AddLeaderRegion(1, 1, 2, 3, 4)
	f.UpdateRegionCount(2, 100)
	region := f.GetRegion(1)

	f.SetStoreBusy(5, true)
	fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
	strategy := f.strategy(region, rule, false)
	old, candidates, temporary := strategy.getBetterLocationCandidates(f.Cluster, region, fit, fit.RuleFits[0])
	re.Equal(uint64(2), old)
	re.Nil(candidates)
	re.True(temporary)
	feedback := f.checkRegionPlacement(region, fit, false)
	re.Equal(affinityPlacementRejected, feedback.status)
	re.Greater(feedback.scans, 1)

	f.SetStoreDown(5)
	fit = f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
	old, candidates, temporary = strategy.getBetterLocationCandidates(f.Cluster, region, fit, fit.RuleFits[0])
	re.Equal(uint64(2), old)
	re.Nil(candidates)
	re.False(temporary)
	re.Equal(affinityPlacementRejected, f.checkRegionPlacement(region, fit, false).status)
}
