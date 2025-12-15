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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
)

func newAffinityTestOptions() *config.PersistOptions {
	opt := mockconfig.NewTestOptions()
	cfg := opt.GetScheduleConfig().Clone()
	cfg.AffinityScheduleLimit = 4 // Set affinity schedule limit to enable affinity scheduling
	opt.SetScheduleConfig(cfg)
	return opt
}

// createAffinityGroupForTest is a test helper that creates an affinity group with the specified peers.
// If startKey and endKey are provided (both non-nil), creates the group with that key range.
// Otherwise creates the group without key ranges.
func createAffinityGroupForTest(manager *affinity.Manager, group *affinity.Group, startKey, endKey []byte) error {
	var keyRanges []affinity.GroupKeyRanges
	if startKey != nil || endKey != nil {
		keyRanges = []affinity.GroupKeyRanges{{
			GroupID: group.ID,
			KeyRanges: []keyutil.KeyRange{{
				StartKey: startKey,
				EndKey:   endKey,
			}},
		}}
	} else {
		keyRanges = []affinity.GroupKeyRanges{{GroupID: group.ID}}
	}

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

func TestAffinityCheckerTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3) // Leader on store 1

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group with leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should create transfer leader operator
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
}

func TestAffinityCheckerMovePeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddLeaderRegion(1, 1, 2, 4) // Peers on 1, 2, 4

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group expecting peers on 1, 2, 3
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should create move peer operator (from 4 to 3)
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
}

func TestAffinityCheckerPaused(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group with leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddLeaderRegion(100, 4, 2, 3)
	tc.AddLeaderRegion(200, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	affinityChecker := NewAffinityChecker(tc, opt)

	// Create affinity group with expected leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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

// TestAffinityAvailabilityCheckWithOfflineStore tests that groups are invalidated when stores go offline.
func TestAffinityAvailabilityCheckWithOfflineStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"))
	}()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"))
	}()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	time.Sleep(2 * time.Second)

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
		_, isAffinity := affinityManager.GetRegionAffinityGroupState(region)
		return isAffinity
	})

	// Now group should be restored
	groupInfo = affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(groupInfo)
	re.True(groupInfo.AffinitySchedulingAllowed, "Group should be restored when all stores are healthy")
	re.Equal(affinity.PhaseStable, groupInfo.Phase)
}

// TestAffinityCheckerNoOperatorWhenAligned tests that no operator is created when region matches group.
func TestAffinityCheckerNoOperatorWhenAligned(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3) // Perfect match

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group matching current state
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should return nil because region is already aligned
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityCheckerTransferLeaderWithoutPeer tests leader transfer when target store has no peer.
func TestAffinityCheckerTransferLeaderWithoutPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddLeaderRegion(1, 1, 2, 4) // Leader on 1, but need leader on 3

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group with leader on store 3, but store 3 has no peer yet
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 3,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should create move peer operator first (replace 4 with 3)
	// NOT transfer leader, because target store doesn't have a peer
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
}

// TestAffinityCheckerMultipleGroups tests checker with multiple affinity groups.
func TestAffinityCheckerMultipleGroups(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Region 1 belongs to group1
	tc.AddLeaderRegion(1, 1, 2, 3)
	// Region 2 belongs to group2
	tc.AddLeaderRegion(2, 2, 3, 4)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

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
	err := createAffinityGroupForTest(affinityManager, group1, []byte(""), []byte("m"))
	re.NoError(err)
	err = createAffinityGroupForTest(affinityManager, group2, []byte("m"), []byte(""))
	re.NoError(err)

	// Manually set region metadata to have the right key ranges
	region1 := tc.GetRegion(1).Clone(
		core.WithStartKey([]byte("")),
		core.WithEndKey([]byte("m")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.WithStartKey([]byte("m")),
		core.WithEndKey([]byte("")),
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create an affinity group with a key range that doesn't include region 1
	// Region 1 has default key range, so use ["z", "") to exclude it
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte("z"), []byte(""))
	re.NoError(err)

	// Check should return nil because region 1 is not in the group's key range
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityCheckerConcurrentGroupDeletion tests checker behavior during group deletion.
func TestAffinityCheckerConcurrentGroupDeletion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20) // Small size to trigger merge
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions in the same group
	tc.AddLeaderRegion(1, 1, 2, 3) // Small region
	tc.AddLeaderRegion(2, 1, 2, 3) // Adjacent small region
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	// Set regions to be small
	region1 = region1.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))
	region2 = region2.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))

	// Make them adjacent
	region1 = region1.Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b")))
	region2 = region2.Clone(core.WithStartKey([]byte("b")), core.WithEndKey([]byte("c")))

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// MergeCheck should create merge operator
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.NotNil(ops)
	re.Len(ops, 2) // Merge operation creates 2 operators
	re.Contains(ops[0].Desc(), "merge")
}

// TestAffinityCheckerMergePath ensures affinity merge path is triggered from Check when region is already in affinity.
func TestAffinityCheckerMergePath(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(5),
		core.SetApproximateKeys(5),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(5),
		core.SetApproximateKeys(5),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)
	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create a small region with no adjacent regions
	tc.AddLeaderRegion(1, 1, 2, 3)
	region1 := tc.GetRegion(1)
	region1 = region1.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))
	tc.PutRegion(region1)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	region1 = region1.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 = region2.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

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
	err := createAffinityGroupForTest(affinityManager, group1, []byte("a"), []byte("b"))
	re.NoError(err)
	err = createAffinityGroupForTest(affinityManager, group2, []byte("b"), []byte("c"))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(60)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create one small and one large adjacent region
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	region1 = region1.Clone(
		core.SetApproximateSize(70), // Too large to merge under 60 MB limit
		core.SetApproximateKeys(700000),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 = region2.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	tc.AddLeaderRegion(1, 1, 2, 3) // This one is affinity-compliant
	tc.AddLeaderRegion(2, 2, 1, 3) // This one has wrong leader (leader on store 2 instead of 1)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)

	region1 = region1.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 = region2.Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1, // Expect leader on store 1
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create a small region with wrong leader (not matching affinity requirement)
	tc.AddLeaderRegion(1, 2, 1, 3) // Leader on store 2, but group expects leader on store 1
	region1 := tc.GetRegion(1)
	region1 = region1.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))
	tc.PutRegion(region1)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1, // Expect leader on store 1
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create a small region with down peer
	tc.AddLeaderRegion(1, 1, 2, 3)
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
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	// One-way merge is disabled by default
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create three small adjacent regions
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 1, 2, 3)

	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)
	region3 := tc.GetRegion(3).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("c")),
		core.WithEndKey([]byte("d")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)
	tc.PutRegion(region3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(60)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two adjacent regions whose total size exceeds the limit
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(45), // Source fits under 60 MB limit
		core.SetApproximateKeys(450000),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(25), // Total: 70 > 60
		core.SetApproximateKeys(250000),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20) // Smaller than the default MaxMergeRegionSize (54)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(15),
		core.SetApproximateKeys(150000),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(6), // Total size: 21, allowed by global limit 54
		core.SetApproximateKeys(60000),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Merge should be blocked by the stricter affinity merge limit")
}

func TestAffinityMergeCheckDisabledByZeroConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

			cluster.AddLeaderRegion(1, 1, 2, 3)
			cluster.AddLeaderRegion(2, 1, 2, 3)
			region1 := cluster.GetRegion(1).Clone(
				core.SetApproximateSize(10),
				core.SetApproximateKeys(10),
				core.WithStartKey([]byte("a")),
				core.WithEndKey([]byte("b")),
			)
			region2 := cluster.GetRegion(2).Clone(
				core.SetApproximateSize(5),
				core.SetApproximateKeys(5),
				core.WithStartKey([]byte("b")),
				core.WithEndKey([]byte("c")),
			)
			cluster.PutRegion(region1)
			cluster.PutRegion(region2)

			affinityManager := cluster.GetAffinityManager()
			checker := NewAffinityChecker(cluster, opt)
			group := &affinity.Group{
				ID:            "test_group",
				LeaderStoreID: 1,
				VoterStoreIDs: []uint64{1, 2, 3},
			}
			err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two adjacent regions
	tc.AddLeaderRegion(1, 1, 2, 3) // Region 1 (Source)
	tc.AddLeaderRegion(2, 1, 2, 3) // Region 2 (Target, Unhealthy)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
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
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// MergeCheck should return nil (Adjacent region is unhealthy)
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Should not merge into an unhealthy adjacent region")
}

// TestAffinityCheckerComplexMove tests moving multiple peers and transferring leader in one operation.
// This tests the combo operator's ability to handle complex transformations.
func TestAffinityCheckerComplexMove(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)
	tc.AddRegionStore(6, 10)

	// Current: peers on [1, 2, 4], leader on 1
	// Expected: peers on [3, 5, 6], leader on 5
	// This requires replacing all 3 peers AND transferring leader
	tc.AddLeaderRegion(1, 1, 2, 4)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 5,
		VoterStoreIDs: []uint64{3, 5, 6},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should create a combo operator for the complex transformation
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
	re.Equal(operator.OpLeader, ops[0].Kind()&operator.OpLeader)
	re.Equal(operator.OpRegion, ops[0].Kind()&operator.OpRegion)
}

// TestAffinityCheckerPartialOverlap tests when current and expected peers partially overlap.
func TestAffinityCheckerPartialOverlap(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)

	// Current: peers on [1, 2, 3], leader on 1
	// Expected: peers on [1, 4, 5], leader on 4
	// Stores 2, 3 need to be replaced with 4, 5, and leader transferred to 4
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 4,
		VoterStoreIDs: []uint64{1, 4, 5},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

// TestAffinityCheckerOnlyLeaderTransfer tests when only leader transfer is needed (no peer changes).
func TestAffinityCheckerOnlyLeaderTransfer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	// Current: peers on [1, 2, 3], leader on 1
	// Expected: peers on [1, 2, 3], leader on 3
	// Only leader transfer is needed
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 3,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	// Should have OpLeader since we're transferring leader
	re.Equal(operator.OpLeader, ops[0].Kind()&operator.OpLeader)
}

// TestAffinityCheckerOnlyPeerChange tests when only peer changes are needed (no leader transfer).
func TestAffinityCheckerOnlyPeerChange(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Current: peers on [1, 2, 4], leader on 1
	// Expected: peers on [1, 2, 3], leader on 1
	// Only peer change is needed (replace 4 with 3)
	tc.AddLeaderRegion(1, 1, 2, 4)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())

	// Verify OpLeader is NOT set (since leader doesn't change)
	re.Equal(operator.OpKind(0), ops[0].Kind()&operator.OpLeader, "OpLeader should not be set when leader doesn't change")
	// Verify OpRegion is set
	re.Equal(operator.OpRegion, ops[0].Kind()&operator.OpRegion)
}

// TestAffinityCheckerSameStoreOrder tests when voter stores are in different order.
func TestAffinityCheckerSameStoreOrder(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	// Current: peers on [1, 2, 3], leader on 1
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Expected: same stores but in different order [3, 1, 2], leader on 1
	// This should NOT require any operator since stores are the same
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{3, 1, 2}, // Different order
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Should return nil since stores are the same (order doesn't matter)
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityCheckerReplicaCountMatch tests that affinity checker only works
// when the region's replica count matches both placement rules and affinity group requirements.
func TestAffinityCheckerReplicaCountMatch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	// Add 10 stores
	for i := uint64(1); i <= 10; i++ {
		tc.AddRegionStore(i, 10)
	}

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Test case 1: Single replica - placement rules and affinity group both require 1 replica
	tc.SetMaxReplicasWithLabel(true, 1)
	tc.AddLeaderRegion(1, 1)

	group1 := &affinity.Group{
		ID:            "test_group_1",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{2},
	}
	err := createAffinityGroupForTest(affinityManager, group1, []byte(""), []byte("m"))
	re.NoError(err)

	// Set region 1 to have the right key range
	region1 := tc.GetRegion(1).Clone(
		core.WithStartKey([]byte("")),
		core.WithEndKey([]byte("m")),
	)
	tc.PutRegion(region1)

	// Should create operator when replica counts match
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops, "Should create operator when replica counts match (1 replica)")
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())

	// Test case 2: 5 replicas - placement rules and affinity group both require 5 replicas
	tc.SetMaxReplicasWithLabel(true, 5)
	tc.AddLeaderRegion(2, 1, 2, 3, 4, 5)

	group2 := &affinity.Group{
		ID:            "test_group_2",
		LeaderStoreID: 8,
		VoterStoreIDs: []uint64{6, 7, 8, 9, 10},
	}
	err = createAffinityGroupForTest(affinityManager, group2, []byte("m"), []byte(""))
	re.NoError(err)

	// Set region 2 to have the right key range
	region2 := tc.GetRegion(2).Clone(
		core.WithStartKey([]byte("m")),
		core.WithEndKey([]byte("")),
	)
	tc.PutRegion(region2)

	// Should create operator when replica counts match
	ops = checker.Check(tc.GetRegion(2))
	re.NotNil(ops, "Should create operator when replica counts match (5 replicas)")
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())

	// Test case 3: Mismatch - region has 3 replicas but affinity group requires 5
	tc.SetMaxReplicasWithLabel(true, 3)
	tc.AddLeaderRegion(3, 1, 2, 3)

	group3 := &affinity.Group{
		ID:            "test_group_3",
		LeaderStoreID: 8,
		VoterStoreIDs: []uint64{6, 7, 8, 9, 10}, // 5 replicas
	}
	err = createAffinityGroupForTest(affinityManager, group3, nil, nil)
	re.NoError(err)

	// Should NOT create operator when replica counts don't match
	ops = checker.Check(tc.GetRegion(3))
	re.Nil(ops, "Should not create operator when region has 3 replicas but affinity group requires 5")
}

// TestAffinityCheckerRegionNoLeader tests region without leader.
func TestAffinityCheckerRegionNoLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	// Create a region and manually remove the leader
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)
	// Clone with no leader
	region = region.Clone(core.WithLeader(nil))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Should return nil because region has no leader
	ops := checker.Check(region)
	re.Nil(ops)
}

// TestAffinityCheckerUnhealthyRegion tests that regions with down peers are skipped.
func TestAffinityCheckerUnhealthyRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	tc.AddLeaderRegion(1, 1, 2, 3)
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
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	ops := checker.Check(region)
	re.Nil(ops, "Unhealthy region should be skipped")
}

// TestAffinityCheckerPreserveLearners tests that existing learner peers are preserved.
func TestAffinityCheckerPreserveLearners(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	tc.AddLeaderRegion(1, 1, 2, 3)
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
	checker := NewAffinityChecker(tc, opt)

	// Affinity group expects voters on [1, 2, 3] with leader on 2
	// This should only transfer leader, not touch the learner on store 4
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	tc.AddLeaderRegion(1, 1, 2, 4)
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
	checker := NewAffinityChecker(tc, opt)

	// Affinity group expects voters on [1, 2, 3] with leader on 1
	// This should replace voter 4 with 3, and preserve learner on store 5
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	tc.AddLeaderRegion(1, 1, 2, 3)
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
	checker := NewAffinityChecker(tc, opt)

	// Affinity group expects voters on [1, 2, 6] with leader on 2
	// This should replace voter 3 with 6, transfer leader to 2, and preserve both learners on [4, 5]
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 6},
	}
	err = createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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

// TestAffinityCheckerLearnerVoterConflict tests that when a learner peer conflicts with affinity voter,
// the checker returns nil and logs the conflict.
func TestAffinityCheckerLearnerVoterConflict(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Create region with voters on [1, 2, 3] and learner on [4]
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)

	// Add a learner peer on store 4
	learnerPeer := &metapb.Peer{
		Id:      4,
		StoreId: 4,
		Role:    metapb.PeerRole_Learner,
	}
	newPeers := append(region.GetMeta().GetPeers(), learnerPeer)
	region = region.Clone(core.SetPeers(newPeers))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Affinity group expects voters on [1, 2, 4] with leader on 1
	// This creates a conflict: store 4 has a learner but group expects it to be a voter
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 4}, // Store 4 conflicts with existing learner
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should return nil due to learner-voter conflict
	ops := checker.Check(region)
	re.Nil(ops, "Should not create operator when learner conflicts with affinity voter")
}

// TestAffinityCheckerGroupScheduleDisallowed verifies group state that forbids affinity scheduling is respected.
func TestAffinityCheckerGroupScheduleDisallowed(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"))
	}()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	// Make store 2 unavailable for leader (evict-leader).
	tc.SetStoreEvictLeader(2, true)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

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

// TestAffinityCheckerExpireGroupWhenPlacementRuleMismatch verifies that when the affinity group peers
// don't satisfy placement rules, the group will be expired and scheduling is disabled.
func TestAffinityCheckerExpireGroupWhenPlacementRuleMismatch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3) // enable placement rules

	// Add stores with zone labels. Only stores 1 and 4 satisfy zone=z1.
	tc.AddLabelsStore(1, 10, map[string]string{"zone": "z1"})
	tc.AddLabelsStore(2, 10, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(3, 10, map[string]string{"zone": "z3"})
	tc.AddLabelsStore(4, 10, map[string]string{"zone": "z1"})

	// Region currently on stores [1,2,3]; not in affinity state for the group below.
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Affinity group expects 4 voters while placement rules (and the region) only allow 3,
	// so isGroupReplicated should be false and the group should be expired/refreshed.
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3, 4},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops, "Group should not schedule when expected peers violate placement rules; it should refetch state instead")

	groupState := affinityManager.GetAffinityGroupState("test_group")
	re.NotNil(groupState)
	re.False(groupState.AffinitySchedulingAllowed, "Group should be expired when peer count violates placement rules")
	re.Equal(affinity.PhasePending, groupState.Phase)
	re.Equal([]uint64{1, 2, 3, 4}, groupState.VoterStoreIDs, "Peers remain as configured until a valid available region is observed")
}

// TestAffinityCheckerTargetStoreEvictLeader tests that operator is not created when target store has evict-leader.
func TestAffinityCheckerTargetStoreEvictLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3) // Leader on store 1

	// Set store 2 to evict leader
	tc.SetStoreEvictLeader(2, true)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group with leader on store 2 (which has evict-leader)
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should return nil because target store doesn't allow leader transfer in
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops, "Should not create operator when target store has evict-leader")
}

// TestAffinityCheckerTargetStoreRejectLeader tests that operator is not created when target store has reject-leader label.
func TestAffinityCheckerTargetStoreRejectLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3) // Leader on store 1

	// Set reject-leader label property for store 2
	tc.SetLabelProperty("reject-leader", "reject", "leader")
	tc.AddLabelsStore(2, 10, map[string]string{"reject": "leader"})

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	// Create affinity group with leader on store 2 (which has reject-leader label)
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// Check should return nil because target store has reject-leader label
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops, "Should not create operator when target store has reject-leader label")
}

// TestAffinityMergeCheckPeerStoreMismatch tests that merge is rejected when peer stores don't match.
func TestAffinityMergeCheckPeerStoreMismatch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)
	tc.AddRegionStore(4, 100)

	// Create two small adjacent regions with different peer stores
	tc.AddLeaderRegion(1, 1, 2, 3) // Region 1: stores [1, 2, 3]
	tc.AddLeaderRegion(2, 1, 2, 4) // Region 2: stores [1, 2, 4] - different from region 1
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3) // Require 3 replicas
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two adjacent regions - region 2 has only 2 replicas (abnormal)
	tc.AddLeaderRegion(1, 1, 2, 3) // Region 1: 3 replicas (normal)
	tc.AddLeaderRegion(2, 1, 2)    // Region 2: 2 replicas (abnormal)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetMaxReplicasWithLabel(true, 3)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	// Add a placement rule that requires split at key "b" (between the two regions)
	err := tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID:  "test",
		ID:       "test_rule",
		Role:     placement.Voter,
		Count:    3,
		StartKey: []byte("b"),
		EndKey:   []byte("c"),
	})
	re.NoError(err)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := newAffinityTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two small adjacent regions
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(10),
		core.SetApproximateKeys(10),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	// Add a region label rule that requires split at key "b"
	regionLabeler := tc.GetRegionLabeler()
	err := regionLabeler.SetLabelRule(&labeler.LabelRule{
		ID:       "test_rule",
		Labels:   []labeler.RegionLabel{{Key: "test", Value: "value"}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges("62", "63"), // hex for "b" and "c"
	})
	re.NoError(err)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = createAffinityGroupForTest(affinityManager, group, []byte(""), []byte(""))
	re.NoError(err)

	// MergeCheck should return nil because region labeler requires split at key "b"
	groupState, _ := affinityManager.GetRegionAffinityGroupState(region1)
	re.NotNil(groupState)
	ops := checker.mergeCheck(region1, groupState)
	re.Nil(ops, "Should not merge when region labeler requires split")
}

func TestCloneRegionWithPeerStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
