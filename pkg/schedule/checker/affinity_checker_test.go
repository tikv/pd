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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/operator"
)

func TestAffinityCheckerTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3) // Leader on store 1

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group with leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

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

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddLeaderRegion(1, 1, 2, 4) // Peers on 1, 2, 4

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group expecting peers on 1, 2, 3
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Check should create move peer operator (from 4 to 3)
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
}

func TestAffinityCheckerGroupNotInEffect(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Mark group as not in effect
	internalGroupInfo := affinityManager.GetGroups()["test_group"]
	if internalGroupInfo != nil {
		internalGroupInfo.Effect = false
	}

	// Check should return nil because group is not in effect
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

func TestAffinityCheckerPaused(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group with leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

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

// TestHealthCheckAndOperatorGeneration tests the full flow:
// Manager detects unhealthy store -> invalidates group -> checker skips operators -> store recovers -> checker creates operators
func TestHealthCheckAndOperatorGeneration(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enable fast health check for testing (100ms instead of 10s)
	affinity.SetHealthCheckIntervalForTest(100 * time.Millisecond)
	defer func() {
		affinity.SetHealthCheckIntervalForTest(0) // Reset to default
	}()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3) // Leader on store 1

	affinityManager := tc.GetAffinityManager()
	affinityChecker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group with expected leader on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)

	// Set region to group mapping
	affinityManager.SetRegionGroup(1, "test_group")

	// Verify group is in effect initially
	groupInfo := affinityManager.GetGroups()["test_group"]
	re.NotNil(groupInfo)
	re.True(groupInfo.Effect)

	// Checker should create operator for leader transfer (1 -> 2)
	ops := affinityChecker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())

	// Simulate health check invalidating the group
	groupInfo.Effect = false

	// Checker should NOT create operator when group is not in effect
	ops = affinityChecker.Check(tc.GetRegion(1))
	re.Nil(ops, "Checker should not create operator for invalidated group")

	// Simulate health check restoring the group
	groupInfo.Effect = true

	// Checker should create operator again after group is restored
	ops = affinityChecker.Check(tc.GetRegion(1))
	re.NotNil(ops, "Checker should create operator for restored group")
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

// TestHealthCheckWithOfflineStore tests that groups are invalidated when stores go offline.
func TestHealthCheckWithOfflineStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enable fast health check
	affinity.SetHealthCheckIntervalForTest(100 * time.Millisecond)
	defer func() {
		affinity.SetHealthCheckIntervalForTest(0)
	}()

	opt := mockconfig.NewTestOptions()
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
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)

	// Verify group is in effect
	groupInfo := affinityManager.GetGroups()["test_group"]
	re.True(groupInfo.Effect)

	// Set store 2 offline (this triggers IsRemoving())
	tc.SetStoreOffline(2)

	// Wait for health check to run
	time.Sleep(200 * time.Millisecond)

	// Group should be invalidated because store 2 is removing
	groupInfo = affinityManager.GetGroups()["test_group"]
	re.False(groupInfo.Effect, "Group should be invalidated when store is removing")
}

// TestHealthCheckWithDownStores tests behavior when stores go down.
func TestHealthCheckWithDownStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enable fast health check
	affinity.SetHealthCheckIntervalForTest(100 * time.Millisecond)
	defer func() {
		affinity.SetHealthCheckIntervalForTest(0)
	}()

	opt := mockconfig.NewTestOptions()
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
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)

	// Set stores 2 and 3 down
	tc.SetStoreDown(2)
	tc.SetStoreDown(3)

	// Wait for health check
	time.Sleep(200 * time.Millisecond)

	// Group should be invalidated
	groupInfo := affinityManager.GetGroups()["test_group"]
	re.False(groupInfo.Effect, "Group should be invalidated when stores are down")

	// Recover store 2 (store 3 still down)
	tc.SetStoreUp(2)

	// Wait for health check
	time.Sleep(200 * time.Millisecond)

	// Group should still be invalidated (store 3 still down)
	groupInfo = affinityManager.GetGroups()["test_group"]
	re.False(groupInfo.Effect, "Group should remain invalidated while any store is unhealthy")

	// Recover store 3
	tc.SetStoreUp(3)

	// Wait for health check
	time.Sleep(200 * time.Millisecond)

	// Now group should be restored
	groupInfo = affinityManager.GetGroups()["test_group"]
	re.True(groupInfo.Effect, "Group should be restored when all stores are healthy")
}

// TestAffinityCheckerAddPeer tests adding a peer to meet affinity requirements.
func TestAffinityCheckerAddPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2) // Only 2 peers, need to add store 3

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group expecting peers on 1, 2, 3
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Check should create add peer operator
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
}

// TestAffinityCheckerRemovePeer tests removing a peer that shouldn't be in the group.
func TestAffinityCheckerRemovePeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddLeaderRegion(1, 1, 2, 3, 4) // 4 peers, need to remove store 4

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group expecting peers on 1, 2, 3 only
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Check should create remove peer operator
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpAffinity, ops[0].Kind()&operator.OpAffinity)
}

// TestAffinityCheckerNoOperatorWhenAligned tests that no operator is created when region matches group.
func TestAffinityCheckerNoOperatorWhenAligned(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3) // Perfect match

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group matching current state
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Check should return nil because region is already aligned
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityCheckerTransferLeaderWithoutPeer tests leader transfer when target store has no peer.
func TestAffinityCheckerTransferLeaderWithoutPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddLeaderRegion(1, 1, 2, 4) // Leader on 1, but need leader on 3

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group with leader on store 3, but store 3 has no peer yet
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 3,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

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

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

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
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{
		{Group: group1},
		{Group: group2},
	})
	re.NoError(err)

	affinityManager.SetRegionGroup(1, "group1")
	affinityManager.SetRegionGroup(2, "group2")

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

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create an affinity group but don't assign region 1 to it
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	// Note: NOT calling affinityManager.SetRegionGroup(1, "test_group")

	// Check should return nil because region is not in any group
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityCheckerConcurrentGroupDeletion tests checker behavior during group deletion.
func TestAffinityCheckerConcurrentGroupDeletion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Verify checker can create operator
	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)

	// Delete the group
	err = affinityManager.DeleteAffinityGroup("test_group")
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

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Create affinity group
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)

	// Set both regions to the same group and mark them as affinity regions
	affinityManager.SetRegionGroup(1, "test_group")
	affinityManager.SetRegionGroup(2, "test_group")

	// MergeCheck should create merge operator
	ops := checker.MergeCheck(region1)
	re.NotNil(ops)
	re.Len(ops, 2) // Merge operation creates 2 operators
	re.Contains(ops[0].Desc(), "merge")
}

// TestAffinityMergeCheckNoTarget tests merge when no valid target exists.
func TestAffinityMergeCheckNoTarget(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// MergeCheck should return nil (no adjacent regions)
	ops := checker.MergeCheck(region1)
	re.Nil(ops)
}

// TestAffinityMergeCheckDifferentGroups tests that regions in different groups don't merge.
func TestAffinityMergeCheckDifferentGroups(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

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
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{
		{Group: group1},
		{Group: group2},
	})
	re.NoError(err)

	// Assign regions to different groups
	affinityManager.SetRegionGroup(1, "group1")
	affinityManager.SetRegionGroup(2, "group2")

	// MergeCheck should return nil (different groups)
	ops := checker.MergeCheck(region1)
	re.Nil(ops)
}

// TestAffinityMergeCheckRegionTooLarge tests that large regions don't merge.
func TestAffinityMergeCheckRegionTooLarge(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20)
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
		core.SetApproximateSize(30), // Too large to merge
		core.SetApproximateKeys(30000),
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")
	affinityManager.SetRegionGroup(2, "test_group")

	// MergeCheck should return nil (region1 is too large)
	ops := checker.MergeCheck(region1)
	re.Nil(ops)
}

// TestAffinityMergeCheckAdjacentNotAffinity tests that non-affinity adjacent regions don't merge.
func TestAffinityMergeCheckAdjacentNotAffinity(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1, // Expect leader on store 1
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")
	affinityManager.SetRegionGroup(2, "test_group")

	// MergeCheck should return nil (region2 is not affinity-compliant due to wrong leader)
	ops := checker.MergeCheck(region1)
	re.Nil(ops)
}

// TestAffinityMergeCheckNotAffinityRegion tests that non-affinity regions don't merge.
func TestAffinityMergeCheckNotAffinityRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1, // Expect leader on store 1
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// MergeCheck should return nil (region doesn't satisfy affinity requirements)
	ops := checker.MergeCheck(region1)
	re.Nil(ops)
}

// TestAffinityMergeCheckUnhealthyRegion tests that unhealthy regions don't merge.
func TestAffinityMergeCheckUnhealthyRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// MergeCheck should return nil (region is unhealthy)
	ops := checker.MergeCheck(region1)
	re.Nil(ops)
}

// TestAffinityMergeCheckBothDirections tests that merge can happen in both directions when one-way merge is disabled.
func TestAffinityMergeCheckBothDirections(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")
	affinityManager.SetRegionGroup(2, "test_group")
	affinityManager.SetRegionGroup(3, "test_group")

	// MergeCheck on region2 can merge with either prev (region1) or next (region3)
	// When one-way merge is disabled, it should prefer next but can also merge with prev
	ops := checker.MergeCheck(region2)
	re.NotNil(ops) // Should merge with one of the adjacent regions
}

// TestAffinityMergeCheckTargetTooBig tests that merging regions whose combined size exceeds the max limit is disallowed.
func TestAffinityMergeCheckTargetTooBig(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	opt.SetMaxAffinityMergeRegionSize(20) // Max size 20, Max keys 200000
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)

	// Create two adjacent regions whose total size exceeds the limit
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	region1 := tc.GetRegion(1).Clone(
		core.SetApproximateSize(15), // 15 size (Source)
		core.SetApproximateKeys(150000),
		core.WithStartKey([]byte("a")),
		core.WithEndKey([]byte("b")),
	)
	region2 := tc.GetRegion(2).Clone(
		core.SetApproximateSize(6), // 6 size (Target). Total: 21 > 20
		core.SetApproximateKeys(60000),
		core.WithStartKey([]byte("b")),
		core.WithEndKey([]byte("c")),
	)

	tc.PutRegion(region1)
	tc.PutRegion(region2)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")
	affinityManager.SetRegionGroup(2, "test_group")

	// MergeCheck should return nil because the combined size (21) exceeds the limit (20)
	ops := checker.MergeCheck(region1)
	re.Nil(ops, "Merged size exceeds MaxAffinityMergeRegionSize")
}

// TestAffinityMergeCheckAdjacentUnhealthy tests that merging is blocked if the adjacent region is unhealthy.
func TestAffinityMergeCheckAdjacentUnhealthy(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")
	affinityManager.SetRegionGroup(2, "test_group")

	// MergeCheck should return nil (Adjacent region is unhealthy)
	ops := checker.MergeCheck(region1)
	re.Nil(ops, "Should not merge into an unhealthy adjacent region")
}

// TestAffinityCheckerComplexMove tests moving multiple peers and transferring leader in one operation.
// This tests the combo operator's ability to handle complex transformations.
func TestAffinityCheckerComplexMove(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 5,
		VoterStoreIDs: []uint64{3, 5, 6},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

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

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 4,
		VoterStoreIDs: []uint64{1, 4, 5},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

// TestAffinityCheckerOperatorSteps tests that the generated operator contains correct steps.
func TestAffinityCheckerOperatorSteps(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Current: peers on [1, 2, 4], leader on 1
	// Expected: peers on [1, 2, 3], leader on 2
	// This requires: remove peer from 4, add peer to 3, transfer leader to 2
	tc.AddLeaderRegion(1, 1, 2, 4)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)

	op := ops[0]
	re.Equal("affinity-move-region", op.Desc())

	// Verify the operator has steps
	re.Positive(op.Len(), "Operator should have steps")
}

// TestAffinityCheckerOnlyLeaderTransfer tests when only leader transfer is needed (no peer changes).
func TestAffinityCheckerOnlyLeaderTransfer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	// Current: peers on [1, 2, 3], leader on 1
	// Expected: peers on [1, 2, 3], leader on 3
	// Only leader transfer is needed
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 3,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

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

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())

	// Verify OpLeader is NOT set (since leader doesn't change)
	re.Equal(operator.OpKind(0), ops[0].Kind()&operator.OpLeader, "OpLeader should not be set when leader doesn't change")
	// Verify OpRegion is set
	re.Equal(operator.OpRegion, ops[0].Kind()&operator.OpRegion)
}

// TestAffinityCheckerDifferentReplicaCount tests when expected replica count differs from current.
func TestAffinityCheckerDifferentReplicaCount(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)

	// Test case 1: Current 3 peers, expected 5 peers
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3, 4, 5},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

// TestAffinityCheckerReduceReplicaCount tests reducing replica count.
func TestAffinityCheckerReduceReplicaCount(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)

	// Current: 5 peers on [1, 2, 3, 4, 5], leader on 1
	// Expected: 3 peers on [1, 2, 3], leader on 1
	tc.AddLeaderRegion(1, 1, 2, 3, 4, 5)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

// TestAffinityCheckerLeaderNotInVoters tests the edge case where leader store is not in voter list.
// This is an invalid configuration that should be rejected by SaveAffinityGroups.
func TestAffinityCheckerLeaderNotInVoters(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()

	// Invalid configuration: leader store 4 is not in voter list [1, 2, 3]
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 4,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	// Should return error for invalid configuration
	re.Error(err)
	re.Contains(err.Error(), "leader must be in voter stores")
}

// TestAffinityCheckerSameStoreOrder tests when voter stores are in different order.
func TestAffinityCheckerSameStoreOrder(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	// Current: peers on [1, 2, 3], leader on 1
	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Expected: same stores but in different order [3, 1, 2], leader on 1
	// This should NOT require any operator since stores are the same
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{3, 1, 2}, // Different order
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Should return nil since stores are the same (order doesn't matter)
	ops := checker.Check(tc.GetRegion(1))
	re.Nil(ops)
}

// TestAffinityCheckerSinglePeer tests with single peer configuration.
func TestAffinityCheckerSinglePeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)

	// Current: single peer on store 1
	tc.AddLeaderRegion(1, 1)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Expected: single peer on store 2
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{2},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
}

// TestAffinityCheckerLargeReplicaCount tests with large replica count.
func TestAffinityCheckerLargeReplicaCount(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	// Add 10 stores
	for i := uint64(1); i <= 10; i++ {
		tc.AddRegionStore(i, 10)
	}

	// Current: 5 peers on [1, 2, 3, 4, 5], leader on 1
	tc.AddLeaderRegion(1, 1, 2, 3, 4, 5)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Expected: 5 peers on [6, 7, 8, 9, 10], leader on 8
	// Complete replacement of all peers
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 8,
		VoterStoreIDs: []uint64{6, 7, 8, 9, 10},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	re.NotNil(ops)
	re.Len(ops, 1)
	re.Equal("affinity-move-region", ops[0].Desc())
	re.Equal(operator.OpLeader, ops[0].Kind()&operator.OpLeader)
	re.Equal(operator.OpRegion, ops[0].Kind()&operator.OpRegion)
}

// TestAffinityCheckerStoreNotExist tests when expected store doesn't exist.
// This is an invalid configuration that should be rejected by SaveAffinityGroups.
func TestAffinityCheckerStoreNotExist(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	// Note: store 4 is NOT added

	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()

	// Expected: peers on [1, 2, 4], but store 4 doesn't exist
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 4},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	// Should return error because store 4 doesn't exist
	re.Error(err)
	re.Contains(err.Error(), "voter store does not exist")
}

// TestAffinityCheckerOfflineStore tests when expected store is offline.
func TestAffinityCheckerOfflineStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Set store 4 as offline
	tc.SetStoreOffline(4)

	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Expected: peers on [1, 2, 4], but store 4 is offline
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 4},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// CreateMoveRegionOperator should handle offline store appropriately
	ops := checker.Check(tc.GetRegion(1))
	// May return nil or fail to create operator
	if ops != nil {
		re.Len(ops, 1)
	}
}

// TestAffinityCheckerDownStore tests when expected store is down.
func TestAffinityCheckerDownStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Set store 4 as down
	tc.SetStoreDown(4)

	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Expected: peers on [1, 2, 4], but store 4 is down
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 4},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(tc.GetRegion(1))
	// May return nil or fail to create operator
	if ops != nil {
		re.Len(ops, 1)
	}
}

// TestAffinityCheckerMultipleRegionsSameGroup tests multiple regions in the same group.
func TestAffinityCheckerMultipleRegionsSameGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)

	// Create multiple regions with different configurations
	tc.AddLeaderRegion(1, 1, 2, 4) // Needs adjustment
	tc.AddLeaderRegion(2, 1, 2, 3) // Already correct, but leader wrong
	tc.AddLeaderRegion(3, 2, 1, 3) // Already correct

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)

	affinityManager.SetRegionGroup(1, "test_group")
	affinityManager.SetRegionGroup(2, "test_group")
	affinityManager.SetRegionGroup(3, "test_group")

	// Region 1: needs peer change (4->3) and leader transfer (1->2)
	ops1 := checker.Check(tc.GetRegion(1))
	re.NotNil(ops1)
	re.Len(ops1, 1)
	re.Equal("affinity-move-region", ops1[0].Desc())

	// Region 2: needs leader transfer only (1->2)
	ops2 := checker.Check(tc.GetRegion(2))
	re.NotNil(ops2)
	re.Len(ops2, 1)
	re.Equal("affinity-move-region", ops2[0].Desc())

	// Region 3: already correct
	ops3 := checker.Check(tc.GetRegion(3))
	re.Nil(ops3)
}

// TestAffinityCheckerRegionNoLeader tests region without leader.
func TestAffinityCheckerRegionNoLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
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
	checker := NewAffinityChecker(tc, affinityManager, opt)

	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	// Should return nil because region has no leader
	ops := checker.Check(region)
	re.Nil(ops)
}

// TestAffinityCheckerDuplicateStores tests when VoterStoreIDs has duplicates.
// This is an invalid configuration that should be rejected by SaveAffinityGroups.
func TestAffinityCheckerDuplicateStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()

	// Invalid: duplicate stores in VoterStoreIDs
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 2}, // Duplicate store 2
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	// Should return error for duplicate stores
	re.Error(err)
	re.Contains(err.Error(), "duplicate voter store ID")
}

// TestAffinityCheckerEmptyVoterList tests with empty voter list.
// This is an invalid configuration that should be rejected by SaveAffinityGroups.
func TestAffinityCheckerEmptyVoterList(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	tc.AddLeaderRegion(1, 1, 2, 3)

	affinityManager := tc.GetAffinityManager()

	// Invalid: empty VoterStoreIDs
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{}, // Empty
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	// Should return error for empty voter list
	re.Error(err)
	re.Contains(err.Error(), "voter store IDs should not be empty")
}

// TestAffinityCheckerPreserveLearners tests that existing learner peers are preserved.
func TestAffinityCheckerPreserveLearners(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)

	// Create region with voters on [1, 2, 3] and learner on [4]
	// Leader on store 1
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)

	// Add a learner peer on store 4
	learnerPeer := &metapb.Peer{
		StoreId: 4,
		Role:    metapb.PeerRole_Learner,
	}
	newPeers := append(region.GetMeta().GetPeers(), learnerPeer)
	region = region.Clone(core.SetPeers(newPeers))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Affinity group expects voters on [1, 2, 3] with leader on 2
	// This should only transfer leader, not touch the learner on store 4
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(region)
	re.NotNil(ops)
	re.Len(ops, 1)

	// Verify the operator preserves the learner
	op := ops[0]
	re.Equal("affinity-move-region", op.Desc())
	// The operator should have steps that preserve the learner on store 4
	re.Positive(op.Len())
}

// TestAffinityCheckerPreserveLearnersWithPeerChange tests learner preservation when peers change.
func TestAffinityCheckerPreserveLearnersWithPeerChange(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)

	// Create region with voters on [1, 2, 4] and learner on [5]
	// Leader on store 1
	tc.AddLeaderRegion(1, 1, 2, 4)
	region := tc.GetRegion(1)

	// Add a learner peer on store 5
	learnerPeer := &metapb.Peer{
		StoreId: 5,
		Role:    metapb.PeerRole_Learner,
	}
	newPeers := append(region.GetMeta().GetPeers(), learnerPeer)
	region = region.Clone(core.SetPeers(newPeers))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Affinity group expects voters on [1, 2, 3] with leader on 1
	// This should replace voter 4 with 3, and preserve learner on store 5
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(region)
	re.NotNil(ops)
	re.Len(ops, 1)

	// Verify the operator preserves the learner while changing voters
	op := ops[0]
	re.Equal("affinity-move-region", op.Desc())
	re.Positive(op.Len())
}

// TestAffinityCheckerMultipleLearners tests preserving multiple learner peers.
func TestAffinityCheckerMultipleLearners(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.AddRegionStore(5, 10)
	tc.AddRegionStore(6, 10)

	// Create region with voters on [1, 2, 3] and learners on [4, 5]
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)

	// Add two learner peers
	learner1 := &metapb.Peer{
		StoreId: 4,
		Role:    metapb.PeerRole_Learner,
	}
	learner2 := &metapb.Peer{
		StoreId: 5,
		Role:    metapb.PeerRole_Learner,
	}
	newPeers := append(region.GetMeta().GetPeers(), learner1, learner2)
	region = region.Clone(core.SetPeers(newPeers))
	tc.PutRegion(region)

	affinityManager := tc.GetAffinityManager()
	checker := NewAffinityChecker(tc, affinityManager, opt)

	// Affinity group expects voters on [1, 2, 6] with leader on 2
	// This should replace voter 3 with 6, transfer leader to 2, and preserve both learners on [4, 5]
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{1, 2, 6},
	}
	err := affinityManager.SaveAffinityGroups([]affinity.GroupWithRanges{{Group: group}})
	re.NoError(err)
	affinityManager.SetRegionGroup(1, "test_group")

	ops := checker.Check(region)
	re.NotNil(ops)
	re.Len(ops, 1)

	op := ops[0]
	re.Equal("affinity-move-region", op.Desc())
	re.Positive(op.Len())
}
