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
	re.Equal("affinity-transfer-leader", ops[0].Desc())
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
	re.Equal("affinity-move-peer", ops[0].Desc())
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
	re.Equal("affinity-transfer-leader", ops[0].Desc())
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
	re.Equal("affinity-transfer-leader", ops[0].Desc())

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
	re.Equal("affinity-transfer-leader", ops[0].Desc())
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
	re.Equal("affinity-add-peer", ops[0].Desc())
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
	re.Equal("affinity-remove-peer", ops[0].Desc())
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
	re.Equal("affinity-move-peer", ops[0].Desc())
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
	re.Equal("affinity-transfer-leader", ops1[0].Desc())

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
