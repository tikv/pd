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

package affinity

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestAffinityWatcher(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	// Create basic cluster and affinity manager
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)

	// Create config for affinity manager
	conf := mockconfig.NewTestOptions()
	affinityManager, err := affinity.NewManager(ctx, storage, basicCluster, conf, labelerManager)
	re.NoError(err)

	// Create and start the watcher
	watcher, err := NewWatcher(ctx, client, affinityManager)
	re.NoError(err)
	defer watcher.Close()

	// Test 1: Write an affinity group to etcd and verify the watcher receives it
	testGroup := &affinity.Group{
		ID:              "test-group-1",
		CreateTimestamp: uint64(time.Now().Unix()),
		LeaderStoreID:   1,
		VoterStoreIDs:   []uint64{1, 2, 3},
	}

	groupKey := keypath.AffinityGroupPath(testGroup.ID)
	groupValue, err := json.Marshal(testGroup)
	re.NoError(err)

	_, err = client.Put(ctx, groupKey, string(groupValue))
	re.NoError(err)

	// Wait for the watcher to process the group creation event
	testutil.Eventually(re, func() bool {
		return affinityManager.IsGroupExist(testGroup.ID)
	})

	// Test 2: Write an affinity label rule to etcd
	labelRule := &labeler.LabelRule{
		ID:       affinity.LabelRuleIDPrefix + "test-group-1",
		Labels:   []labeler.RegionLabel{{Key: "affinity_group", Value: "test-group-1"}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges("7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8"),
	}

	labelKey := keypath.RegionLabelKeyPath(labelRule.ID)
	labelValue, err := json.Marshal(labelRule)
	re.NoError(err)

	_, err = client.Put(ctx, labelKey, string(labelValue))
	re.NoError(err)

	// Wait for label rule to be processed
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount > 0
	})

	// Verify the group has the label rule
	groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(1, groupState.RangeCount)

	// Test 3: Delete the label rule
	_, err = client.Delete(ctx, labelKey)
	re.NoError(err)

	// Wait for label rule deletion to be processed
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount == 0
	})

	// Verify the label rule was removed
	groupState = affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(0, groupState.RangeCount)

	// Test 4: Delete the affinity group
	_, err = client.Delete(ctx, groupKey)
	re.NoError(err)

	// Wait for the watcher to process the group deletion event
	testutil.Eventually(re, func() bool {
		return !affinityManager.IsGroupExist(testGroup.ID)
	})
}

func TestAffinityLabelFilter(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	// Create basic cluster and affinity manager
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)

	// Create config for affinity manager
	conf := mockconfig.NewTestOptions()
	affinityManager, err := affinity.NewManager(ctx, storage, basicCluster, conf, labelerManager)
	re.NoError(err)

	// Create and start the watcher
	watcher, err := NewWatcher(ctx, client, affinityManager)
	re.NoError(err)
	defer watcher.Close()

	// Create an affinity group first (needed for label rules to be processed)
	testGroup := &affinity.Group{
		ID:              "test-group-2",
		CreateTimestamp: uint64(time.Now().Unix()),
		LeaderStoreID:   1,
		VoterStoreIDs:   []uint64{1, 2, 3},
	}

	groupKey := keypath.AffinityGroupPath(testGroup.ID)
	groupValue, err := json.Marshal(testGroup)
	re.NoError(err)

	_, err = client.Put(ctx, groupKey, string(groupValue))
	re.NoError(err)

	// Wait for the watcher to process the group creation event
	testutil.Eventually(re, func() bool {
		return affinityManager.IsGroupExist(testGroup.ID)
	})

	// Write a non-affinity label rule (should be filtered out)
	nonAffinityRule := &labeler.LabelRule{
		ID:       "some-other-rule",
		Labels:   []labeler.RegionLabel{{Key: "zone", Value: "z1"}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges("7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8"),
	}

	labelKey := keypath.RegionLabelKeyPath(nonAffinityRule.ID)
	labelValue, err := json.Marshal(nonAffinityRule)
	re.NoError(err)

	_, err = client.Put(ctx, labelKey, string(labelValue))
	re.NoError(err)

	// Verify the non-affinity rule was filtered (RangeCount should still be 0)
	// Note: We don't need to wait here because if the filter failed,
	// the affinity rule write below would find RangeCount > 0 before we expect it.
	groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(0, groupState.RangeCount)

	// Write an affinity label rule (should be processed)
	affinityRule := &labeler.LabelRule{
		ID:       affinity.LabelRuleIDPrefix + "test-group-2",
		Labels:   []labeler.RegionLabel{{Key: "affinity_group", Value: "test-group-2"}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges("7480000000000000ff2000000000000000f8", "7480000000000000ff3000000000000000f8"),
	}

	affinityLabelKey := keypath.RegionLabelKeyPath(affinityRule.ID)
	affinityLabelValue, err := json.Marshal(affinityRule)
	re.NoError(err)

	_, err = client.Put(ctx, affinityLabelKey, string(affinityLabelValue))
	re.NoError(err)

	// Wait for affinity label rule to be processed
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount > 0
	})

	// Verify the affinity rule was processed
	groupState = affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(1, groupState.RangeCount)
}

func TestAffinityGroupUpdate(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	// Create basic cluster and affinity manager
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)

	// Create config for affinity manager
	conf := mockconfig.NewTestOptions()
	affinityManager, err := affinity.NewManager(ctx, storage, basicCluster, conf, labelerManager)
	re.NoError(err)

	// Create and start the watcher
	watcher, err := NewWatcher(ctx, client, affinityManager)
	re.NoError(err)
	defer watcher.Close()

	// Test 1: Create an affinity group
	testGroup := &affinity.Group{
		ID:              "test-group-update",
		CreateTimestamp: uint64(time.Now().Unix()),
		LeaderStoreID:   1,
		VoterStoreIDs:   []uint64{1, 2, 3},
	}

	groupKey := keypath.AffinityGroupPath(testGroup.ID)
	groupValue, err := json.Marshal(testGroup)
	re.NoError(err)

	_, err = client.Put(ctx, groupKey, string(groupValue))
	re.NoError(err)

	// Verify the group was created
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil &&
			groupState.LeaderStoreID == 1 &&
			len(groupState.VoterStoreIDs) == 3
	})

	groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(uint64(1), groupState.LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, groupState.VoterStoreIDs)

	// Test 2: Update the group (change leader and voters)
	testGroup.LeaderStoreID = 2
	testGroup.VoterStoreIDs = []uint64{2, 3, 4}

	groupValue, err = json.Marshal(testGroup)
	re.NoError(err)

	_, err = client.Put(ctx, groupKey, string(groupValue))
	re.NoError(err)

	// Verify the group was updated
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil &&
			groupState.LeaderStoreID == 2 &&
			len(groupState.VoterStoreIDs) == 3 &&
			groupState.VoterStoreIDs[0] == 2
	})

	groupState = affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(uint64(2), groupState.LeaderStoreID)
	re.Equal([]uint64{2, 3, 4}, groupState.VoterStoreIDs)

	// Test 3: Delete the group, then delete label rule (should not error)
	_, err = client.Delete(ctx, groupKey)
	re.NoError(err)

	// Verify the group was deleted
	testutil.Eventually(re, func() bool {
		return !affinityManager.IsGroupExist(testGroup.ID)
	})

	groupState = affinityManager.GetAffinityGroupState(testGroup.ID)
	re.Nil(groupState)

	// Now delete a label rule for the non-existent group (should not error)
	labelRuleKey := keypath.RegionLabelKeyPath(affinity.LabelRuleIDPrefix + testGroup.ID)
	_, err = client.Delete(ctx, labelRuleKey)
	re.NoError(err)
}

// TestLabelRuleBeforeGroup tests the scenario where a label rule arrives before the group is created.
// The group should automatically load the existing label rule when it's created.
func TestLabelRuleBeforeGroup(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	// Create basic cluster and affinity manager
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)

	// Create config for affinity manager
	conf := mockconfig.NewTestOptions()
	affinityManager, err := affinity.NewManager(ctx, storage, basicCluster, conf, labelerManager)
	re.NoError(err)

	// Create and start the watcher
	watcher, err := NewWatcher(ctx, client, affinityManager)
	re.NoError(err)
	defer watcher.Close()

	testGroupID := "test-label-before-group"

	// Step 1: Write a label rule BEFORE creating the group
	affinityRule := &labeler.LabelRule{
		ID:       affinity.LabelRuleIDPrefix + testGroupID,
		Labels:   []labeler.RegionLabel{{Key: "affinity_group", Value: testGroupID}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges("7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8"),
	}

	// Write to regionLabeler (which will also write to storage/etcd)
	err = labelerManager.SetLabelRule(affinityRule)
	re.NoError(err)

	// Also write to etcd for affinity watcher to see
	affinityLabelKey := keypath.RegionLabelKeyPath(affinityRule.ID)
	affinityLabelValue, err := json.Marshal(affinityRule)
	re.NoError(err)

	_, err = client.Put(ctx, affinityLabelKey, string(affinityLabelValue))
	re.NoError(err)

	// Step 2: Now create the group
	testGroup := &affinity.Group{
		ID:              testGroupID,
		CreateTimestamp: uint64(time.Now().Unix()),
		LeaderStoreID:   1,
		VoterStoreIDs:   []uint64{1, 2, 3},
	}

	groupKey := keypath.AffinityGroupPath(testGroup.ID)
	groupValue, err := json.Marshal(testGroup)
	re.NoError(err)

	_, err = client.Put(ctx, groupKey, string(groupValue))
	re.NoError(err)

	// Step 3: Verify the group was created AND has the label rule loaded
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		if groupState == nil {
			return false
		}
		// Check that the group has the label rule (RangeCount should be > 0)
		return groupState.RangeCount > 0
	})

	// Verify the group has the correct label rule
	groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(1, groupState.RangeCount)

	// Clean up
	_, err = client.Delete(ctx, groupKey)
	re.NoError(err)
	_, err = client.Delete(ctx, affinityLabelKey)
	re.NoError(err)
}

// TestGroupDeletionCleansKeyRanges tests that deleting a group properly cleans up its keyRanges cache.
func TestGroupDeletionCleansKeyRanges(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	// Create basic cluster and affinity manager
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)

	// Create config for affinity manager
	conf := mockconfig.NewTestOptions()
	affinityManager, err := affinity.NewManager(ctx, storage, basicCluster, conf, labelerManager)
	re.NoError(err)

	// Create and start the watcher
	watcher, err := NewWatcher(ctx, client, affinityManager)
	re.NoError(err)
	defer watcher.Close()

	testGroupID := "test-keyranges-cleanup"

	// Step 1: Create a group
	testGroup := &affinity.Group{
		ID:              testGroupID,
		CreateTimestamp: uint64(time.Now().Unix()),
		LeaderStoreID:   1,
		VoterStoreIDs:   []uint64{1, 2, 3},
	}

	groupKey := keypath.AffinityGroupPath(testGroup.ID)
	groupValue, err := json.Marshal(testGroup)
	re.NoError(err)

	_, err = client.Put(ctx, groupKey, string(groupValue))
	re.NoError(err)

	// Wait for group to be created
	testutil.Eventually(re, func() bool {
		return affinityManager.IsGroupExist(testGroup.ID)
	})

	// Step 2: Add a label rule to the group
	affinityRule := &labeler.LabelRule{
		ID:       affinity.LabelRuleIDPrefix + testGroupID,
		Labels:   []labeler.RegionLabel{{Key: "affinity_group", Value: testGroupID}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges("7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8"),
	}

	// Write to regionLabeler (which will also write to storage/etcd)
	err = labelerManager.SetLabelRule(affinityRule)
	re.NoError(err)

	// Also write to etcd for affinity watcher to see
	affinityLabelKey := keypath.RegionLabelKeyPath(affinityRule.ID)
	affinityLabelValue, err := json.Marshal(affinityRule)
	re.NoError(err)

	_, err = client.Put(ctx, affinityLabelKey, string(affinityLabelValue))
	re.NoError(err)

	// Wait for label to be processed
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount > 0
	})

	// Verify the group has the label rule
	groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(1, groupState.RangeCount)

	// Step 3: Delete the group (label rule still exists)
	_, err = client.Delete(ctx, groupKey)
	re.NoError(err)

	// Wait for group deletion to complete
	testutil.Eventually(re, func() bool {
		return !affinityManager.IsGroupExist(testGroup.ID)
	})

	// Step 4: Recreate the group with the same ID
	// If keyRanges wasn't properly cleaned up, the group should still pick up the existing label rule
	_, err = client.Put(ctx, groupKey, string(groupValue))
	re.NoError(err)

	// Wait for group to be recreated
	testutil.Eventually(re, func() bool {
		return affinityManager.IsGroupExist(testGroup.ID)
	})

	// Wait for label to be loaded
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount > 0
	})

	// Verify the recreated group has the label rule
	groupState = affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(1, groupState.RangeCount)

	// Clean up
	_, err = client.Delete(ctx, groupKey)
	re.NoError(err)
	_, err = client.Delete(ctx, affinityLabelKey)
	re.NoError(err)
}

func prepare(t require.TestingT) (context.Context, *clientv3.Client, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestSingleConfig()
	var err error
	cfg.Dir, err = os.MkdirTemp("", "pd_affinity_watcher_tests")
	re.NoError(err)
	os.RemoveAll(cfg.Dir)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	return ctx, client, func() {
		cancel()
		etcd.Close()
		client.Close()
		os.RemoveAll(cfg.Dir)
	}
}
