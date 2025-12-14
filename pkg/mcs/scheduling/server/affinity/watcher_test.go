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
	"sync"
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

func TestAffinityWatcherLifecycle(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	affinityManager, watcher, err := setupAffinityManager(ctx, client)
	re.NoError(err)
	defer watcher.Close()

	// Step 1: Create an affinity group
	testGroup := makeTestGroup("test-group-lifecycle", 1, []uint64{1, 2, 3})
	err = putGroup(ctx, client, testGroup)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil &&
			groupState.LeaderStoreID == 1 &&
			len(groupState.VoterStoreIDs) == 3
	})

	// Step 2: Add a label rule
	labelRule := makeTestLabelRule(testGroup.ID, "7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8")
	err = putLabelRule(ctx, client, labelRule)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount == 1
	})

	// Step 3: Update the label rule (add more ranges)
	labelRule = makeTestLabelRule(testGroup.ID,
		"7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8",
		"7480000000000000ff2000000000000000f8", "7480000000000000ff3000000000000000f8",
	)
	err = putLabelRule(ctx, client, labelRule)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount == 2
	})

	// Step 4: Update the group (change leader and voters)
	testGroup.LeaderStoreID = 2
	testGroup.VoterStoreIDs = []uint64{2, 3, 4}
	err = putGroup(ctx, client, testGroup)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil &&
			groupState.LeaderStoreID == 2 &&
			len(groupState.VoterStoreIDs) == 3 &&
			groupState.VoterStoreIDs[0] == 2 &&
			groupState.RangeCount == 2
	})

	// Step 5: Delete the label rule
	_, err = client.Delete(ctx, keypath.RegionLabelKeyPath(labelRule.ID))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount == 0
	})

	// Step 6: Delete the affinity group
	_, err = client.Delete(ctx, keypath.AffinityGroupPath(testGroup.ID))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !affinityManager.IsGroupExist(testGroup.ID)
	})
}

// TestOnlyProcessAffinityGroupLabelRules verifies that only label rules with "affinity_group/" prefix are processed.
// Even if the watcher receives events for other prefixes (e.g., "affinity_group_v2/"), the business logic should filter them out.
func TestOnlyProcessAffinityGroupLabelRules(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	affinityManager, watcher, err := setupAffinityManager(ctx, client)
	re.NoError(err)
	defer watcher.Close()

	testGroup := makeTestGroup("test-group", 1, []uint64{1, 2, 3})
	re.NoError(putGroup(ctx, client, testGroup))

	testutil.Eventually(re, func() bool {
		return affinityManager.IsGroupExist(testGroup.ID)
	})

	// Write a label rule with non-standard prefix "affinity_group_v2/"
	// Watcher will receive this event (matches etcd prefix /pd/0/region_label/affinity_group*),
	// but business logic should filter it out (ID doesn't start with "affinity_group/")
	nonStandardRule := &labeler.LabelRule{
		ID:       "affinity_group_v2/" + testGroup.ID,
		Labels:   []labeler.RegionLabel{{Key: "affinity_group", Value: testGroup.ID}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges("7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8"),
	}

	labelKey := keypath.RegionLabelKeyPath(nonStandardRule.ID)
	labelValue, err := json.Marshal(nonStandardRule)
	re.NoError(err)
	_, err = client.Put(ctx, labelKey, string(labelValue))
	re.NoError(err)

	time.Sleep(100 * time.Millisecond)

	groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(0, groupState.RangeCount)

	affinityRule := makeTestLabelRule(testGroup.ID, "7480000000000000ff2000000000000000f8", "7480000000000000ff3000000000000000f8")
	re.NoError(putLabelRule(ctx, client, affinityRule))

	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount == 1
	})
}

// TestWatcherLoadExistingData tests that the watcher can load data that already exists in etcd before it starts.
func TestWatcherLoadExistingData(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	testGroupID := "test-preload"

	// Step 1: Write group and label rule to etcd BEFORE creating the watcher
	testGroup := makeTestGroup(testGroupID, 1, []uint64{1, 2, 3})
	err := putGroup(ctx, client, testGroup)
	re.NoError(err)

	labelRule := makeTestLabelRule(testGroupID, "7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8")
	err = putLabelRule(ctx, client, labelRule)
	re.NoError(err)

	// Step 2: NOW create the affinity manager and watcher
	affinityManager, watcher, err := setupAffinityManager(ctx, client)
	re.NoError(err)
	defer watcher.Close()

	// Step 3: Verify the watcher loaded the existing data
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount > 0
	})

	groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
	re.NotNil(groupState)
	re.Equal(uint64(1), groupState.LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, groupState.VoterStoreIDs)
	re.Equal(1, groupState.RangeCount)
}

// TestConcurrentLabelRuleUpdates tests multiple label rule updates happening concurrently.
func TestConcurrentLabelRuleUpdates(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	affinityManager, watcher, err := setupAffinityManager(ctx, client)
	re.NoError(err)
	defer watcher.Close()

	// Step 1: Create multiple groups
	groupCount := 5
	groups := make([]*affinity.Group, groupCount)
	for i := range groupCount {
		groups[i] = makeTestGroup(
			"test-concurrent-"+string(rune('a'+i)),
			uint64(i+1),
			[]uint64{uint64(i + 1), uint64(i + 2), uint64(i + 3)},
		)
		err = putGroup(ctx, client, groups[i])
		re.NoError(err)
	}
	testutil.Eventually(re, func() bool {
		for i := range groupCount {
			if !affinityManager.IsGroupExist(groups[i].ID) {
				return false
			}
		}
		return true
	})

	// Step 2: Concurrently create label rules for all groups
	var wg sync.WaitGroup
	for i := range groupCount {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			labelRule := makeTestLabelRule(groups[idx].ID,
				"7480000000000000ff"+string(rune('0'+idx))+"000000000000000f8",
				"7480000000000000ff"+string(rune('0'+idx))+"100000000000000f8",
			)
			_ = putLabelRule(ctx, client, labelRule)
		}(i)
	}
	wg.Wait()
	testutil.Eventually(re, func() bool {
		for i := range groupCount {
			groupState := affinityManager.GetAffinityGroupState(groups[i].ID)
			if groupState == nil || groupState.RangeCount != 1 {
				return false
			}
		}
		return true
	})

	// Step 3: Concurrently update all label rules
	for i := range groupCount {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			labelRule := makeTestLabelRule(groups[idx].ID,
				"7480000000000000ff"+string(rune('0'+idx))+"000000000000000f8",
				"7480000000000000ff"+string(rune('0'+idx))+"100000000000000f8",
				"7480000000000000ff"+string(rune('0'+idx))+"200000000000000f8",
				"7480000000000000ff"+string(rune('0'+idx))+"300000000000000f8",
			)
			_ = putLabelRule(ctx, client, labelRule)
		}(i)
	}
	wg.Wait()

	testutil.Eventually(re, func() bool {
		for i := range groupCount {
			groupState := affinityManager.GetAffinityGroupState(groups[i].ID)
			if groupState == nil || groupState.RangeCount != 2 {
				return false
			}
		}
		return true
	})
}

// TestLabelRuleBeforeGroup tests the scenario where a label rule arrives before the group is created.
func TestLabelRuleBeforeGroup(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	affinityManager, watcher, err := setupAffinityManager(ctx, client)
	re.NoError(err)
	defer watcher.Close()

	testGroupID := "test-label-before-group"

	// Step 1: Write a label rule BEFORE creating the group
	affinityRule := makeTestLabelRule(testGroupID, "7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8")
	err = putLabelRule(ctx, client, affinityRule)
	re.NoError(err)

	testGroup := makeTestGroup(testGroupID, 1, []uint64{1, 2, 3})
	err = putGroup(ctx, client, testGroup)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount > 0
	})
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
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls, etcdutil.TestEtcdClientPurpose, true)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	return ctx, client, func() {
		cancel()
		etcd.Close()
		client.Close()
		os.RemoveAll(cfg.Dir)
	}
}

// setupAffinityManager creates and initializes an affinity manager and watcher.
func setupAffinityManager(ctx context.Context, client *clientv3.Client) (*affinity.Manager, *Watcher, error) {
	mgr, watcher, _, err := setupAffinityManagerWithComponents(ctx, client)
	return mgr, watcher, err
}

// setupAffinityManagerWithComponents creates and initializes an affinity manager and watcher, returning internal components.
func setupAffinityManagerWithComponents(ctx context.Context, client *clientv3.Client) (*affinity.Manager, *Watcher, *labeler.RegionLabeler, error) {
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	if err != nil {
		return nil, nil, nil, err
	}

	conf := mockconfig.NewTestOptions()
	affinityManager, err := affinity.NewManager(ctx, storage, basicCluster, conf, labelerManager)
	if err != nil {
		return nil, nil, nil, err
	}

	watcher, err := NewWatcher(ctx, client, affinityManager)
	if err != nil {
		return nil, nil, nil, err
	}

	return affinityManager, watcher, labelerManager, nil
}

// putGroup writes an affinity group to etcd.
func putGroup(ctx context.Context, client *clientv3.Client, group *affinity.Group) error {
	groupKey := keypath.AffinityGroupPath(group.ID)
	groupValue, err := json.Marshal(group)
	if err != nil {
		return err
	}
	_, err = client.Put(ctx, groupKey, string(groupValue))
	return err
}

// putLabelRule writes a label rule to etcd.
func putLabelRule(ctx context.Context, client *clientv3.Client, labelRule *labeler.LabelRule) error {
	labelKey := keypath.RegionLabelKeyPath(labelRule.ID)
	labelValue, err := json.Marshal(labelRule)
	if err != nil {
		return err
	}
	_, err = client.Put(ctx, labelKey, string(labelValue))
	return err
}

// makeTestGroup creates a test affinity group with the given ID.
func makeTestGroup(groupID string, leaderStoreID uint64, voterStoreIDs []uint64) *affinity.Group {
	return &affinity.Group{
		ID:              groupID,
		CreateTimestamp: uint64(time.Now().Unix()),
		LeaderStoreID:   leaderStoreID,
		VoterStoreIDs:   voterStoreIDs,
	}
}

// makeTestLabelRule creates a test label rule with the given ID and key ranges.
func makeTestLabelRule(groupID string, keyRanges ...string) *labeler.LabelRule {
	return &labeler.LabelRule{
		ID:       affinity.LabelRuleIDPrefix + groupID,
		Labels:   []labeler.RegionLabel{{Key: "affinity_group", Value: groupID}},
		RuleType: labeler.KeyRange,
		Data:     labeler.MakeKeyRanges(keyRanges...),
	}
}
