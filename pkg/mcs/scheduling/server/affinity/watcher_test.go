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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type targetWriterFunc func(context.Context, []affinity.GroupTarget) (affinity.GroupTargetsResponse, error)

func (f targetWriterFunc) WriteGroupTargets(ctx context.Context, targets []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
	return f(ctx, targets)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
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

	testutil.Eventually(re, func() bool {
		groupState := affinityManager.GetAffinityGroupState(testGroup.ID)
		return groupState != nil && groupState.RangeCount == 0
	})

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

	watcher.Close()
	affinityManager.RestoreAffinityGroup(testGroup.ID)
	re.True(affinityManager.GetAffinityGroupState(testGroup.ID).AffinitySchedulingAllowed)
	_, err = client.Put(ctx, keypath.RegionLabelKeyPath(labelRule.ID), "invalid JSON")
	re.NoError(err)
	_, err = NewWatcher(ctx, client, affinityManager)
	re.Error(err)
	// A failed label load must end the batch so later changes can reconcile.
	data, err := json.Marshal(makeTestLabelRule(testGroup.ID, "61", "64", "62", "63"))
	re.NoError(err)
	rule, err := labeler.NewLabelRuleFromJSON(data)
	re.NoError(err)
	re.NoError(affinityManager.SyncKeyRangesFromEtcd(rule))
	affinityManager.ReconcileRangeConflicts()
	re.False(affinityManager.GetAffinityGroupState(testGroup.ID).AffinitySchedulingAllowed)
}

func TestAffinityLabelWatcherLogRedact(t *testing.T) {
	const startKey, endKey = "7365637265742d7374617274", "7365637265742d7a"
	const secondStartKey, secondEndKey = "7365637265742d7a61", "7365637265742d7a62"
	ctx, client, clean := prepare(t)
	defer clean()

	for _, mode := range []struct {
		name string
		typ  logutil.RedactInfoLogType
	}{
		{"off", logutil.RedactInfoLogOFF},
		{"on", logutil.RedactInfoLogON},
		{"marker", logutil.RedactInfoLogMarker},
	} {
		t.Run(mode.name, func(t *testing.T) {
			re := require.New(t)
			_, err := client.Delete(ctx, keypath.AffinityGroupsPrefix(), clientv3.WithPrefix())
			re.NoError(err)
			_, err = client.Delete(ctx, keypath.RegionLabelPathPrefix(), clientv3.WithPrefix())
			re.NoError(err)
			fname := testutil.InitTempFileLogger(t, "debug")
			var (
				lg    *zap.Logger
				props *log.ZapProperties
			)
			re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, mode.typ))
			t.Cleanup(func() {
				re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, logutil.RedactInfoLogOFF))
			})
			readLog := func() string {
				output, err := os.ReadFile(fname)
				re.NoError(err)
				return string(output)
			}

			watchCtx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			group := makeTestGroup("redact-group", 1, []uint64{1, 2, 3})
			labelRule := makeTestLabelRule(group.ID, startKey, endKey)
			re.NoError(putGroup(ctx, client, group))
			re.NoError(putLabelRule(ctx, client, labelRule))
			manager, watcher, err := setupAffinityManager(watchCtx, client)
			re.NoError(err)
			closeWatcher := sync.OnceFunc(watcher.Close)
			t.Cleanup(closeWatcher)
			re.Equal(1, manager.GetAffinityGroupState(group.ID).RangeCount)

			// Exercise live updates as well as the initial snapshot logs.
			labelRule = makeTestLabelRule(group.ID, startKey, endKey, secondStartKey, secondEndKey)
			re.NoError(putLabelRule(ctx, client, labelRule))
			testutil.Eventually(re, func() bool {
				return manager.GetAffinityGroupState(group.ID).RangeCount == 2
			})

			closeWatcher()

			output := readLog()
			for _, msg := range []string{"update affinity label rule", "put successfully in watch loop when loading", "put successfully in watch loop"} {
				re.Contains(output, msg)
			}
			for _, key := range []string{startKey, endKey, secondStartKey, secondEndKey} {
				if mode.typ == logutil.RedactInfoLogON {
					re.NotContains(output, key)
				} else {
					re.Contains(output, key)
				}
			}
			switch mode.typ {
			case logutil.RedactInfoLogON:
				re.Contains(output, "[value=?]")
			case logutil.RedactInfoLogMarker:
				re.Contains(output, `[value="‹{`)
			}
			stored, err := client.Get(ctx, keypath.RegionLabelKeyPath(labelRule.ID))
			re.NoError(err)
			re.Len(stored.Kvs, 1)
			labelJSON, err := json.Marshal(labelRule)
			re.NoError(err)
			re.JSONEq(string(labelJSON), string(stored.Kvs[0].Value))
		})
	}
}

func TestAffinityLabelWatcherReloadsAfterCompaction(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	keepGroup := makeTestGroup("compaction-keep", 1, []uint64{1})
	deleteGroup := makeTestGroup("compaction-delete", 2, []uint64{2})
	keepRule := makeTestLabelRule(keepGroup.ID, "7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8")
	deleteRule := makeTestLabelRule(deleteGroup.ID, "7480000000000000ff2000000000000000f8", "7480000000000000ff3000000000000000f8")
	bufferedRule := makeTestLabelRule("compaction-buffered", "7480000000000000ff4000000000000000f8", "7480000000000000ff5000000000000000f8")
	re.NoError(putGroup(ctx, client, keepGroup))
	re.NoError(putGroup(ctx, client, deleteGroup))
	re.NoError(putLabelRule(ctx, client, keepRule))
	re.NoError(putLabelRule(ctx, client, deleteRule))
	re.NoError(putLabelRule(ctx, client, bufferedRule))

	manager, watcher, err := setupAffinityManager(ctx, client)
	re.NoError(err)
	failpointEnabled := false
	defer func() {
		if failpointEnabled {
			_ = failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock")
		}
		watcher.Close()
	}()

	re.Equal(1, manager.GetAffinityGroupState(keepGroup.ID).RangeCount)
	re.Equal(1, manager.GetAffinityGroupState(deleteGroup.ID).RangeCount)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)"))
	failpointEnabled = true

	updatedKeepRule := makeTestLabelRule(keepGroup.ID,
		"7480000000000000ff0000000000000000f8", "7480000000000000ff1000000000000000f8",
		"7480000000000000ff2000000000000000f8", "7480000000000000ff3000000000000000f8",
	)
	re.NoError(putLabelRule(ctx, client, updatedKeepRule))
	_, err = client.Delete(ctx, keypath.RegionLabelKeyPath(deleteRule.ID))
	re.NoError(err)
	deleteResp, err := client.Delete(ctx, keypath.RegionLabelKeyPath(bufferedRule.ID))
	re.NoError(err)
	_, err = client.Compact(ctx, deleteResp.Header.Revision)
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		watcher.labelWatcher.ForceLoad()
		keepState := manager.GetAffinityGroupState(keepGroup.ID)
		deleteState := manager.GetAffinityGroupState(deleteGroup.ID)
		return keepState != nil && keepState.RangeCount == 2 &&
			deleteState != nil && deleteState.RangeCount == 0
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"))
	failpointEnabled = false

	re.NoError(putGroup(ctx, client, makeTestGroup("compaction-buffered", 3, []uint64{3})))
	testutil.Eventually(re, func() bool {
		groupState := manager.GetAffinityGroupState("compaction-buffered")
		return groupState != nil && groupState.RangeCount == 0
	})
}

func TestAffinityGroupWatcherReloadsAfterCompaction(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	keepGroup := makeTestGroup("group-compaction-keep", 1, []uint64{1})
	deleteGroup := makeTestGroup("group-compaction-delete", 2, []uint64{2})
	re.NoError(putGroup(ctx, client, keepGroup))
	re.NoError(putGroup(ctx, client, deleteGroup))

	manager, watcher, err := setupAffinityManager(ctx, client)
	re.NoError(err)
	failpointEnabled := false
	defer func() {
		if failpointEnabled {
			_ = failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock")
		}
		watcher.Close()
	}()

	re.NotNil(manager.GetAffinityGroupState(keepGroup.ID))
	re.NotNil(manager.GetAffinityGroupState(deleteGroup.ID))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)"))
	failpointEnabled = true

	keepGroup.LeaderStoreID = 3
	re.NoError(putGroup(ctx, client, keepGroup))
	deleteResp, err := client.Delete(ctx, keypath.AffinityGroupPath(deleteGroup.ID))
	re.NoError(err)
	_, err = client.Compact(ctx, deleteResp.Header.Revision)
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		watcher.groupWatcher.ForceLoad()
		keepState := manager.GetAffinityGroupState(keepGroup.ID)
		return keepState != nil && keepState.LeaderStoreID == keepGroup.LeaderStoreID &&
			!manager.IsGroupExist(deleteGroup.ID)
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"))
	failpointEnabled = false
}

func TestAffinityWatcherSkipsEventsBeforePDConfirmation(t *testing.T) {
	re := require.New(t)
	ctx, client, _ := prepare(t)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	const checkInterval = "github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"
	re.NoError(failpoint.Enable(checkInterval, "return(true)"))
	defer func() { re.NoError(failpoint.Disable(checkInterval)) }()

	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 3; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
	}
	newManager := func(store *endpoint.StorageEndpoint) *affinity.Manager {
		labels, err := labeler.NewRegionLabeler(ctx, store, time.Hour)
		re.NoError(err)
		manager, err := affinity.NewManager(ctx, store, stores, mockconfig.NewTestOptions(), labels)
		re.NoError(err)
		return manager
	}
	group := makeTestGroup("confirmed", 1, []uint64{1, 2, 3})
	key := keypath.AffinityGroupPath(group.ID)
	re.NoError(putGroup(ctx, client, group))
	resp, err := client.Get(ctx, key)
	re.NoError(err)
	re.Len(resp.Kvs, 1)
	mcs := newManager(endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil))
	watcher := &Watcher{affinityManager: mcs}
	re.NoError(watcher.applyGroupPut(resp.Kvs[0]))
	testutil.Eventually(re, func() bool { return mcs.GetAffinityGroupState(group.ID).AffinitySchedulingAllowed })
	mcs.ExpireAffinityGroup(group.ID)

	// Consume the actual etcd watch events after delivering PD's confirmation.
	events := client.Watch(ctx, key, clientv3.WithRev(resp.Header.Revision+1))
	group.LeaderStoreID = 3
	re.NoError(putGroup(ctx, client, group))
	_, err = client.Delete(ctx, key)
	re.NoError(err)
	group.LeaderStoreID = 2
	re.NoError(putGroup(ctx, client, group))
	stored, err := client.Get(ctx, key)
	re.NoError(err)
	re.Len(stored.Kvs, 1)
	pd := newManager(endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil))

	const barrierID = "sender-barrier"
	barrier := make(chan struct{})
	reply := make(chan affinity.GroupTargetsResponse, 1)
	allowReply := make(chan struct{})
	mcs.SyncGroupFromEtcd(&affinity.Group{ID: barrierID})
	mcs.SetTargetWriter(targetWriterFunc(func(ctx context.Context, targets []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
		if targets[0].Expected.ID == barrierID {
			close(barrier)
			return affinity.GroupTargetsResponse{}, nil
		}
		result, err := pd.UpdateAffinityGroupTargets(ctx, targets)
		reply <- result
		select {
		case <-allowReply:
		case <-ctx.Done():
		}
		return result, err
	}))
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[1])
	mcs.ObserveAvailableRegion(region, mcs.GetAffinityGroupState(group.ID))
	select {
	case result := <-reply:
		re.Equal([]string{group.ID}, result.WrittenGroupIDs)
		re.Equal(stored.Kvs[0].ModRevision, result.WrittenRevisions[group.ID])
	case <-ctx.Done():
		t.Fatal("timed out waiting for the target confirmation")
	}
	mcs.ObserveAvailableRegion(region, mcs.GetAffinityGroupState(barrierID))
	close(allowReply)
	select {
	case <-barrier:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the sender to process the confirmation")
	}

	for applied := 0; applied < 3; {
		select {
		case batch := <-events:
			re.NoError(batch.Err())
			for _, event := range batch.Events {
				if event.Type == clientv3.EventTypeDelete {
					re.NoError(watcher.applyGroupDelete(event.Kv))
				} else {
					re.NoError(watcher.applyGroupPut(event.Kv))
				}
				state := mcs.GetAffinityGroupState(group.ID)
				re.NotNil(state)
				if event.Kv.ModRevision < stored.Kvs[0].ModRevision {
					re.Equal(uint64(1), state.LeaderStoreID)
					re.False(state.AffinitySchedulingAllowed)
				} else {
					re.Equal(*group, state.Group)
					re.True(state.AffinitySchedulingAllowed)
				}
				applied++
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for the affinity watch events")
		}
	}
	after, err := client.Get(ctx, key)
	re.NoError(err)
	re.Equal(stored.Kvs, after.Kvs, "confirming an already stored target must not rewrite it")
}

func TestAffinityWatcherReconcilesLocalTargets(t *testing.T) {
	re := require.New(t)
	ctx, client, _ := prepare(t)
	const checkInterval = "github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"
	re.NoError(failpoint.Enable(checkInterval, "return(true)"))
	defer func() { re.NoError(failpoint.Disable(checkInterval)) }()

	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 2; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
	}
	newManager := func(store *endpoint.StorageEndpoint) *affinity.Manager {
		labels, err := labeler.NewRegionLabeler(ctx, store, time.Hour)
		re.NoError(err)
		manager, err := affinity.NewManager(ctx, store, stores, mockconfig.NewTestOptions(), labels)
		re.NoError(err)
		return manager
	}
	const reloadID, adminID = "reload-target", "admin-target"
	read := func(id string) *mvccpb.KeyValue {
		resp, err := client.Get(ctx, keypath.AffinityGroupPath(id))
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		return resp.Kvs[0]
	}
	initial := make(map[string]*mvccpb.KeyValue, 2)
	expected := make(map[string]affinity.GroupTarget, 2)
	for _, id := range []string{reloadID, adminID} {
		group := makeTestGroup(id, 2, []uint64{1, 2})
		re.NoError(putGroup(ctx, client, group))
		initial[id] = read(id)
		expected[id] = affinity.GroupTarget{
			Expected: *group, ExpectedModRevision: initial[id].ModRevision,
			LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2},
		}
	}

	pd := newManager(endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil))
	manager := newManager(endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil))
	var upgraded atomic.Bool
	sent := make(chan affinity.GroupTarget, 2)
	allowAdminRetry := make(chan struct{})
	adminDone := make(chan struct{})
	var adminWritten affinity.GroupTargetsResponse
	var adminErr error
	manager.SetTargetWriter(targetWriterFunc(func(ctx context.Context, targets []affinity.GroupTarget) (affinity.GroupTargetsResponse, error) {
		if !upgraded.Load() {
			return affinity.GroupTargetsResponse{}, affinity.ErrTargetWriterUnsupported
		}
		target := targets[0]
		select {
		case sent <- target:
		case <-ctx.Done():
			return affinity.GroupTargetsResponse{}, ctx.Err()
		}
		if target.Expected.ID == adminID {
			select {
			case <-allowAdminRetry:
			case <-ctx.Done():
				return affinity.GroupTargetsResponse{}, ctx.Err()
			}
			adminWritten, adminErr = pd.UpdateAffinityGroupTargets(ctx, targets)
			close(adminDone)
			return adminWritten, adminErr
		}
		return pd.UpdateAffinityGroupTargets(ctx, targets)
	}))
	startWatcher := func() *Watcher {
		watcher, err := NewWatcher(ctx, client, manager)
		re.NoError(err)
		return watcher
	}
	watcher := startWatcher()
	defer func() { watcher.Close() }()
	state := manager.GetAffinityGroupState
	testutil.Eventually(re, func() bool { return state(reloadID).AffinitySchedulingAllowed })
	store2 := stores.GetStore(2)
	stores.PutStore(store2.Clone(core.PauseLeaderTransfer(constant.In)))
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	observe := func(id string) { manager.ObserveAvailableRegion(region, state(id)) }
	for _, id := range []string{reloadID, adminID} {
		testutil.Eventually(re, func() bool {
			observe(id)
			group := state(id)
			return group.HasUnpersistedTarget() && group.LeaderStoreID == 1
		})
	}

	watcher.Close()
	// Advance the cluster revision without changing either group's key.
	marker, err := client.Put(ctx, "affinity-watcher-compaction-marker", "")
	re.NoError(err)
	_, err = client.Compact(ctx, marker.Header.Revision)
	re.NoError(err)
	watcher = startWatcher()
	local := state(reloadID)
	re.True(local.HasUnpersistedTarget())
	re.Equal(uint64(1), local.LeaderStoreID)

	// Hold the upgrade retry until the same-peers PUT reaches the watcher.
	upgraded.Store(true)
	var retry affinity.GroupTarget
	testutil.Eventually(re, func() bool {
		observe(adminID)
		select {
		case retry = <-sent:
			return true
		default:
			return false
		}
	})
	re.Equal(expected[adminID], retry)
	admin := expected[adminID].Expected
	stores.PutStore(store2)
	testutil.Eventually(re, func() bool {
		_, err := pd.UpdateAffinityGroupPeers(adminID, admin.LeaderStoreID, admin.VoterStoreIDs)
		return err == nil
	})
	reasserted := read(adminID)
	re.Equal(initial[adminID].Value, reasserted.Value)
	re.Greater(reasserted.ModRevision, initial[adminID].ModRevision)
	testutil.Eventually(re, func() bool {
		group := state(adminID)
		return !group.HasUnpersistedTarget() && group.AffinitySchedulingAllowed && group.LeaderStoreID == admin.LeaderStoreID
	})
	close(allowAdminRetry)
	select {
	case <-adminDone:
		re.NoError(adminErr)
		re.Empty(adminWritten.WrittenGroupIDs, "the in-flight retry must not overwrite the admin target")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for in-flight retry")
	}

	observe(reloadID)
	select {
	case retry := <-sent:
		re.Equal(expected[reloadID], retry)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the reload group retry")
	}
	testutil.Eventually(re, func() bool { return !state(reloadID).HasUnpersistedTarget() })
	persisted := read(reloadID)
	re.Greater(persisted.ModRevision, initial[reloadID].ModRevision)
	var group affinity.Group
	re.NoError(json.Unmarshal(persisted.Value, &group))
	re.Equal(local.Group, group)
	re.Equal(admin, state(adminID).Group)
	re.Equal(reasserted, read(adminID))
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
	wg := &sync.WaitGroup{}
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

func prepare(t testing.TB) (context.Context, *clientv3.Client, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestEtcdConfig()
	cfg.Dir = t.TempDir()
	re.NoError(os.RemoveAll(cfg.Dir))
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	var client *clientv3.Client
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			cancel()
			if client != nil {
				if err := client.Close(); err != nil {
					t.Errorf("close etcd client: %v", err)
				}
			}
			etcd.Close()
		})
	}
	t.Cleanup(cleanup)
	client, err = etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls, etcdutil.TestEtcdClientPurpose, true)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	return ctx, client, cleanup
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
