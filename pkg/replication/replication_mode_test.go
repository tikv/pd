// Copyright 2020 TiKV Project Authors.
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

package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	pb "github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

func TestInitial(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeMajority}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{Mode: pb.ReplicationMode_MAJORITY}, rep.GetReplicationStatus())

	conf = config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "dr-label",
		Primary:          "l1",
		DR:               "l2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	rep, err = NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             1,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())
}

func TestStatus(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey: "dr-label",
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             1,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())

	re.NoError(rep.drSwitchToAsync(nil))
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_ASYNC,
			StateId:             2,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())

	re.NoError(rep.drSwitchToSyncRecover())
	stateID := rep.drAutoSync.StateID
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC_RECOVER,
			StateId:             stateID,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())

	// test reload
	rep, err = NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(drStateSyncRecover, rep.drAutoSync.State)

	err = rep.drSwitchToSync()
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             rep.drAutoSync.StateID,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())
}

type mockFileReplicator struct {
	memberIDs []uint64
	lastData  map[uint64]string
	errors    map[uint64]error
}

func (rep *mockFileReplicator) GetMembers() ([]*pdpb.Member, error) {
	var members []*pdpb.Member
	for _, id := range rep.memberIDs {
		members = append(members, &pdpb.Member{MemberId: id})
	}
	return members, nil
}

func (rep *mockFileReplicator) ReplicateFileToMember(_ context.Context, member *pdpb.Member, _ string, data []byte) error {
	if err := rep.errors[member.GetMemberId()]; err != nil {
		return err
	}
	rep.lastData[member.GetMemberId()] = string(data)
	return nil
}

func newMockReplicator(ids []uint64) *mockFileReplicator {
	return &mockFileReplicator{
		memberIDs: ids,
		lastData:  make(map[uint64]string),
		errors:    make(map[uint64]error),
	}
}

func assertLastData(t *testing.T, data string, state string, stateID uint64, availableStores []uint64) {
	type status struct {
		State           string   `json:"state"`
		StateID         uint64   `json:"state_id"`
		AvailableStores []uint64 `json:"available_stores"`
	}
	var s status
	err := json.Unmarshal([]byte(data), &s)
	require.NoError(t, err)
	require.Equal(t, state, s.State)
	require.Equal(t, stateID, s.StateID)
	require.Equal(t, availableStores, s.AvailableStores)
}

func TestStateSwitch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	re.NoError(err)
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "zone", value: "zone1", role: placement.Voter, count: 4},
			{key: "zone", value: "zone2", role: placement.Voter, count: 2},
		}), true)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(4, 1, map[string]string{"zone": "zone1"})

	// initial state is sync
	re.Equal(drStateSync, rep.drGetState())
	stateID := rep.drAutoSync.StateID
	re.NotEqual(uint64(0), stateID)
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "sync", stateID, nil)
	assertStateIDUpdate := func() {
		re.NotEqual(stateID, rep.drAutoSync.StateID)
		stateID = rep.drAutoSync.StateID
	}
	syncStoreStatus := func(storeIDs ...uint64) {
		state := rep.GetReplicationStatus()
		for _, s := range storeIDs {
			rep.UpdateStoreDRStatus(s, &pb.StoreDRAutoSyncStatus{State: state.GetDrAutoSync().State, StateId: state.GetDrAutoSync().GetStateId()})
		}
	}

	// only one zone, sync -> async_wait -> async
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	assertStateIDUpdate()
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", stateID, []uint64{1, 2, 3, 4})

	re.False(rep.GetReplicationStatus().GetDrAutoSync().GetPauseRegionSplit())
	conf.DRAutoSync.PauseRegionSplit = true
	rep.UpdateConfig(conf)
	re.True(rep.GetReplicationStatus().GetDrAutoSync().GetPauseRegionSplit())

	syncStoreStatus(1, 2, 3, 4)
	rep.tickUpdateState()
	assertStateIDUpdate()
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async", stateID, []uint64{1, 2, 3, 4})

	// add new store in dr zone.
	cluster.AddLabelsStore(5, 1, map[string]string{"zone": "zone2"})
	cluster.AddLabersStoreWithLearnerCount(6, 1, 1, map[string]string{"zone": "zone2"})
	// async -> sync
	rep.tickUpdateState()
	re.Equal(drStateSyncRecover, rep.drGetState())
	rep.drSwitchToSync()
	re.Equal(drStateSync, rep.drGetState())
	assertStateIDUpdate()

	// sync -> async_wait
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())
	setStoreState(cluster, "down", "up", "up", "up", "up", "up")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())
	setStoreState(cluster, "down", "down", "up", "up", "up", "up")
	setStoreState(cluster, "down", "down", "down", "up", "up", "up")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState()) // cannot guarantee majority, keep sync.

	setStoreState(cluster, "up", "up", "up", "up", "up", "down")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())

	// once zone2 down, switch to async state.
	setStoreState(cluster, "up", "up", "up", "up", "down", "down")
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())

	rep.drSwitchToSync()
	replicator.errors[2] = errors.New("fail to replicate")
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	assertStateIDUpdate()
	delete(replicator.errors, 1)

	// async_wait -> sync
	setStoreState(cluster, "up", "up", "up", "up", "up", "up")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())
	re.False(rep.GetReplicationStatus().GetDrAutoSync().GetPauseRegionSplit())

	// async_wait -> async_wait
	setStoreState(cluster, "up", "up", "up", "up", "down", "down")
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	assertStateIDUpdate()

	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", stateID, []uint64{1, 2, 3, 4})
	setStoreState(cluster, "down", "up", "up", "up", "down", "down")
	rep.tickUpdateState()
	assertStateIDUpdate()
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", stateID, []uint64{2, 3, 4})
	setStoreState(cluster, "up", "down", "up", "up", "down", "down")
	rep.tickUpdateState()
	assertStateIDUpdate()
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", stateID, []uint64{1, 3, 4})

	// async_wait -> async
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	syncStoreStatus(1, 3)
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	syncStoreStatus(4)
	rep.tickUpdateState()
	assertStateIDUpdate()
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async", stateID, []uint64{1, 3, 4})

	// async -> async
	setStoreState(cluster, "up", "up", "up", "up", "down", "down")
	rep.tickUpdateState()
	// store 2 won't be available before it syncs status.
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async", stateID, []uint64{1, 3, 4})
	syncStoreStatus(1, 2, 3, 4)
	rep.tickUpdateState()
	assertStateIDUpdate()
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async", stateID, []uint64{1, 2, 3, 4})

	// async -> sync_recover
	setStoreState(cluster, "up", "up", "up", "up", "up", "up")
	rep.tickUpdateState()
	re.Equal(drStateSyncRecover, rep.drGetState())
	assertStateIDUpdate()

	re.NoError(rep.drSwitchToAsync([]uint64{1, 2, 3, 4, 5}))
	rep.config.DRAutoSync.WaitRecoverTimeout = typeutil.NewDuration(time.Hour)
	rep.tickUpdateState()
	re.Equal(drStateAsync, rep.drGetState()) // wait recover timeout

	rep.config.DRAutoSync.WaitRecoverTimeout = typeutil.NewDuration(0)
	setStoreState(cluster, "down", "up", "up", "up", "up", "up")
	rep.tickUpdateState()
	re.Equal(drStateSyncRecover, rep.drGetState())
	assertStateIDUpdate()

	// sync_recover -> async
	rep.tickUpdateState()
	re.Equal(drStateSyncRecover, rep.drGetState())
	setStoreState(cluster, "up", "up", "up", "up", "down", "down")
	rep.tickUpdateState()
	re.Equal(drStateAsync, rep.drGetState())
	assertStateIDUpdate()
	// lost majority, does not switch to async.
	re.NoError(rep.drSwitchToSyncRecover())
	assertStateIDUpdate()
	setStoreState(cluster, "down", "down", "up", "up", "down", "down")
	rep.tickUpdateState()
	re.Equal(drStateSyncRecover, rep.drGetState())

	// sync_recover -> sync
	re.NoError(rep.drSwitchToSyncRecover())
	assertStateIDUpdate()
	setStoreState(cluster, "up", "up", "up", "up", "up", "up")
	cluster.AddLeaderRegion(1, 1, 2, 3, 4, 5)
	region := cluster.GetRegion(1)

	region = region.Clone(core.WithStartKey(nil), core.WithEndKey(nil), core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State: pb.RegionReplicationState_SIMPLE_MAJORITY,
	}))
	cluster.PutRegion(region)
	rep.tickUpdateState()
	re.Equal(drStateSyncRecover, rep.drGetState())

	region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
		StateId: rep.drAutoSync.StateID - 1, // mismatch state id
	}))
	cluster.PutRegion(region)
	rep.tickUpdateState()
	re.Equal(drStateSyncRecover, rep.drGetState())
	region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
		StateId: rep.drAutoSync.StateID,
	}))
	cluster.PutRegion(region)
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())
	assertStateIDUpdate()
}

func TestReplicateState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "zone", value: "zone1", role: placement.Voter, count: 2},
			{key: "zone", value: "zone2", role: placement.Voter, count: 1},
		}), true)
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	re.NoError(err)
	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})

	stateID := rep.drAutoSync.StateID
	// replicate after initialized
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "sync", stateID, nil)

	// replicate state to new member
	replicator.memberIDs = append(replicator.memberIDs, 2, 3)
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[2], "sync", stateID, nil)
	assertLastData(t, replicator.lastData[3], "sync", stateID, nil)

	// inject error
	replicator.errors[2] = errors.New("failed to persist")
	rep.tickUpdateState() // switch async_wait since there is only one zone
	newStateID := rep.drAutoSync.StateID
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", newStateID, []uint64{1, 2})
	assertLastData(t, replicator.lastData[2], "sync", stateID, nil)
	assertLastData(t, replicator.lastData[3], "async_wait", newStateID, []uint64{1, 2})

	// clear error, replicate to node 2 next time
	delete(replicator.errors, 2)
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[2], "async_wait", newStateID, []uint64{1, 2})
}

func TestAsynctimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "zone", value: "zone1", role: placement.Voter, count: 2},
			{key: "zone", value: "zone2", role: placement.Voter, count: 1},
		}), true)
	var replicator mockFileReplicator
	rep, err := NewReplicationModeManager(conf, store, cluster, &replicator)
	re.NoError(err)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone2"})

	setStoreState(cluster, "up", "up", "down")
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
}

func setStoreState(cluster *mockcluster.Cluster, states ...string) {
	for i, state := range states {
		store := cluster.GetStore(uint64(i + 1))
		if state == "down" {
			store.GetMeta().LastHeartbeat = time.Now().Add(-time.Minute * 10).UnixNano()
		} else if state == "up" {
			store.GetMeta().LastHeartbeat = time.Now().UnixNano()
		}
		cluster.PutStore(store)
	}
}

func TestRecoverProgress(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	regionScanBatchSize = 10
	regionMinSampleSize = 5

	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "zone", value: "zone1", role: placement.Voter, count: 2},
			{key: "zone", value: "zone2", role: placement.Voter, count: 1},
		}), true)
	cluster.AddLabelsStore(1, 1, map[string]string{})
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)

	prepare := func(n int, asyncRegions []int) {
		re.NoError(rep.drSwitchToSyncRecover())
		regions := genRegions(cluster, rep.drAutoSync.StateID, n)
		for _, i := range asyncRegions {
			regions[i] = regions[i].Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
				State:   pb.RegionReplicationState_SIMPLE_MAJORITY,
				StateId: regions[i].GetReplicationStatus().GetStateId(),
			}))
		}
		for _, r := range regions {
			cluster.PutRegion(r)
		}
		rep.updateProgress()
	}

	prepare(20, nil)
	re.Equal(20, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())

	prepare(10, []int{9})
	re.Equal(9, rep.drRecoverCount)
	re.Equal(10, rep.drTotalRegion)
	re.Equal(1, rep.drSampleTotalRegion)
	re.Equal(0, rep.drSampleRecoverCount)
	re.Equal(float32(9)/float32(10), rep.estimateProgress())

	prepare(30, []int{3, 4, 5, 6, 7, 8, 9})
	re.Equal(3, rep.drRecoverCount)
	re.Equal(30, rep.drTotalRegion)
	re.Equal(7, rep.drSampleTotalRegion)
	re.Equal(0, rep.drSampleRecoverCount)
	re.Equal(float32(3)/float32(30), rep.estimateProgress())

	prepare(30, []int{9, 13, 14})
	re.Equal(9, rep.drRecoverCount)
	re.Equal(30, rep.drTotalRegion)
	re.Equal(6, rep.drSampleTotalRegion) // 9 + 10,11,12,13,14
	re.Equal(3, rep.drSampleRecoverCount)
	re.Equal((float32(9)+float32(30-9)/2)/float32(30), rep.estimateProgress())
}

func TestRecoverProgressWithSplitAndMerge(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	regionScanBatchSize = 10
	regionMinSampleSize = 5

	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "zone", value: "zone1", role: placement.Voter, count: 2},
			{key: "zone", value: "zone2", role: placement.Voter, count: 1},
		}), true)
	cluster.AddLabelsStore(1, 1, map[string]string{})
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)

	prepare := func(n int, asyncRegions []int) {
		re.NoError(rep.drSwitchToSyncRecover())
		regions := genRegions(cluster, rep.drAutoSync.StateID, n)
		for _, i := range asyncRegions {
			regions[i] = regions[i].Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
				State:   pb.RegionReplicationState_SIMPLE_MAJORITY,
				StateId: regions[i].GetReplicationStatus().GetStateId(),
			}))
		}
		for _, r := range regions {
			cluster.PutRegion(r)
		}
	}

	// merged happened in ahead of the scan
	prepare(20, nil)
	r := cluster.GetRegion(1).Clone(core.WithEndKey(cluster.GetRegion(2).GetEndKey()))
	cluster.PutRegion(r)
	rep.updateProgress()
	re.Equal(19, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())

	// merged happened during the scan
	prepare(20, nil)
	r1 := cluster.GetRegion(1)
	r2 := cluster.GetRegion(2)
	r = r1.Clone(core.WithEndKey(r2.GetEndKey()))
	cluster.PutRegion(r)
	rep.drRecoverCount = 1
	rep.drRecoverKey = r1.GetEndKey()
	rep.updateProgress()
	re.Equal(20, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())

	// split, region gap happened during the scan
	rep.drRecoverCount, rep.drRecoverKey = 0, nil
	cluster.PutRegion(r1)
	rep.updateProgress()
	re.Equal(1, rep.drRecoverCount)
	re.NotEqual(float32(1.0), rep.estimateProgress())
	// region gap missing
	cluster.PutRegion(r2)
	rep.updateProgress()
	re.Equal(20, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())
}

func TestComplexPlacementRules(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	re.NoError(err)
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "logic", value: "logic1", role: placement.Voter, count: 1},
			{key: "logic", value: "logic2", role: placement.Voter, count: 1},
			{key: "logic", value: "logic3", role: placement.Voter, count: 1},
			{key: "logic", value: "logic4", role: placement.Voter, count: 1},
			{key: "logic", value: "logic5", role: placement.Voter, count: 1},
		}), true)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1", "logic": "logic1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1", "logic": "logic1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone1", "logic": "logic2"})
	cluster.AddLabelsStore(4, 1, map[string]string{"zone": "zone1", "logic": "logic2"})
	cluster.AddLabelsStore(5, 1, map[string]string{"zone": "zone1", "logic": "logic3"})
	cluster.AddLabelsStore(6, 1, map[string]string{"zone": "zone1", "logic": "logic3"})
	cluster.AddLabelsStore(7, 1, map[string]string{"zone": "zone2", "logic": "logic4"})
	cluster.AddLabelsStore(8, 1, map[string]string{"zone": "zone2", "logic": "logic4"})
	cluster.AddLabelsStore(9, 1, map[string]string{"zone": "zone2", "logic": "logic5"})
	cluster.AddLabelsStore(10, 1, map[string]string{"zone": "zone2", "logic": "logic5"})

	// initial state is sync
	re.Equal(drStateSync, rep.drGetState())

	// down logic3 + logic5, can remain sync
	setStoreState(cluster, "up", "up", "up", "up", "down", "down", "up", "up", "down", "down")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())

	// down 1 tikv from logic4 + 1 tikv from logic5, cannot sync
	setStoreState(cluster, "up", "up", "up", "up", "up", "up", "up", "down", "up", "down")
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", rep.drAutoSync.StateID, []uint64{1, 2, 3, 4, 5, 6})

	// reset to sync
	setStoreState(cluster, "up", "up", "up", "up", "up", "up", "up", "up", "up", "up")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())

	// lost majority, down 1 tikv from logic2 + 1 tikv from logic3 + 1tikv from logic5, remain sync state
	setStoreState(cluster, "up", "up", "up", "down", "up", "down", "up", "up", "up", "down")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())
}

func TestComplexPlacementRules2(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	re.NoError(err)
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "logic", value: "logic1", role: placement.Voter, count: 2},
			{key: "logic", value: "logic2", role: placement.Voter, count: 1},
			{key: "logic", value: "logic3", role: placement.Voter, count: 2},
		}), true)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1", "logic": "logic1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1", "logic": "logic1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone1", "logic": "logic2"})
	cluster.AddLabelsStore(4, 1, map[string]string{"zone": "zone1", "logic": "logic2"})
	cluster.AddLabelsStore(5, 1, map[string]string{"zone": "zone2", "logic": "logic3"})
	cluster.AddLabelsStore(6, 1, map[string]string{"zone": "zone2", "logic": "logic3"})
	cluster.AddLabelsStore(7, 1, map[string]string{"zone": "zone2", "logic": "logic3"})

	// initial state is sync
	re.Equal(drStateSync, rep.drGetState())

	// down 1 from logic3, can remain sync
	setStoreState(cluster, "up", "up", "up", "up", "up", "down", "up")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())

	// down 1 from logic1, 1 from logic2, can remain sync
	setStoreState(cluster, "up", "down", "up", "down", "up", "up", "up")
	rep.tickUpdateState()
	re.Equal(drStateSync, rep.drGetState())

	// down another from logic3, cannot sync
	setStoreState(cluster, "up", "up", "up", "up", "down", "down", "up")
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", rep.drAutoSync.StateID, []uint64{1, 2, 3, 4})
}

func TestComplexPlacementRules3(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	re.NoError(err)
	cluster.GetRuleManager().SetAllGroupBundles(
		genPlacementRuleConfig([]ruleConfig{
			{key: "logic", value: "logic1", role: placement.Voter, count: 2},
			{key: "logic", value: "logic2", role: placement.Learner, count: 1},
			{key: "logic", value: "logic3", role: placement.Voter, count: 1},
		}), true)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1", "logic": "logic1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1", "logic": "logic1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone1", "logic": "logic2"})
	cluster.AddLabelsStore(4, 1, map[string]string{"zone": "zone1", "logic": "logic2"})
	cluster.AddLabelsStore(5, 1, map[string]string{"zone": "zone2", "logic": "logic3"})

	// initial state is sync
	re.Equal(drStateSync, rep.drGetState())

	// zone2 down, switch state, available stores should contain logic2 (learner)
	setStoreState(cluster, "up", "up", "up", "up", "down")
	rep.tickUpdateState()
	re.Equal(drStateAsyncWait, rep.drGetState())
	rep.tickReplicateStatus()
	assertLastData(t, replicator.lastData[1], "async_wait", rep.drAutoSync.StateID, []uint64{1, 2, 3, 4})
}

func genRegions(cluster *mockcluster.Cluster, stateID uint64, n int) []*core.RegionInfo {
	var regions []*core.RegionInfo
	for i := 1; i <= n; i++ {
		cluster.AddLeaderRegion(uint64(i), 1)
		region := cluster.GetRegion(uint64(i))
		if i == 1 {
			region = region.Clone(core.WithStartKey(nil))
		}
		if i == n {
			region = region.Clone(core.WithEndKey(nil))
		}
		region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
			State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
			StateId: stateID,
		}))
		regions = append(regions, region)
	}
	return regions
}

type ruleConfig struct {
	key   string
	value string
	role  placement.PeerRoleType
	count int
}

func genPlacementRuleConfig(rules []ruleConfig) []placement.GroupBundle {
	group := placement.GroupBundle{
		ID: "group1",
	}
	for i, r := range rules {
		group.Rules = append(group.Rules, &placement.Rule{
			ID:   fmt.Sprintf("rule%d", i),
			Role: r.role,
			LabelConstraints: []placement.LabelConstraint{
				{Key: r.key, Op: placement.In, Values: []string{r.value}},
			},
			Count: r.count,
		})
	}
	return []placement.GroupBundle{group}
}
