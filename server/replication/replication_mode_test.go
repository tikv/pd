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
	"errors"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pb "github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
)

func TestReplicationMode(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testReplicationMode{})

type testReplicationMode struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testReplicationMode) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testReplicationMode) TearDownTest(c *C) {
	s.cancel()
}

func (s *testReplicationMode) TestInitial(c *C) {
	store := storage.NewBuilder().WithMemoryBackend().Build()
	conf := config.ReplicationModeConfig{ReplicationMode: modeMajority}
	cluster := mockcluster.NewCluster(s.ctx, config.NewTestOptions())
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicationStatus(), DeepEquals, &pb.ReplicationStatus{Mode: pb.ReplicationMode_MAJORITY})

	conf = config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "dr-label",
		Primary:          "l1",
		DR:               "l2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
	}}
	rep, err = NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicationStatus(), DeepEquals, &pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             1,
			WaitSyncTimeoutHint: 60,
		},
	})
}

func (s *testReplicationMode) TestStatus(c *C) {
	store := storage.NewBuilder().WithMemoryBackend().Build()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:        "dr-label",
		WaitSyncTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(s.ctx, config.NewTestOptions())
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicationStatus(), DeepEquals, &pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             1,
			WaitSyncTimeoutHint: 60,
		},
	})

	err = rep.drSwitchToAsync()
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicationStatus(), DeepEquals, &pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_ASYNC,
			StateId:             2,
			WaitSyncTimeoutHint: 60,
		},
	})

	err = rep.drSwitchToSyncRecover()
	c.Assert(err, IsNil)
	stateID := rep.drAutoSync.StateID
	c.Assert(rep.GetReplicationStatus(), DeepEquals, &pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC_RECOVER,
			StateId:             stateID,
			WaitSyncTimeoutHint: 60,
		},
	})

	// test reload
	rep, err = NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	c.Assert(err, IsNil)
	c.Assert(rep.drAutoSync.State, Equals, drStateSyncRecover)

	err = rep.drSwitchToSync()
	c.Assert(err, IsNil)
	c.Assert(rep.GetReplicationStatus(), DeepEquals, &pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             rep.drAutoSync.StateID,
			WaitSyncTimeoutHint: 60,
		},
	})
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

func (rep *mockFileReplicator) ReplicateFileToMember(ctx context.Context, member *pdpb.Member, name string, data []byte) error {
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

func (s *testReplicationMode) TestStateSwitch(c *C) {
	store := storage.NewBuilder().WithMemoryBackend().Build()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(s.ctx, config.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	c.Assert(err, IsNil)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone1"})

	// initial state is sync
	c.Assert(rep.drGetState(), Equals, drStateSync)
	stateID := rep.drAutoSync.StateID
	c.Assert(stateID, Not(Equals), uint64(0))
	c.Assert(replicator.lastData[1], Equals, fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID))
	assertStateIDUpdate := func() {
		c.Assert(rep.drAutoSync.StateID, Not(Equals), stateID)
		stateID = rep.drAutoSync.StateID
	}

	// only one zone, sync -> async
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)
	assertStateIDUpdate()
	c.Assert(replicator.lastData[1], Equals, fmt.Sprintf(`{"state":"async","state_id":%d}`, stateID))

	// add new store in dr zone.
	cluster.AddLabelsStore(4, 1, map[string]string{"zone": "zone2"})
	cluster.AddLabelsStore(5, 1, map[string]string{"zone": "zone2"})
	// async -> sync
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)
	rep.drSwitchToSync()
	c.Assert(rep.drGetState(), Equals, drStateSync)
	assertStateIDUpdate()

	// sync -> async
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync)
	s.setStoreState(cluster, 1, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync)
	s.setStoreState(cluster, 2, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync) // cannot guarantee majority, keep sync.
	s.setStoreState(cluster, 1, "up")
	s.setStoreState(cluster, 2, "up")
	s.setStoreState(cluster, 5, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)
	assertStateIDUpdate()
	rep.drSwitchToSync()
	replicator.errors[1] = errors.New("fail to replicate")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)
	assertStateIDUpdate()

	// async -> sync_recover
	s.setStoreState(cluster, 5, "up")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)
	assertStateIDUpdate()
	rep.drSwitchToAsync()
	s.setStoreState(cluster, 1, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)
	assertStateIDUpdate()

	// sync_recover -> async
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)
	s.setStoreState(cluster, 1, "up")
	s.setStoreState(cluster, 4, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)
	assertStateIDUpdate()

	// sync_recover -> sync
	rep.drSwitchToSyncRecover()
	assertStateIDUpdate()
	s.setStoreState(cluster, 4, "up")
	cluster.AddLeaderRegion(1, 1, 2, 5)
	region := cluster.GetRegion(1)

	region = region.Clone(core.WithStartKey(nil), core.WithEndKey(nil), core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State: pb.RegionReplicationState_SIMPLE_MAJORITY,
	}))
	cluster.PutRegion(region)
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)

	region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
		StateId: rep.drAutoSync.StateID - 1, // mismatch state id
	}))
	cluster.PutRegion(region)
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSyncRecover)
	region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
		StateId: rep.drAutoSync.StateID,
	}))
	cluster.PutRegion(region)
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync)
	assertStateIDUpdate()
}

func (s *testReplicationMode) TestReplicateState(c *C) {
	store := storage.NewBuilder().WithMemoryBackend().Build()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(s.ctx, config.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	c.Assert(err, IsNil)

	stateID := rep.drAutoSync.StateID
	// replicate after initialized
	c.Assert(replicator.lastData[1], Equals, fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID))

	// repliate state to new member
	replicator.memberIDs = append(replicator.memberIDs, 2, 3)
	rep.checkReplicateFile()
	c.Assert(replicator.lastData[2], Equals, fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID))
	c.Assert(replicator.lastData[3], Equals, fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID))

	// inject error
	replicator.errors[2] = errors.New("failed to persist")
	rep.tickDR() // switch async since there is only one zone
	newStateID := rep.drAutoSync.StateID
	c.Assert(replicator.lastData[1], Equals, fmt.Sprintf(`{"state":"async","state_id":%d}`, newStateID))
	c.Assert(replicator.lastData[2], Equals, fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID))
	c.Assert(replicator.lastData[3], Equals, fmt.Sprintf(`{"state":"async","state_id":%d}`, newStateID))

	// clear error, replicate to node 2 next time
	delete(replicator.errors, 2)
	rep.checkReplicateFile()
	c.Assert(replicator.lastData[2], Equals, fmt.Sprintf(`{"state":"async","state_id":%d}`, newStateID))
}

func (s *testReplicationMode) TestAsynctimeout(c *C) {
	store := storage.NewBuilder().WithMemoryBackend().Build()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
		WaitAsyncTimeout: typeutil.Duration{Duration: 2 * time.Minute},
	}}
	cluster := mockcluster.NewCluster(s.ctx, config.NewTestOptions())
	var replicator mockFileReplicator
	rep, err := NewReplicationModeManager(conf, store, cluster, &replicator)
	c.Assert(err, IsNil)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone2"})

	s.setStoreState(cluster, 3, "down")
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync) // cannot switch state due to recently start

	rep.initTime = time.Now().Add(-3 * time.Minute)
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)

	rep.drSwitchToSync()
	rep.UpdateMemberWaitAsyncTime(42)
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateSync) // cannot switch state due to member not timeout

	rep.drMemberWaitAsyncTime[42] = time.Now().Add(-3 * time.Minute)
	rep.tickDR()
	c.Assert(rep.drGetState(), Equals, drStateAsync)
}

func (s *testReplicationMode) setStoreState(cluster *mockcluster.Cluster, id uint64, state string) {
	store := cluster.GetStore(id)
	if state == "down" {
		store.GetMeta().LastHeartbeat = time.Now().Add(-time.Minute * 10).UnixNano()
	} else if state == "up" {
		store.GetMeta().LastHeartbeat = time.Now().UnixNano()
	}
	cluster.PutStore(store)
}

func (s *testReplicationMode) TestRecoverProgress(c *C) {
	regionScanBatchSize = 10
	regionMinSampleSize = 5

	store := storage.NewBuilder().WithMemoryBackend().Build()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(s.ctx, config.NewTestOptions())
	cluster.AddLabelsStore(1, 1, map[string]string{})
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	c.Assert(err, IsNil)

	prepare := func(n int, asyncRegions []int) {
		rep.drSwitchToSyncRecover()
		regions := s.genRegions(cluster, rep.drAutoSync.StateID, n)
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
	c.Assert(rep.drRecoverCount, Equals, 20)
	c.Assert(rep.estimateProgress(), Equals, float32(1.0))

	prepare(10, []int{9})
	c.Assert(rep.drRecoverCount, Equals, 9)
	c.Assert(rep.drTotalRegion, Equals, 10)
	c.Assert(rep.drSampleTotalRegion, Equals, 1)
	c.Assert(rep.drSampleRecoverCount, Equals, 0)
	c.Assert(rep.estimateProgress(), Equals, float32(9)/float32(10))

	prepare(30, []int{3, 4, 5, 6, 7, 8, 9})
	c.Assert(rep.drRecoverCount, Equals, 3)
	c.Assert(rep.drTotalRegion, Equals, 30)
	c.Assert(rep.drSampleTotalRegion, Equals, 7)
	c.Assert(rep.drSampleRecoverCount, Equals, 0)
	c.Assert(rep.estimateProgress(), Equals, float32(3)/float32(30))

	prepare(30, []int{9, 13, 14})
	c.Assert(rep.drRecoverCount, Equals, 9)
	c.Assert(rep.drTotalRegion, Equals, 30)
	c.Assert(rep.drSampleTotalRegion, Equals, 6) // 9 + 10,11,12,13,14
	c.Assert(rep.drSampleRecoverCount, Equals, 3)
	c.Assert(rep.estimateProgress(), Equals, (float32(9)+float32(30-9)/2)/float32(30))
}

func (s *testReplicationMode) TestRecoverProgressWithSplitAndMerge(c *C) {
	regionScanBatchSize = 10
	regionMinSampleSize = 5

	store := storage.NewBuilder().WithMemoryBackend().Build()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
		WaitSyncTimeout:  typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(s.ctx, config.NewTestOptions())
	cluster.AddLabelsStore(1, 1, map[string]string{})
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	c.Assert(err, IsNil)

	prepare := func(n int, asyncRegions []int) {
		rep.drSwitchToSyncRecover()
		regions := s.genRegions(cluster, rep.drAutoSync.StateID, n)
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
	c.Assert(rep.drRecoverCount, Equals, 19)
	c.Assert(rep.estimateProgress(), Equals, float32(1.0))

	// merged happened during the scan
	prepare(20, nil)
	r1 := cluster.GetRegion(1)
	r2 := cluster.GetRegion(2)
	r = r1.Clone(core.WithEndKey(r2.GetEndKey()))
	cluster.PutRegion(r)
	rep.drRecoverCount = 1
	rep.drRecoverKey = r1.GetEndKey()
	rep.updateProgress()
	c.Assert(rep.drRecoverCount, Equals, 20)
	c.Assert(rep.estimateProgress(), Equals, float32(1.0))

	// split, region gap happened during the scan
	rep.drRecoverCount, rep.drRecoverKey = 0, nil
	cluster.PutRegion(r1)
	rep.updateProgress()
	c.Assert(rep.drRecoverCount, Equals, 1)
	c.Assert(rep.estimateProgress(), Not(Equals), float32(1.0))
	// region gap missing
	cluster.PutRegion(r2)
	rep.updateProgress()
	c.Assert(rep.drRecoverCount, Equals, 20)
	c.Assert(rep.estimateProgress(), Equals, float32(1.0))
}

func (s *testReplicationMode) genRegions(cluster *mockcluster.Cluster, stateID uint64, n int) []*core.RegionInfo {
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
