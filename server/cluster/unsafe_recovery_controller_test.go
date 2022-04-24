// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
)

var _ = Suite(&testUnsafeRecoverSuite{})

type testUnsafeRecoverSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testUnsafeRecoverSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testUnsafeRecoverSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testUnsafeRecoverSuite) TestRegionTree(c *C) {

}

// func newRegion(peerID uint64, start, end []byte, epoch *metapb.RegionEpoch, peers []*metapb.Peer) *metapb.Region {

// }

// func newPeer(peerID uint64, storeID uint64) *metapb.Peer {
// 	return &metapb.Peer{
// 		Id: peerID,
// 		StoreId: storeID,
// 	}
// }

func newStoreHeartbeat(storeID uint64) *pdpb.StoreHeartbeatRequest {
	return &pdpb.StoreHeartbeatRequest{
		Stats: &pdpb.StoreStats{
			StoreId: storeID,
		},
	}
}

func (s *testUnsafeRecoverSuite) TestWholeProcess(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]interface{}{
		2: "",
		3: "",
	}), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	c.Assert(recoveryController.GetStage(), Equals, collectReport)
	// require peer report
	for storeID, _ := range reports {
		req := newStoreHeartbeat(storeID)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Creates), Equals, 0)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 0)
		c.Assert(resp.RecoveryPlan.ForceLeader, IsNil)
	}

	// receive all reports and dispatch plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(resp.RecoveryPlan.ForceLeader, NotNil)
		c.Assert(len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders), Equals, 1)
		c.Assert(resp.RecoveryPlan.ForceLeader.FailedStores, NotNil)
	}
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 1)
	}
	c.Assert(recoveryController.GetStage(), Equals, removePeer)
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID)
		resp := &pdpb.StoreHeartbeatResponse{}
		req.StoreReport = report
		// remove the two failed peers
		req.StoreReport.PeerReports[0].RegionState.Region.Peers = req.StoreReport.PeerReports[0].RegionState.Region.Peers[:1]
		resp = &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, IsNil)
	}
	c.Assert(recoveryController.GetStage(), Equals, idle)
}

// func (s *testUnsafeRecoverSuite) TestPlanGenerationOneUnhealthyRegion(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		2: "",
// 		3: "",
// 	}
// 	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
// 		1: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 	}
// 	recoveryController.generateRecoveryPlan()
// 	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
// 	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
// 	c.Assert(ok, IsTrue)
// 	c.Assert(len(store1Plan.Updates), Equals, 1)
// 	update := store1Plan.Updates[0]
// 	c.Assert(bytes.Compare(update.StartKey, []byte("")), Equals, 0)
// 	c.Assert(bytes.Compare(update.EndKey, []byte("")), Equals, 0)
// 	c.Assert(len(update.Peers), Equals, 1)
// 	c.Assert(update.Peers[0].StoreId, Equals, uint64(1))
// }

// func (s *testUnsafeRecoverSuite) TestPlanGenerationEmptyRange(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		3: "",
// 	}
// 	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
// 		1: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						EndKey:      []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 		2: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          2,
// 						StartKey:    []byte("d"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 12, StoreId: 1}, {Id: 22, StoreId: 2}, {Id: 32, StoreId: 3}}}}},
// 		}},
// 	}
// 	recoveryController.generateRecoveryPlan()
// 	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
// 	for storeID, plan := range recoveryController.storeRecoveryPlans {
// 		c.Assert(len(plan.Creates), Equals, 1)
// 		create := plan.Creates[0]
// 		c.Assert(bytes.Compare(create.StartKey, []byte("c")), Equals, 0)
// 		c.Assert(bytes.Compare(create.EndKey, []byte("d")), Equals, 0)
// 		c.Assert(len(create.Peers), Equals, 1)
// 		c.Assert(create.Peers[0].StoreId, Equals, storeID)
// 		c.Assert(create.Peers[0].Role, Equals, metapb.PeerRole_Voter)
// 	}
// }

// func (s *testUnsafeRecoverSuite) TestPlanGenerationEmptyRangeAtTheEnd(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		3: "",
// 	}
// 	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
// 		1: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						StartKey:    []byte(""),
// 						EndKey:      []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 		2: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						StartKey:    []byte(""),
// 						EndKey:      []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 	}
// 	recoveryController.generateRecoveryPlan()
// 	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
// 	for storeID, plan := range recoveryController.storeRecoveryPlans {
// 		c.Assert(len(plan.Creates), Equals, 1)
// 		create := plan.Creates[0]
// 		c.Assert(bytes.Compare(create.StartKey, []byte("c")), Equals, 0)
// 		c.Assert(bytes.Compare(create.EndKey, []byte("")), Equals, 0)
// 		c.Assert(len(create.Peers), Equals, 1)
// 		c.Assert(create.Peers[0].StoreId, Equals, storeID)
// 		c.Assert(create.Peers[0].Role, Equals, metapb.PeerRole_Voter)
// 	}
// }

// func (s *testUnsafeRecoverSuite) TestPlanGenerationUseNewestRanges(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		3: "",
// 		4: "",
// 	}
// 	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
// 		1: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						StartKey:    []byte(""),
// 						EndKey:      []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 20},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          2,
// 						StartKey:    []byte("a"),
// 						EndKey:      []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
// 						Peers: []*metapb.Peer{
// 							{Id: 12, StoreId: 1}, {Id: 22, StoreId: 2}, {Id: 32, StoreId: 3}}}}},
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          4,
// 						StartKey:    []byte("m"),
// 						EndKey:      []byte("p"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
// 						Peers: []*metapb.Peer{
// 							{Id: 14, StoreId: 1}, {Id: 24, StoreId: 2}, {Id: 44, StoreId: 4}}}}},
// 		}},
// 		2: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          3,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 5},
// 						Peers: []*metapb.Peer{
// 							{Id: 23, StoreId: 2}, {Id: 33, StoreId: 3}, {Id: 43, StoreId: 4}}}}},
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          2,
// 						StartKey:    []byte("a"),
// 						EndKey:      []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
// 						Peers: []*metapb.Peer{
// 							{Id: 12, StoreId: 1}, {Id: 22, StoreId: 2}, {Id: 32, StoreId: 3}}}}},
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          4,
// 						StartKey:    []byte("m"),
// 						EndKey:      []byte("p"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
// 						Peers: []*metapb.Peer{
// 							{Id: 14, StoreId: 1}, {Id: 24, StoreId: 2}, {Id: 44, StoreId: 4}}}}},
// 		}},
// 	}
// 	recoveryController.generateRecoveryPlan()
// 	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 2)
// 	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
// 	c.Assert(ok, IsTrue)
// 	updatedRegion1 := store1Plan.Updates[0]
// 	c.Assert(updatedRegion1.Id, Equals, uint64(1))
// 	c.Assert(len(updatedRegion1.Peers), Equals, 1)
// 	c.Assert(bytes.Compare(updatedRegion1.StartKey, []byte("")), Equals, 0)
// 	c.Assert(bytes.Compare(updatedRegion1.EndKey, []byte("a")), Equals, 0)

// 	store2Plan := recoveryController.storeRecoveryPlans[2]
// 	updatedRegion3 := store2Plan.Updates[0]
// 	c.Assert(updatedRegion3.Id, Equals, uint64(3))
// 	c.Assert(len(updatedRegion3.Peers), Equals, 1)
// 	c.Assert(bytes.Compare(updatedRegion3.StartKey, []byte("c")), Equals, 0)
// 	c.Assert(bytes.Compare(updatedRegion3.EndKey, []byte("m")), Equals, 0)
// 	create := store2Plan.Creates[0]
// 	c.Assert(bytes.Compare(create.StartKey, []byte("p")), Equals, 0)
// 	c.Assert(bytes.Compare(create.EndKey, []byte("")), Equals, 0)
// }

// func (s *testUnsafeRecoverSuite) TestPlanGenerationMembershipChange(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		4: "",
// 		5: "",
// 	}
// 	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
// 		1: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						EndKey:      []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 41, StoreId: 4}, {Id: 51, StoreId: 5}}}}},
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          2,
// 						StartKey:    []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 		2: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          2,
// 						StartKey:    []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 		3: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          2,
// 						StartKey:    []byte("c"),
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 	}
// 	recoveryController.generateRecoveryPlan()
// 	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 3)
// 	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
// 	c.Assert(ok, IsTrue)
// 	updatedRegion1 := store1Plan.Updates[0]
// 	c.Assert(updatedRegion1.Id, Equals, uint64(1))
// 	c.Assert(len(updatedRegion1.Peers), Equals, 1)
// 	c.Assert(bytes.Compare(updatedRegion1.StartKey, []byte("")), Equals, 0)
// 	c.Assert(bytes.Compare(updatedRegion1.EndKey, []byte("c")), Equals, 0)

// 	store2Plan := recoveryController.storeRecoveryPlans[2]
// 	deleteStaleRegion1 := store2Plan.Deletes[0]
// 	c.Assert(deleteStaleRegion1, Equals, uint64(1))

// 	store3Plan := recoveryController.storeRecoveryPlans[3]
// 	deleteStaleRegion1 = store3Plan.Deletes[0]
// 	c.Assert(deleteStaleRegion1, Equals, uint64(1))
// }

// func (s *testUnsafeRecoverSuite) TestPlanGenerationPromotingLearner(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		2: "",
// 		3: "",
// 	}
// 	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
// 		1: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Learner}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
// 		}},
// 	}
// 	recoveryController.generateRecoveryPlan()
// 	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
// 	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
// 	c.Assert(ok, IsTrue)
// 	c.Assert(len(store1Plan.Updates), Equals, 1)
// 	update := store1Plan.Updates[0]
// 	c.Assert(bytes.Compare(update.StartKey, []byte("")), Equals, 0)
// 	c.Assert(bytes.Compare(update.EndKey, []byte("")), Equals, 0)
// 	c.Assert(len(update.Peers), Equals, 1)
// 	c.Assert(update.Peers[0].StoreId, Equals, uint64(1))
// 	c.Assert(update.Peers[0].Role, Equals, metapb.PeerRole_Voter)
// }

// func (s *testUnsafeRecoverSuite) TestPlanGenerationKeepingOneReplica(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		3: "",
// 		4: "",
// 	}
// 	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
// 		1: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
// 		}},
// 		2: {PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
// 		}},
// 	}
// 	recoveryController.generateRecoveryPlan()
// 	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 2)
// 	foundUpdate := false
// 	foundDelete := false
// 	for storeID, plan := range recoveryController.storeRecoveryPlans {
// 		if len(plan.Updates) == 1 {
// 			foundUpdate = true
// 			update := plan.Updates[0]
// 			c.Assert(bytes.Compare(update.StartKey, []byte("")), Equals, 0)
// 			c.Assert(bytes.Compare(update.EndKey, []byte("")), Equals, 0)
// 			c.Assert(len(update.Peers), Equals, 1)
// 			c.Assert(update.Peers[0].StoreId, Equals, storeID)
// 		} else if len(plan.Deletes) == 1 {
// 			foundDelete = true
// 			c.Assert(plan.Deletes[0], Equals, uint64(1))
// 		}
// 	}
// 	c.Assert(foundUpdate, Equals, true)
// 	c.Assert(foundDelete, Equals, true)
// }

// func (s *testUnsafeRecoverSuite) TestReportCollection(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.stage = collectingClusterInfo
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		3: "",
// 		4: "",
// 	}
// 	recoveryController.storeReports[uint64(1)] = nil
// 	recoveryController.storeReports[uint64(2)] = nil
// 	store1Report := &pdpb.StoreReport{
// 		PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
// 		}}
// 	store2Report := &pdpb.StoreReport{
// 		PeerReports: []*pdpb.PeerReport{
// 			{
// 				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:          1,
// 						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
// 						Peers: []*metapb.Peer{
// 							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
// 		}}
// 	heartbeat := &pdpb.StoreHeartbeatRequest{Stats: &pdpb.StoreStats{StoreId: 1}}
// 	resp := &pdpb.StoreHeartbeatResponse{}
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(resp.RequireDetailedReport, Equals, true)
// 	// Second and following heartbeats in a short period of time are ignored.
// 	resp = &pdpb.StoreHeartbeatResponse{}
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(resp.RequireDetailedReport, Equals, false)

// 	heartbeat.StoreReport = store1Report
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(recoveryController.numStoresReported, Equals, 1)
// 	c.Assert(recoveryController.storeReports[uint64(1)], Equals, store1Report)

// 	heartbeat.Stats.StoreId = uint64(2)
// 	heartbeat.StoreReport = store2Report
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(recoveryController.numStoresReported, Equals, 2)
// 	c.Assert(recoveryController.storeReports[uint64(2)], Equals, store2Report)
// }

// func (s *testUnsafeRecoverSuite) TestPlanExecution(c *C) {
// 	_, opt, _ := newTestScheduleConfig()
// 	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
// 	recoveryController := newUnsafeRecoveryController(cluster)
// 	recoveryController.stage = recovering
// 	recoveryController.failedStores = map[uint64]interface{}{
// 		3: "",
// 		4: "",
// 	}
// 	recoveryController.storeReports[uint64(1)] = nil
// 	recoveryController.storeReports[uint64(2)] = nil
// 	recoveryController.storeRecoveryPlans[uint64(1)] = &pdpb.RecoveryPlan{
// 		Creates: []*metapb.Region{
// 			{
// 				Id:       4,
// 				StartKey: []byte("f"),
// 				Peers:    []*metapb.Peer{{Id: 14, StoreId: 1}},
// 			},
// 		},
// 		Updates: []*metapb.Region{
// 			{
// 				Id:       5,
// 				StartKey: []byte("c"),
// 				EndKey:   []byte("f"),
// 				Peers:    []*metapb.Peer{{Id: 15, StoreId: 1}},
// 			},
// 		},
// 	}
// 	recoveryController.storeRecoveryPlans[uint64(2)] = &pdpb.RecoveryPlan{
// 		Updates: []*metapb.Region{
// 			{
// 				Id:     3,
// 				EndKey: []byte("c"),
// 				Peers:  []*metapb.Peer{{Id: 23, StoreId: 2}},
// 			},
// 		},
// 		Deletes: []uint64{2},
// 	}
// 	store1Report := &pdpb.StoreReport{
// 		PeerReports: []*pdpb.PeerReport{
// 			{
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:       4,
// 						StartKey: []byte("f"),
// 						Peers:    []*metapb.Peer{{Id: 14, StoreId: 1}}}},
// 			},
// 			{
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:       5,
// 						StartKey: []byte("c"),
// 						EndKey:   []byte("f"),
// 						Peers:    []*metapb.Peer{{Id: 15, StoreId: 1}}}},
// 			},
// 		}}
// 	heartbeat := &pdpb.StoreHeartbeatRequest{Stats: &pdpb.StoreStats{StoreId: 1}, StoreReport: store1Report}
// 	resp := &pdpb.StoreHeartbeatResponse{}
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(recoveryController.numStoresPlanExecuted, Equals, 1)

// 	store2Report := &pdpb.StoreReport{
// 		PeerReports: []*pdpb.PeerReport{
// 			{
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:       2,
// 						StartKey: []byte("g"),
// 						Peers:    []*metapb.Peer{{Id: 12, StoreId: 2}}}},
// 			},
// 		}}
// 	heartbeat.Stats.StoreId = uint64(2)
// 	heartbeat.StoreReport = store2Report
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(recoveryController.numStoresPlanExecuted, Equals, 1)

// 	store2Report = &pdpb.StoreReport{
// 		PeerReports: []*pdpb.PeerReport{
// 			{
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:     3,
// 						EndKey: []byte("f"),
// 						Peers:  []*metapb.Peer{{Id: 13, StoreId: 2}}}},
// 			},
// 		}}
// 	heartbeat.StoreReport = store2Report
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(recoveryController.numStoresPlanExecuted, Equals, 1)

// 	store2Report = &pdpb.StoreReport{
// 		PeerReports: []*pdpb.PeerReport{
// 			{
// 				RegionState: &raft_serverpb.RegionLocalState{
// 					Region: &metapb.Region{
// 						Id:     3,
// 						EndKey: []byte("c"),
// 						Peers:  []*metapb.Peer{{Id: 13, StoreId: 2}}}},
// 			},
// 		}}
// 	heartbeat.StoreReport = store2Report
// 	recoveryController.HandleStoreHeartbeat(heartbeat, resp)
// 	c.Assert(recoveryController.numStoresPlanExecuted, Equals, 2)
// 	c.Assert(recoveryController.stage, Equals, finished)
// }

func (s *testUnsafeRecoverSuite) TestRemoveFailedStores(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)

	// Store 3 doesn't exist, reject to remove.
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]interface{}{
		1: "",
		3: "",
	}), NotNil)

	c.Assert(recoveryController.RemoveFailedStores(map[uint64]interface{}{
		1: "",
	}), IsNil)
	c.Assert(cluster.GetStore(uint64(1)).IsRemoved(), IsTrue)

	// Store 2's last heartbeat is recent, and is not allowed to be removed.
	c.Assert(recoveryController.RemoveFailedStores(
		map[uint64]interface{}{
			2: "",
		}), NotNil)
}
