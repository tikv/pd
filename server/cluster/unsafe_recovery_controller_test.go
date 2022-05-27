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
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/storage"
)

var _ = Suite(&testUnsafeRecoverySuite{})

type testUnsafeRecoverySuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testUnsafeRecoverySuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testUnsafeRecoverySuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func newStoreHeartbeat(storeID uint64, report *pdpb.StoreReport) *pdpb.StoreHeartbeatRequest {
	return &pdpb.StoreHeartbeatRequest{
		Stats: &pdpb.StoreStats{
			StoreId: storeID,
		},
		StoreReport: report,
	}
}

func applyRecoveryPlan(c *C, storeID uint64, storeReports map[uint64]*pdpb.StoreReport, resp *pdpb.StoreHeartbeatResponse) {
	plan := resp.GetRecoveryPlan()
	if plan == nil {
		return
	}

	reports := storeReports[storeID]
	reports.Step = plan.GetStep()

	forceLeaders := plan.GetForceLeader()
	if forceLeaders != nil {
		for _, forceLeader := range forceLeaders.GetEnterForceLeaders() {
			for _, report := range reports.PeerReports {
				region := report.GetRegionState().GetRegion()
				if region.GetId() == forceLeader {
					report.IsForceLeader = true
					break
				}
			}
		}
		return
	}

	for _, create := range plan.GetCreates() {
		reports.PeerReports = append(reports.PeerReports, &pdpb.PeerReport{
			RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
			RegionState: &raft_serverpb.RegionLocalState{
				Region: create,
			},
		})
	}

	for _, tombstone := range plan.GetTombstones() {
		for i, report := range reports.PeerReports {
			if report.GetRegionState().GetRegion().GetId() == tombstone {
				reports.PeerReports = append(reports.PeerReports[:i], reports.PeerReports[i+1:]...)
				break
			}
		}
	}

	for _, demote := range plan.GetDemotes() {
		for store, storeReport := range storeReports {
			for _, report := range storeReport.PeerReports {
				region := report.GetRegionState().GetRegion()
				if region.GetId() == demote.GetRegionId() {
					for _, peer := range region.GetPeers() {
						// promote learner
						if peer.StoreId == storeID && peer.Role == metapb.PeerRole_Learner {
							peer.Role = metapb.PeerRole_Voter
						}
						// exit joint state
						if peer.Role == metapb.PeerRole_DemotingVoter {
							peer.Role = metapb.PeerRole_Learner
						} else if peer.Role == metapb.PeerRole_IncomingVoter {
							peer.Role = metapb.PeerRole_Voter
						}
					}
					for _, failedVoter := range demote.GetFailedVoters() {
						for _, peer := range region.GetPeers() {
							if failedVoter.GetId() == peer.GetId() {
								peer.Role = metapb.PeerRole_Learner
								break
							}
						}
					}
					region.RegionEpoch.ConfVer += 1
					if store == storeID {
						c.Assert(report.IsForceLeader, IsTrue)
					}
					break
				}
			}
		}
	}

	for _, report := range reports.PeerReports {
		report.IsForceLeader = false
	}
}

func advanceUntilFinished(c *C, recoveryController *unsafeRecoveryController, reports map[uint64]*pdpb.StoreReport) {
	retry := 0

	for {
		for storeID, report := range reports {
			req := newStoreHeartbeat(storeID, report)
			req.StoreReport = report
			resp := &pdpb.StoreHeartbeatResponse{}
			recoveryController.HandleStoreHeartbeat(req, resp)
			applyRecoveryPlan(c, storeID, reports, resp)
		}
		if recoveryController.GetStage() == finished {
			break
		} else if recoveryController.GetStage() == failed {
			panic("failed to recovery")
		} else if retry >= 10 {
			panic("retry timeout")
		}
		retry += 1
	}
}

func (s *testUnsafeRecoverySuite) TestFinished(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	c.Assert(recoveryController.GetStage(), Equals, collectReport)
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		// require peer report by empty plan
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Creates), Equals, 0)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 0)
		c.Assert(resp.RecoveryPlan.ForceLeader, IsNil)
		c.Assert(resp.RecoveryPlan.Step, Equals, uint64(1))
		applyRecoveryPlan(c, storeID, reports, resp)
	}

	// receive all reports and dispatch plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(resp.RecoveryPlan.ForceLeader, NotNil)
		c.Assert(len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders), Equals, 1)
		c.Assert(resp.RecoveryPlan.ForceLeader.FailedStores, NotNil)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 1)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, demoteFailedVoter)
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, IsNil)
		// remove the two failed peers
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, finished)
}

func (s *testUnsafeRecoverySuite) TestFailed(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	c.Assert(recoveryController.GetStage(), Equals, collectReport)
	// require peer report
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Creates), Equals, 0)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 0)
		c.Assert(resp.RecoveryPlan.ForceLeader, IsNil)
		applyRecoveryPlan(c, storeID, reports, resp)
	}

	// receive all reports and dispatch plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(resp.RecoveryPlan.ForceLeader, NotNil)
		c.Assert(len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders), Equals, 1)
		c.Assert(resp.RecoveryPlan.ForceLeader.FailedStores, NotNil)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 1)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, demoteFailedVoter)

	// received heartbeat from failed store, abort
	req := newStoreHeartbeat(2, nil)
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(resp.RecoveryPlan, IsNil)
	c.Assert(recoveryController.GetStage(), Equals, exitForceLeader)

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		applyRecoveryPlan(c, storeID, reports, resp)
	}

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, failed)
}

func (s *testUnsafeRecoverySuite) TestForceLeaderFail(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 1), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {
			PeerReports: []*pdpb.PeerReport{
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1001,
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
			},
		},
	}

	req := newStoreHeartbeat(1, reports[1])
	resp := &pdpb.StoreHeartbeatResponse{}
	req.StoreReport.Step = 1
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	applyRecoveryPlan(c, 1, reports, resp)
	// force leader doesn't succeed
	reports[1].PeerReports[0].IsForceLeader = false
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	// force leader succeed this time
	applyRecoveryPlan(c, 1, reports, resp)
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, demoteFailedVoter)
}

func (s *testUnsafeRecoverySuite) TestAffectedTableID(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {
			PeerReports: []*pdpb.PeerReport{
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1001,
							StartKey:    codec.EncodeBytes(codec.GenerateTableKey(6)),
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
			},
		},
	}

	advanceUntilFinished(c, recoveryController, reports)

	c.Assert(len(recoveryController.affectedTableIDs), Equals, 1)
	_, exists := recoveryController.affectedTableIDs[6]
	c.Assert(exists, IsTrue)
}

func (s *testUnsafeRecoverySuite) TestForceLeaderForCommitMerge(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {
			PeerReports: []*pdpb.PeerReport{
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1001,
							StartKey:    []byte(""),
							EndKey:      []byte("x"),
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1002,
							StartKey:    []byte("x"),
							EndKey:      []byte(""),
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}},
					HasCommitMerge: true,
				},
			},
		},
	}

	req := newStoreHeartbeat(1, reports[1])
	resp := &pdpb.StoreHeartbeatResponse{}
	req.StoreReport.Step = 1
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, forceLeaderForCommitMerge)

	// force leader on regions of commit merge first
	c.Assert(resp.RecoveryPlan, NotNil)
	c.Assert(resp.RecoveryPlan.ForceLeader, NotNil)
	c.Assert(len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders), Equals, 1)
	c.Assert(resp.RecoveryPlan.ForceLeader.EnterForceLeaders[0], Equals, uint64(1002))
	c.Assert(resp.RecoveryPlan.ForceLeader.FailedStores, NotNil)
	applyRecoveryPlan(c, 1, reports, resp)

	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	// force leader on the rest regions
	c.Assert(resp.RecoveryPlan, NotNil)
	c.Assert(resp.RecoveryPlan.ForceLeader, NotNil)
	c.Assert(len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders), Equals, 1)
	c.Assert(resp.RecoveryPlan.ForceLeader.EnterForceLeaders[0], Equals, uint64(1001))
	c.Assert(resp.RecoveryPlan.ForceLeader.FailedStores, NotNil)
	applyRecoveryPlan(c, 1, reports, resp)

	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, demoteFailedVoter)
}

func (s *testUnsafeRecoverySuite) TestOneLearner(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Learner}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}}}},
		}},
	}

	advanceUntilFinished(c, recoveryController, reports)

	expects := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 13, StoreId: 3, Role: metapb.PeerRole_Learner}}}}},
		}},
	}

	for storeID, report := range reports {
		if result, ok := expects[storeID]; ok {
			c.Assert(report.PeerReports, DeepEquals, result.PeerReports)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

func (s *testUnsafeRecoverySuite) TestUninitializedPeer(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					// uninitialized region that has no peer list
					Region: &metapb.Region{
						Id: 1001,
					}}},
		}},
	}

	advanceUntilFinished(c, recoveryController, reports)

	expects := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 2, StoreId: 1}}}}},
		}},
	}

	for storeID, report := range reports {
		if result, ok := expects[storeID]; ok {
			c.Assert(report.PeerReports, DeepEquals, result.PeerReports)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

func (s *testUnsafeRecoverySuite) TestJointState(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
							{Id: 12, StoreId: 4, Role: metapb.PeerRole_DemotingVoter},
							{Id: 13, StoreId: 5, Role: metapb.PeerRole_DemotingVoter},
							{Id: 14, StoreId: 2, Role: metapb.PeerRole_IncomingVoter},
							{Id: 15, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
						}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
							{Id: 22, StoreId: 4},
							{Id: 23, StoreId: 5},
							{Id: 24, StoreId: 2, Role: metapb.PeerRole_IncomingVoter},
						}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
							{Id: 12, StoreId: 4, Role: metapb.PeerRole_DemotingVoter},
							{Id: 13, StoreId: 5, Role: metapb.PeerRole_DemotingVoter},
							{Id: 14, StoreId: 2, Role: metapb.PeerRole_IncomingVoter},
							{Id: 15, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
						}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
							{Id: 22, StoreId: 4},
							{Id: 23, StoreId: 5},
							{Id: 24, StoreId: 2, Role: metapb.PeerRole_IncomingVoter},
						}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
							{Id: 12, StoreId: 4, Role: metapb.PeerRole_DemotingVoter},
							{Id: 13, StoreId: 5, Role: metapb.PeerRole_DemotingVoter},
							{Id: 14, StoreId: 2, Role: metapb.PeerRole_IncomingVoter},
							{Id: 15, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
						}}}},
		}},
	}

	advanceUntilFinished(c, recoveryController, reports)

	expects := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
							{Id: 12, StoreId: 4, Role: metapb.PeerRole_Learner},
							{Id: 13, StoreId: 5, Role: metapb.PeerRole_Learner},
							{Id: 14, StoreId: 2, Role: metapb.PeerRole_Voter},
							{Id: 15, StoreId: 3, Role: metapb.PeerRole_Voter},
						}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 1, Role: metapb.PeerRole_Learner},
							{Id: 22, StoreId: 4, Role: metapb.PeerRole_Learner},
							{Id: 23, StoreId: 5, Role: metapb.PeerRole_Learner},
							{Id: 24, StoreId: 2, Role: metapb.PeerRole_Voter},
						}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
							{Id: 12, StoreId: 4, Role: metapb.PeerRole_Learner},
							{Id: 13, StoreId: 5, Role: metapb.PeerRole_Learner},
							{Id: 14, StoreId: 2, Role: metapb.PeerRole_Voter},
							{Id: 15, StoreId: 3, Role: metapb.PeerRole_Voter},
						}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 1, Role: metapb.PeerRole_Learner},
							{Id: 22, StoreId: 4, Role: metapb.PeerRole_Learner},
							{Id: 23, StoreId: 5, Role: metapb.PeerRole_Learner},
							{Id: 24, StoreId: 2, Role: metapb.PeerRole_Voter},
						}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
							{Id: 12, StoreId: 4, Role: metapb.PeerRole_Learner},
							{Id: 13, StoreId: 5, Role: metapb.PeerRole_Learner},
							{Id: 14, StoreId: 2, Role: metapb.PeerRole_Voter},
							{Id: 15, StoreId: 3, Role: metapb.PeerRole_Voter},
						}}}},
		}},
	}

	for storeID, report := range reports {
		if result, ok := expects[storeID]; ok {
			c.Assert(report.PeerReports, DeepEquals, result.PeerReports)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

func (s *testUnsafeRecoverySuite) TestTimeout(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 1), IsNil)

	time.Sleep(time.Second)
	req := newStoreHeartbeat(1, nil)
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, exitForceLeader)
	req.StoreReport = &pdpb.StoreReport{Step: 2}
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, failed)
}

func (s *testUnsafeRecoverySuite) TestExitForceLeader(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {
			PeerReports: []*pdpb.PeerReport{
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1001,
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 31, StoreId: 3, Role: metapb.PeerRole_Learner}}}},
					IsForceLeader: true,
				},
			},
			Step: 1,
		},
	}

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, exitForceLeader)

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, finished)

	expects := map[uint64]*pdpb.StoreReport{
		1: {
			PeerReports: []*pdpb.PeerReport{
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1001,
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 31, StoreId: 3, Role: metapb.PeerRole_Learner}}}}},
			},
		},
	}

	for storeID, report := range reports {
		if result, ok := expects[storeID]; ok {
			c.Assert(report.PeerReports, DeepEquals, result.PeerReports)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

func (s *testUnsafeRecoverySuite) TestStep(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {
			PeerReports: []*pdpb.PeerReport{
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1001,
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
			},
		},
	}

	req := newStoreHeartbeat(1, reports[1])
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	// step is not set, ignore
	c.Assert(recoveryController.GetStage(), Equals, collectReport)

	// valid store report
	req.StoreReport.Step = 1
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	// duplicate report with same step, ignore
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)
	applyRecoveryPlan(c, 1, reports, resp)
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, demoteFailedVoter)
	applyRecoveryPlan(c, 1, reports, resp)
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(recoveryController.GetStage(), Equals, finished)
}

func (s *testUnsafeRecoverySuite) TestOnHealthyRegions(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	c.Assert(recoveryController.GetStage(), Equals, collectReport)
	// require peer report
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Creates), Equals, 0)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 0)
		c.Assert(resp.RecoveryPlan.ForceLeader, IsNil)
		applyRecoveryPlan(c, storeID, reports, resp)
	}

	// receive all reports and dispatch no plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, IsNil)
		applyRecoveryPlan(c, storeID, reports, resp)
	}
	// nothing to do, finish directly
	c.Assert(recoveryController.GetStage(), Equals, finished)
}

func (s *testUnsafeRecoverySuite) TestCreateEmptyRegion(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte("a"),
						EndKey:      []byte("b"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("e"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}}}},
		}},
	}

	advanceUntilFinished(c, recoveryController, reports)

	expects := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte("a"),
						EndKey:      []byte("b"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 13, StoreId: 3, Role: metapb.PeerRole_Learner}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("e"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 13, StoreId: 3, Role: metapb.PeerRole_Learner}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("a"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers:       []*metapb.Peer{{Id: 2, StoreId: 1}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          3,
						StartKey:    []byte("b"),
						EndKey:      []byte("e"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers:       []*metapb.Peer{{Id: 4, StoreId: 1}}}}},
		}},
	}

	for storeID, report := range reports {
		if expect, ok := expects[storeID]; ok {
			c.Assert(report.PeerReports, DeepEquals, expect.PeerReports)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

// TODO: can't handle this case now
// +──────────────────────────────────+───────────────────+───────────────────+───────────────────+───────────────────+──────────+──────────+
// |                                  | Store 1           | Store 2           | Store 3           | Store 4           | Store 5  | Store 6  |
// +──────────────────────────────────+───────────────────+───────────────────+───────────────────+───────────────────+──────────+──────────+
// | Initial                          | A=[a,m), B=[m,z)  | A=[a,m), B=[m,z)  | A=[a,m), B=[m,z)  |                   |          |          |
// | A merge B                        | isolate           | A=[a,z)           | A=[a,z)           |                   |          |          |
// | Conf Change A: store 1 -> 4      |                   | A=[a,z)           | A=[a,z)           | A=[a,z)           |          |          |
// | A split C                        |                   | isolate           | C=[a,g), A=[g,z)  | C=[a,g), A=[g,z)  |          |          |
// | Conf Change A: store 3,4 -> 5,6  |                   |                   | C=[a,g)           | C=[a,g)           | A=[g,z)  | A=[g,z)  |
// | Store 4, 5 and 6 fail            | A=[a,m), B=[m,z)  | A=[a,z)           | C=[a,g)           | fail              | fail     | fail     |
// +──────────────────────────────────+───────────────────+───────────────────+───────────────────+───────────────────+──────────+──────────+

func (s *testUnsafeRecoverySuite) TestRangeOverlap1(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 4}, {Id: 13, StoreId: 5}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte(""),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 5, Version: 8},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 1}, {Id: 22, StoreId: 4}, {Id: 23, StoreId: 5}}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1003,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 31, StoreId: 1}, {Id: 32, StoreId: 4}, {Id: 33, StoreId: 5}}}}},
		}},
	}

	advanceUntilFinished(c, recoveryController, reports)

	expects := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 4, Role: metapb.PeerRole_Learner}, {Id: 13, StoreId: 5, Role: metapb.PeerRole_Learner}}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1003,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 5, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 31, StoreId: 1}, {Id: 32, StoreId: 4, Role: metapb.PeerRole_Learner}, {Id: 33, StoreId: 5, Role: metapb.PeerRole_Learner}}}}},
		}},
	}

	for storeID, report := range reports {
		if result, ok := expects[storeID]; ok {
			c.Assert(report.PeerReports, DeepEquals, result.PeerReports)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

func (s *testUnsafeRecoverySuite) TestRangeOverlap2(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60), IsNil)

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 4}, {Id: 13, StoreId: 5}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte(""),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 5, Version: 8},
						Peers: []*metapb.Peer{
							{Id: 24, StoreId: 1}, {Id: 22, StoreId: 4}, {Id: 23, StoreId: 5}}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 1}, {Id: 22, StoreId: 4}, {Id: 23, StoreId: 5}}}}},
		}},
	}

	advanceUntilFinished(c, recoveryController, reports)

	expects := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 8, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 4, Role: metapb.PeerRole_Learner}, {Id: 13, StoreId: 5, Role: metapb.PeerRole_Learner}}}}},
			// newly created empty region
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte("x"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 2, StoreId: 1}}}}},
		}},
	}

	for storeID, report := range reports {
		if result, ok := expects[storeID]; ok {
			c.Assert(report.PeerReports, DeepEquals, result.PeerReports)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

func (s *testUnsafeRecoverySuite) TestRemoveFailedStores(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)

	// Store 3 doesn't exist, reject to remove.
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		1: {},
		3: {},
	}, 60), NotNil)

	c.Assert(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		1: {},
	}, 60), IsNil)
	c.Assert(cluster.GetStore(uint64(1)).IsRemoved(), IsTrue)
	for _, s := range cluster.GetSchedulers() {
		paused, err := cluster.IsSchedulerAllowed(s)
		if s != "split-bucket-scheduler" {
			c.Assert(err, IsNil)
			c.Assert(paused, IsTrue)
		}
	}

	// Store 2's last heartbeat is recent, and is not allowed to be removed.
	c.Assert(recoveryController.RemoveFailedStores(
		map[uint64]struct{}{
			2: {},
		}, 60), NotNil)
}

func (s *testUnsafeRecoverySuite) TestSplitPaused(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	cluster.Lock()
	cluster.unsafeRecoveryController = recoveryController
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.meta.GetId(), cluster, true))
	cluster.Unlock()
	cluster.coordinator.run()
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	failedStores := map[uint64]struct{}{
		1: {},
	}
	c.Assert(recoveryController.RemoveFailedStores(failedStores, 60), IsNil)
	askSplitReq := &pdpb.AskSplitRequest{}
	_, err := cluster.HandleAskSplit(askSplitReq)
	c.Assert(err.Error(), Equals, "[PD:unsaferecovery:ErrUnsafeRecoveryIsRunning]unsafe recovery is running")
	askBatchSplitReq := &pdpb.AskBatchSplitRequest{}
	_, err = cluster.HandleAskBatchSplit(askBatchSplitReq)
	c.Assert(err.Error(), Equals, "[PD:unsaferecovery:ErrUnsafeRecoveryIsRunning]unsafe recovery is running")
}
