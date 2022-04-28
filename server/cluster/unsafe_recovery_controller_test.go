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
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/hbstream"
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

func newStoreHeartbeat(storeID uint64, report *pdpb.StoreReport) *pdpb.StoreHeartbeatRequest {
	return &pdpb.StoreHeartbeatRequest{
		Stats: &pdpb.StoreStats{
			StoreId: storeID,
		},
		StoreReport: report,
	}
}

func applyRecoveryPlan(c *C, reports *pdpb.StoreReport, resp *pdpb.StoreHeartbeatResponse) {
	plan := resp.GetRecoveryPlan()
	if plan == nil {
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
		for _, report := range reports.PeerReports {
			region := report.GetRegionState().GetRegion()
			if region.GetId() == demote.GetRegionId() {
				for _, failedVoter := range demote.GetFailedVoters() {
					for _, peer := range region.GetPeers() {
						if failedVoter.GetId() == peer.GetId() {
							peer.Role = metapb.PeerRole_Learner
							break
						}
					}
				}
				c.Assert(report.IsForceLeader, IsTrue)
				report.IsForceLeader = false
				break
			}
		}
	}

	forceLeaders := plan.GetForceLeader()
	if forceLeaders == nil {
		return
	}
	for _, forceLeader := range forceLeaders.GetEnterForceLeaders() {
		for _, report := range reports.PeerReports {
			region := report.GetRegionState().GetRegion()
			if region.GetId() == forceLeader {
				report.IsForceLeader = true
				break
			}
		}
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
			applyRecoveryPlan(c, report, resp)
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

func (s *testUnsafeRecoverSuite) TestRecoveryFinished(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.getClusterID(), cluster, true))
	cluster.coordinator.run()
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
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		// require peer report by empty plan
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Creates), Equals, 0)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 0)
		c.Assert(resp.RecoveryPlan.ForceLeader, IsNil)
		applyRecoveryPlan(c, report, resp)
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
		applyRecoveryPlan(c, report, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 1)
		applyRecoveryPlan(c, report, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, demoteFailedVoter)
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, IsNil)
		// remove the two failed peers
		applyRecoveryPlan(c, report, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, finished)
}

func (s *testUnsafeRecoverSuite) TestRecoveryFailed(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.getClusterID(), cluster, true))
	cluster.coordinator.run()
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
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Creates), Equals, 0)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 0)
		c.Assert(resp.RecoveryPlan.ForceLeader, IsNil)
		applyRecoveryPlan(c, report, resp)
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
		applyRecoveryPlan(c, report, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, forceLeader)

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 1)
		applyRecoveryPlan(c, report, resp)
	}
	c.Assert(recoveryController.GetStage(), Equals, demoteFailedVoter)

	// received heartbeat from failed store, abort
	req := newStoreHeartbeat(2, nil)
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	c.Assert(resp.RecoveryPlan, IsNil)
	c.Assert(recoveryController.GetStage(), Equals, failed)
}

// TODO:
// 1. handle retry
// 2. force leader is not executed correctly

func (s *testUnsafeRecoverSuite) TestRecoveryOnHealthyRegions(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.getClusterID(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]interface{}{
		4: "",
		5: "",
	}), IsNil)

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
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, NotNil)
		c.Assert(len(resp.RecoveryPlan.Creates), Equals, 0)
		c.Assert(len(resp.RecoveryPlan.Demotes), Equals, 0)
		c.Assert(resp.RecoveryPlan.ForceLeader, IsNil)
		applyRecoveryPlan(c, report, resp)
	}

	// receive all reports and dispatch no plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		c.Assert(resp.RecoveryPlan, IsNil)
		applyRecoveryPlan(c, report, resp)
	}
	// nothing to do, finish directly
	c.Assert(recoveryController.GetStage(), Equals, finished)
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

func (s *testUnsafeRecoverSuite) TestRangeOverlap1(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.getClusterID(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]interface{}{
		4: "",
		5: "",
	}), IsNil)

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

	expect_results := map[uint64]*pdpb.StoreReport{
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
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 31, StoreId: 1}, {Id: 32, StoreId: 4, Role: metapb.PeerRole_Learner}, {Id: 33, StoreId: 5, Role: metapb.PeerRole_Learner}}}}},
		}},
	}

	for storeID, report := range reports {
		if result, ok := expect_results[storeID]; ok {
			c.Assert(report, DeepEquals, result)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

func (s *testUnsafeRecoverSuite) TestRangeOverlap2(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.getClusterID(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	c.Assert(recoveryController.RemoveFailedStores(map[uint64]interface{}{
		4: "",
		5: "",
	}), IsNil)

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

	expect_results := map[uint64]*pdpb.StoreReport{
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
		if result, ok := expect_results[storeID]; ok {
			c.Assert(report, DeepEquals, result)
		} else {
			c.Assert(len(report.PeerReports), Equals, 0)
		}
	}
}

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

// 	// retry
// 	// normal store heartbeat within the timeout

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

func (s *testUnsafeRecoverSuite) TestRemoveFailedStores(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.getClusterID(), cluster, true))
	cluster.coordinator.run()
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
	for _, s := range cluster.GetSchedulers() {
		paused, err := cluster.IsSchedulerAllowed(s)
		c.Assert(err, IsNil)
		c.Assert(paused, IsTrue)
	}

	// Store 2's last heartbeat is recent, and is not allowed to be removed.
	c.Assert(recoveryController.RemoveFailedStores(
		map[uint64]interface{}{
			2: "",
		}), NotNil)
}

func (s *testUnsafeRecoverSuite) TestSplitPaused(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, hbstream.NewTestHeartbeatStreams(s.ctx, cluster.getClusterID(), cluster, true))
	cluster.coordinator.run()
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		c.Assert(cluster.PutStore(store.GetMeta()), IsNil)
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	cluster.unsafeRecoveryController = recoveryController
	failedStores := map[uint64]interface{}{
		1: "",
	}
	c.Assert(recoveryController.RemoveFailedStores(failedStores), IsNil)
	askSplitReq := &pdpb.AskSplitRequest{}
	_, err := cluster.HandleAskSplit(askSplitReq)
	c.Assert(err.Error(), Equals, "[PD:unsaferecovery:ErrUnsafeRecoveryIsRunning]unsafe recovery is running")
	askBatchSplitReq := &pdpb.AskBatchSplitRequest{}
	_, err = cluster.HandleAskBatchSplit(askBatchSplitReq)
	c.Assert(err.Error(), Equals, "[PD:unsaferecovery:ErrUnsafeRecoveryIsRunning]unsafe recovery is running")
}
