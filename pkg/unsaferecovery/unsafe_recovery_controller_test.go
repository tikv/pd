// Copyright 2021 TiKV Project Authors.
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

// change the package to avoid import cycle
package unsaferecovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/hbstream"
)

func newStoreHeartbeat(storeID uint64, report *pdpb.StoreReport) *pdpb.StoreHeartbeatRequest {
	return &pdpb.StoreHeartbeatRequest{
		Stats: &pdpb.StoreStats{
			StoreId: storeID,
		},
		StoreReport: report,
	}
}

func hasQuorum(region *metapb.Region, failedStores []uint64) bool {
	hasQuorum := func(voters []*metapb.Peer) bool {
		numFailedVoters := 0
		numLiveVoters := 0

		for _, voter := range voters {
			found := false
			for _, store := range failedStores {
				if store == voter.GetStoreId() {
					found = true
					break
				}
			}
			if found {
				numFailedVoters += 1
			} else {
				numLiveVoters += 1
			}
		}
		return numFailedVoters < numLiveVoters
	}

	// consider joint consensus
	var incomingVoters []*metapb.Peer
	var outgoingVoters []*metapb.Peer

	for _, peer := range region.Peers {
		if peer.Role == metapb.PeerRole_Voter || peer.Role == metapb.PeerRole_IncomingVoter {
			incomingVoters = append(incomingVoters, peer)
		}
		if peer.Role == metapb.PeerRole_Voter || peer.Role == metapb.PeerRole_DemotingVoter {
			outgoingVoters = append(outgoingVoters, peer)
		}
	}

	return hasQuorum(incomingVoters) && hasQuorum(outgoingVoters)
}

func applyRecoveryPlan(re *require.Assertions, storeID uint64, storeReports map[uint64]*pdpb.StoreReport, resp *pdpb.StoreHeartbeatResponse) {
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
					if hasQuorum(region, forceLeaders.GetFailedStores()) {
						re.FailNow("should not enter force leader when quorum is still alive")
					}
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
						re.True(report.IsForceLeader)
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

func advanceUntilFinished(re *require.Assertions, recoveryController *Controller, reports map[uint64]*pdpb.StoreReport) {
	retry := 0

	for {
		for storeID, report := range reports {
			req := newStoreHeartbeat(storeID, report)
			req.StoreReport = report
			resp := &pdpb.StoreHeartbeatResponse{}
			recoveryController.HandleStoreHeartbeat(req, resp)
			applyRecoveryPlan(re, storeID, reports, resp)
		}
		if recoveryController.GetStage() == Finished {
			break
		} else if recoveryController.GetStage() == Failed {
			panic("Failed to recovery")
		} else if retry >= 10 {
			panic("retry timeout")
		}
		retry += 1
	}
}

func TestFinished(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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
	re.Equal(CollectReport, recoveryController.GetStage())
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		// require peer report by empty plan
		re.NotNil(resp.RecoveryPlan)
		re.Empty(resp.RecoveryPlan.Creates)
		re.Empty(resp.RecoveryPlan.Demotes)
		re.Nil(resp.RecoveryPlan.ForceLeader)
		re.Equal(uint64(1), resp.RecoveryPlan.Step)
		applyRecoveryPlan(re, storeID, reports, resp)
	}

	// receive all reports and dispatch plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.NotNil(resp.RecoveryPlan.ForceLeader)
		re.Len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders, 1)
		re.NotNil(resp.RecoveryPlan.ForceLeader.FailedStores)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(ForceLeader, recoveryController.GetStage())

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Len(resp.RecoveryPlan.Demotes, 1)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(DemoteFailedVoter, recoveryController.GetStage())
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.Nil(resp.RecoveryPlan)
		// remove the two failed peers
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(Finished, recoveryController.GetStage())
}

func TestFailed(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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
	re.Equal(CollectReport, recoveryController.GetStage())
	// require peer report
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Empty(resp.RecoveryPlan.Creates)
		re.Empty(resp.RecoveryPlan.Demotes)
		re.Nil(resp.RecoveryPlan.ForceLeader)
		applyRecoveryPlan(re, storeID, reports, resp)
	}

	// receive all reports and dispatch plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.NotNil(resp.RecoveryPlan.ForceLeader)
		re.Len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders, 1)
		re.NotNil(resp.RecoveryPlan.ForceLeader.FailedStores)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(ForceLeader, recoveryController.GetStage())

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Len(resp.RecoveryPlan.Demotes, 1)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(DemoteFailedVoter, recoveryController.GetStage())

	// received heartbeat from Failed store, abort
	req := newStoreHeartbeat(2, nil)
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(ExitForceLeader, recoveryController.GetStage())

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		applyRecoveryPlan(re, storeID, reports, resp)
	}

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(Failed, recoveryController.GetStage())
}

func TestForceLeaderFail(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(4, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		3: {},
		4: {},
	}, 60, false))

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
								{Id: 11, StoreId: 1}, {Id: 21, StoreId: 3}, {Id: 31, StoreId: 4}}}}},
			},
		},
		2: {
			PeerReports: []*pdpb.PeerReport{
				{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1002,
							StartKey:    []byte("x"),
							EndKey:      []byte(""),
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 10, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 12, StoreId: 2}, {Id: 22, StoreId: 3}, {Id: 32, StoreId: 4}}}}},
			},
		},
	}

	req1 := newStoreHeartbeat(1, reports[1])
	resp1 := &pdpb.StoreHeartbeatResponse{}
	req1.StoreReport.Step = 1
	recoveryController.HandleStoreHeartbeat(req1, resp1)
	req2 := newStoreHeartbeat(2, reports[2])
	resp2 := &pdpb.StoreHeartbeatResponse{}
	req2.StoreReport.Step = 1
	recoveryController.HandleStoreHeartbeat(req2, resp2)
	re.Equal(ForceLeader, recoveryController.GetStage())
	recoveryController.HandleStoreHeartbeat(req1, resp1)

	// force leader on store 1 succeed
	applyRecoveryPlan(re, 1, reports, resp1)
	applyRecoveryPlan(re, 2, reports, resp2)
	// force leader on store 2 doesn't succeed
	reports[2].PeerReports[0].IsForceLeader = false

	// force leader should retry on store 2
	recoveryController.HandleStoreHeartbeat(req1, resp1)
	recoveryController.HandleStoreHeartbeat(req2, resp2)
	re.Equal(ForceLeader, recoveryController.GetStage())
	recoveryController.HandleStoreHeartbeat(req1, resp1)

	// force leader succeed this time
	applyRecoveryPlan(re, 1, reports, resp1)
	applyRecoveryPlan(re, 2, reports, resp2)
	recoveryController.HandleStoreHeartbeat(req1, resp1)
	recoveryController.HandleStoreHeartbeat(req2, resp2)
	re.Equal(DemoteFailedVoter, recoveryController.GetStage())
}

func TestAffectedTableID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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

	advanceUntilFinished(re, recoveryController, reports)

	re.Len(recoveryController.AffectedTableIDs, 1)
	_, exists := recoveryController.AffectedTableIDs[6]
	re.True(exists)
}

func TestForceLeaderForCommitMerge(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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
	re.Equal(ForceLeaderForCommitMerge, recoveryController.GetStage())

	// force leader on regions of commit merge first
	re.NotNil(resp.RecoveryPlan)
	re.NotNil(resp.RecoveryPlan.ForceLeader)
	re.Len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders, 1)
	re.Equal(uint64(1002), resp.RecoveryPlan.ForceLeader.EnterForceLeaders[0])
	re.NotNil(resp.RecoveryPlan.ForceLeader.FailedStores)
	applyRecoveryPlan(re, 1, reports, resp)

	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(ForceLeader, recoveryController.GetStage())

	// force leader on the rest regions
	re.NotNil(resp.RecoveryPlan)
	re.NotNil(resp.RecoveryPlan.ForceLeader)
	re.Len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders, 1)
	re.Equal(uint64(1001), resp.RecoveryPlan.ForceLeader.EnterForceLeaders[0])
	re.NotNil(resp.RecoveryPlan.ForceLeader.FailedStores)
	applyRecoveryPlan(re, 1, reports, resp)

	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(DemoteFailedVoter, recoveryController.GetStage())
}

func TestAutoDetectMode(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(1, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(nil, 60, true))

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}}}},
		}},
	}

	advanceUntilFinished(re, recoveryController, reports)

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
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

// Failed learner replica store should be considered by auto-detect mode.
func TestAutoDetectWithOneLearner(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(1, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(nil, 60, true))

	storeReport := pdpb.StoreReport{
		PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3, Role: metapb.PeerRole_Learner}}}}},
		},
	}
	req := newStoreHeartbeat(1, &storeReport)
	req.StoreReport.Step = 1
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	hasStore3AsFailedStore := false
	for _, failedStore := range resp.RecoveryPlan.ForceLeader.FailedStores {
		if failedStore == 3 {
			hasStore3AsFailedStore = true
			break
		}
	}
	re.True(hasStore3AsFailedStore)
}

func TestOneLearner(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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

	advanceUntilFinished(re, recoveryController, reports)

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
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

func TestTiflashLearnerPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(5, "6.0.0") {
		if store.GetID() == 3 {
			store.GetMeta().Labels = []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}
		}
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60, false))

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
							{Id: 11, StoreId: 1},
							{Id: 12, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 13, StoreId: 5},
						}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte("z"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 2, Role: metapb.PeerRole_Learner},
							{Id: 22, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 23, StoreId: 4},
						}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 11, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("x"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1},
							{Id: 12, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 13, StoreId: 5},
						}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte("z"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 2, Role: metapb.PeerRole_Learner},
							{Id: 22, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 23, StoreId: 4},
						}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1003,
						StartKey:    []byte("z"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 7, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 31, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 32, StoreId: 4},
							{Id: 33, StoreId: 5},
						}}}},
		}},
	}

	advanceUntilFinished(re, recoveryController, reports)

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
							{Id: 11, StoreId: 1},
							{Id: 12, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 13, StoreId: 5, Role: metapb.PeerRole_Learner},
						}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte("z"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter},
							{Id: 22, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 23, StoreId: 4, Role: metapb.PeerRole_Learner},
						}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("x"),
						EndKey:      []byte("z"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
						Peers: []*metapb.Peer{
							{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter},
							{Id: 22, StoreId: 3, Role: metapb.PeerRole_Learner},
							{Id: 23, StoreId: 4, Role: metapb.PeerRole_Learner},
						}}}},
		}},
	}

	for storeID, report := range reports {
		for i, p := range report.PeerReports {
			// As the store of newly created region is not fixed, check it separately
			if p.RegionState.Region.GetId() == 1 {
				re.Equal(&pdpb.PeerReport{
					RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
					RegionState: &raft_serverpb.RegionLocalState{
						Region: &metapb.Region{
							Id:          1,
							StartKey:    []byte("z"),
							EndKey:      []byte(""),
							RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
							Peers: []*metapb.Peer{
								{Id: 2, StoreId: storeID},
							},
						},
					},
				}, p)
				report.PeerReports = append(report.PeerReports[:i], report.PeerReports[i+1:]...)
				break
			}
		}
		if result, ok := expects[storeID]; ok {
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

func TestUninitializedPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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

	advanceUntilFinished(re, recoveryController, reports)

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
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

func TestJointState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(5, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60, false))

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

	advanceUntilFinished(re, recoveryController, reports)

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
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

func TestExecutionTimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 1, false))

	time.Sleep(time.Second)
	req := newStoreHeartbeat(1, nil)
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(ExitForceLeader, recoveryController.GetStage())
	req.StoreReport = &pdpb.StoreReport{Step: 2}
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(Failed, recoveryController.GetStage())

	output := recoveryController.Show()
	re.Len(output, 3)
	re.Contains(output[1].Details[0], "triggered by error: Exceeds timeout")
}

func TestNoHeartbeatTimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 1, false))

	time.Sleep(time.Second)
	recoveryController.Show()
	re.Equal(ExitForceLeader, recoveryController.GetStage())
}

func TestExitForceLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(ExitForceLeader, recoveryController.GetStage())

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(Finished, recoveryController.GetStage())

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
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

func TestStep(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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
	re.Equal(CollectReport, recoveryController.GetStage())

	// valid store report
	req.StoreReport.Step = 1
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(ForceLeader, recoveryController.GetStage())

	// duplicate report with same step, ignore
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(ForceLeader, recoveryController.GetStage())
	applyRecoveryPlan(re, 1, reports, resp)
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(DemoteFailedVoter, recoveryController.GetStage())
	applyRecoveryPlan(re, 1, reports, resp)
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(Finished, recoveryController.GetStage())
}

func TestOnHealthyRegions(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(5, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60, false))

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
	re.Equal(CollectReport, recoveryController.GetStage())
	// require peer report
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Empty(resp.RecoveryPlan.Creates)
		re.Empty(resp.RecoveryPlan.Demotes)
		re.Nil(resp.RecoveryPlan.ForceLeader)
		applyRecoveryPlan(re, storeID, reports, resp)
	}

	// receive all reports and dispatch no plan
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.Nil(resp.RecoveryPlan)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	// nothing to do, finish directly
	re.Equal(Finished, recoveryController.GetStage())
}

func TestCreateEmptyRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

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

	advanceUntilFinished(re, recoveryController, reports)

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
			re.Equal(expect.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
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

func TestRangeOverlap1(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(5, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60, false))

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

	advanceUntilFinished(re, recoveryController, reports)

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
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

func TestRangeOverlap2(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(5, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		4: {},
		5: {},
	}, 60, false))

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

	advanceUntilFinished(re, recoveryController, reports)

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
			re.Equal(result.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

func TestRemoveFailedStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	// Store 3 doesn't exist, reject to remove.
	re.Error(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		1: {},
		3: {},
	}, 60, false))

	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		1: {},
	}, 60, false))
	re.True(cluster.GetStore(uint64(1)).IsRemoved())
	schedulers := coordinator.GetSchedulersController()
	for _, s := range schedulers.GetSchedulerNames() {
		paused, err := schedulers.IsSchedulerAllowed(s)
		if s != "split-bucket-scheduler" {
			re.NoError(err)
			re.True(paused)
		}
	}

	// Store 2's last heartbeat is recent, and is not allowed to be removed.
	re.Error(recoveryController.RemoveFailedStores(
		map[uint64]struct{}{
			2: {},
		}, 60, false))
}

func TestRunning(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		cluster.PutStore(store)
	}
	failedStores := map[uint64]struct{}{
		1: {},
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(failedStores, 60, false))
	re.True(recoveryController.IsRunning())
}

func TestEpochComparison(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	for _, store := range newTestStores(3, "6.0.0") {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 60, false))

	reports := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1001,
						StartKey:    []byte(""),
						EndKey:      []byte("b"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 10, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},

			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("a"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 12, StoreId: 1}, {Id: 22, StoreId: 2}, {Id: 32, StoreId: 3}}}}},
		}},
	}

	advanceUntilFinished(re, recoveryController, reports)
	expects := map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1002,
						StartKey:    []byte("a"),
						EndKey:      []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 12, StoreId: 1}, {Id: 22, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 32, StoreId: 3, Role: metapb.PeerRole_Learner}}}}},

			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10, HardState: &eraftpb.HardState{Term: 1, Commit: 10}},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("a"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 2, StoreId: 1}}}}},
		}},
	}
	for storeID, report := range reports {
		if expect, ok := expects[storeID]; ok {
			re.Equal(expect.PeerReports, report.PeerReports)
		} else {
			re.Empty(report.PeerReports)
		}
	}
}

// TODO: remove them
// Create n stores (0..n).
func newTestStores(n uint64, version string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id:            i,
			Address:       fmt.Sprintf("127.0.0.1:%d", i),
			StatusAddress: fmt.Sprintf("127.0.0.1:%d", i),
			State:         metapb.StoreState_Up,
			Version:       version,
			DeployPath:    getTestDeployPath(i),
			NodeState:     metapb.NodeState_Serving,
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

func getTestDeployPath(storeID uint64) string {
	return fmt.Sprintf("test/store%d", storeID)
}

func TestSelectLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	coordinator := schedule.NewCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster, true))
	coordinator.Run()
	stores := newTestStores(6, "6.0.0")
	labels := []*metapb.StoreLabel{
		{
			Key:   core.EngineKey,
			Value: core.EngineTiFlash,
		},
	}
	stores[5].IsTiFlash()
	core.SetStoreLabels(labels)(stores[5])
	for _, store := range stores {
		cluster.PutStore(store)
	}
	recoveryController := NewController(cluster)

	cases := []struct {
		peers    []*regionItem
		leaderID uint64
	}{
		{
			peers: []*regionItem{
				newPeer(1, 1, 10, 5, 4),
				newPeer(2, 2, 9, 9, 8),
			},
			leaderID: 2,
		},
		{
			peers: []*regionItem{
				newPeer(1, 1, 10, 10, 9),
				newPeer(2, 1, 8, 8, 15),
				newPeer(3, 1, 12, 11, 11),
			},
			leaderID: 2,
		},
		{
			peers: []*regionItem{
				newPeer(1, 1, 9, 9, 11),
				newPeer(2, 1, 10, 8, 7),
				newPeer(3, 1, 11, 7, 6),
			},
			leaderID: 3,
		},
		{
			peers: []*regionItem{
				newPeer(1, 1, 11, 11, 8),
				newPeer(2, 1, 11, 10, 10),
				newPeer(3, 1, 11, 9, 8),
			},
			leaderID: 1,
		},
		{
			peers: []*regionItem{
				newPeer(6, 1, 11, 11, 9),
				newPeer(1, 1, 11, 11, 8),
				newPeer(2, 1, 11, 10, 10),
				newPeer(3, 1, 11, 9, 8),
			},
			leaderID: 1,
		},
	}

	for i, c := range cases {
		peersMap := map[uint64][]*regionItem{
			1: c.peers,
		}
		region := &metapb.Region{
			Id: 1,
		}
		leader := recoveryController.selectLeader(peersMap, region)
		re.Equal(leader.region().Id, c.leaderID, "case: %d", i)
	}
}

func newPeer(storeID, term, lastIndex, committedIndex, appliedIndex uint64) *regionItem {
	return &regionItem{
		storeID: storeID,
		report: &pdpb.PeerReport{
			RaftState: &raft_serverpb.RaftLocalState{
				HardState: &eraftpb.HardState{
					Term:   term,
					Commit: committedIndex,
				},
				LastIndex: lastIndex,
			},
			RegionState: &raft_serverpb.RegionLocalState{
				Region: &metapb.Region{
					Id: storeID,
				},
			},
			AppliedIndex: appliedIndex,
		},
	}
}
