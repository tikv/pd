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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/storage"
)

func newStoreHeartbeat(storeID uint64, report *pdpb.StoreReport) *pdpb.StoreHeartbeatRequest {
	return &pdpb.StoreHeartbeatRequest{
		Stats: &pdpb.StoreStats{
			StoreId: storeID,
		},
		StoreReport: report,
	}
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

func advanceUntilFinished(re *require.Assertions, recoveryController *unsafeRecoveryController, reports map[uint64]*pdpb.StoreReport) {
	retry := 0

	for {
		for storeID, report := range reports {
			req := newStoreHeartbeat(storeID, report)
			req.StoreReport = report
			resp := &pdpb.StoreHeartbeatResponse{}
			recoveryController.HandleStoreHeartbeat(req, resp)
			applyRecoveryPlan(re, storeID, reports, resp)
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

func TestFinished(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
	re.Equal(collectReport, recoveryController.GetStage())
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		// require peer report by empty plan
		re.NotNil(resp.RecoveryPlan)
		re.Empty(len(resp.RecoveryPlan.Creates))
		re.Empty(len(resp.RecoveryPlan.Demotes))
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
	re.Equal(forceLeader, recoveryController.GetStage())

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Len(resp.RecoveryPlan.Demotes, 1)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(demoteFailedVoter, recoveryController.GetStage())
	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.Nil(resp.RecoveryPlan)
		// remove the two failed peers
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(finished, recoveryController.GetStage())
}

func TestFailed(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
	re.Equal(collectReport, recoveryController.GetStage())
	// require peer report
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Empty(len(resp.RecoveryPlan.Creates))
		re.Empty(len(resp.RecoveryPlan.Demotes))
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
	re.Equal(forceLeader, recoveryController.GetStage())

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Len(resp.RecoveryPlan.Demotes, 1)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(demoteFailedVoter, recoveryController.GetStage())

	// received heartbeat from failed store, abort
	req := newStoreHeartbeat(2, nil)
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Nil(resp.RecoveryPlan)
	re.Equal(exitForceLeader, recoveryController.GetStage())

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
	re.Equal(failed, recoveryController.GetStage())
}

func TestForceLeaderFail(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(4, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
	re.Equal(forceLeader, recoveryController.GetStage())
	recoveryController.HandleStoreHeartbeat(req1, resp1)

	// force leader on store 1 succeed
	applyRecoveryPlan(re, 1, reports, resp1)
	applyRecoveryPlan(re, 2, reports, resp2)
	// force leader on store 2 doesn't succeed
	reports[2].PeerReports[0].IsForceLeader = false

	// force leader should retry on store 2
	recoveryController.HandleStoreHeartbeat(req1, resp1)
	recoveryController.HandleStoreHeartbeat(req2, resp2)
	re.Equal(forceLeader, recoveryController.GetStage())
	recoveryController.HandleStoreHeartbeat(req1, resp1)

	// force leader succeed this time
	applyRecoveryPlan(re, 1, reports, resp1)
	applyRecoveryPlan(re, 2, reports, resp2)
	recoveryController.HandleStoreHeartbeat(req1, resp1)
	recoveryController.HandleStoreHeartbeat(req2, resp2)
	re.Equal(demoteFailedVoter, recoveryController.GetStage())
}

func TestAffectedTableID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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

	re.Len(recoveryController.affectedTableIDs, 1)
	_, exists := recoveryController.affectedTableIDs[6]
	re.True(exists)
}

func TestForceLeaderForCommitMerge(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
	re.Equal(forceLeaderForCommitMerge, recoveryController.GetStage())

	// force leader on regions of commit merge first
	re.NotNil(resp.RecoveryPlan)
	re.NotNil(resp.RecoveryPlan.ForceLeader)
	re.Len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders, 1)
	re.Equal(uint64(1002), resp.RecoveryPlan.ForceLeader.EnterForceLeaders[0])
	re.NotNil(resp.RecoveryPlan.ForceLeader.FailedStores)
	applyRecoveryPlan(re, 1, reports, resp)

	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(forceLeader, recoveryController.GetStage())

	// force leader on the rest regions
	re.NotNil(resp.RecoveryPlan)
	re.NotNil(resp.RecoveryPlan.ForceLeader)
	re.Len(resp.RecoveryPlan.ForceLeader.EnterForceLeaders, 1)
	re.Equal(uint64(1001), resp.RecoveryPlan.ForceLeader.EnterForceLeaders[0])
	re.NotNil(resp.RecoveryPlan.ForceLeader.FailedStores)
	applyRecoveryPlan(re, 1, reports, resp)

	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(demoteFailedVoter, recoveryController.GetStage())
}

func TestAutoDetectMode(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(1, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
			re.Empty(len(report.PeerReports))
		}
	}
}

func TestOneLearner(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
			re.Empty(len(report.PeerReports))
		}
	}
}

func TestTiflashLearnerPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		if store.GetID() == 3 {
			store.GetMeta().Labels = []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}
		}
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
			re.Empty(len(report.PeerReports))
		}
	}
}

func TestUninitializedPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
			re.Empty(len(report.PeerReports))
		}
	}
}

func TestTimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		2: {},
		3: {},
	}, 1, false))

	time.Sleep(time.Second)
	req := newStoreHeartbeat(1, nil)
	resp := &pdpb.StoreHeartbeatResponse{}
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(exitForceLeader, recoveryController.GetStage())
	req.StoreReport = &pdpb.StoreReport{Step: 2}
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(failed, recoveryController.GetStage())
}

func TestExitForceLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
	re.Equal(exitForceLeader, recoveryController.GetStage())

	for storeID, report := range reports {
		req := newStoreHeartbeat(storeID, report)
		req.StoreReport = report
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		applyRecoveryPlan(re, storeID, reports, resp)
	}
	re.Equal(finished, recoveryController.GetStage())

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
			re.Empty(len(report.PeerReports))
		}
	}
}

func TestStep(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
	re.Equal(collectReport, recoveryController.GetStage())

	// valid store report
	req.StoreReport.Step = 1
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(forceLeader, recoveryController.GetStage())

	// duplicate report with same step, ignore
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(forceLeader, recoveryController.GetStage())
	applyRecoveryPlan(re, 1, reports, resp)
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(demoteFailedVoter, recoveryController.GetStage())
	applyRecoveryPlan(re, 1, reports, resp)
	recoveryController.HandleStoreHeartbeat(req, resp)
	re.Equal(finished, recoveryController.GetStage())
}

func TestOnHealthyRegions(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
	re.Equal(collectReport, recoveryController.GetStage())
	// require peer report
	for storeID := range reports {
		req := newStoreHeartbeat(storeID, nil)
		resp := &pdpb.StoreHeartbeatResponse{}
		recoveryController.HandleStoreHeartbeat(req, resp)
		re.NotNil(resp.RecoveryPlan)
		re.Empty(len(resp.RecoveryPlan.Creates))
		re.Empty(len(resp.RecoveryPlan.Demotes))
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
	re.Equal(finished, recoveryController.GetStage())
}

func TestCreateEmptyRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(3, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
			re.Empty(len(report.PeerReports))
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

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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
			re.Empty(len(report.PeerReports))
		}
	}
}

func TestRangeOverlap2(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	for _, store := range newTestStores(5, "6.0.0") {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)
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

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.coordinator.run()
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	recoveryController := newUnsafeRecoveryController(cluster)

	// Store 3 doesn't exist, reject to remove.
	re.Error(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		1: {},
		3: {},
	}, 60, false))

	re.NoError(recoveryController.RemoveFailedStores(map[uint64]struct{}{
		1: {},
	}, 60, false))
	re.True(cluster.GetStore(uint64(1)).IsRemoved())
	for _, s := range cluster.GetSchedulers() {
		paused, err := cluster.IsSchedulerAllowed(s)
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

func TestSplitPaused(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	cluster.Lock()
	cluster.unsafeRecoveryController = recoveryController
	cluster.coordinator = newCoordinator(ctx, cluster, hbstream.NewTestHeartbeatStreams(ctx, cluster.meta.GetId(), cluster, true))
	cluster.Unlock()
	cluster.coordinator.run()
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		re.NoError(cluster.PutStore(store.GetMeta()))
	}
	failedStores := map[uint64]struct{}{
		1: {},
	}
	re.NoError(recoveryController.RemoveFailedStores(failedStores, 60, false))
	askSplitReq := &pdpb.AskSplitRequest{}
	_, err := cluster.HandleAskSplit(askSplitReq)
	re.Equal("[PD:unsaferecovery:ErrUnsafeRecoveryIsRunning]unsafe recovery is running", err.Error())
	askBatchSplitReq := &pdpb.AskBatchSplitRequest{}
	_, err = cluster.HandleAskBatchSplit(askBatchSplitReq)
	re.Equal("[PD:unsaferecovery:ErrUnsafeRecoveryIsRunning]unsafe recovery is running", err.Error())
}
