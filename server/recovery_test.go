// Copyright 2026 TiKV Project Authors.
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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/core"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/server/cluster"
)

func TestLogOnlyProtocolVersionFollowsSchedulingPrimary(t *testing.T) {
	s := &Server{isKeyspaceGroupEnabled: true, cluster: &cluster.RaftCluster{}}
	s.isRunning.Store(1)
	require.Equal(t, core.LogOnlyProtocolVersion, s.getLogOnlyProtocolVersion())
	s.cluster.SetServiceIndependent(mcs.SchedulingServiceName)
	require.Zero(t, s.getLogOnlyProtocolVersion())
	s.servicePrimaryMap.Store(mcs.SchedulingServiceName, "http://primary")
	for _, version := range []uint32{0, 1, 2, 3} {
		s.schedulingPrimaryCapability.Store(&schedulingpb.Participant{ListenUrls: []string{"http://primary"}, LogOnlyProtocolVersion: version})
		require.Equal(t, min(core.LogOnlyProtocolVersion, version), s.getLogOnlyProtocolVersion())
	}
	s.schedulingPrimaryCapability.Store(&schedulingpb.Participant{ListenUrls: []string{"http://primary"}, LogOnlyProtocolVersion: 0})
	require.Zero(t, s.getLogOnlyProtocolVersion()) // A downgrade after a positive result is visible.
	s.servicePrimaryMap.Store(mcs.SchedulingServiceName, "http://new-primary")
	require.Zero(t, s.getLogOnlyProtocolVersion())
	s.schedulingPrimaryCapability.Store(nil)
	require.Zero(t, s.getLogOnlyProtocolVersion())
}

func TestForwardedSchedulingResponseCannotBypassRecoveryState(t *testing.T) {
	c := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	for _, id := range []uint64{1, 2, 4} {
		c.AddLabelsStore(id, 0, map[string]string{core.StoreCapabilityLabel: core.FullStoreCapability})
	}
	c.AddLabelsStore(3, 0, map[string]string{core.StoreCapabilityLabel: core.LogOnlyStoreCapability})
	for _, store := range c.GetStores() {
		c.PutStore(store.Clone(core.SetStoreLogOnlyProtocolVersion(core.LogOnlyProtocolVersion)))
	}
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, IsWitness: true}}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[2], core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER}))
	response := &schedulingpb.RegionHeartbeatResponse{RegionId: 1, TargetPeer: peers[2], TransferLeader: &pdpb.TransferLeader{Peer: peers[0]}}
	require.ErrorContains(t, validateForwardedSchedulingCommand(c.GetBasicCluster(), region, response), "recovery")
	response.TransferLeader = nil
	response.ChangePeer = &pdpb.ChangePeer{Peer: &metapb.Peer{Id: 4, StoreId: 4}, ChangeType: eraftpb.ConfChangeType_AddLearnerNode}
	require.NoError(t, validateForwardedSchedulingCommand(c.GetBasicCluster(), region, response))
	response.TargetPeer = peers[0]
	require.ErrorContains(t, validateForwardedSchedulingCommand(c.GetBasicCluster(), region, response), "stale")
}

func TestRecoveryForwardingPreservesReaderFloorWithoutReusingReadiness(t *testing.T) {
	cached := core.NewRegionInfo(&metapb.Region{Id: 1}, nil, core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{RequiredReaderProtocolVersion: 2, DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT, SessionId: []byte("cached")}))
	for _, status := range []*pdpb.RegionRecoveryStatus{nil, {RequiredReaderProtocolVersion: 1, State: pdpb.RegionRecoveryState_RECOVERING_LEADER, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL, SessionId: []byte("reported")}} {
		report := cached.Clone(core.WithRecoveryStatus(status))
		forwarded := recoveryStatusForForwarding(report, nil, cached)
		require.Equal(t, uint32(2), forwarded.GetRequiredReaderProtocolVersion())
		require.Equal(t, status.GetState(), forwarded.GetState())
		require.Equal(t, status.GetDataServiceReady(), forwarded.GetDataServiceReady())
		require.Equal(t, status.GetSizeSource(), forwarded.GetSizeSource())
		require.Equal(t, status.GetSessionId(), forwarded.GetSessionId())
		require.Same(t, status, report.GetRecoveryStatus())
	}
	require.Equal(t, []byte("cached"), cached.GetRecoveryStatus().GetSessionId())
	legacy := cached.Clone(core.WithRecoveryStatus(nil))
	require.Nil(t, recoveryStatusForForwarding(legacy, nil, legacy))
}
