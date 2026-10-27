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

package operator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func newRecoveryOperatorCluster(t *testing.T) (*mockcluster.Cluster, *core.RegionInfo, *Controller, *hbstream.HeartbeatStreams) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	for _, id := range []uint64{1, 2, 4} {
		cluster.AddLabelsStore(id, 1, map[string]string{core.StoreCapabilityLabel: core.FullStoreCapability})
	}
	for _, id := range []uint64{3, 5} {
		cluster.AddLabelsStore(id, 1, map[string]string{core.StoreCapabilityLabel: core.LogOnlyStoreCapability})
	}
	for _, store := range cluster.GetStores() {
		cluster.PutStore(store.Clone(core.SetStoreLogOnlyProtocolVersion(core.LogOnlyProtocolVersion)))
	}
	cluster.SetEnableWitness(true)
	require.NoError(t, cluster.SetRules([]*placement.Rule{
		{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Voter, Count: 2},
		{GroupID: placement.DefaultGroupID, ID: "log", Role: placement.Voter, Count: 1, IsWitness: true},
	}))
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, IsWitness: true}}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{}, Peers: peers}, peers[2],
		core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL}),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: peers[1], DownSeconds: 1800}}))
	cluster.PutRegion(region)
	streams := hbstream.NewTestHeartbeatStreams(ctx, cluster, false)
	t.Cleanup(streams.Close)
	return cluster, region, NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), streams), streams
}

func TestRecoveringLeaderRejectsConflictingOperatorsAtAdmissionAndDispatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		step OpStep
	}{
		{"split", SplitRegion{}}, {"merge", MergeRegion{}},
		{"ordinary-transfer", TransferLeader{FromStore: 3, ToStore: 1}},
		{"remove-healthy-full", RemovePeer{FromStore: 1, PeerID: 1}},
		{"remove-recovery-leader", RemovePeer{FromStore: 3, PeerID: 3}},
		{"add-witness", AddLearner{ToStore: 5, PeerID: 50, IsWitness: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, region, controller, streams := newRecoveryOperatorCluster(t)
			op := NewTestOperator(region.GetID(), region.GetRegionEpoch(), OpAdmin, tc.step)
			require.False(t, controller.AddOperator(op))
			controller.SendScheduleCommand(region, tc.step, DispatchFromNotifierQueue)
			require.Zero(t, streams.MsgLength())
		})
	}
}

func TestRecoveringLeaderCanRepairFullWithoutOrdinaryLeaderTransfer(t *testing.T) {
	cluster, region, controller, streams := newRecoveryOperatorCluster(t)
	op, err := CreateMovePeerOperator("replace-failed-full", cluster, region, OpReplica, 2, &metapb.Peer{StoreId: 4})
	require.NoError(t, err)
	for i := range op.Len() {
		_, ordinaryTransfer := op.Step(i).(TransferLeader)
		require.False(t, ordinaryTransfer)
	}
	require.True(t, controller.AddOperator(op))
	require.Equal(t, 1, streams.MsgLength())
	require.NoError(t, streams.Drain(1))
	// Each later membership step is revalidated against the reported state.
	for advances := 0; op.Check(region) != nil; advances++ {
		require.Less(t, advances, op.Len()+1, "repair operator must make bounded progress")
		region = ApplyOperatorStep(region, op)
		cluster.PutRegion(region)
		controller.Dispatch(region, DispatchFromHeartBeat, nil)
		if streams.MsgLength() > 0 {
			require.NoError(t, streams.Drain(streams.MsgLength()))
		}
	}
	require.NotNil(t, region.GetStorePeer(4))
	require.Nil(t, region.GetStorePeer(2))
	require.Equal(t, uint64(3), region.GetLeader().GetStoreId())
}

func TestDataNotReadyAlsoFreezesWithoutRecoveringLeaderEnum(t *testing.T) {
	cluster, region, controller, streams := newRecoveryOperatorCluster(t)
	region = region.Clone(core.WithLeader(region.GetStorePeer(1)), core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_NORMAL, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}))
	cluster.PutRegion(region)
	controller.SendScheduleCommand(region, TransferLeader{FromStore: 1, ToStore: 2}, DispatchFromCreate)
	require.Zero(t, streams.MsgLength())
	region = region.Clone(core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_NORMAL, DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}))
	cluster.PutRegion(region)
	controller.SendScheduleCommand(region, TransferLeader{FromStore: 1, ToStore: 2}, DispatchFromCreate)
	require.Equal(t, 1, streams.MsgLength())
}

func TestDedicatedGroupProtocolCannotBypassBuilderOrRawDispatch(t *testing.T) {
	cluster, region, controller, streams := newRecoveryOperatorCluster(t)
	cluster.PutStore(cluster.GetStore(1).Clone(core.SetStoreLogOnlyProtocolVersion(0)))
	_, err := CreateAddPeerOperator("new-full", cluster, region, &metapb.Peer{StoreId: 4}, OpReplica)
	require.ErrorContains(t, err, "protocol")
	op := NewTestOperator(region.GetID(), region.GetRegionEpoch(), OpAdmin, AddLearner{ToStore: 4, PeerID: 40})
	require.False(t, controller.AddOperator(op))
	controller.SendScheduleCommand(region, AddLearner{ToStore: 4, PeerID: 40}, DispatchFromNotifierQueue)
	require.Zero(t, streams.MsgLength())

	// Pure cleanup stays available, even if another member is incompatible.
	region = region.Clone(core.WithLeader(region.GetStorePeer(2)), core.WithDownPeers(nil), core.WithRecoveryStatus(nil))
	cluster.PutRegion(region)
	op, err = CreateRemovePeerOperator("retire-old-peer", cluster, OpAdmin, region, 1)
	require.NoError(t, err)
	require.True(t, controller.AddOperator(op))
	require.Equal(t, 1, streams.MsgLength())
}

func TestUnknownRecoveryStateCannotActivateMembership(t *testing.T) {
	cluster, region, controller, streams := newRecoveryOperatorCluster(t)
	region = region.Clone(core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState(99), DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}))
	cluster.PutRegion(region)
	op := NewTestOperator(region.GetID(), region.GetRegionEpoch(), OpAdmin, AddLearner{ToStore: 4, PeerID: 40})
	require.False(t, controller.AddOperator(op))
	controller.SendScheduleCommand(region, AddLearner{ToStore: 4, PeerID: 40}, DispatchFromCreate)
	require.Zero(t, streams.MsgLength())
}
