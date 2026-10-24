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

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
)

func TestLogStoreRejectsConversionsAndIncompatibleOperators(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.AddLabelsStore(1, 1, map[string]string{core.StoreCapabilityLabel: core.FullStoreCapability})
	cluster.AddLabelsStore(2, 1, map[string]string{core.StoreCapabilityLabel: core.FullStoreCapability})
	cluster.AddLabelsStore(3, 1, map[string]string{core.StoreCapabilityLabel: core.LogOnlyStoreCapability})
	cluster.AddLabelsStore(4, 1, map[string]string{core.StoreCapabilityLabel: core.LogOnlyStoreCapability})
	cluster.AddLabelsStore(5, 1, map[string]string{core.StoreCapabilityLabel: core.FullStoreCapability})
	cluster.AddLeaderRegion(1, 1, 2, 3)
	region := cluster.GetRegion(1)
	region = region.Clone(core.WithWitnesses([]*metapb.Peer{region.GetStorePeer(3)}))
	_, err := CreateNonWitnessPeerOperator("invalid", cluster, region, region.GetStorePeer(3))
	require.ErrorContains(t, err, "capability")
	_, err = CreateWitnessPeerOperator("invalid", cluster, region, region.GetStorePeer(2))
	require.ErrorContains(t, err, "capability")
	_, err = CreateAddPeerOperator("invalid", cluster, region, &metapb.Peer{StoreId: 4}, OpReplica)
	require.ErrorContains(t, err, "capability")
	_, err = CreateAddPeerOperator("invalid", cluster, region, &metapb.Peer{StoreId: 5, IsWitness: true}, OpReplica)
	require.ErrorContains(t, err, "capability")

	// Raw steps bypassing Builder must still be rejected by the controller.
	steps := []OpStep{
		AddLearner{ToStore: 4, PeerID: 40}, AddPeer{ToStore: 4, PeerID: 40},
		BecomeNonWitness{StoreID: 3, PeerID: region.GetStorePeer(3).GetId()},
		BecomeWitness{StoreID: 2, PeerID: region.GetStorePeer(2).GetId()},
	}
	for _, step := range steps {
		require.ErrorContains(t, step.CheckInProgress(cluster.GetBasicCluster(), cluster.GetSharedConfig(), region, false), "capability")
	}
}

func TestLogStoreFirstCommandCannotBypassCapability(t *testing.T) {
	for _, tc := range []struct {
		name string
		step OpStep
	}{
		{"add-full-learner", AddLearner{ToStore: 4, PeerID: 40}},
		{"add-full-voter", AddPeer{ToStore: 4, PeerID: 40}},
		{"add-witness-on-full", AddLearner{ToStore: 5, PeerID: 50, IsWitness: true}},
		{"convert-witness", BecomeNonWitness{StoreID: 3, PeerID: 3}},
		{"convert-full", BecomeWitness{StoreID: 2, PeerID: 2}},
		{"batch-convert", BatchSwitchWitness{ToNonWitnesses: []BecomeNonWitness{{StoreID: 3, PeerID: 3}}}},
		{"joint-add-full", ChangePeerV2Enter{PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 40}}}},
		{"transfer-to-witness", TransferLeader{FromStore: 1, ToStore: 3}},
		{"multi-target-includes-witness", TransferLeader{FromStore: 1, ToStore: 2, ToStores: []uint64{2, 3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
			for _, id := range []uint64{1, 2, 5} {
				cluster.AddLabelsStore(id, 1, map[string]string{core.StoreCapabilityLabel: core.FullStoreCapability})
			}
			for _, id := range []uint64{3, 4} {
				cluster.AddLabelsStore(id, 1, map[string]string{core.StoreCapabilityLabel: core.LogOnlyStoreCapability})
			}
			peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, IsWitness: true}}
			region := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{}, Peers: peers}, peers[0])
			cluster.PutRegion(region)
			streams := hbstream.NewTestHeartbeatStreams(ctx, cluster, false)
			defer streams.Close()
			controller := NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), streams)
			op := NewTestOperator(region.GetID(), region.GetRegionEpoch(), OpAdmin, tc.step)
			// AddOperator synchronously sends the first command; no heartbeat
			// or CheckInProgress has run yet. Neither this nor notifier retries
			// may send an incompatible command to TiKV.
			require.False(t, controller.AddOperator(op))
			require.Zero(t, streams.MsgLength())
			// The send boundary must also guard callers bypassing admission.
			controller.SendScheduleCommand(region, tc.step, DispatchFromNotifierQueue)
			require.Zero(t, streams.MsgLength())
		})
	}
}

func TestLogStoreCommandGuardAllowsCompatibleRepairAndRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	for _, id := range []uint64{1, 2, 5} {
		cluster.AddLabelsStore(id, 1, map[string]string{core.StoreCapabilityLabel: core.FullStoreCapability})
	}
	for _, id := range []uint64{3, 4} {
		cluster.AddLabelsStore(id, 1, map[string]string{core.StoreCapabilityLabel: core.LogOnlyStoreCapability})
	}
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, IsWitness: true}}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	streams := hbstream.NewTestHeartbeatStreams(ctx, cluster, false)
	defer streams.Close()
	controller := NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), streams)
	for _, step := range []OpStep{
		AddLearner{ToStore: 4, PeerID: 40, IsWitness: true},
		AddLearner{ToStore: 5, PeerID: 50},
		TransferLeader{FromStore: 1, ToStore: 2},
		RemovePeer{FromStore: 3, PeerID: 3},
	} {
		controller.SendScheduleCommand(region, step, DispatchFromCreate)
		require.Equal(t, 1, streams.MsgLength())
		require.NoError(t, streams.Drain(1))
	}

	// The source may be a temporary W leader: only the destination must be Full.
	// This is an ordinary PD transfer, separate from TiKV's typed recovery RPC.
	recovering := region.Clone(core.WithLeader(region.GetStorePeer(3)))
	cluster.PutRegion(recovering)
	op, err := CreateTransferLeaderOperator("transfer-from-witness", cluster, recovering, 2, nil, 0)
	require.NoError(t, err)
	require.True(t, controller.AddOperator(op))
	require.Equal(t, 1, streams.MsgLength())
	require.NoError(t, streams.Drain(1))

	// Even malformed historical metadata must not prevent retiring the peer.
	peers[2].IsWitness = false
	region = core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	controller.SendScheduleCommand(region, RemovePeer{FromStore: 3, PeerID: 3}, DispatchFromCreate)
	require.Equal(t, 1, streams.MsgLength())
}
