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
