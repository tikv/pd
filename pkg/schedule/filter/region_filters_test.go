// Copyright 2022 TiKV Project Authors.
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

package filter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

func TestRegionPendingFilter(t *testing.T) {
	re := require.New(t)

	filter := NewRegionPendingFilter()
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2},
		{StoreId: 3, Id: 3},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	re.Equal(filter.Select(region), statusOK)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{{StoreId: 2, Id: 2}}))
	re.Equal(filter.Select(region), statusRegionPendingPeer)
}

func TestRegionDownFilter(t *testing.T) {
	re := require.New(t)

	filter := NewRegionDownFilter()
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2},
		{StoreId: 3, Id: 3},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	re.Equal(filter.Select(region), statusOK)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(3),
		DownSeconds: 24 * 60 * 60,
	}
	region = region.Clone(core.WithDownPeers(append(region.GetDownPeers(), downPeer)))
	re.Equal(filter.Select(region), statusRegionDownPeer)
}

func TestRegionReplicatedFilter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	opt.SetMaxReplicas(3)
	testCluster := mockcluster.NewCluster(ctx, opt)
	filter := NewRegionReplicatedFilter(testCluster)
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2},
		{StoreId: 3, Id: 3},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	re.Equal(filter.Select(region), statusOK)
	region = core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	re.Equal(filter.Select(region), statusRegionNotReplicated)
}

func TestRegionEmptyFilter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	opt.SetMaxReplicas(3)
	testCluster := mockcluster.NewCluster(ctx, opt)
	filter := NewRegionEmptyFilter(testCluster)
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2},
		{StoreId: 3, Id: 3},
	}}, &metapb.Peer{StoreId: 1, Id: 1}, core.SetApproximateSize(30))
	re.Equal(filter.Select(region), statusOK)

	region = region.Clone(core.SetApproximateSize(0))
	for i := range uint64(100) {
		testCluster.PutRegion(core.NewRegionInfo(&metapb.Region{
			Id: i,
			Peers: []*metapb.Peer{
				{StoreId: i + 1, Id: i + 1}},
			StartKey: []byte(fmt.Sprintf("%3da", i+1)),
			EndKey:   []byte(fmt.Sprintf("%3dz", i+1)),
		}, &metapb.Peer{StoreId: i + 1, Id: i + 1}))
	}
	re.Equal(filter.Select(region), statusRegionEmpty)
}

func TestRegionWitnessFilter(t *testing.T) {
	re := require.New(t)

	filter := NewRegionWitnessFilter(2)
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2, IsWitness: true},
		{StoreId: 3, Id: 3},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	re.Equal(filter.Select(region), statusRegionWitnessPeer)
	region = core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2},
		{StoreId: 3, Id: 3, IsWitness: true},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	re.Equal(filter.Select(region), statusOK)
}
