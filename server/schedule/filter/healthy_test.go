// Copyright 2019 TiKV Project Authors.
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
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

func TestIsRegionHealthy(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peers := func(ids ...uint64) []*metapb.Peer {
		var peers []*metapb.Peer
		for _, id := range ids {
			p := &metapb.Peer{
				Id:      id,
				StoreId: id,
			}
			peers = append(peers, p)
		}
		return peers
	}

	region := func(peers []*metapb.Peer, opts ...core.RegionCreateOption) *core.RegionInfo {
		return core.NewRegionInfo(&metapb.Region{Peers: peers}, peers[0], opts...)
	}

	type testCase struct {
		region *core.RegionInfo
		// disable placement rules
		healthy1             bool
		healthyAllowPending1 bool
		replicated1          bool
		// enable placement rules
		healthy2             bool
		healthyAllowPending2 bool
		replicated2          bool
	}

	// healthy only check down peer and pending peer
	testCases := []testCase{
		{region(peers(1, 2, 3)), true, true, true, true, true, true},
		{region(peers(1, 2, 3), core.WithPendingPeers(peers(1))), false, true, true, false, true, true},
		{region(peers(1, 2, 3), core.WithLearners(peers(1))), true, true, false, true, true, false},
		{region(peers(1, 2, 3), core.WithDownPeers([]*pdpb.PeerStats{{Peer: peers(1)[0]}})), false, false, true, false, false, true},
		{region(peers(1, 2)), true, true, false, true, true, false},
		{region(peers(1, 2, 3, 4), core.WithLearners(peers(1))), true, true, false, true, true, false},
	}

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 1)
	tc.AddRegionStore(3, 1)
	tc.AddRegionStore(4, 1)
	for _, testCase := range testCases {
		tc.SetEnablePlacementRules(false)
		re.Equal(testCase.healthy1, IsRegionHealthy(testCase.region))
		re.Equal(testCase.healthyAllowPending1, IsRegionHealthyAllowPending(testCase.region))
		re.Equal(testCase.replicated1, IsRegionReplicated(tc, testCase.region))
		tc.SetEnablePlacementRules(true)
		re.Equal(testCase.healthy2, IsRegionHealthy(testCase.region))
		re.Equal(testCase.healthyAllowPending2, IsRegionHealthyAllowPending(testCase.region))
		re.Equal(testCase.replicated2, IsRegionReplicated(tc, testCase.region))
	}
}
