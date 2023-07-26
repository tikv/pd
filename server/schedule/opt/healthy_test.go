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
// See the License for the specific language governing permissions and
// limitations under the License.

package opt

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

func TestOpt(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRegionHealthySuite{})

type testRegionHealthySuite struct{}

func (s *testRegionHealthySuite) TestIsRegionHealthy(c *C) {
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

	cases := []testCase{
		{region(peers(1, 2, 3)), true, true, true, true, true, true},
		{region(peers(1, 2, 3), core.WithPendingPeers(peers(1))), false, true, true, false, true, true},
		{region(peers(1, 2, 3), core.WithLearners(peers(1))), false, false, false, true, true, false},
		{region(peers(1, 2, 3), core.WithDownPeers([]*pdpb.PeerStats{{Peer: peers(1)[0]}})), false, false, true, false, false, true},
		{region(peers(1, 2)), true, true, false, true, true, false},
		{region(peers(1, 2, 3, 4), core.WithLearners(peers(1))), false, false, false, true, true, false},
	}

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 1)
	tc.AddRegionStore(3, 1)
	tc.AddRegionStore(4, 1)
	for _, t := range cases {
		tc.SetEnablePlacementRules(false)
		c.Assert(IsRegionHealthy(tc, t.region), Equals, t.healthy1)
		c.Assert(IsHealthyAllowPending(tc, t.region), Equals, t.healthyAllowPending1)
		c.Assert(IsRegionReplicated(tc, t.region), Equals, t.replicated1)
		tc.SetEnablePlacementRules(true)
		c.Assert(IsRegionHealthy(tc, t.region), Equals, t.healthy2)
		c.Assert(IsHealthyAllowPending(tc, t.region), Equals, t.healthyAllowPending2)
		c.Assert(IsRegionReplicated(tc, t.region), Equals, t.replicated2)
	}
}
