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

package placement

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core"
)

func (s *testRuleSuite) TestRegionRuleFitCache(c *C) {
	cache := mockRegionRuleFitCache()
	testcases := []struct {
		name      string
		region    *core.RegionInfo
		rules     []*Rule
		unchanged bool
	}{
		{
			name:      "unchanged",
			region:    mockRegion(3, 0),
			rules:     addExtraRules(0),
			unchanged: true,
		},
		{
			name: "leader changed",
			region: func() *core.RegionInfo {
				region := mockRegion(3, 0)
				region = region.Clone(
					core.WithLeader(&metapb.Peer{Role: metapb.PeerRole_Voter, Id: 2, StoreId: 2}))
				return region
			}(),
			rules:     addExtraRules(0),
			unchanged: false,
		},
		{
			name: "have down peers",
			region: func() *core.RegionInfo {
				region := mockRegion(3, 0)
				region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
					{
						Peer:        region.GetPeer(3),
						DownSeconds: 42,
					},
				}))
				return region
			}(),
			rules:     addExtraRules(0),
			unchanged: false,
		},
		{
			name: "peers changed",
			region: func() *core.RegionInfo {
				region := mockRegion(3, 1)
				region = region.Clone(core.WithIncConfVer())
				return region
			}(),
			rules:     addExtraRules(0),
			unchanged: false,
		},
		{
			name: "replace peer",
			region: func() *core.RegionInfo {
				region := mockRegion(3, 0)
				region = region.Clone(core.WithAddPeer(&metapb.Peer{
					Id:      4,
					StoreId: 4,
					Role:    metapb.PeerRole_Voter,
				}), core.WithRemoveStorePeer(2), core.WithIncConfVer(), core.WithIncConfVer())
				return region
			}(),
			rules:     addExtraRules(0),
			unchanged: false,
		},
		{
			name:   "rule updated",
			region: mockRegion(3, 0),
			rules: []*Rule{
				{
					GroupID:        "pd",
					ID:             "default",
					Role:           Voter,
					Count:          4,
					Version:        1,
					LocationLabels: []string{},
				},
			},
			unchanged: false,
		},
		{
			name:   "rule re-created",
			region: mockRegion(3, 0),
			rules: []*Rule{
				{
					GroupID:         "pd",
					ID:              "default",
					Role:            Voter,
					Count:           3,
					CreateTimestamp: 1,
					LocationLabels:  []string{},
				},
			},
			unchanged: false,
		},
		{
			name:      "add rules",
			region:    mockRegion(3, 0),
			rules:     addExtraRules(1),
			unchanged: false,
		},
		{
			name:      "remove rule",
			region:    mockRegion(3, 0),
			rules:     []*Rule{},
			unchanged: false,
		},
		{
			name:   "change rule",
			region: mockRegion(3, 0),
			rules: []*Rule{
				{
					GroupID:        "pd",
					ID:             "default-2",
					Role:           Voter,
					Count:          3,
					LocationLabels: []string{},
				},
			},
			unchanged: false,
		},
	}
	for _, testcase := range testcases {
		c.Assert(cache.IsUnchanged(testcase.region, testcase.rules, mockStores(3)), Equals, testcase.unchanged)
	}
}

func mockRegionRuleFitCache() *RegionRuleFitCache {
	return &RegionRuleFitCache{
		region:       toRegionCache(mockRegion(3, 0)),
		regionStores: toStoreCacheList(mockStores(3)),
		rules:        toRuleCacheList(addExtraRules(0)),
		bestFit: &RegionFit{
			regionStores: mockStores(3),
			rules:        addExtraRules(0),
		},
	}
}

func mockStores(num int) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, num)
	for i := 1; i <= num; i++ {
		stores = append(stores, core.NewStoreInfo(&metapb.Store{Id: uint64(i)}))
	}
	return stores
}
