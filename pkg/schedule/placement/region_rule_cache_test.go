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
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
)

func TestRegionRuleFitCache(t *testing.T) {
	re := require.New(t)
	originRegion := mockRegion(3, 0)
	originRules := addExtraRules(0)
	originStores := mockStores(3)
	cacheManager := NewRegionRuleFitCacheManager()
	cache := cacheManager.mockRegionRuleFitCache(originRegion, originRules, originStores)
	testCases := []struct {
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
					GroupID:        DefaultGroupID,
					ID:             DefaultRuleID,
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
					GroupID:         DefaultGroupID,
					ID:              DefaultRuleID,
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
					GroupID:        DefaultGroupID,
					ID:             "default-2",
					Role:           Voter,
					Count:          3,
					LocationLabels: []string{},
				},
			},
			unchanged: false,
		},
		{
			name:   "invalid input1",
			region: nil,
			rules: []*Rule{
				{
					GroupID:        DefaultGroupID,
					ID:             "default-2",
					Role:           Voter,
					Count:          3,
					LocationLabels: []string{},
				},
			},
			unchanged: false,
		},
		{
			name:      "invalid input2",
			region:    mockRegion(3, 0),
			rules:     []*Rule{},
			unchanged: false,
		},
		{
			name:      "invalid input3",
			region:    mockRegion(3, 0),
			rules:     nil,
			unchanged: false,
		},
	}
	for _, testCase := range testCases {
		t.Log(testCase.name)
		re.Equal(testCase.unchanged, cache.IsUnchanged(testCase.region, testCase.rules, mockStores(3)))
	}
	for _, testCase := range testCases {
		t.Log(testCase.name)
		re.False(cache.IsUnchanged(testCase.region, testCase.rules, mockStoresNoHeartbeat(3)))
	}
	// Invalid Input4
	re.False(cache.IsUnchanged(mockRegion(3, 0), addExtraRules(0), nil))
	// Invalid Input5
	re.False(cache.IsUnchanged(mockRegion(3, 0), addExtraRules(0), []*core.StoreInfo{}))
	// origin rules changed, assert whether cache is changed
	originRules[0].Version++
	re.False(cache.IsUnchanged(originRegion, originRules, originStores))
}

func TestPublicStoreCaches(t *testing.T) {
	re := require.New(t)
	cacheManager := NewRegionRuleFitCacheManager()

	stores1 := mockStores(3)
	rules1 := addExtraRules(0)
	region1 := mockRegion(3, 0)
	cache1 := cacheManager.mockRegionRuleFitCache(region1, rules1, stores1)

	stores2 := mockStores(3)
	rules2 := addExtraRules(0)
	region2 := mockRegion(3, 0)
	cache2 := cacheManager.mockRegionRuleFitCache(region2, rules2, stores2)

	for i, storeCache1 := range cache1.regionStores {
		storeCache2 := cache2.regionStores[i]
		re.Equal(unsafe.Pointer(storeCache2), unsafe.Pointer(storeCache1))
	}
}

func (manager *RegionRuleFitCacheManager) mockRegionRuleFitCache(region *core.RegionInfo, rules []*Rule, regionStores []*core.StoreInfo) *regionRuleFitCache {
	return &regionRuleFitCache{
		region:       toRegionCache(region),
		regionStores: manager.toStoreCacheList(regionStores),
		rules:        toRuleCacheList(rules),
		bestFit: &RegionFit{
			regionStores: regionStores,
			rules:        rules,
		},
	}
}

// nolint:unparam
func mockStores(num int) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, num)
	now := time.Now()
	for i := 1; i <= num; i++ {
		stores = append(stores, core.NewStoreInfo(&metapb.Store{Id: uint64(i)},
			core.SetLastHeartbeatTS(now)))
	}
	return stores
}

func mockStoresNoHeartbeat(num int) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, num)
	for i := 1; i <= num; i++ {
		stores = append(stores, core.NewStoreInfo(&metapb.Store{Id: uint64(i)}))
	}
	return stores
}
