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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
)

type mockStoresSet struct {
	stores     map[uint64]*core.StoreInfo
	storelists []*core.StoreInfo
}

func newMockStoresSet(storeNum int) mockStoresSet {
	lists := make([]*core.StoreInfo, 0)
	for i := 1; i <= storeNum; i++ {
		lists = append(lists, core.NewStoreInfo(&metapb.Store{Id: uint64(i)},
			core.SetLastHeartbeatTS(time.Now())))
	}
	mm := make(map[uint64]*core.StoreInfo)
	for _, store := range lists {
		mm[store.GetID()] = store
	}
	return mockStoresSet{
		stores:     mm,
		storelists: lists,
	}
}

func (ms mockStoresSet) GetStores() []*core.StoreInfo {
	return ms.storelists
}

func (ms mockStoresSet) GetStore(id uint64) *core.StoreInfo {
	return ms.stores[id]
}

func addExtraRules(extraRules int) []*Rule {
	rules := make([]*Rule, 0)
	rules = append(rules, &Rule{
		GroupID:        "pd",
		ID:             "default",
		Role:           Voter,
		Count:          3,
		LocationLabels: []string{},
	})
	for i := 1; i <= extraRules; i++ {
		rules = append(rules, &Rule{
			GroupID:        "tiflash",
			ID:             fmt.Sprintf("%v", i),
			Role:           Learner,
			Count:          1,
			LocationLabels: []string{},
		})
	}
	return rules
}

func mockRegion(votersNum, learnerNums int) *core.RegionInfo {
	peers := make([]*metapb.Peer, 0)
	for i := 1; i <= votersNum; i++ {
		peers = append(peers, &metapb.Peer{
			Id:      uint64(i),
			StoreId: uint64(i),
			Role:    metapb.PeerRole_Voter,
		})
	}
	for i := 1 + votersNum; i <= votersNum+learnerNums; i++ {
		peers = append(peers, &metapb.Peer{
			Id:      uint64(i),
			StoreId: uint64(i),
			Role:    metapb.PeerRole_Learner,
		})
	}

	region := core.NewRegionInfo(
		&metapb.Region{
			Id:       1,
			StartKey: []byte("1"),
			EndKey:   []byte("2"),
			Peers:    peers,
		},
		&metapb.Peer{Id: 1, StoreId: 1},
	)
	return region
}

func BenchmarkFitRegion(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           Voter,
			Count:          3,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}

func BenchmarkFitRegionMoreStores(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           Voter,
			Count:          3,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}

func BenchmarkFitRegionMorePeers(b *testing.B) {
	region := mockRegion(5, 0)
	rules := []*Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           Voter,
			Count:          5,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}

func BenchmarkFitRegionMorePeersEquals(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           Leader,
			Count:          1,
			LocationLabels: []string{},
		},
		{
			GroupID:        "pd",
			ID:             "default-2",
			Role:           Follower,
			Count:          4,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}

func BenchmarkFitRegionMorePeersSplitRules(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           Leader,
			Count:          1,
			LocationLabels: []string{},
		},
	}
	for i := 0; i < 4; i++ {
		rules = append(rules, &Rule{
			GroupID:        "pd",
			ID:             fmt.Sprintf("%v", i),
			Role:           Follower,
			Count:          1,
			LocationLabels: []string{},
		})
	}
	storesSet := newMockStoresSet(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}

func BenchmarkFitRegionMoreVotersSplitRules(b *testing.B) {
	region := mockRegion(5, 0)
	rules := []*Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           Voter,
			Count:          1,
			LocationLabels: []string{},
		},
	}
	for i := 0; i < 4; i++ {
		rules = append(rules, &Rule{
			GroupID:        "pd",
			ID:             fmt.Sprintf("%v", i),
			Role:           Voter,
			Count:          1,
			LocationLabels: []string{},
		})
	}
	storesSet := newMockStoresSet(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}

func BenchmarkFitRegionTiflash(b *testing.B) {
	region := mockRegion(3, 0)
	rules := addExtraRules(1)
	storesSet := newMockStoresSet(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}

func BenchmarkFitRegionCrossRegion(b *testing.B) {
	region := mockRegion(5, 0)
	rules := make([]*Rule, 0)
	rules = append(rules, &Rule{
		GroupID:        "pd",
		ID:             "1",
		Role:           Leader,
		Count:          1,
		LocationLabels: []string{},
	})
	for i := 0; i < 2; i++ {
		rules = append(rules, &Rule{
			GroupID:        "pd",
			ID:             fmt.Sprintf("%v", i),
			Role:           Follower,
			Count:          1,
			LocationLabels: []string{},
		})
	}
	storesSet := newMockStoresSet(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FitRegion(storesSet.GetStores(), region, rules)
	}
}
