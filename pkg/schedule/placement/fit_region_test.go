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

	"github.com/tikv/pd/pkg/core"
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
		GroupID:        DefaultGroupID,
		ID:             DefaultRuleID,
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
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 0,
				Version: 0,
			},
		},
		&metapb.Peer{Id: 1, StoreId: 1},
	)
	return region
}

func BenchmarkFitRegion(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        DefaultGroupID,
			ID:             DefaultRuleID,
			Role:           Voter,
			Count:          3,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(100)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionMoreStores(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        DefaultGroupID,
			ID:             DefaultRuleID,
			Role:           Voter,
			Count:          3,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(200)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionMorePeers(b *testing.B) {
	region := mockRegion(5, 0)
	rules := []*Rule{
		{
			GroupID:        DefaultGroupID,
			ID:             DefaultRuleID,
			Role:           Voter,
			Count:          5,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(100)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionMorePeersEquals(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        DefaultGroupID,
			ID:             DefaultRuleID,
			Role:           Leader,
			Count:          1,
			LocationLabels: []string{},
		},
		{
			GroupID:        DefaultGroupID,
			ID:             "default-2",
			Role:           Follower,
			Count:          4,
			LocationLabels: []string{},
		},
	}
	storesSet := newMockStoresSet(100)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionMorePeersSplitRules(b *testing.B) {
	region := mockRegion(3, 0)
	rules := []*Rule{
		{
			GroupID:        DefaultGroupID,
			ID:             DefaultRuleID,
			Role:           Leader,
			Count:          1,
			LocationLabels: []string{},
		},
	}
	for i := range 4 {
		rules = append(rules, &Rule{
			GroupID:        DefaultGroupID,
			ID:             fmt.Sprintf("%v", i),
			Role:           Follower,
			Count:          1,
			LocationLabels: []string{},
		})
	}
	storesSet := newMockStoresSet(100)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionMoreVotersSplitRules(b *testing.B) {
	region := mockRegion(5, 0)
	rules := []*Rule{
		{
			GroupID:        DefaultGroupID,
			ID:             DefaultRuleID,
			Role:           Voter,
			Count:          1,
			LocationLabels: []string{},
		},
	}
	for i := range 4 {
		rules = append(rules, &Rule{
			GroupID:        DefaultGroupID,
			ID:             fmt.Sprintf("%v", i),
			Role:           Voter,
			Count:          1,
			LocationLabels: []string{},
		})
	}
	storesSet := newMockStoresSet(100)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionTiflash(b *testing.B) {
	region := mockRegion(3, 0)
	rules := addExtraRules(1)
	storesSet := newMockStoresSet(100)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionCrossRegion(b *testing.B) {
	region := mockRegion(5, 0)
	rules := make([]*Rule, 0)
	rules = append(rules, &Rule{
		GroupID:        DefaultGroupID,
		ID:             "1",
		Role:           Leader,
		Count:          1,
		LocationLabels: []string{},
	})
	for i := range 2 {
		rules = append(rules, &Rule{
			GroupID:        DefaultGroupID,
			ID:             fmt.Sprintf("%v", i),
			Role:           Follower,
			Count:          1,
			LocationLabels: []string{},
		})
	}
	storesSet := newMockStoresSet(100)
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionWithMoreRulesAndStoreLabels(b *testing.B) {
	region := mockRegion(5, 0)
	rules := []*Rule{}
	// create 100 rules, with each rule has 101 LabelConstraints.
	for i := range 100 {
		rule := &Rule{
			GroupID:          DefaultGroupID,
			ID:               fmt.Sprintf("%v", i),
			Role:             Follower,
			Count:            3,
			LocationLabels:   []string{},
			LabelConstraints: []LabelConstraint{},
		}
		values := []string{}
		for id := 1; id < 100; id++ {
			values = append(values, fmt.Sprintf("value_%08d", id))
			labelConstraint := LabelConstraint{
				Key:    fmt.Sprintf("key_%08d", id),
				Op:     NotIn,
				Values: values,
			}
			rule.LabelConstraints = append(rule.LabelConstraints, labelConstraint)
		}
		// add an exclusive constraint.
		values = append(values, "exclusive")
		labelConstraint := LabelConstraint{
			Key:    "exclusive",
			Op:     In,
			Values: values,
		}
		rule.LabelConstraints = append(rule.LabelConstraints, labelConstraint)
		rules = append(rules, rule)
	}
	// create stores, with each store has 101 normal labels(1 exclusive label).
	lists := make([]*core.StoreInfo, 0)
	labels := make([]*metapb.StoreLabel, 0, 101)
	for labID := range 100 {
		label := &metapb.StoreLabel{Key: fmt.Sprintf("store_%08d", labID), Value: fmt.Sprintf("value_%08d", labID)}
		labels = append(labels, label)
	}
	label := &metapb.StoreLabel{Key: "exclusive", Value: "exclusive"}
	labels = append(labels, label)
	// 5 peers in 5 different stores,
	// split the stores(peers) to three zones, make the number of peers in each zone: 2:2:1
	for _, peer := range region.GetPeers() {
		storeID := peer.StoreId
		store := core.NewStoreInfo(&metapb.Store{Id: storeID}, core.SetLastHeartbeatTS(time.Now()), core.SetStoreLabels(labels))
		lists = append(lists, store)
	}
	mm := make(map[uint64]*core.StoreInfo)
	for _, store := range lists {
		mm[store.GetID()] = store
	}
	storesSet := mockStoresSet{
		stores:     mm,
		storelists: lists,
	}
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}

func BenchmarkFitRegionWithLocationLabels(b *testing.B) {
	region := mockRegion(5, 5)
	var rules []*Rule
	rule := &Rule{
		GroupID:          DefaultGroupID,
		ID:               "followers",
		Role:             Follower,
		Count:            3,
		LocationLabels:   []string{"zone", "rack", "host"},
		LabelConstraints: []LabelConstraint{},
	}
	rules = append(rules, rule)
	rule = &Rule{
		GroupID:          DefaultGroupID,
		ID:               "learner",
		Role:             Learner,
		Count:            3,
		LocationLabels:   []string{"zone", "rack", "host"},
		LabelConstraints: []LabelConstraint{},
	}
	rules = append(rules, rule)
	rule = &Rule{
		GroupID:          DefaultGroupID,
		ID:               "voters",
		Role:             Voter,
		Count:            4,
		LocationLabels:   []string{"zone", "rack", "host"},
		LabelConstraints: []LabelConstraint{},
	}
	rules = append(rules, rule)
	// create stores
	lists := make([]*core.StoreInfo, 0)
	// 10 peers in 10 different stores,
	// split the stores(peers) to three zones, make the number of peers in each zone: 4:3:3
	for idx, peer := range region.GetPeers() {
		storeID := peer.StoreId
		zoneInfo := &metapb.StoreLabel{Key: "zone", Value: fmt.Sprintf("z_%02d", idx%3)}
		rackInfo := &metapb.StoreLabel{Key: "rack", Value: fmt.Sprintf("r_%02d", idx%2)}
		host := &metapb.StoreLabel{Key: "host", Value: fmt.Sprintf("r_%02d", idx)}
		curLabels := []*metapb.StoreLabel{zoneInfo, rackInfo, host}
		store := core.NewStoreInfo(&metapb.Store{Id: storeID}, core.SetLastHeartbeatTS(time.Now()), core.SetStoreLabels(curLabels))
		lists = append(lists, store)
	}
	mm := make(map[uint64]*core.StoreInfo)
	for _, store := range lists {
		mm[store.GetID()] = store
	}
	storesSet := mockStoresSet{
		stores:     mm,
		storelists: lists,
	}
	stores := getStoresByRegion(storesSet, region)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fitRegion(stores, region, rules, false)
	}
}
