// Copyright 2018 TiKV Project Authors.
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
package filter

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFiltersSuite{})

type testFiltersSuite struct{}

func (s *testFiltersSuite) TestDistinctScoreFilter(c *C) {
	labels := []string{"zone", "rack", "host"}
	allStores := []*core.StoreInfo{
		core.NewStoreInfoWithLabel(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}),
		core.NewStoreInfoWithLabel(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"}),
		core.NewStoreInfoWithLabel(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"}),
		core.NewStoreInfoWithLabel(4, 1, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"}),
		core.NewStoreInfoWithLabel(5, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"}),
		core.NewStoreInfoWithLabel(6, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"}),
	}

	testCases := []struct {
		stores       []uint64
		source       uint64
		target       uint64
		safeGuardRes bool
		improverRes  bool
	}{
		{[]uint64{1, 2, 3}, 1, 4, true, true},
		{[]uint64{1, 3, 4}, 1, 2, true, false},
		{[]uint64{1, 4, 6}, 4, 2, false, false},
	}
	for _, tc := range testCases {
		var stores []*core.StoreInfo
		for _, id := range tc.stores {
			stores = append(stores, allStores[id-1])
		}
		ls := NewLocationSafeguard("", labels, stores, allStores[tc.source-1])
		li := NewLocationImprover("", labels, stores, allStores[tc.source-1])
		c.Assert(ls.Target(config.NewTestOptions(), allStores[tc.target-1]), Equals, tc.safeGuardRes)
		c.Assert(li.Target(config.NewTestOptions(), allStores[tc.target-1]), Equals, tc.improverRes)
	}
}

func (s *testFiltersSuite) TestLabelConstraintsFilter(c *C) {
	opt := config.NewTestOptions()
	testCluster := mockcluster.NewCluster(opt)
	store := core.NewStoreInfoWithLabel(1, 1, map[string]string{"id": "1"})

	testCases := []struct {
		key    string
		op     string
		values []string
		res    bool
	}{
		{"id", "in", []string{"1"}, true},
		{"id", "in", []string{"2"}, false},
		{"id", "in", []string{"1", "2"}, true},
		{"id", "notIn", []string{"2", "3"}, true},
		{"id", "notIn", []string{"1", "2"}, false},
		{"id", "exists", []string{}, true},
		{"_id", "exists", []string{}, false},
		{"id", "notExists", []string{}, false},
		{"_id", "notExists", []string{}, true},
	}
	for _, tc := range testCases {
		filter := NewLabelConstaintFilter("", []placement.LabelConstraint{{Key: tc.key, Op: placement.LabelConstraintOp(tc.op), Values: tc.values}})
		c.Assert(filter.Source(testCluster.GetOpts(), store), Equals, tc.res)
	}
}

func (s *testFiltersSuite) TestRuleFitFilter(c *C) {
	opt := config.NewTestOptions()
	testCluster := mockcluster.NewCluster(opt)
	testCluster.SetLocationLabels([]string{"zone"})
	testCluster.SetEnablePlacementRules(true)
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 3, Id: 3},
		{StoreId: 5, Id: 5},
	}}, &metapb.Peer{StoreId: 1, Id: 1})

	testCases := []struct {
		storeID     uint64
		regionCount int
		labels      map[string]string
		sourceRes   bool
		targetRes   bool
	}{
		{1, 1, map[string]string{"zone": "z1"}, true, true},
		{2, 1, map[string]string{"zone": "z1"}, true, true},
		{3, 1, map[string]string{"zone": "z2"}, true, false},
		{4, 1, map[string]string{"zone": "z2"}, true, false},
		{5, 1, map[string]string{"zone": "z3"}, true, false},
		{6, 1, map[string]string{"zone": "z4"}, true, true},
	}
	// Init cluster
	for _, tc := range testCases {
		testCluster.AddLabelsStore(tc.storeID, tc.regionCount, tc.labels)
	}
	for _, tc := range testCases {
		filter := newRuleFitFilter("", testCluster, region, 1)
		c.Assert(filter.Source(testCluster.GetOpts(), testCluster.GetStore(tc.storeID)), Equals, tc.sourceRes)
		c.Assert(filter.Target(testCluster.GetOpts(), testCluster.GetStore(tc.storeID)), Equals, tc.targetRes)
	}
}

func (s *testFiltersSuite) TestStoreStateFilter(c *C) {
	filters := []Filter{
		StoreStateFilter{TransferLeader: true},
		StoreStateFilter{MoveRegion: true},
		StoreStateFilter{TransferLeader: true, MoveRegion: true},
		StoreStateFilter{MoveRegion: true, AllowTemporaryStates: true},
	}
	opt := config.NewTestOptions()
	store := core.NewStoreInfoWithLabel(1, 0, map[string]string{})

	type testCase struct {
		filterIdx int
		sourceRes bool
		targetRes bool
	}

	check := func(store *core.StoreInfo, testCases []testCase) {
		for _, tc := range testCases {
			c.Assert(filters[tc.filterIdx].Source(opt, store), Equals, tc.sourceRes)
			c.Assert(filters[tc.filterIdx].Target(opt, store), Equals, tc.targetRes)
		}
	}

	store = store.Clone(core.SetLastHeartbeatTS(time.Now()))
	testCases := []testCase{
		{2, true, true},
	}
	check(store, testCases)

	// Disconn
	store = store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-5 * time.Minute)))
	testCases = []testCase{
		{0, false, false},
		{1, true, false},
		{2, false, false},
		{3, true, true},
	}
	check(store, testCases)

	// Busy
	store = store.Clone(core.SetLastHeartbeatTS(time.Now())).
		Clone(core.SetStoreStats(&pdpb.StoreStats{IsBusy: true}))
	testCases = []testCase{
		{0, true, false},
		{1, false, false},
		{2, false, false},
		{3, true, true},
	}
	check(store, testCases)
}

func (s *testFiltersSuite) TestIsolationFilter(c *C) {
	opt := config.NewTestOptions()
	testCluster := mockcluster.NewCluster(opt)
	testCluster.SetLocationLabels([]string{"zone", "rack", "host"})
	allStores := []struct {
		storeID     uint64
		regionCount int
		labels      map[string]string
	}{
		{1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}},
		{2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}},
		{3, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"}},
		{4, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"}},
		{5, 1, map[string]string{"zone": "z1", "rack": "r3", "host": "h1"}},
		{6, 1, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"}},
		{7, 1, map[string]string{"zone": "z3", "rack": "r3", "host": "h1"}},
	}
	for _, store := range allStores {
		testCluster.AddLabelsStore(store.storeID, store.regionCount, store.labels)
	}

	testCases := []struct {
		region         *core.RegionInfo
		isolationLevel string
		sourceRes      []bool
		targetRes      []bool
	}{
		{
			core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1},
				{Id: 2, StoreId: 6},
			}}, &metapb.Peer{StoreId: 1, Id: 1}),
			"zone",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, false, false, false, false, true},
		},
		{
			core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1},
				{Id: 2, StoreId: 4},
				{Id: 3, StoreId: 7},
			}}, &metapb.Peer{StoreId: 1, Id: 1}),
			"rack",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, false, false, true, true, false},
		},
		{
			core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1},
				{Id: 2, StoreId: 4},
				{Id: 3, StoreId: 6},
			}}, &metapb.Peer{StoreId: 1, Id: 1}),
			"host",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, true, false, true, false, true},
		},
	}

	for _, tc := range testCases {
		filter := NewIsolationFilter("", tc.isolationLevel, testCluster.GetLocationLabels(), testCluster.GetRegionStores(tc.region))
		for idx, store := range allStores {
			c.Assert(filter.Source(testCluster.GetOpts(), testCluster.GetStore(store.storeID)), Equals, tc.sourceRes[idx])
			c.Assert(filter.Target(testCluster.GetOpts(), testCluster.GetStore(store.storeID)), Equals, tc.targetRes[idx])
		}
	}
}

func (s *testFiltersSuite) TestPlacementGuard(c *C) {
	opt := config.NewTestOptions()
	testCluster := mockcluster.NewCluster(opt)
	testCluster.SetLocationLabels([]string{"zone"})
	testCluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 3, Id: 3},
		{StoreId: 5, Id: 5},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	store := testCluster.GetStore(1)

	c.Assert(NewPlacementSafeguard("", testCluster, region, store),
		FitsTypeOf,
		NewLocationSafeguard("", []string{"zone"}, testCluster.GetRegionStores(region), store))
	testCluster.SetEnablePlacementRules(true)
	c.Assert(NewPlacementSafeguard("", testCluster, region, store),
		FitsTypeOf,
		newRuleFitFilter("", testCluster, region, 1))
}

func BenchmarkCloneRegionTest(b *testing.B) {
	epoch := &metapb.RegionEpoch{
		ConfVer: 1,
		Version: 1,
	}
	region := core.NewRegionInfo(
		&metapb.Region{
			Id:       4,
			StartKey: []byte("x"),
			EndKey:   []byte(""),
			Peers: []*metapb.Peer{
				{Id: 108, StoreId: 4},
			},
			RegionEpoch: epoch,
		},
		&metapb.Peer{Id: 108, StoreId: 4},
		core.SetApproximateSize(50),
		core.SetApproximateKeys(20),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = createRegionForRuleFit(region.GetStartKey(), region.GetEndKey(), region.GetPeers(), region.GetLeader())
	}
}
