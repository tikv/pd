// Copyright 2018 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/placement"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFiltersSuite{})

type testFiltersSuite struct{}

func (s *testFiltersSuite) TestPendingPeerFilter(c *C) {
	filter := NewPendingPeerCountFilter("")
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	store := core.NewStoreInfo(&metapb.Store{Id: 1})
	c.Assert(filter.Source(tc, store), IsTrue)
	newStore := store.Clone(core.SetPendingPeerCount(30))
	c.Assert(filter.Source(tc, newStore), IsFalse)
	c.Assert(filter.Target(tc, newStore), IsFalse)
	// set to 0 means no limit
	opt.MaxPendingPeerCount = 0
	c.Assert(filter.Source(tc, newStore), IsTrue)
	c.Assert(filter.Target(tc, newStore), IsTrue)
}

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
		safeGuradRes bool
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
		ls := newLocationSafeguard("", labels, stores, allStores[tc.source-1])
		li := NewLocationImprover("", labels, stores, allStores[tc.source-1])
		c.Assert(ls.Target(mockoption.NewScheduleOptions(), allStores[tc.target-1]), Equals, tc.safeGuradRes)
		c.Assert(li.Target(mockoption.NewScheduleOptions(), allStores[tc.target-1]), Equals, tc.improverRes)
	}
}

func (s *testFiltersSuite) TestLabelConstraintsFilter(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	store1 := core.NewStoreInfoWithLabel(1, 1, map[string]string{"id": "1"})
	store2 := core.NewStoreInfoWithLabel(1, 1, map[string]string{"id": "2"})
	filter1 := NewLabelConstaintFilter("", []placement.LabelConstraint{{Key: "id", Op: "in", Values: []string{"1"}}})
	filter2 := NewLabelConstaintFilter("", []placement.LabelConstraint{{Key: "id", Op: "in", Values: []string{"2"}}})
	c.Assert(filter1.Source(tc, store1), IsTrue)
	c.Assert(filter1.Target(tc, store2), IsFalse)
	c.Assert(filter2.Source(tc, store1), IsFalse)
	c.Assert(filter2.Target(tc, store2), IsTrue)
}

func (s *testFiltersSuite) TestRuleFitFilter(c *C) {
	opt := mockoption.NewScheduleOptions()
	opt.EnablePlacementRules = true
	opt.LocationLabels = []string{"zone"}
	tc := mockcluster.NewCluster(opt)
	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(4, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 3, Id: 3},
		{StoreId: 5, Id: 5},
	}}, &metapb.Peer{StoreId: 1, Id: 1})

	filter := newRuleFitFilter("", tc, region, 1)
	c.Assert(filter.Target(tc, tc.GetStore(2)), IsTrue)
	c.Assert(filter.Target(tc, tc.GetStore(4)), IsFalse)
	c.Assert(filter.Source(tc, tc.GetStore(4)), IsTrue)
}

func (s *testFiltersSuite) TestPlacementGuard(c *C) {
	opt := mockoption.NewScheduleOptions()
	opt.LocationLabels = []string{"zone"}
	tc := mockcluster.NewCluster(opt)
	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(4, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 3, Id: 3},
		{StoreId: 5, Id: 5},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	store1 := tc.GetStore(1)

	c.Assert(NewPlacementSafeguard("", tc, region, store1),
		FitsTypeOf,
		newLocationSafeguard("", []string{"zone"}, tc.GetRegionStores(region), store1))
	opt.EnablePlacementRules = true
	c.Assert(NewPlacementSafeguard("", tc, region, store1),
		FitsTypeOf,
		newRuleFitFilter("", tc, region, 1))
}
