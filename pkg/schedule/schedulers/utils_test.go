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

package schedulers

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestRetryQuota(t *testing.T) {
	re := require.New(t)

	q := newRetryQuota()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2})
	keepStores := []*core.StoreInfo{store1}

	// test getLimit
	re.Equal(10, q.getLimit(store1))

	// test attenuate
	for _, expected := range []int{5, 2, 1, 1, 1} {
		q.attenuate(store1)
		re.Equal(expected, q.getLimit(store1))
	}

	// test GC
	re.Equal(10, q.getLimit(store2))
	q.attenuate(store2)
	re.Equal(5, q.getLimit(store2))
	q.gc(keepStores)
	re.Equal(1, q.getLimit(store1))
	re.Equal(10, q.getLimit(store2))

	// test resetLimit
	q.resetLimit(store1)
	re.Equal(10, q.getLimit(store1))
}

func TestGetCountThreshold(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	tc.PutStoreWithLabels(1, "region", "z1", "zone", "z1")
	tc.PutStoreWithLabels(2, "region", "z1", "zone", "z1")
	tc.PutStoreWithLabels(3, "region", "z1", "zone", "z1")
	tc.PutStoreWithLabels(4, "region", "z2", "zone", "z1")
	tc.PutStoreWithLabels(5, "region", "z2", "zone", "z1")

	rule1 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_0",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Leader,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z1"}},
		},
		LocationLabels: []string{"zone"},
	}
	rule2 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_1",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Follower,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z1"}},
		},
		LocationLabels: []string{"zone"},
	}

	rule3 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_2",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Learner,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z2"}},
		},
		LocationLabels: []string{"zone"},
	}

	re.NoError(tc.SetRules([]*placement.Rule{rule1, rule2, rule3}))
	re.NoError(tc.GetRuleManager().DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID))

	for i := range 100 {
		starKey, endKey := 100+i-1, 100+i
		tc.AddLeaderRegionWithRange(uint64(i), strconv.Itoa(starKey), strconv.Itoa(endKey), 1, 2, 4)
	}

	available := 0
	for _, store := range tc.GetStores() {
		count := getCountThreshold(tc, tc.GetStores(), store, keyutil.NewKeyRange("100", "200"), core.LeaderScatter)
		if count > 0 {
			available++
		}
	}
	re.Equal(3, available)
}

func TestRangeLeaderDemand(t *testing.T) {
	rule := func(role placement.PeerRoleType, count int, zones ...string) *placement.Rule {
		r := &placement.Rule{Role: role, Count: count}
		if len(zones) > 0 {
			r.LabelConstraints = []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: zones}}
		}
		return r
	}
	for _, tt := range []struct {
		name  string
		rules []*placement.Rule
		want  []float64
	}{
		{"explicit leader", []*placement.Rule{rule(placement.Leader, 1, "A"), rule(placement.Voter, 2, "B")}, []float64{1, 0, 0}},
		{"explicit leader after voter", []*placement.Rule{rule(placement.Voter, 2, "B"), rule(placement.Leader, 1, "A")}, []float64{1, 0, 0}},
		{"single voter", []*placement.Rule{rule(placement.Voter, 3)}, []float64{1.0 / 3, 1.0 / 3, 1.0 / 3}},
		{"disjoint voters", []*placement.Rule{rule(placement.Voter, 1, "A"), rule(placement.Voter, 2, "B")}, []float64{1.0 / 3, 1.0 / 3, 1.0 / 3}},
		{"overlapping voters", []*placement.Rule{rule(placement.Voter, 1, "A"), rule(placement.Voter, 2)}, []float64{1.0 / 3, 1.0 / 3, 1.0 / 3}},
		{"followers and learners excluded", []*placement.Rule{rule(placement.Voter, 1, "A"), rule(placement.Follower, 1, "B"), rule(placement.Learner, 1, "B")}, []float64{1, 0, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, _ := prepareSchedulersTest()
			defer cancel()
			tc.SetEnablePlacementRules(true)
			for id, zone := range []string{"A", "B", "B"} {
				tc.AddLabelsStore(uint64(id+1), 0, map[string]string{"zone": zone})
			}
			rm := tc.GetRuleManager()
			re.NoError(rm.SetRuleGroup(&placement.RuleGroup{ID: "test", Index: 1, Override: true}))
			for i, r := range tt.rules {
				r.GroupID = "test"
				r.ID = strconv.Itoa(i)
			}
			re.NoError(rm.SetRules(tt.rules))
			tc.AddLeaderRegion(1, 1, 2, 3)
			total := 0.0
			for i, want := range tt.want {
				got := getCountThreshold(tc, tc.GetStores(), tc.GetStore(uint64(i+1)), keyutil.KeyRange{}, core.LeaderScatter)
				re.InDelta(want, got, 1e-9)
				total += got
			}
			re.InDelta(1.0, total, 1e-9)
			if tt.name == "disjoint voters" {
				// Peer demand still uses each rule's replica count.
				for i := range 3 {
					re.InDelta(1.0, getCountThreshold(tc, tc.GetStores(), tc.GetStore(uint64(i+1)), keyutil.KeyRange{}, core.PeerScatter), 1e-9)
				}
			}
		})
	}
}

func TestRangeLeaderWeightUnavailableStores(t *testing.T) {
	re := require.New(t)
	stores := []*core.StoreInfo{
		core.NewStoreInfo(&metapb.Store{Id: 1}),
		core.NewStoreInfo(&metapb.Store{Id: 2, NodeState: metapb.NodeState_Removing}),
		core.NewStoreInfo(&metapb.Store{Id: 3, NodeState: metapb.NodeState_Removed}),
	}
	rules := []*placement.Rule{{Role: placement.Voter, Count: 3}}
	re.Equal(1.0, rangeLeaderWeight(stores, stores[0], rules))
	re.Zero(rangeLeaderWeight(stores, stores[1], rules))
	re.Zero(rangeLeaderWeight(stores, stores[2], rules))
	re.Zero(rangeLeaderWeight(stores[:0], stores[0], rules))
	rules[0].IsWitness = true
	re.Zero(rangeLeaderWeight(stores, stores[0], rules))
	// An unavailable explicit leader rule must not fall back to voter stores.
	rules = []*placement.Rule{
		{Role: placement.Leader, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"missing"}}}},
		{Role: placement.Voter, Count: 2},
	}
	re.Zero(rangeLeaderWeight(stores, stores[0], rules))
}

func TestRangeLeaderDemandAcrossRuleRanges(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	tc.SetEnablePlacementRules(true)
	tc.AddLabelsStore(1, 0, map[string]string{"zone": "A"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "B"})
	rm := tc.GetRuleManager()
	re.NoError(rm.SetRuleGroup(&placement.RuleGroup{ID: "test", Index: 1, Override: true}))
	re.NoError(rm.SetRules([]*placement.Rule{
		{GroupID: "test", ID: "left", StartKeyHex: "61", EndKeyHex: "6d", Role: placement.Leader, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}},
		{GroupID: "test", ID: "right", StartKeyHex: "6d", EndKeyHex: "7a", Role: placement.Leader, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"B"}}}},
	}))
	tc.AddLeaderRegionWithRange(1, "a", "m", 1)
	tc.AddLeaderRegionWithRange(2, "m", "z", 2)
	for _, store := range tc.GetStores() {
		re.Equal(1.0, getCountThreshold(tc, tc.GetStores(), store, keyutil.NewKeyRange("a", "z"), core.LeaderScatter))
	}
}

func TestRangeLeaderWeightLabelChanges(t *testing.T) {
	re := require.New(t)
	stores := []*core.StoreInfo{
		core.NewStoreInfo(&metapb.Store{Id: 1, Labels: []*metapb.StoreLabel{{Key: "zone", Value: "A"}}}),
		core.NewStoreInfo(&metapb.Store{Id: 2, Labels: []*metapb.StoreLabel{{Key: "zone", Value: "B"}}}),
		core.NewStoreInfo(&metapb.Store{Id: 3, Labels: []*metapb.StoreLabel{{Key: "zone", Value: "B"}, {Key: "engine", Value: "tiflash"}}}),
	}
	rules := []*placement.Rule{
		{Role: placement.Leader, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}}},
		{Role: placement.Voter, Count: 2},
	}
	check := func(want []float64) {
		for i, store := range stores {
			re.Equal(want[i], rangeLeaderWeight(stores, store, rules))
		}
	}
	check([]float64{1, 0, 0})
	rules[0].LabelConstraints[0].Values = []string{"B"}
	// The TiFlash store needs an explicit engine constraint to match.
	check([]float64{0, 1, 0})
	rules = rules[1:]
	check([]float64{0.5, 0.5, 0})
	// Matching labels alone do not make a store part of the supplied population.
	re.Zero(rangeLeaderWeight(stores[:1], stores[1], rules))
}
