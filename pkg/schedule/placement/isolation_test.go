// Copyright 2026 TiKV Project Authors.
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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

// Compare the incremental candidate predicate with the complete-layout oracle,
// including missing labels, case folding and non-transitive conflict graphs.
func TestReplacementIsolationMatchesFullComparison(t *testing.T) {
	labels := []map[string]string{
		{"zone": "A", "host": "a"}, {"zone": "A", "host": "b"},
		{"zone": "B", "host": "a"}, {"zone": "a", "host": "A"},
		{"zone": "A"}, {"host": "a"}, {},
	}
	for _, n := range []int{3, 4} {
		layouts := 1
		for range n {
			layouts *= len(labels)
		}
		for layout := range layouts {
			stores := make([]*core.StoreInfo, n)
			peers := make([]*metapb.Peer, n)
			code := layout
			for i := range n {
				stores[i] = core.NewStoreInfoWithLabel(uint64(i+1), labels[code%len(labels)])
				peers[i] = &metapb.Peer{Id: uint64(i + 1), StoreId: uint64(i + 1)}
				code /= len(labels)
			}
			rule := &Rule{Role: Voter, Count: n, LocationLabels: []string{"zone", "host"}, IsolationLevel: "host"}
			region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
			fit := fitRegion(stores, region, []*Rule{rule}, false)
			before := fit.RuleFits[0]
			for source := range n {
				check := fit.NewReplacementChecker(uint64(source + 1))
				for _, label := range labels {
					target := core.NewStoreInfoWithLabel(100, label)
					afterStores := slices.Clone(before.Stores)
					for i, store := range afterStores {
						if store.GetID() == uint64(source+1) {
							afterStores[i] = target
						}
					}
					after := &RuleFit{Rule: rule, Stores: afterStores}
					expected := before.IsIsolationPreserved(after) && before.IsolationScore <= isolationStoreScore(uint64(source+1), target, before.Stores, rule.LocationLabels)
					require.Equal(t, expected, check(target), "layout=%d source=%d labels=%v", layout, source, label)
					require.Equal(t, expected, fit.Replace(uint64(source+1), target))
				}
				require.False(t, check(nil))
			}
		}
	}
}

func TestReplacementIsolationSnapshot(t *testing.T) {
	stores := []*core.StoreInfo{
		core.NewStoreInfoWithLabel(1, map[string]string{"host": "a"}),
		core.NewStoreInfoWithLabel(2, map[string]string{"host": "b"}),
		core.NewStoreInfoWithLabel(3, map[string]string{"host": "c"}),
	}
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}}
	rule := &Rule{Role: Voter, Count: 3, LocationLabels: []string{"host"}, IsolationLevel: "host"}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	fit := fitRegion(stores, region, []*Rule{rule}, false)
	check := fit.NewReplacementChecker(1)
	target := core.NewStoreInfoWithLabel(4, map[string]string{"host": "d"})
	for i := range 8 {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()
			for range 100 {
				require.True(t, check(target))
				require.False(t, check(core.NewStoreInfoWithLabel(5, map[string]string{"host": "b"})))
			}
		})
	}
	updated := slices.Clone(stores)
	updated[1] = core.NewStoreInfoWithLabel(2, map[string]string{"host": "d"})
	newFit := fitRegion(updated, region, []*Rule{rule}, false)
	require.True(t, check(target))
	require.False(t, newFit.NewReplacementChecker(1)(target))
	require.Equal(t, "b", fit.RuleFits[0].Stores[1].GetLabelValue("host"))
}
