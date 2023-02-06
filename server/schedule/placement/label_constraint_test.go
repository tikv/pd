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

package placement

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
)

func TestLabelConstraint(t *testing.T) {
	re := require.New(t)
	stores := []map[string]string{
		{"zone": "zone1", "rack": "rack1"},                    // 1
		{"zone": "zone1", "rack": "rack2"},                    // 2
		{"zone": "zone2"},                                     // 3
		{"zone": "zone1", "engine": "rocksdb", "disk": "hdd"}, // 4
		{"rack": "rack1", "disk": "ssd"},                      // 5
		{"zone": "zone3"},                                     // 6
	}
	constraints := []LabelConstraint{
		{Key: "zone", Op: "in", Values: []string{"zone1"}},
		{Key: "zone", Op: "in", Values: []string{"zone1", "zone2"}},
		{Key: "zone", Op: "notIn", Values: []string{"zone1"}},
		{Key: "zone", Op: "notIn", Values: []string{"zone1", "zone2"}},
		{Key: "zone", Op: "exists"},
		{Key: "disk", Op: "notExists"},
	}
	expect := [][]int{
		{1, 2, 4},
		{1, 2, 3, 4},
		{3, 5, 6},
		{5, 6},
		{1, 2, 3, 4, 6},
		{1, 2, 3, 6},
	}
	for i, constraint := range constraints {
		var matched []int
		for j, store := range stores {
			if constraint.MatchStore(core.NewStoreInfoWithLabel(uint64(j), store)) {
				matched = append(matched, j+1)
			}
		}
		re.Equal(expect[i], matched)
	}
}
func TestLabelConstraints(t *testing.T) {
	re := require.New(t)
	stores := []map[string]string{
		{},                                       // 1
		{"k1": "v1"},                             // 2
		{"k1": "v2"},                             // 3
		{"k2": "v1"},                             // 4
		{"k2": "v2"},                             // 5
		{"engine": "e1"},                         // 6
		{"engine": "e2"},                         // 7
		{"k1": "v1", "k2": "v1"},                 // 8
		{"k2": "v2", "engine": "e1"},             // 9
		{"k1": "v1", "k2": "v2", "engine": "e2"}, // 10
		{"$foo": "bar"},                          // 11
	}
	constraints := [][]LabelConstraint{
		{},
		{{Key: "engine", Op: "in", Values: []string{"e1", "e2"}}},
		{{Key: "k1", Op: "notExists"}, {Key: "k2", Op: "exists"}},
		{{Key: "engine", Op: "exists"}, {Key: "k1", Op: "in", Values: []string{"v1", "v2"}}, {Key: "k2", Op: "notIn", Values: []string{"v3"}}},
		{{Key: "$foo", Op: "in", Values: []string{"bar", "baz"}}},
	}
	expect := [][]int{
		{1, 2, 3, 4, 5, 8},
		{6, 7, 9, 10},
		{4, 5},
		{10},
		{11},
	}
	for i, cs := range constraints {
		var matched []int
		for j, store := range stores {
			if MatchLabelConstraints(core.NewStoreInfoWithLabel(uint64(j), store), cs) {
				matched = append(matched, j+1)
			}
		}
		re.Equal(expect[i], matched)
	}
}
