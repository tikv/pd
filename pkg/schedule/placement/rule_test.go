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
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareRulesForApply(t *testing.T) {
	re := require.New(t)
	rules := []*Rule{
		{GroupID: "g1", Index: 0, ID: "id5"},
		{GroupID: "g1", Index: 0, ID: "id6"},
		{GroupID: "g1", Index: 1, ID: "id4"},
		{GroupID: "g1", Index: 99, ID: "id3"},

		{GroupID: "g2", Index: 0, ID: "id1"},
		{GroupID: "g2", Index: 0, ID: "id2"},
		{GroupID: "g2", Index: 1, ID: "id0"},

		{GroupID: "g3", Index: 0, ID: "id6"},
		{GroupID: "g3", Index: 2, ID: "id1", Override: true},
		{GroupID: "g3", Index: 2, ID: "id2"},
		{GroupID: "g3", Index: 3, ID: "id0"},

		{GroupID: "g4", Index: 0, ID: "id9", Override: true},
		{GroupID: "g4", Index: 1, ID: "id8", Override: true},
		{GroupID: "g4", Index: 2, ID: "id7", Override: true},
	}
	expected := [][2]string{
		{"g1", "id5"}, {"g1", "id6"}, {"g1", "id4"}, {"g1", "id3"},
		{"g2", "id1"}, {"g2", "id2"}, {"g2", "id0"},
		{"g3", "id1"}, {"g3", "id2"}, {"g3", "id0"},
		{"g4", "id7"},
	}

	rand.Shuffle(len(rules), func(i, j int) { rules[i], rules[j] = rules[j], rules[i] })
	sortRules(rules)
	rules = prepareRulesForApply(rules)

	re.Len(rules, len(expected))
	for i := range rules {
		re.Equal(expected[i], rules[i].Key())
	}
}
func TestGroupProperties(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		rules  []*Rule
		expect [][2]string
	}{
		{
			rules: []*Rule{
				{GroupID: "g1", ID: "id1"},
				{GroupID: "g2", ID: "id2"},
			},
			expect: [][2]string{
				{"g1", "id1"}, {"g2", "id2"},
			},
		},
		{ // test group index
			rules: []*Rule{
				{GroupID: "g1", ID: "id1"},
				{GroupID: "g2", ID: "id2", group: &RuleGroup{ID: "g2", Index: 2}},
				{GroupID: "g3", ID: "id3", group: &RuleGroup{ID: "g3", Index: 1}},
			},
			expect: [][2]string{
				{"g1", "id1"}, {"g3", "id3"}, {"g2", "id2"},
			},
		},
		{ // test group override
			rules: []*Rule{
				{GroupID: "g1", ID: "id1"},
				{GroupID: "g2", ID: "id2"},
				{GroupID: "g3", ID: "id3", group: &RuleGroup{ID: "g3", Index: 1, Override: true}},
				{GroupID: "g4", ID: "id4", group: &RuleGroup{ID: "g4", Index: 2}},
			},
			expect: [][2]string{
				{"g3", "id3"}, {"g4", "id4"},
			},
		},
	}

	for _, testCase := range testCases {
		rand.Shuffle(len(testCase.rules), func(i, j int) { testCase.rules[i], testCase.rules[j] = testCase.rules[j], testCase.rules[i] })
		sortRules(testCase.rules)
		rules := prepareRulesForApply(testCase.rules)
		re.Len(rules, len(testCase.expect))
		for i := range rules {
			re.Equal(testCase.expect[i], rules[i].Key())
		}
	}
}
func TestBuildRuleList(t *testing.T) {
	re := require.New(t)
	defaultRule := &Rule{
		GroupID:  DefaultGroupID,
		ID:       DefaultRuleID,
		Role:     Voter,
		StartKey: []byte{},
		EndKey:   []byte{},
		Count:    3,
	}
	byteStart, err := hex.DecodeString("a1")
	re.NoError(err)
	byteEnd, err := hex.DecodeString("a2")
	re.NoError(err)
	ruleMeta := &Rule{
		GroupID:  DefaultGroupID,
		ID:       "meta",
		Index:    1,
		Override: true,
		StartKey: byteStart,
		EndKey:   byteEnd,
		Role:     Voter,
		Count:    5,
	}

	testCases := []struct {
		name   string
		rules  map[[2]string]*Rule
		expect ruleList
	}{
		{
			name: "default rule",
			rules: map[[2]string]*Rule{
				{DefaultGroupID, DefaultRuleID}: defaultRule,
			},
			expect: ruleList{
				ranges: []rangeRules{
					{
						startKey:   []byte{},
						rules:      []*Rule{defaultRule},
						applyRules: []*Rule{defaultRule},
					},
				},
			},
		},
		{
			name: "metadata case",
			rules: map[[2]string]*Rule{
				{DefaultGroupID, DefaultRuleID}: defaultRule,
				{DefaultGroupID, "meta"}:        ruleMeta,
			},
			expect: ruleList{ranges: []rangeRules{
				{
					startKey:   []byte{},
					rules:      []*Rule{defaultRule},
					applyRules: []*Rule{defaultRule},
				},
				{
					startKey:   byteStart,
					rules:      []*Rule{defaultRule, ruleMeta},
					applyRules: []*Rule{ruleMeta},
				},
				{
					startKey:   byteEnd,
					rules:      []*Rule{defaultRule},
					applyRules: []*Rule{defaultRule},
				},
			}},
		},
	}

	for _, testCase := range testCases {
		t.Log(testCase.name)
		config := &ruleConfig{rules: testCase.rules}
		result, err := buildRuleList(config)
		re.NoError(err)
		re.Equal(testCase.expect.ranges, result.ranges)
	}
}

// startKey and endKey are json:"-" which means cannot be Unmarshal from json
// We need to take care of `Clone` method.
func TestRuleKeyClone(t *testing.T) {
	re := require.New(t)
	r := &Rule{
		StartKey: []byte{1, 2, 3},
		EndKey:   []byte{4, 5, 6},
	}

	clone := r.Clone()
	// Modify the original rule
	r.StartKey[0] = 9
	r.EndKey[0] = 9

	// The clone should not be affected
	re.Equal([]byte{1, 2, 3}, clone.StartKey)
	re.Equal([]byte{4, 5, 6}, clone.EndKey)

	// Modify the clone
	clone.StartKey[0] = 8
	clone.EndKey[0] = 8

	// The original rule should not be affected
	re.Equal([]byte{9, 2, 3}, r.StartKey)
	re.Equal([]byte{9, 5, 6}, r.EndKey)
}
