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
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"encoding/hex"
	"math/rand"

	. "github.com/pingcap/check"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct{}

func (s *testRuleSuite) TestPrepareRulesForApply(c *C) {
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

	c.Assert(len(rules), Equals, len(expected))
	for i := range rules {
		c.Assert(rules[i].Key(), Equals, expected[i])
	}
}

func (s *testRuleSuite) TestGroupProperties(c *C) {
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

	for _, tc := range testCases {
		rand.Shuffle(len(tc.rules), func(i, j int) { tc.rules[i], tc.rules[j] = tc.rules[j], tc.rules[i] })
		sortRules(tc.rules)
		rules := prepareRulesForApply(tc.rules)
		c.Assert(rules, HasLen, len(tc.expect))
		for i := range rules {
			c.Assert(rules[i].Key(), Equals, tc.expect[i])
		}
	}
}

// TODO: fulfill unit test case to cover BuildRuleList
func (s *testRuleSuite) TestBuildRuleList(c *C) {
	defaultRule := &Rule{
		GroupID:  "pd",
		ID:       "default",
		Role:     "voter",
		StartKey: []byte{},
		EndKey:   []byte{},
		Count:    3,
	}
	byteStart, err := hex.DecodeString("a1")
	c.Check(err, IsNil)
	byteEnd, err := hex.DecodeString("a2")
	c.Check(err, IsNil)
	ruleMeta := &Rule{
		GroupID:  "pd",
		ID:       "meta",
		Index:    1,
		Override: true,
		StartKey: byteStart,
		EndKey:   byteEnd,
		Role:     "voter",
		Count:    5,
	}

	testcases := []struct {
		name   string
		rules  map[[2]string]*Rule
		expect ruleList
	}{
		{
			name: "default rule",
			rules: map[[2]string]*Rule{
				{"pd", "default"}: defaultRule,
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
				{"pd", "default"}: defaultRule,
				{"pd", "meta"}:    ruleMeta,
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

	for _, testcase := range testcases {
		c.Log(testcase.name)
		config := &ruleConfig{rules: testcase.rules}
		result, err := buildRuleList(config)
		c.Assert(err, IsNil)
		c.Assert(result, DeepEquals, testcase.expect)
	}
}
