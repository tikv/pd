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

package labeler

import (
	"encoding/hex"
	"encoding/json"
	"sort"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLabelerSuite{})

type testLabelerSuite struct {
	store   *core.Storage
	labeler *RegionLabeler
}

func (s *testLabelerSuite) SetUpTest(c *C) {
	s.store = core.NewStorage(kv.NewMemoryKV())
	var err error
	s.labeler, err = NewRegionLabeler(s.store)
	c.Assert(err, IsNil)
}

func (s *testLabelerSuite) TestAdjustRule(c *C) {
	rule := LabelRule{
		ID: "foo",
		Labels: []RegionLabel{
			{Key: "k1", Value: "v1"},
		},
		RuleType: "key-range",
		Rule: map[string]interface{}{
			"start_key": "12abcd",
			"end_key":   "34cdef",
		},
	}
	err := s.labeler.adjustRule(&rule)
	c.Assert(err, IsNil)
	c.Assert(rule.Rule.(*KeyRangeRule).StartKey, BytesEquals, []byte{0x12, 0xab, 0xcd})
	c.Assert(rule.Rule.(*KeyRangeRule).EndKey, BytesEquals, []byte{0x34, 0xcd, 0xef})

	rule = LabelRule{
		ID: "foo",
		Labels: []RegionLabel{
			{Key: "k1", Value: "v1"},
		},
		RuleType: "key-ranges",
		Rule: []map[string]interface{}{
			{
				"start_key": "12abcd",
				"end_key":   "34cdef",
			},
			{
				"start_key": "56abcd",
				"end_key":   "78cdef",
			},
		},
	}
	err = s.labeler.adjustRule(&rule)
	c.Assert(err, IsNil)
	c.Assert(rule.Rule.([]*KeyRangeRule), HasLen, 2)
	c.Assert(rule.Rule.([]*KeyRangeRule)[0].StartKey, BytesEquals, []byte{0x12, 0xab, 0xcd})
	c.Assert(rule.Rule.([]*KeyRangeRule)[0].EndKey, BytesEquals, []byte{0x34, 0xcd, 0xef})
	c.Assert(rule.Rule.([]*KeyRangeRule)[1].StartKey, BytesEquals, []byte{0x56, 0xab, 0xcd})
	c.Assert(rule.Rule.([]*KeyRangeRule)[1].EndKey, BytesEquals, []byte{0x78, 0xcd, 0xef})
}

func (s *testLabelerSuite) TestAdjustRule2(c *C) {
	ruleData := `{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":"", "end_key":""}}`
	var rule LabelRule
	err := json.Unmarshal([]byte(ruleData), &rule)
	c.Assert(err, IsNil)
	err = s.labeler.adjustRule(&rule)
	c.Assert(err, IsNil)

	badRuleData := []string{
		// no id
		`{"id":"", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":"", "end_key":""}}`,
		// no labels
		`{"id":"id", "labels": [], "rule_type":"key-range", "rule": {"start_key":"", "end_key":""}}`,
		// empty label key
		`{"id":"id", "labels": [{"key": "", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":"", "end_key":""}}`,
		// empty label value
		`{"id":"id", "labels": [{"key": "k1", "value": ""}], "rule_type":"key-range", "rule": {"start_key":"", "end_key":""}}`,
		// unknown rule type
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"unknown", "rule": {"start_key":"", "end_key":""}}`,
		// wrong rule content
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule":123}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":123, "end_key":""}}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":"", "end_key":123}}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":"123", "end_key":"abcd"}}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":"abcd", "end_key":"123"}}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "rule": {"start_key":"abcd", "end_key":"1234"}}`,
		// type = key-ranges, wrong type for rule.
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-ranges", "rule": {"start_key":"", "end_key":""}}`,
	}
	for i, str := range badRuleData {
		var rule LabelRule
		err := json.Unmarshal([]byte(str), &rule)
		c.Assert(err, IsNil, Commentf("#%d", i))
		err = s.labeler.adjustRule(&rule)
		c.Assert(err, NotNil, Commentf("#%d", i))
	}
}

func (s *testLabelerSuite) TestGetSetRule(c *C) {
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "1234", "end_key": "5678"}},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "ab12", "end_key": "cd12"}},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "abcd", "end_key": "efef"}},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	allRules := s.labeler.GetAllLabelRules()
	sort.Slice(allRules, func(i, j int) bool { return allRules[i].ID < allRules[j].ID })
	c.Assert(allRules, DeepEquals, rules)

	byIDs, err := s.labeler.GetLabelRules([]string{"rule3", "rule1"})
	c.Assert(err, IsNil)
	c.Assert(byIDs, DeepEquals, []*LabelRule{rules[2], rules[0]})

	err = s.labeler.DeleteLabelRule("rule2")
	c.Assert(err, IsNil)
	c.Assert(s.labeler.GetLabelRule("rule2"), IsNil)
	byIDs, err = s.labeler.GetLabelRules([]string{"rule1", "rule2"})
	c.Assert(err, IsNil)
	c.Assert(byIDs, DeepEquals, []*LabelRule{rules[0]})

	// patch
	patch := LabelRulePatch{
		SetRules: []*LabelRule{
			{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "ab12", "end_key": "cd12"}},
		},
		DeleteRules: []string{"rule1"},
	}
	err = s.labeler.Patch(patch)
	c.Assert(err, IsNil)
	allRules = s.labeler.GetAllLabelRules()
	sort.Slice(allRules, func(i, j int) bool { return allRules[i].ID < allRules[j].ID })
	c.Assert(allRules, DeepEquals, rules[1:])
}

func (s *testLabelerSuite) TestIndex(c *C) {
	rules := []*LabelRule{
		{ID: "rule0", Labels: []RegionLabel{{Key: "k1", Value: "v0"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "", "end_key": ""}},
		{ID: "rule1", Index: 1, Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "1234", "end_key": "5678"}},
		{ID: "rule2", Index: 2, Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "ab12", "end_key": "cd12"}},
		{ID: "rule3", Index: 1, Labels: []RegionLabel{{Key: "k2", Value: "v3"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "abcd", "end_key": "efef"}},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	type testCase struct {
		start, end string
		labels     map[string]string
	}
	testCases := []testCase{
		{"", "1234", map[string]string{"k1": "v0"}},
		{"1234", "5678", map[string]string{"k1": "v1"}},
		{"ab12", "abcd", map[string]string{"k1": "v0", "k2": "v2"}},
		{"abcd", "cd12", map[string]string{"k1": "v0", "k2": "v2"}},
		{"cdef", "efef", map[string]string{"k1": "v0", "k2": "v3"}},
	}
	for _, tc := range testCases {
		start, _ := hex.DecodeString(tc.start)
		end, _ := hex.DecodeString(tc.end)
		region := core.NewTestRegionInfo(start, end)
		labels := s.labeler.GetRegionLabels(region)
		c.Assert(labels, HasLen, len(tc.labels))
		for _, l := range labels {
			c.Assert(l.Value, Equals, tc.labels[l.Key])
		}
		for _, k := range []string{"k1", "k2"} {
			c.Assert(s.labeler.GetRegionLabel(region, k), Equals, tc.labels[k])
		}
	}
}

func (s *testLabelerSuite) TestSaveLoadRule(c *C) {
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "1234", "end_key": "5678"}},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "ab12", "end_key": "cd12"}},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "abcd", "end_key": "efef"}},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	labeler, err := NewRegionLabeler(s.store)
	c.Assert(err, IsNil)
	for _, r := range rules {
		r2 := labeler.GetLabelRule(r.ID)
		c.Assert(r2, DeepEquals, r)
	}
}

func (s *testLabelerSuite) TestKeyRange(c *C) {
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "1234", "end_key": "5678"}},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "ab12", "end_key": "cd12"}},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Rule: map[string]interface{}{"start_key": "abcd", "end_key": "efef"}},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	type testCase struct {
		start, end string
		labels     map[string]string
	}
	testCases := []testCase{
		{"1234", "5678", map[string]string{"k1": "v1"}},
		{"1234", "aaaa", map[string]string{}},
		{"abcd", "abff", map[string]string{"k2": "v2", "k3": "v3"}},
		{"cd12", "dddd", map[string]string{"k3": "v3"}},
		{"ffee", "ffff", map[string]string{}},
	}
	for _, tc := range testCases {
		start, _ := hex.DecodeString(tc.start)
		end, _ := hex.DecodeString(tc.end)
		region := core.NewTestRegionInfo(start, end)
		labels := s.labeler.GetRegionLabels(region)
		c.Assert(labels, HasLen, len(tc.labels))
		for _, l := range labels {
			c.Assert(tc.labels[l.Key], Equals, l.Value)
		}
		for _, k := range []string{"k1", "k2", "k3"} {
			c.Assert(s.labeler.GetRegionLabel(region, k), Equals, tc.labels[k])
		}
	}
}
