// Copyright 2019 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/kv"
)

var _ = Suite(&testManagerSuite{})

type testManagerSuite struct {
	store   *core.Storage
	manager *RuleManager
}

func (s *testManagerSuite) SetUpTest(c *C) {
	s.store = core.NewStorage(kv.NewMemoryKV())
	var err error
	s.manager = NewRuleManager(s.store)
	err = s.manager.Initialize(3, []string{"zone", "rack", "host"})
	c.Assert(err, IsNil)
}

func (s *testManagerSuite) TestDefault(c *C) {
	rules := s.manager.GetAllRules()
	c.Assert(rules, HasLen, 1)
	c.Assert(rules[0].GroupID, Equals, "pd")
	c.Assert(rules[0].ID, Equals, "default")
	c.Assert(rules[0].Index, Equals, 0)
	c.Assert(rules[0].StartKey, HasLen, 0)
	c.Assert(rules[0].EndKey, HasLen, 0)
	c.Assert(rules[0].Role, Equals, Voter)
	c.Assert(rules[0].LocationLabels, DeepEquals, []string{"zone", "rack", "host"})
}

func (s *testManagerSuite) TestAdjustRule(c *C) {
	rules := []Rule{
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123ab", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "1123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123aaa", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "master", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 0},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: -1},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3, LabelConstraints: []LabelConstraint{{Op: "foo"}}},
	}
	c.Assert(s.manager.adjustRule(&rules[0]), IsNil)
	c.Assert(rules[0].StartKey, DeepEquals, []byte{0x12, 0x3a, 0xbc})
	c.Assert(rules[0].EndKey, DeepEquals, []byte{0x12, 0x3a, 0xbf})
	for i := 1; i < len(rules); i++ {
		c.Assert(s.manager.adjustRule(&rules[i]), NotNil)
	}
}

func (s *testManagerSuite) TestSaveLoad(c *C) {
	rules := []*Rule{
		{GroupID: "pd", ID: "default", Role: "voter", Count: 5},
		{GroupID: "foo", ID: "bar", StartKeyHex: "", EndKeyHex: "abcd", Role: "learner", Count: 1},
		{GroupID: "foo", ID: "baz", Role: "voter", Count: 1},
	}
	for _, r := range rules {
		s.manager.SetRule(r)
	}
	m2 := NewRuleManager(s.store)
	err := m2.Initialize(3, []string{"no", "labels"})
	c.Assert(err, IsNil)
	c.Assert(m2.GetAllRules(), HasLen, 3)
	c.Assert(m2.GetRule("pd", "default"), DeepEquals, rules[0])
	c.Assert(m2.GetRule("foo", "bar"), DeepEquals, rules[1])
	c.Assert(m2.GetRule("foo", "baz"), DeepEquals, rules[2])
}

func (s *testManagerSuite) TestKeys(c *C) {
	s.manager.DeleteRule("pd", "default")
	rules := []*Rule{
		{GroupID: "1", ID: "1", Role: "voter", Count: 1, StartKeyHex: "", EndKeyHex: ""},
		{GroupID: "2", ID: "2", Role: "voter", Count: 1, StartKeyHex: "11", EndKeyHex: "ff"},
		{GroupID: "2", ID: "3", Role: "voter", Count: 1, StartKeyHex: "22", EndKeyHex: "dd"},
		{GroupID: "3", ID: "4", Role: "voter", Count: 1, StartKeyHex: "44", EndKeyHex: "ee"},
		{GroupID: "3", ID: "5", Role: "voter", Count: 1, StartKeyHex: "44", EndKeyHex: "dd"},
	}
	for _, r := range rules {
		s.manager.SetRule(r)
	}

	splitKeys := [][]string{
		{"", "", "11", "22", "44", "dd", "ee", "ff"},
		{"44", "", "dd", "ee", "ff"},
		{"44", "dd"},
		{"22", "ef", "44", "dd", "ee"},
	}
	for _, keys := range splitKeys {
		splits := s.manager.GetSplitKeys(s.dhex(keys[0]), s.dhex(keys[1]))
		c.Assert(splits, HasLen, len(keys)-2)
		for i := range splits {
			c.Assert(splits[i], DeepEquals, s.dhex(keys[i+2]))
		}
	}

	regionKeys := [][][2]string{
		{{"", ""}},
		{{"aa", "bb"}, {"", ""}, {"11", "ff"}, {"22", "dd"}, {"44", "ee"}, {"44", "dd"}},
		{{"11", "22"}, {"", ""}, {"11", "ff"}},
		{{"11", "33"}},
	}
	for _, keys := range regionKeys {
		region := core.NewRegionInfo(&metapb.Region{StartKey: s.dhex(keys[0][0]), EndKey: s.dhex(keys[0][1])}, nil)
		rules := s.manager.GetRulesForApplyRegion(region)
		c.Assert(rules, HasLen, len(keys)-1)
		for i := range rules {
			c.Assert(rules[i].StartKeyHex, Equals, keys[i+1][0])
			c.Assert(rules[i].EndKeyHex, Equals, keys[i+1][1])
		}
	}

	ruleByKeys := [][]string{ // first is query key, rests are rule keys.
		{"", "", ""},
		{"11", "", "", "11", "ff"},
		{"33", "", "", "11", "ff", "22", "dd"},
	}
	for _, keys := range ruleByKeys {
		rules := s.manager.GetRulesByKey(s.dhex(keys[0]))
		c.Assert(rules, HasLen, (len(keys)-1)/2)
		for i := range rules {
			c.Assert(rules[i].StartKeyHex, Equals, keys[i*2+1])
			c.Assert(rules[i].EndKeyHex, Equals, keys[i*2+2])
		}
	}

	rulesByGroup := [][]string{ // first is group, rests are rule keys.
		{"1", "", ""},
		{"2", "11", "ff", "22", "dd"},
		{"3", "44", "ee", "44", "dd"},
		{"4"},
	}
	for _, keys := range rulesByGroup {
		rules := s.manager.GetRulesByGroup(keys[0])
		c.Assert(rules, HasLen, (len(keys)-1)/2)
		for i := range rules {
			c.Assert(rules[i].StartKeyHex, Equals, keys[i*2+1])
			c.Assert(rules[i].EndKeyHex, Equals, keys[i*2+2])
		}
	}
}

func (s *testManagerSuite) dhex(hk string) []byte {
	k, err := hex.DecodeString(hk)
	if err != nil {
		panic("decode fail")
	}
	return k
}
