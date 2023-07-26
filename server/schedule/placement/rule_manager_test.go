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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
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

func (s *testManagerSuite) TestLeaderCheck(c *C) {
	c.Assert(s.manager.SetRule(&Rule{GroupID: "pd", ID: "default", Role: "learner", Count: 3}), ErrorMatches, ".*needs at least one leader or voter.*")
	c.Assert(s.manager.SetRule(&Rule{GroupID: "g2", ID: "33", Role: "leader", Count: 2}), ErrorMatches, ".*define multiple leaders by count 2.*")
	c.Assert(s.manager.Batch([]RuleOp{
		{
			Rule:   &Rule{GroupID: "g2", ID: "foo1", Role: "leader", Count: 1},
			Action: RuleOpAdd,
		},
		{
			Rule:   &Rule{GroupID: "g2", ID: "foo2", Role: "leader", Count: 1},
			Action: RuleOpAdd,
		},
	}), ErrorMatches, ".*multiple leader replicas.*")
}

func (s *testManagerSuite) TestSaveLoad(c *C) {
	rules := []*Rule{
		{GroupID: "pd", ID: "default", Role: "voter", Count: 5},
		{GroupID: "foo", ID: "baz", StartKeyHex: "", EndKeyHex: "abcd", Role: "voter", Count: 1},
		{GroupID: "foo", ID: "bar", Role: "learner", Count: 1},
	}
	for _, r := range rules {
		c.Assert(s.manager.SetRule(r), IsNil)
	}

	m2 := NewRuleManager(s.store)
	err := m2.Initialize(3, []string{"no", "labels"})
	c.Assert(err, IsNil)
	c.Assert(m2.GetAllRules(), HasLen, 3)
	c.Assert(m2.GetRule("pd", "default"), DeepEquals, rules[0])
	c.Assert(m2.GetRule("foo", "baz"), DeepEquals, rules[1])
	c.Assert(m2.GetRule("foo", "bar"), DeepEquals, rules[2])
}

func (s *testManagerSuite) checkRules(c *C, rules []*Rule, expect [][2]string) {
	c.Assert(rules, HasLen, len(expect))
	for i := range rules {
		c.Assert(rules[i].Key(), DeepEquals, expect[i])
	}
}

func (s *testManagerSuite) TestKeys(c *C) {
	rules := []*Rule{
		{GroupID: "1", ID: "1", Role: "voter", Count: 1, StartKeyHex: "", EndKeyHex: ""},
		{GroupID: "2", ID: "2", Role: "voter", Count: 1, StartKeyHex: "11", EndKeyHex: "ff"},
		{GroupID: "2", ID: "3", Role: "voter", Count: 1, StartKeyHex: "22", EndKeyHex: "dd"},
	}

	toDelete := []RuleOp{}
	for _, r := range rules {
		s.manager.SetRule(r)
		toDelete = append(toDelete, RuleOp{
			Rule:             r,
			Action:           RuleOpDel,
			DeleteByIDPrefix: false,
		})
	}
	s.checkRules(c, s.manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"pd", "default"}})
	s.manager.Batch(toDelete)
	s.checkRules(c, s.manager.GetAllRules(), [][2]string{{"pd", "default"}})

	rules = append(rules, &Rule{GroupID: "3", ID: "4", Role: "voter", Count: 1, StartKeyHex: "44", EndKeyHex: "ee"},
		&Rule{GroupID: "3", ID: "5", Role: "voter", Count: 1, StartKeyHex: "44", EndKeyHex: "dd"})
	s.manager.SetRules(rules)
	s.checkRules(c, s.manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"3", "4"}, {"3", "5"}, {"pd", "default"}})

	s.manager.DeleteRule("pd", "default")
	s.checkRules(c, s.manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"3", "4"}, {"3", "5"}})

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

func (s *testManagerSuite) TestDeleteByIDPrefix(c *C) {
	s.manager.SetRules([]*Rule{
		{GroupID: "g1", ID: "foo1", Role: "voter", Count: 1},
		{GroupID: "g2", ID: "foo1", Role: "voter", Count: 1},
		{GroupID: "g2", ID: "foobar", Role: "voter", Count: 1},
		{GroupID: "g2", ID: "baz2", Role: "voter", Count: 1},
	})
	s.manager.DeleteRule("pd", "default")
	s.checkRules(c, s.manager.GetAllRules(), [][2]string{{"g1", "foo1"}, {"g2", "baz2"}, {"g2", "foo1"}, {"g2", "foobar"}})

	s.manager.Batch([]RuleOp{{
		Rule:             &Rule{GroupID: "g2", ID: "foo"},
		Action:           RuleOpDel,
		DeleteByIDPrefix: true,
	}})
	s.checkRules(c, s.manager.GetAllRules(), [][2]string{{"g1", "foo1"}, {"g2", "baz2"}})
}

func (s *testManagerSuite) TestRangeGap(c *C) {
	// |--  default  --|
	// cannot delete the last rule
	err := s.manager.DeleteRule("pd", "default")
	c.Assert(err, NotNil)

	err = s.manager.SetRule(&Rule{GroupID: "pd", ID: "foo", StartKeyHex: "", EndKeyHex: "abcd", Role: "voter", Count: 1})
	c.Assert(err, IsNil)
	// |-- default --|
	// |-- foo --|
	// still cannot delete default since it will cause ("abcd", "") has no rules inside.
	err = s.manager.DeleteRule("pd", "default")
	c.Assert(err, NotNil)
	err = s.manager.SetRule(&Rule{GroupID: "pd", ID: "bar", StartKeyHex: "abcd", EndKeyHex: "", Role: "voter", Count: 1})
	c.Assert(err, IsNil)
	// now default can be deleted.
	err = s.manager.DeleteRule("pd", "default")
	c.Assert(err, IsNil)
	// cannot change range since it will cause ("abaa", "abcd") has no rules inside.
	err = s.manager.SetRule(&Rule{GroupID: "pd", ID: "foo", StartKeyHex: "", EndKeyHex: "abaa", Role: "voter", Count: 1})
	c.Assert(err, NotNil)
}

func (s *testManagerSuite) TestGroupConfig(c *C) {
	// group pd
	pd1 := &RuleGroup{ID: "pd"}
	c.Assert(s.manager.GetRuleGroup("pd"), DeepEquals, pd1)

	// update group pd
	pd2 := &RuleGroup{ID: "pd", Index: 100, Override: true}
	err := s.manager.SetRuleGroup(pd2)
	c.Assert(err, IsNil)
	c.Assert(s.manager.GetRuleGroup("pd"), DeepEquals, pd2)

	// new group g without config
	err = s.manager.SetRule(&Rule{GroupID: "g", ID: "1", Role: "voter", Count: 1})
	c.Assert(err, IsNil)
	g1 := &RuleGroup{ID: "g"}
	c.Assert(s.manager.GetRuleGroup("g"), DeepEquals, g1)
	c.Assert(s.manager.GetRuleGroups(), DeepEquals, []*RuleGroup{g1, pd2})

	// update group g
	g2 := &RuleGroup{ID: "g", Index: 2, Override: true}
	err = s.manager.SetRuleGroup(g2)
	c.Assert(err, IsNil)
	c.Assert(s.manager.GetRuleGroups(), DeepEquals, []*RuleGroup{g2, pd2})

	// delete pd group, restore to default config
	err = s.manager.DeleteRuleGroup("pd")
	c.Assert(err, IsNil)
	c.Assert(s.manager.GetRuleGroups(), DeepEquals, []*RuleGroup{pd1, g2})

	// delete rule, the group is removed too
	err = s.manager.DeleteRule("pd", "default")
	c.Assert(err, IsNil)
	c.Assert(s.manager.GetRuleGroups(), DeepEquals, []*RuleGroup{g2})
}

func (s *testManagerSuite) TestCheckApplyRules(c *C) {
	err := checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
	})
	c.Assert(err, IsNil)

	err = checkApplyRules([]*Rule{
		{
			Role:  Voter,
			Count: 1,
		},
	})
	c.Assert(err, IsNil)

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
		{
			Role:  Voter,
			Count: 1,
		},
	})
	c.Assert(err, IsNil)

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 3,
		},
	})
	c.Assert(err, ErrorMatches, "multiple leader replicas")

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
		{
			Role:  Leader,
			Count: 1,
		},
	})
	c.Assert(err, ErrorMatches, "multiple leader replicas")

	err = checkApplyRules([]*Rule{
		{
			Role:  Learner,
			Count: 1,
		},
		{
			Role:  Follower,
			Count: 1,
		},
	})
	c.Assert(err, ErrorMatches, "needs at least one leader or voter")
}

func (s *testManagerSuite) dhex(hk string) []byte {
	k, err := hex.DecodeString(hk)
	if err != nil {
		panic("decode fail")
	}
	return k
}
