// Copyright 2020 TiKV Project Authors.
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
	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct {
}

func (s *testConfigSuite) TestTrim(c *C) {
	rc := newRuleConfig()
	rc.setRule(&Rule{GroupID: "g1", ID: "id1"})
	rc.setRule(&Rule{GroupID: "g1", ID: "id2"})
	rc.setRule(&Rule{GroupID: "g2", ID: "id3"})
	rc.setGroup(&RuleGroup{ID: "g1", Index: 1})
	rc.setGroup(&RuleGroup{ID: "g2", Index: 2})

	testCases := []struct {
		ops       func(p *ruleConfigPatch)
		mutRules  map[[2]string]*Rule
		mutGroups map[string]*RuleGroup
	}{
		{
			func(p *ruleConfigPatch) {
				p.setRule(&Rule{GroupID: "g1", ID: "id1", Index: 100})
				p.setRule(&Rule{GroupID: "g1", ID: "id2"})
				p.setGroup(&RuleGroup{ID: "g1", Index: 100})
				p.setGroup(&RuleGroup{ID: "g2", Index: 2})
			},
			map[[2]string]*Rule{{"g1", "id1"}: {GroupID: "g1", ID: "id1", Index: 100}},
			map[string]*RuleGroup{"g1": {ID: "g1", Index: 100}},
		},
		{
			func(p *ruleConfigPatch) {
				p.deleteRule("g1", "id1")
				p.deleteGroup("g2")
				p.deleteRule("g3", "id3")
				p.deleteGroup("g3")
			},
			map[[2]string]*Rule{{"g1", "id1"}: nil},
			map[string]*RuleGroup{"g2": {ID: "g2"}},
		},
		{
			func(p *ruleConfigPatch) {
				p.setRule(&Rule{GroupID: "g1", ID: "id2", Index: 200})
				p.setRule(&Rule{GroupID: "g1", ID: "id2"})
				p.setRule(&Rule{GroupID: "g3", ID: "id3"})
				p.deleteRule("g3", "id3")
				p.setGroup(&RuleGroup{ID: "g1", Index: 100})
				p.setGroup(&RuleGroup{ID: "g1", Index: 1})
				p.setGroup(&RuleGroup{ID: "g3", Index: 3})
				p.deleteGroup("g3")
			},
			map[[2]string]*Rule{},
			map[string]*RuleGroup{},
		},
	}

	for _, tc := range testCases {
		p := rc.beginPatch()
		tc.ops(p)
		p.trim()
		c.Assert(p.mut.rules, DeepEquals, tc.mutRules)
		c.Assert(p.mut.groups, DeepEquals, tc.mutGroups)
	}
}
