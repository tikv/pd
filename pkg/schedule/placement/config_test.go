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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrim(t *testing.T) {
	re := require.New(t)
	rc := newRuleConfig()
	rc.setRule(&Rule{GroupID: "g1", ID: "id1"})
	rc.setRule(&Rule{GroupID: "g1", ID: "id2"})
	rc.setRule(&Rule{GroupID: "g2", ID: "id3"})
	rc.setGroup(&RuleGroup{ID: "g1", Index: 1})
	rc.setGroup(&RuleGroup{ID: "g2", Index: 2})

	testCases := []struct {
		ops       func(p *RuleConfigPatch)
		mutRules  map[[2]string]*Rule
		mutGroups map[string]*RuleGroup
	}{
		{
			func(p *RuleConfigPatch) {
				p.SetRule(&Rule{GroupID: "g1", ID: "id1", Index: 100})
				p.SetRule(&Rule{GroupID: "g1", ID: "id2"})
				p.SetGroup(&RuleGroup{ID: "g1", Index: 100})
				p.SetGroup(&RuleGroup{ID: "g2", Index: 2})
			},
			map[[2]string]*Rule{{"g1", "id1"}: {GroupID: "g1", ID: "id1", Index: 100}},
			map[string]*RuleGroup{"g1": {ID: "g1", Index: 100}},
		},
		{
			func(p *RuleConfigPatch) {
				p.DeleteRule("g1", "id1")
				p.DeleteGroup("g2")
				p.DeleteRule("g3", "id3")
				p.DeleteGroup("g3")
			},
			map[[2]string]*Rule{{"g1", "id1"}: nil},
			map[string]*RuleGroup{"g2": {ID: "g2"}},
		},
		{
			func(p *RuleConfigPatch) {
				p.SetRule(&Rule{GroupID: "g1", ID: "id2", Index: 200})
				p.SetRule(&Rule{GroupID: "g1", ID: "id2"})
				p.SetRule(&Rule{GroupID: "g3", ID: "id3"})
				p.DeleteRule("g3", "id3")
				p.SetGroup(&RuleGroup{ID: "g1", Index: 100})
				p.SetGroup(&RuleGroup{ID: "g1", Index: 1})
				p.SetGroup(&RuleGroup{ID: "g3", Index: 3})
				p.DeleteGroup("g3")
			},
			map[[2]string]*Rule{},
			map[string]*RuleGroup{},
		},
	}

	for _, testCase := range testCases {
		p := rc.beginPatch()
		testCase.ops(p)
		p.trim()
		re.Equal(testCase.mutRules, p.mut.rules)
		re.Equal(testCase.mutGroups, p.mut.groups)
	}
}
