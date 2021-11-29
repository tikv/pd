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
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/schedule/rangelist"
)

func checkApplyRules(rules []*Rule) error {
	// check raft constraint
	// one and only one leader
	leaderCount := 0
	voterCount := 0
	for _, rule := range rules {
		if rule.Role == Leader {
			leaderCount += rule.Count
		} else if rule.Role == Voter {
			voterCount += rule.Count
		}
		if leaderCount > 1 {
			return errors.New("multiple leader replicas")
		}
	}
	if (leaderCount + voterCount) < 1 {
		return errors.New("needs at least one leader or voter")
	}
	return nil
}

type rangeRules struct {
	startKey []byte
	// rules indicates all the rules match the given range
	rules []*Rule
	// applyRules indicates the selected rules(filtered by prepareRulesForApply) from the given rules
	applyRules []*Rule
}

type ruleList struct {
	ranges    []rangeRules // ranges[i] contains rules apply to [ranges[i].startKey, ranges[i+1].startKey).
	rangeList rangelist.List
}

type ruleContainer interface {
	iterateRules(func(*Rule))
}

// buildRuleList builds the applied ruleList for the give rules
// rules indicates the map (rule's GroupID, ID) => rule
func buildRuleList(rules ruleContainer) (ruleList, error) {
	builder := rangelist.NewBuilder()
	builder.SetCompareFunc(func(a, b interface{}) int {
		return compareRule(a.(*Rule), b.(*Rule))
	})
	rules.iterateRules(func(r *Rule) {
		builder.AddItem(r.StartKey, r.EndKey, r)
	})
	rangeList := builder.Build()

	if rangeList.Len() == 0 {
		return ruleList{}, errs.ErrBuildRuleList.FastGenByArgs("no rule left")
	}

	rl := ruleList{
		rangeList: rangeList,
	}
	for i := 0; i < rangeList.Len(); i++ {
		start, data := rangeList.Get(i)
		var end []byte
		if i < rangeList.Len()-1 {
			end, _ = rangeList.Get(i + 1)
		}

		if len(data) == 0 {
			return ruleList{}, errs.ErrBuildRuleList.FastGenByArgs(fmt.Sprintf("no rule for range {%s, %s}",
				strings.ToUpper(hex.EncodeToString(start)),
				strings.ToUpper(hex.EncodeToString(end))))
		}

		rules := make([]*Rule, len(data))
		for i := range data {
			rules[i] = data[i].(*Rule)
		}

		applyRules := prepareRulesForApply(rules)
		if err := checkApplyRules(applyRules); err != nil {
			return ruleList{}, errs.ErrBuildRuleList.FastGenByArgs(fmt.Sprintf("%s for range {%s, %s}",
				err,
				strings.ToUpper(hex.EncodeToString(start)),
				strings.ToUpper(hex.EncodeToString(end))))
		}

		rl.ranges = append(rl.ranges, rangeRules{
			startKey:   start,
			rules:      rules,
			applyRules: applyRules,
		})
	}
	return rl, nil
}

func (rl ruleList) getRulesByKey(key []byte) []*Rule {
	i, _ := rl.rangeList.GetDataByKey(key)
	if i < 0 {
		return nil
	}
	return rl.ranges[i].rules
}

func (rl ruleList) getRulesForApplyRegion(start, end []byte) []*Rule {
	i, data := rl.rangeList.GetData(start, end)
	if i < 0 || len(data) == 0 {
		return nil
	}
	return rl.ranges[i].applyRules
}
