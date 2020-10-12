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
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/errs"
)

type splitPointType int

const (
	tStart splitPointType = iota
	tEnd
)

// splitPoint represents key that exists in rules.
type splitPoint struct {
	typ  splitPointType
	key  []byte
	rule *Rule
}

// A rule slice that keeps ascending order.
type sortedRules struct {
	rules []*Rule
}

// insertRule inserts a rule into sortedRules while keeping the order unchanged
func (sr *sortedRules) insertRule(rule *Rule) {
	i := sort.Search(len(sr.rules), func(i int) bool {
		return compareRule(sr.rules[i], rule) > 0
	})
	if i == len(sr.rules) {
		sr.rules = append(sr.rules, rule)
		return
	}
	sr.rules = append(sr.rules[:i+1], sr.rules[i:]...)
	sr.rules[i] = rule
}

func (sr *sortedRules) deleteRule(rule *Rule) {
	for i, r := range sr.rules {
		if r.Key() == rule.Key() {
			sr.rules = append(sr.rules[:i], sr.rules[i+1:]...)
			return
		}
	}
}

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
	ranges []rangeRules // ranges[i] contains rules apply to [ranges[i].startKey, ranges[i+1].startKey).
}

type ruleContainer interface {
	iterateRules(func(*Rule))
}

// buildRuleList builds the applied ruleList for the give rules
// rules indicates the map (rule's GroupID, ID) => rule
func buildRuleList(rules ruleContainer) (ruleList, error) {
	// collect and sort split points.
	var points []splitPoint
	rules.iterateRules(func(r *Rule) {
		points = append(points, splitPoint{
			typ:  tStart,
			key:  r.StartKey,
			rule: r,
		})
		if len(r.EndKey) > 0 {
			points = append(points, splitPoint{
				typ:  tEnd,
				key:  r.EndKey,
				rule: r,
			})
		}
	})
	if len(points) == 0 {
		return ruleList{}, errs.ErrBuildRuleList.FastGenByArgs("no rule left")
	}
	sort.Slice(points, func(i, j int) bool {
		return bytes.Compare(points[i].key, points[j].key) < 0
	})

	// determine rules for each range.
	var rl ruleList
	var sr sortedRules
	for i, p := range points {
		switch p.typ {
		case tStart:
			sr.insertRule(p.rule)
		case tEnd:
			sr.deleteRule(p.rule)
		}
		if i == len(points)-1 || !bytes.Equal(p.key, points[i+1].key) {
			var endKey []byte
			if i != len(points)-1 {
				endKey = points[i+1].key
			}

			// next key is different, push sr to rl.
			rr := sr.rules
			if len(rr) == 0 {
				return ruleList{}, errs.ErrBuildRuleList.FastGenByArgs(fmt.Sprintf("no rule for range {%s, %s}",
					strings.ToUpper(hex.EncodeToString(p.key)),
					strings.ToUpper(hex.EncodeToString(endKey))))
			}

			if i != len(points)-1 {
				rr = append(rr[:0:0], rr...) // clone
			}

			arr := prepareRulesForApply(rr) // clone internally
			err := checkApplyRules(arr)
			if err != nil {
				return ruleList{}, errs.ErrBuildRuleList.FastGenByArgs(fmt.Sprintf("%s for range {%s, %s}",
					err,
					strings.ToUpper(hex.EncodeToString(p.key)),
					strings.ToUpper(hex.EncodeToString(endKey))))
			}

			rl.ranges = append(rl.ranges, rangeRules{
				startKey:   p.key,
				rules:      rr,
				applyRules: arr,
			})
		}
	}
	return rl, nil
}

func (rl ruleList) getSplitKeys(start, end []byte) [][]byte {
	var keys [][]byte
	i := sort.Search(len(rl.ranges), func(i int) bool {
		return bytes.Compare(rl.ranges[i].startKey, start) > 0
	})
	for ; i < len(rl.ranges) && (len(end) == 0 || bytes.Compare(rl.ranges[i].startKey, end) < 0); i++ {
		keys = append(keys, rl.ranges[i].startKey)
	}
	return keys
}

func (rl ruleList) getRulesByKey(key []byte) []*Rule {
	i := sort.Search(len(rl.ranges), func(i int) bool {
		return bytes.Compare(rl.ranges[i].startKey, key) > 0
	})
	if i == 0 {
		return nil
	}
	return rl.ranges[i-1].rules
}

func (rl ruleList) getRulesForApplyRegion(start, end []byte) []*Rule {
	i := sort.Search(len(rl.ranges), func(i int) bool {
		return bytes.Compare(rl.ranges[i].startKey, start) > 0
	})
	if i != len(rl.ranges) && (len(end) == 0 || bytes.Compare(end, rl.ranges[i].startKey) > 0) {
		return nil
	}
	return rl.ranges[i-1].applyRules
}
