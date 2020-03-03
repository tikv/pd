// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing perlissions and
// limitations under the License.

package placement

import (
	"bytes"
	"sort"
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

type rangeRules struct {
	startKey   []byte
	rules      []*Rule
	applyRules []*Rule
}

type ruleList struct {
	ranges []rangeRules // ranges[i] contains rules apply to (ranges[i].startKey, ranges[i+1].startKey).
}

func buildRuleList(rules map[[2]string]*Rule) ruleList {
	if len(rules) == 0 {
		return ruleList{}
	}
	// collect and sort split points.
	var points []splitPoint
	for _, r := range rules {
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
			// next key is different, push sr to rl.
			rr := sr.rules
			if i != len(points)-1 {
				rr = append(rr[:0:0], rr...) // clone
			}
			rl.ranges = append(rl.ranges, rangeRules{
				startKey:   p.key,
				rules:      rr,
				applyRules: prepareRulesForApply(rr), // clone internally
			})
		}
	}
	return rl
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
