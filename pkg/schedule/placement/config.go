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
	"bytes"
	"encoding/json"
	"time"
)

// ruleConfig contains rule and rule group configurations.
type ruleConfig struct {
	rules  map[[2]string]*Rule   // {group, id} => Rule
	groups map[string]*RuleGroup // id => RuleGroup
}

func newRuleConfig() *ruleConfig {
	return &ruleConfig{
		rules:  make(map[[2]string]*Rule),
		groups: make(map[string]*RuleGroup),
	}
}

// adjust configs for `buildRuleList` and API use.
func (c *ruleConfig) adjust() {
	// remove all default group configurations.
	// if there are rules belong to the group, it will be re-add later.
	for id, g := range c.groups {
		if g.isDefault() {
			delete(c.groups, id)
		}
	}
	for _, r := range c.rules {
		g := c.groups[r.GroupID]
		if g == nil {
			// create default group configurations.
			g = &RuleGroup{ID: r.GroupID}
			c.groups[r.GroupID] = g
		}
		// setup group for `buildRuleList`
		r.group = g
	}
}

func (c *ruleConfig) getRule(key [2]string) *Rule {
	return c.rules[key]
}

func (c *ruleConfig) iterateRules(f func(*Rule)) {
	for _, r := range c.rules {
		f(r)
	}
}

func (c *ruleConfig) setRule(r *Rule) {
	c.rules[r.Key()] = r
}

func (c *ruleConfig) setGroup(g *RuleGroup) {
	c.groups[g.ID] = g
}

func (c *ruleConfig) getGroup(id string) *RuleGroup {
	if g, ok := c.groups[id]; ok {
		return g
	}
	return &RuleGroup{ID: id}
}

func (c *ruleConfig) beginPatch() *RuleConfigPatch {
	return &RuleConfigPatch{
		c:   c,
		mut: newRuleConfig(),
	}
}

// RuleConfigPatch is a helper data structure to update ruleConfig.
type RuleConfigPatch struct {
	c   *ruleConfig // original configuration to be updated
	mut *ruleConfig // record all to-commit rules and groups
}

// SetRule sets a rule to the patch.
func (p *RuleConfigPatch) SetRule(r *Rule) {
	p.mut.rules[r.Key()] = r
}

// DeleteRule deletes a rule from the patch.
func (p *RuleConfigPatch) DeleteRule(group, id string) {
	p.mut.rules[[2]string{group, id}] = nil
}

func (p *RuleConfigPatch) getGroup(id string) *RuleGroup {
	if g, ok := p.mut.groups[id]; ok {
		return g
	}
	if g, ok := p.c.groups[id]; ok {
		return g
	}
	return &RuleGroup{ID: id}
}

// SetGroup sets a group to the patch.
func (p *RuleConfigPatch) SetGroup(g *RuleGroup) {
	p.mut.groups[g.ID] = g
}

// DeleteGroup deletes a group from the patch.
func (p *RuleConfigPatch) DeleteGroup(id string) {
	p.SetGroup(&RuleGroup{ID: id})
}

func (p *RuleConfigPatch) iterateRules(f func(*Rule)) {
	for _, r := range p.mut.rules {
		if r != nil { // nil means delete.
			f(r)
		}
	}
	for _, r := range p.c.rules {
		if _, ok := p.mut.rules[r.Key()]; !ok { // ignore rules that has been overwritten.
			f(r)
		}
	}
}

func (p *RuleConfigPatch) adjust() {
	// setup rule.group for `buildRuleList` use.
	p.iterateRules(func(r *Rule) { r.group = p.getGroup(r.GroupID) })
}

// trim unnecessary updates. For example, remove a rule then insert the same rule.
func (p *RuleConfigPatch) trim() {
	for key, rule := range p.mut.rules {
		if jsonEquals(rule, p.c.getRule(key)) {
			delete(p.mut.rules, key)
		}
	}
	for id, group := range p.mut.groups {
		if jsonEquals(group, p.c.getGroup(id)) {
			delete(p.mut.groups, id)
		}
	}
}

// merge all mutations to ruleConfig.
func (p *RuleConfigPatch) commit() {
	for key, rule := range p.mut.rules {
		if rule == nil {
			delete(p.c.rules, key)
		} else {
			oldRule, ok := p.c.rules[key]
			version := uint64(0)
			var createTimestamp uint64
			if ok {
				version = oldRule.Version + 1
				createTimestamp = oldRule.CreateTimestamp
			} else {
				createTimestamp = uint64(time.Now().Unix())
			}
			rule.Version = version
			rule.CreateTimestamp = createTimestamp
			p.c.rules[key] = rule
		}
	}
	for id, group := range p.mut.groups {
		p.c.groups[id] = group
	}
	p.c.adjust()
}

func jsonEquals(a, b any) bool {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(aa, bb)
}
