// Copyright 2026 TiKV Project Authors.
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

package checker

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// affinityBalanceCreateGrace lets new groups learn their first targets
// before the table is planned.
const affinityBalanceCreateGrace = time.Minute

type balanceCommitReason string

const (
	balanceCommitDisallowed    balanceCommitReason = "balance-disallowed"
	balanceCommitRulesChanged  balanceCommitReason = "rules-changed"
	balanceCommitCanceled      balanceCommitReason = "canceled"
	balanceCommitAllConflicted balanceCommitReason = "all-conflicted"
	balanceCommitNotReady      balanceCommitReason = "not_ready"
	balanceCommitError         balanceCommitReason = "error"
)

// balancePlan is the planner input of a table and the mapping back to it.
type balancePlan struct {
	input  balanceplan.Input
	stores []uint64               // planner store index -> store ID
	groups []*affinity.GroupState // planner group index -> group
	// retry reports an input that may change without a metadata change: an
	// auto group is still moving to its target, or a store action was
	// temporarily blocked.
	retry bool
}

// balancePlanResult is a validated plan.
type balancePlanResult struct {
	targets []*affinity.Group
	planner balanceplan.Result
	score   balanceplan.Score
	// retry reports that the table may improve soon without a metadata change.
	retry bool
}

// planBalanceTargets solves a table once and keeps validated changes only
// if they still improve the table's score together.
func (c *AffinityChecker) planBalanceTargets(ctx context.Context, snapshot *affinity.BalanceSnapshot) (balancePlanResult, error) {
	plan := c.balancePlanInput(ctx, snapshot)
	if err := ctx.Err(); err != nil {
		return balancePlanResult{}, err
	}
	result := balanceplan.Plan(&plan.input, balanceplan.Options{
		Seed:   snapshot.Version,
		Cancel: func() bool { return ctx.Err() != nil },
	})
	res := balancePlanResult{
		planner: result,
		score:   result.Baseline,
		retry:   plan.retry || result.Stop == balanceplan.StopBudget,
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	if !result.Score.Less(result.Baseline) {
		return res, nil
	}

	targets := plan.changedTargets(&result)
	validTargets := make([]*affinity.Group, 0, len(targets))
	for _, target := range targets {
		status := c.validateBalancePlanTarget(ctx, snapshot, snapshot.Group(target.ID), target)
		if err := ctx.Err(); err != nil {
			return res, err
		}
		switch status {
		case balanceAllowed:
			validTargets = append(validTargets, target)
		case balanceTemporarilyBlocked, balanceUnready:
			res.retry = true
		}
	}
	// The planner baseline includes every group in this table, including
	// fixed and unmodelled groups, and excludes other tables.
	if len(validTargets) > 0 {
		validatedScore := balanceScoreWithTargets(snapshot.Groups, validTargets)
		if validatedScore.Less(result.Baseline) {
			res.targets = validTargets
			res.score = validatedScore
		}
	}
	return res, nil
}

func balanceScoreWithTargets(groups []*affinity.GroupState, targets []*affinity.Group) balanceplan.Score {
	changed := make(map[string]*affinity.Group, len(targets))
	for _, target := range targets {
		changed[target.ID] = target
	}
	voters, leaders := make(map[uint64]int64), make(map[uint64]int64)
	for _, group := range groups {
		target := &group.Group
		if change := changed[group.ID]; change != nil {
			target = change
		}
		for _, id := range target.VoterStoreIDs {
			voters[id]++
		}
		if target.LeaderStoreID != 0 {
			leaders[target.LeaderStoreID]++
		}
	}
	var score balanceplan.Score
	for _, count := range voters {
		score.Voter += count * count
	}
	for _, count := range leaders {
		score.Leader += count * count
	}
	return score
}

// commitBalancePlan writes a validated plan if balancing is still allowed and
// the placement rules have not changed since planning. It returns
// the IDs of the written groups and the reason if no target was written or the
// commit failed. The checks run right before the write, but not atomically with
// it. Changes to other groups of the table, such as an admin update of a fixed
// group, can leave this round without improvement; the next round replans from
// the latest state.
func (c *AffinityChecker) commitBalancePlan(ctx context.Context, snapshot *affinity.BalanceSnapshot, res balancePlanResult, revision uint64) ([]string, balanceCommitReason, error) {
	failpoint.InjectCall("beforeAffinityBalanceCommit")
	if err := ctx.Err(); err != nil {
		return nil, balanceCommitCanceled, err
	}
	if !c.balanceAllowed() {
		return nil, balanceCommitDisallowed, nil
	}
	if c.ruleManager.GetRevision() != revision {
		return nil, balanceCommitRulesChanged, nil
	}
	written, err := c.affinityManager.CommitBalanceTargets(ctx, snapshot, res.targets)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return written, balanceCommitCanceled, err
		case errors.Is(err, affinity.ErrTargetWriterNotReady):
			return written, balanceCommitNotReady, nil
		default:
			return written, balanceCommitError, err
		}
	}
	if len(written) == 0 {
		return written, balanceCommitAllConflicted, nil
	}
	return written, "", nil
}

// validateBalancePlanTarget rechecks the store actions, the current Regions,
// and their proposed layouts before accepting a new target.
func (c *AffinityChecker) validateBalancePlanTarget(ctx context.Context, snapshot *affinity.BalanceSnapshot, group *affinity.GroupState, target *affinity.Group) balanceStatus {
	status := c.balanceTargetExecutionStatus(&group.Group, target)
	if status == balanceRejected {
		return status
	}
	for region := range c.balanceRegions(ctx, snapshot.Ranges[group.ID]) {
		fit, currentStatus := c.validateBalanceCurrentRegion(group, region)
		if currentStatus != balanceAllowed {
			status = balanceUnready
			continue
		}
		switch c.validateBalanceTarget(target, region, fit) {
		case balanceRejected:
			return balanceRejected
		case balanceTemporarilyBlocked:
			if status == balanceAllowed {
				status = balanceTemporarilyBlocked
			}
		}
	}
	return status
}

// balanceCreatingTable reports whether a table has an auto group that was
// created within affinityBalanceCreateGrace and has no target yet.
func balanceCreatingTable(snapshot *affinity.BalanceSnapshot, now time.Time) bool {
	for _, group := range snapshot.Groups {
		if !group.BalancePolicy.IsFixed() && len(group.VoterStoreIDs) == 0 &&
			now.Sub(time.Unix(int64(group.CreateTimestamp), 0)) < affinityBalanceCreateGrace {
			return true
		}
	}
	return false
}

// balancePlanInput turns a table snapshot into planner input. A group may
// change only if it is auto, available and stable, and the planner can model
// its rules; the targets of the other groups are counted as they are.
func (c *AffinityChecker) balancePlanInput(ctx context.Context, snapshot *affinity.BalanceSnapshot) *balancePlan {
	plan := &balancePlan{}
	ids := slices.Clone(snapshot.Stores)
	for _, group := range snapshot.Groups {
		if ctx.Err() != nil {
			return nil
		}
		ids = append(ids, group.VoterStoreIDs...)
		if group.LeaderStoreID != 0 {
			ids = append(ids, group.LeaderStoreID)
		}
	}
	slices.Sort(ids)
	plan.stores = slices.Compact(ids)
	index := make(map[uint64]int, len(plan.stores))
	for i, id := range plan.stores {
		index[id] = i
	}
	n := len(plan.stores)
	in := &plan.input
	in.Stores, in.BaseVoters, in.BaseLeaders = n, make([]int, n), make([]int, n)
	in.OtherVoters, in.OtherLeaders = make([]int, n), make([]int, n)
	for id, count := range snapshot.Counts.Voter {
		if i, ok := index[id]; ok {
			in.OtherVoters[i] += count
		}
	}
	for id, count := range snapshot.Counts.Leader {
		if i, ok := index[id]; ok {
			in.OtherLeaders[i] += count
		}
	}
	check := c.balanceStoreActionChecker()
	allowed := func(id uint64, leader, source bool) bool {
		status := check(balanceStoreAction{id: id, leader: leader, source: source})
		plan.retry = plan.retry || status == balanceTemporarilyBlocked
		return status == balanceAllowed
	}
	up := make(map[uint64]bool, len(snapshot.Stores))
	for _, id := range snapshot.Stores {
		up[id] = true
	}
	canLead := make([]bool, n)
	for i, id := range plan.stores {
		canLead[i] = up[id] && check(balanceStoreAction{id: id, leader: true}) == balanceAllowed
	}
	rules := newBalanceRuleCache(c, plan.stores, func(id uint64, role placement.PeerRoleType) bool {
		if !up[id] {
			return false
		}
		if role != placement.Follower {
			allowed(id, true, false)
		}
		return allowed(id, false, false)
	})
	for _, group := range snapshot.Groups {
		if ctx.Err() != nil {
			return nil
		}
		// The counts of other tables exclude this table.
		for _, id := range group.VoterStoreIDs {
			in.OtherVoters[index[id]]--
		}
		if group.LeaderStoreID != 0 {
			in.OtherLeaders[index[group.LeaderStoreID]]--
		}
		if !group.BalancePolicy.IsFixed() && group.AffinitySchedulingAllowed && group.Phase == affinity.PhasePreparing {
			plan.retry = true
		}
		g, ready := c.balancePlanGroup(ctx, plan, group, snapshot.Ranges[group.ID], rules, index, canLead, allowed)
		if ready {
			in.Groups = append(in.Groups, g)
			plan.groups = append(plan.groups, group)
			continue
		}
		for _, id := range group.VoterStoreIDs {
			in.BaseVoters[index[id]]++
		}
		if group.LeaderStoreID != 0 {
			in.BaseLeaders[index[group.LeaderStoreID]]++
		}
	}
	return plan
}

// balancePlanGroup builds the mutable input for a stable group whose rules
// can be modeled and whose known Regions have reached their current target.
func (c *AffinityChecker) balancePlanGroup(ctx context.Context, plan *balancePlan, group *affinity.GroupState, ranges affinity.GroupKeyRanges, rules *balanceRuleCache,
	index map[uint64]int, canLead []bool, allowed func(id uint64, leader, source bool) bool) (balanceplan.Group, bool) {
	if group.BalancePolicy.IsFixed() || !group.AffinitySchedulingAllowed || group.Phase != affinity.PhaseStable ||
		group.LeaderStoreID == 0 || len(group.VoterStoreIDs) == 0 {
		return balanceplan.Group{}, false
	}
	voterRules, ok := c.balanceGroupRules(ranges)
	if !ok {
		return balanceplan.Group{}, false
	}

	var firstFit *placement.RegionFit
	var exclude []int
	for region := range c.balanceRegions(ctx, ranges) {
		fit, status := c.validateBalanceCurrentRegion(group, region)
		if status != balanceAllowed {
			plan.retry = true
			return balanceplan.Group{}, false
		}
		if firstFit == nil {
			firstFit = fit
		}
		for _, peer := range region.GetLearners() {
			if i, ok := slices.BinarySearch(plan.stores, peer.GetStoreId()); ok && !slices.Contains(exclude, i) {
				exclude = append(exclude, i)
			}
		}
	}
	ruleOf, ok := balanceRuleOf(group, voterRules, firstFit)
	if !ok {
		return balanceplan.Group{}, false
	}
	g := balanceplan.Group{Leader: index[group.LeaderStoreID], CanLead: canLead, RuleOf: ruleOf, Exclude: exclude}
	for _, r := range voterRules {
		rule, ok := rules.get(r)
		if !ok {
			return balanceplan.Group{}, false
		}
		g.Rules = append(g.Rules, rule)
	}
	for k, id := range group.VoterStoreIDs {
		// The location labels of the current voters must be known too.
		if !rules.located(voterRules[ruleOf[k]], id) {
			return balanceplan.Group{}, false
		}
		g.Voters = append(g.Voters, index[id])
		if !allowed(id, false, true) {
			g.Forced = append(g.Forced, index[id])
		}
	}
	g.LeaderPinned = !allowed(group.LeaderStoreID, true, true)

	return g, true
}

// balanceGroupRules returns the voter rules of a group. The planner models
// one set of voter rules per group. Learner rules may differ across segments,
// but the same voter rules must apply to every segment of every range.
func (c *AffinityChecker) balanceGroupRules(ranges affinity.GroupKeyRanges) ([]*placement.Rule, bool) {
	var voters []*placement.Rule
	for _, keyRange := range ranges.KeyRanges {
		start := keyRange.StartKey
		ends := append(c.ruleManager.GetSplitKeys(start, keyRange.EndKey), keyRange.EndKey)
		for _, end := range ends {
			var rules []*placement.Rule
			for _, r := range c.ruleManager.GetRulesForApplyRange(start, end) {
				if r.IsWitness {
					return nil, false
				}
				if r.Role != placement.Learner {
					rules = append(rules, r)
				}
			}
			if len(rules) == 0 {
				return nil, false
			}
			if len(voters) == 0 {
				voters = rules
			} else if !slices.EqualFunc(voters, rules, func(a, b *placement.Rule) bool { return a.Key() == b.Key() }) {
				return nil, false
			}
			start = end
		}
	}
	return voters, len(voters) > 0
}

// balanceRuleOf returns the index of the rule that each current voter fits.
// With more than one voter rule, the fit of the group's first Region decides.
func balanceRuleOf(group *affinity.GroupState, rules []*placement.Rule, fit *placement.RegionFit) ([]int, bool) {
	ruleOf := make([]int, len(group.VoterStoreIDs))
	if len(rules) == 1 {
		return ruleOf, true
	}
	if fit == nil {
		return nil, false
	}
	assigned := make(map[uint64]int, len(group.VoterStoreIDs))
	for _, rf := range fit.RuleFits {
		ri := slices.IndexFunc(rules, func(r *placement.Rule) bool { return r.Key() == rf.Rule.Key() })
		if ri < 0 {
			continue
		}
		for _, peer := range rf.Peers {
			assigned[peer.GetStoreId()] = ri
		}
	}
	for k, id := range group.VoterStoreIDs {
		ri, ok := assigned[id]
		if !ok {
			return nil, false
		}
		ruleOf[k] = ri
	}
	return ruleOf, true
}

// balanceRuleCache gives the groups whose rules have the same content one
// planner rule, so that the planner prepares it once.
type balanceRuleCache struct {
	checker   *AffinityChecker
	stores    []uint64
	addable   func(uint64, placement.PeerRoleType) bool
	rules     map[string]*balanceplan.Rule
	locations map[string][][]int32
}

func newBalanceRuleCache(checker *AffinityChecker, stores []uint64, addable func(uint64, placement.PeerRoleType) bool) *balanceRuleCache {
	return &balanceRuleCache{checker: checker, stores: stores, addable: addable,
		rules: make(map[string]*balanceplan.Rule), locations: make(map[string][][]int32)}
}

// balanceRuleSignature describes what a rule requires, without its ID, keys
// and index.
func balanceRuleSignature(r *placement.Rule) string {
	// These fields contain only strings, integers and slices, so
	// JSON encoding cannot fail and preserves every field boundary.
	encoded, _ := json.Marshal(struct {
		Role             placement.PeerRoleType
		Count            int
		LocationLabels   []string
		IsolationLevel   string
		LabelConstraints []placement.LabelConstraint
	}{r.Role, r.Count, r.LocationLabels, r.IsolationLevel, r.LabelConstraints})
	return string(encoded)
}

// location returns the location path of every store for labels. Values are
// interned per level and compared without case, as in CompareLocation. A
// missing value is -1.
func (rc *balanceRuleCache) location(labels []string) [][]int32 {
	encoded, _ := json.Marshal(labels)
	key := string(encoded)
	if loc, ok := rc.locations[key]; ok {
		return loc
	}
	intern := make([]map[string]int32, len(labels))
	for i := range intern {
		intern[i] = make(map[string]int32)
	}
	loc := make([][]int32, len(rc.stores))
	for i, id := range rc.stores {
		loc[i] = make([]int32, len(labels))
		store := rc.checker.cluster.GetStore(id)
		for lv, label := range labels {
			v := ""
			if store != nil {
				v = strings.Map(func(r rune) rune {
					canonical := r
					for folded := unicode.SimpleFold(r); folded != r; folded = unicode.SimpleFold(folded) {
						canonical = min(canonical, folded)
					}
					return canonical
				}, store.GetLabelValue(label))
			}
			if v == "" {
				loc[i][lv] = -1
				continue
			}
			if _, ok := intern[lv][v]; !ok {
				intern[lv][v] = int32(len(intern[lv]))
			}
			loc[i][lv] = intern[lv][v]
		}
	}
	rc.locations[key] = loc
	return loc
}

// located reports whether a store has every location label of r. PD treats a
// store without a label as co-located with every store, which the planner
// does not model.
func (rc *balanceRuleCache) located(r *placement.Rule, id uint64) bool {
	i, found := slices.BinarySearch(rc.stores, id)
	return found && !slices.Contains(rc.location(r.LocationLabels)[i], -1)
}

// get returns the planner rule of r, or false if the planner cannot model it.
func (rc *balanceRuleCache) get(r *placement.Rule) (*balanceplan.Rule, bool) {
	key := balanceRuleSignature(r)
	if rule, ok := rc.rules[key]; ok {
		return rule, rule != nil
	}
	isolationDepth := 0
	if r.IsolationLevel != "" {
		idx := slices.Index(r.LocationLabels, r.IsolationLevel)
		if idx < 0 {
			rc.rules[key] = nil
			return nil, false
		}
		isolationDepth = idx + 1
	}
	var role balanceplan.Role
	switch r.Role {
	case placement.Voter:
		role = balanceplan.RoleVoter
	case placement.Leader:
		role = balanceplan.RoleLeader
	case placement.Follower:
		role = balanceplan.RoleFollower
	default:
		rc.rules[key] = nil
		return nil, false
	}
	loc := rc.location(r.LocationLabels)
	rule := &balanceplan.Rule{Role: role, Count: r.Count, Location: loc, IsolationDepth: isolationDepth}
	for i, id := range rc.stores {
		store := rc.checker.cluster.GetStore(id)
		if store != nil && placement.MatchLabelConstraints(store, r.LabelConstraints) && !slices.Contains(loc[i], -1) && rc.addable(id, r.Role) {
			rule.Stores = append(rule.Stores, i)
		}
	}
	rc.rules[key] = rule
	return rule, true
}

// changedTargets returns the targets of the groups that the plan changes.
func (p *balancePlan) changedTargets(result *balanceplan.Result) []*affinity.Group {
	var targets []*affinity.Group
	for i, group := range p.groups {
		leader := p.stores[result.Leaders[i]]
		voters := make([]uint64, 0, len(result.Voters[i]))
		for _, s := range result.Voters[i] {
			voters = append(voters, p.stores[s])
		}
		slices.Sort(voters)
		if leader == group.LeaderStoreID && slices.Equal(voters, group.VoterStoreIDs) {
			continue
		}
		target := group.Group
		target.LeaderStoreID, target.VoterStoreIDs = leader, voters
		targets = append(targets, &target)
	}
	return targets
}
