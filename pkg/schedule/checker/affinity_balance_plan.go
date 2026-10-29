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
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const (
	// affinityBalanceCreateGrace lets the new groups of a table get their first
	// targets before the table is planned.
	affinityBalanceCreateGrace = time.Minute
	// maxBalanceValidationRounds bounds the rounds of planning and validating
	// a table. Every failed round excludes a store for a group or keeps a group
	// unchanged.
	maxBalanceValidationRounds = 10
)

type balancePlanGroupKind uint8

const (
	balanceGroupMutable balancePlanGroupKind = iota
	balanceGroupFixed
	balanceGroupUnready
	balanceGroupUnsupported
)

type balanceCommitReason string

const (
	balanceCommitRegionsChanged balanceCommitReason = "regions-changed"
	balanceCommitDisallowed     balanceCommitReason = "balance-disallowed"
	balanceCommitRulesChanged   balanceCommitReason = "rules-changed"
	balanceCommitTableChanged   balanceCommitReason = "table-changed"
	balanceCommitCanceled       balanceCommitReason = "canceled"
	balanceCommitAllConflicted  balanceCommitReason = "all-conflicted"
	balanceCommitError          balanceCommitReason = "error"
)

// balancePlan is the planner input of a table and the mapping back to it.
type balancePlan struct {
	input  balanceplan.Input
	stores []uint64               // planner store index -> store ID
	groups []*affinity.GroupState // planner group index -> group
	// retry reports an input that may change without a metadata change: an
	// auto group is still moving to its target, or a store action was
	// temporarily blocked.
	retry       bool
	groupCounts [4]int // indexed by balancePlanGroupKind
}

// balancePlanResult is a validated plan.
type balancePlanResult struct {
	targets []*affinity.Group
	// checked holds the Regions that the targets were validated against.
	checked map[uint64]balanceCheckedRegion
	// retry reports that the table may improve soon without a metadata change.
	retry bool
	// planner and groupCounts describe the last round; work, visits and durations
	// include every round, including a canceled or rejected round.
	planner          balanceplan.Result
	groupCounts      [4]int
	stops            []balanceplan.StopReason
	work             int64
	visits           int
	planDuration     time.Duration
	validateDuration time.Duration
}

// balanceCheckedRegion is a Region that a target was validated against.
type balanceCheckedRegion struct {
	group  *affinity.GroupState
	region *core.RegionInfo
}

// planBalanceTargets plans a table and validates every changed group on all
// its Regions. A group whose target fails is planned again without the stores
// that hold learners of its Regions, or kept unchanged if no learner is to
// blame. It returns a validated plan that improves the table, or no targets.
func (c *AffinityChecker) planBalanceTargets(ctx context.Context, snapshot *affinity.BalanceSnapshot) (balancePlanResult, error) {
	var res balancePlanResult
	exclude := make(map[string][]uint64)
	pinned := make(map[string]balancePlanGroupKind)
	for range maxBalanceValidationRounds {
		plan := c.balancePlanInput(snapshot, exclude, pinned)
		res.retry = res.retry || plan.retry
		res.groupCounts = plan.groupCounts
		start := time.Now()
		result := balanceplan.Plan(&plan.input, balanceplan.Options{
			Seed:   snapshot.Version,
			Cancel: func() bool { return ctx.Err() != nil },
		})
		res.planDuration += time.Since(start)
		res.planner = result
		res.stops = append(res.stops, result.Stop)
		res.work += result.Work
		res.visits += result.Visits
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if !result.Score.Less(result.Baseline) {
			return res, nil
		}
		targets := plan.changedTargets(&result)
		checked := make(map[uint64]balanceCheckedRegion)
		failed := false
		start = time.Now()
		for _, target := range targets {
			group := snapshot.Group(target.ID)
			status, learners := c.validateBalancePlanTarget(ctx, snapshot, group, target, checked)
			if err := ctx.Err(); err != nil {
				res.validateDuration += time.Since(start)
				return res, err
			}
			if status != balanceAllowed {
				statusName := "rejected"
				switch status {
				case balanceTemporarilyBlocked:
					statusName = "temporarily-blocked"
				case balanceUnready:
					statusName = "unready"
				}
				log.Debug("affinity balance target rejected", zap.String("group-id", target.ID),
					zap.String("status", statusName), zap.Uint64s("excluded-learner-stores", learners))
			}
			switch {
			case status == balanceAllowed:
				continue
			case len(learners) > 0:
				exclude[target.ID] = append(exclude[target.ID], learners...)
			default:
				pinned[target.ID] = balanceGroupUnsupported
				if status != balanceRejected {
					pinned[target.ID] = balanceGroupUnready
				}
				res.retry = res.retry || status != balanceRejected
			}
			failed = true
		}
		res.validateDuration += time.Since(start)
		if !failed {
			res.targets, res.checked = targets, checked
			return res, nil
		}
	}
	res.retry = true
	return res, nil
}

// commitBalancePlan writes a validated plan if the Regions of the changed
// groups are still as they were validated, balancing is still allowed and the
// placement rules and table metadata have not changed since planning. It returns
// the IDs of the written groups and the reason if no target was written or the
// commit failed.
func (c *AffinityChecker) commitBalancePlan(ctx context.Context, snapshot *affinity.BalanceSnapshot, res balancePlanResult, revision uint64) ([]string, balanceCommitReason, error) {
	// The scan may take a while, so the gates are checked after it, right
	// before the write.
	regionsUnchanged := c.verifyBalanceRegions(snapshot, res.targets, res.checked)
	if err := ctx.Err(); err != nil {
		return nil, balanceCommitCanceled, err
	}
	if !regionsUnchanged {
		return nil, balanceCommitRegionsChanged, nil
	}
	if !c.balanceAllowed() {
		return nil, balanceCommitDisallowed, nil
	}
	if c.ruleManager.GetRevision() != revision {
		return nil, balanceCommitRulesChanged, nil
	}
	if version, ok := c.affinityManager.GetBalanceVersion(snapshot.TableID); !ok || version != snapshot.Version {
		return nil, balanceCommitTableChanged, nil
	}
	written, err := c.affinityManager.CommitBalanceTargets(ctx, snapshot, res.targets)
	if err != nil {
		if ctx.Err() != nil {
			return written, balanceCommitCanceled, err
		}
		return written, balanceCommitError, err
	}
	if len(written) == 0 {
		return written, balanceCommitAllConflicted, nil
	}
	return written, "", nil
}

// validateBalancePlanTarget checks the store actions of a changed group and,
// on every Region of its ranges, the current Region and the Region moved to
// the target. On a rejection it also returns the target voters that hold a
// learner of the Region.
func (c *AffinityChecker) validateBalancePlanTarget(ctx context.Context, snapshot *affinity.BalanceSnapshot, group *affinity.GroupState,
	target *affinity.Group, checked map[uint64]balanceCheckedRegion) (balanceStatus, []uint64) {
	if status := c.balanceTargetExecutionStatus(&group.Group, target); status != balanceAllowed {
		return status, nil
	}
	scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
	for ctx.Err() == nil {
		region, covered, done := scan.next(c.cluster)
		if !covered {
			return balanceUnready, nil
		}
		if done {
			return balanceAllowed, nil
		}
		fit, status := c.validateBalanceCurrentRegion(group, region)
		if status != balanceAllowed {
			return status, nil
		}
		if status := c.validateBalanceTarget(target, region, fit); status != balanceAllowed {
			var learners []uint64
			for _, peer := range region.GetLearners() {
				if slices.Contains(target.VoterStoreIDs, peer.GetStoreId()) {
					learners = append(learners, peer.GetStoreId())
				}
			}
			return status, learners
		}
		checked[region.GetID()] = balanceCheckedRegion{group, region}
	}
	return balanceUnready, nil
}

// verifyBalanceRegions scans the ranges of the changed groups again. It
// requires the same Regions, unchanged and still ready, so that a split, a
// merge or a peer change after the validation is not missed.
func (c *AffinityChecker) verifyBalanceRegions(snapshot *affinity.BalanceSnapshot, targets []*affinity.Group,
	checked map[uint64]balanceCheckedRegion) bool {
	var ranges []keyutil.KeyRange
	for _, target := range targets {
		ranges = append(ranges, snapshot.Ranges[target.ID].KeyRanges...)
	}
	slices.SortFunc(ranges, func(a, b keyutil.KeyRange) int { return bytes.Compare(a.StartKey, b.StartKey) })
	regions, err := c.cluster.BatchScanRegions(keyutil.NewKeyRanges(ranges), core.WithOutputMustContainAllKeyRange())
	if err != nil || len(regions) != len(checked) {
		return false
	}
	for _, region := range regions {
		old, ok := checked[region.GetID()]
		if !ok || !sameBalanceRegion(old.region, region) || !c.balanceRegionReady(old.group, region) {
			return false
		}
	}
	return true
}

func sameBalanceRegion(a, b *core.RegionInfo) bool {
	return a.GetRegionEpoch().GetVersion() == b.GetRegionEpoch().GetVersion() &&
		a.GetRegionEpoch().GetConfVer() == b.GetRegionEpoch().GetConfVer() &&
		bytes.Equal(a.GetStartKey(), b.GetStartKey()) && bytes.Equal(a.GetEndKey(), b.GetEndKey()) &&
		a.GetLeader().GetStoreId() == b.GetLeader().GetStoreId() && core.SortedPeersEqual(a.GetPeers(), b.GetPeers())
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
func (c *AffinityChecker) balancePlanInput(snapshot *affinity.BalanceSnapshot, exclude map[string][]uint64, pinned map[string]balancePlanGroupKind) *balancePlan {
	plan := &balancePlan{}
	ids := slices.Clone(snapshot.Stores)
	for _, group := range snapshot.Groups {
		ids = append(ids, group.VoterStoreIDs...)
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
		canLead[i] = up[id] && allowed(id, true, false)
	}
	rules := newBalanceRuleCache(c, plan.stores, func(id uint64) bool { return up[id] && allowed(id, false, false) })
	for _, group := range snapshot.Groups {
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
		g, kind := c.balancePlanGroup(group, snapshot.Ranges[group.ID], rules, index, canLead, allowed)
		if pinnedKind, ok := pinned[group.ID]; kind == balanceGroupMutable && ok {
			kind = pinnedKind
		}
		plan.groupCounts[kind]++
		if kind == balanceGroupMutable {
			for _, id := range exclude[group.ID] {
				if i, known := index[id]; known {
					g.Exclude = append(g.Exclude, i)
				}
			}
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

// balancePlanGroup describes a group that may change, or classifies why it cannot.
func (c *AffinityChecker) balancePlanGroup(group *affinity.GroupState, ranges affinity.GroupKeyRanges, rules *balanceRuleCache,
	index map[uint64]int, canLead []bool, allowed func(id uint64, leader, source bool) bool) (balanceplan.Group, balancePlanGroupKind) {
	if group.BalancePolicy.IsFixed() {
		return balanceplan.Group{}, balanceGroupFixed
	}
	if !group.AffinitySchedulingAllowed || group.Phase != affinity.PhaseStable ||
		group.LeaderStoreID == 0 || len(group.VoterStoreIDs) == 0 {
		return balanceplan.Group{}, balanceGroupUnready
	}
	voterRules, ok := c.balanceGroupRules(ranges)
	if !ok {
		return balanceplan.Group{}, balanceGroupUnsupported
	}
	ruleOf, ok := c.balanceRuleOf(group, ranges, voterRules)
	if !ok {
		return balanceplan.Group{}, balanceGroupUnsupported
	}
	g := balanceplan.Group{Leader: index[group.LeaderStoreID], CanLead: canLead, RuleOf: ruleOf}
	for _, r := range voterRules {
		rule, ok := rules.get(r)
		if !ok {
			return balanceplan.Group{}, balanceGroupUnsupported
		}
		g.Rules = append(g.Rules, rule)
	}
	for k, id := range group.VoterStoreIDs {
		// The location labels of the current voters must be known too.
		if !rules.located(voterRules[ruleOf[k]], id) {
			return balanceplan.Group{}, balanceGroupUnsupported
		}
		g.Voters = append(g.Voters, index[id])
		if !allowed(id, false, true) {
			g.Forced = append(g.Forced, index[id])
		}
	}
	g.LeaderPinned = !allowed(group.LeaderStoreID, true, true)
	return g, balanceGroupMutable
}

// balanceGroupRules returns the voter rules of a group. The planner models
// one set of rules per group, so every range of the group must lie in one
// rule segment and all ranges must have the same rules.
func (c *AffinityChecker) balanceGroupRules(ranges affinity.GroupKeyRanges) ([]*placement.Rule, bool) {
	var voters []*placement.Rule
	for i, keyRange := range ranges.KeyRanges {
		// It returns no rules for a range that crosses a rule boundary.
		var rules []*placement.Rule
		for _, r := range c.ruleManager.GetRulesForApplyRange(keyRange.StartKey, keyRange.EndKey) {
			if r.Role != placement.Learner {
				rules = append(rules, r)
			}
		}
		if len(rules) == 0 {
			return nil, false
		}
		if i == 0 {
			voters = rules
		} else if !slices.EqualFunc(voters, rules, func(a, b *placement.Rule) bool { return a.Key() == b.Key() }) {
			return nil, false
		}
	}
	return voters, len(voters) > 0
}

// balanceRuleOf returns the index of the rule that each current voter fits.
// With more than one voter rule, the fit of the group's first Region decides.
func (c *AffinityChecker) balanceRuleOf(group *affinity.GroupState, ranges affinity.GroupKeyRanges, rules []*placement.Rule) ([]int, bool) {
	ruleOf := make([]int, len(group.VoterStoreIDs))
	if len(rules) == 1 {
		return ruleOf, true
	}
	scan := newBalanceRegionScan(ranges)
	region, covered, done := scan.next(c.cluster)
	if !covered || done {
		return nil, false
	}
	fit := c.ruleManager.FitRegion(c.cluster, region)
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
	addable   func(uint64) bool
	rules     map[string]*balanceplan.Rule
	locations map[string][][]int32
}

func newBalanceRuleCache(checker *AffinityChecker, stores []uint64, addable func(uint64) bool) *balanceRuleCache {
	return &balanceRuleCache{checker: checker, stores: stores, addable: addable,
		rules: make(map[string]*balanceplan.Rule), locations: make(map[string][][]int32)}
}

// balanceRuleSignature describes what a rule requires, without its ID, keys
// and index.
func balanceRuleSignature(r *placement.Rule) string {
	return fmt.Sprintf("%s/%v/%d/%v/%v/%s", r.Role, r.IsWitness, r.Count, r.LabelConstraints, r.LocationLabels, r.IsolationLevel)
}

// location returns the location path of every store for labels. Values are
// interned per level and compared without case, as in CompareLocation. A
// missing value is -1.
func (rc *balanceRuleCache) location(labels []string) [][]int32 {
	key := strings.Join(labels, ",")
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
				v = strings.ToLower(store.GetLabelValue(label))
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
	rule := &balanceplan.Rule{Role: role, Count: r.Count, Location: loc}
	for i, id := range rc.stores {
		store := rc.checker.cluster.GetStore(id)
		if store != nil && rc.addable(id) && placement.MatchLabelConstraints(store, r.LabelConstraints) && !slices.Contains(loc[i], -1) {
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
		current := slices.Clone(group.VoterStoreIDs)
		slices.Sort(current)
		if leader == group.LeaderStoreID && slices.Equal(voters, current) {
			continue
		}
		target := group.Group
		target.LeaderStoreID, target.VoterStoreIDs = leader, voters
		targets = append(targets, &target)
	}
	return targets
}
