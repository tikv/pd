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

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

const (
	// affinityBalanceCreateGrace lets the new groups of a table get their first
	// targets before the table is planned.
	affinityBalanceCreateGrace = time.Minute
	// maxBalanceValidationRounds bounds the rounds of planning and validating
	// a table. Failed rounds add a placement check, exclude a learner store,
	// or keep a group unchanged.
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
	balanceCommitDisallowed    balanceCommitReason = "balance-disallowed"
	balanceCommitRulesChanged  balanceCommitReason = "rules-changed"
	balanceCommitCanceled      balanceCommitReason = "canceled"
	balanceCommitAllConflicted balanceCommitReason = "all-conflicted"
	balanceCommitNotReady      balanceCommitReason = "not_ready"
	balanceCommitUnsupported   balanceCommitReason = "unsupported"
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
	retry       bool
	groupCounts [4]int // indexed by balancePlanGroupKind
}

// balancePlanResult is a validated plan.
type balancePlanResult struct {
	targets []*affinity.Group
	// retry reports that the table may improve soon without a metadata change.
	retry bool
	// planner and groupCounts describe the last round; work and visits include
	// every round, including a canceled or rejected round.
	planner     balanceplan.Result
	groupCounts [4]int
	rounds      int
	work        int64
	visits      int
}

// planBalanceTargets plans a table and validates every changed group on its
// known Regions. A placement rejection enables feedback for all known Regions on the next
// round; a learner conflict excludes the occupied store. Other rejections keep
// the group unchanged. All feedback is local to this invocation.
func (c *AffinityChecker) planBalanceTargets(ctx context.Context, snapshot *affinity.BalanceSnapshot) (balancePlanResult, error) {
	var res balancePlanResult
	exclude := make(map[string][]uint64)
	pinned := make(map[string]balancePlanGroupKind)
	checkPlacement := false
	checkedLearners := false
	checkedUnready := false
	for range maxBalanceValidationRounds {
		withPlacement := checkPlacement
		plan := c.balancePlanInput(snapshot, exclude, pinned)
		res.retry = res.retry || plan.retry
		res.groupCounts = plan.groupCounts
		// Validate complete relaxed plans first. After a placement rejection,
		// check every mutable group to avoid one newly failing group per round.
		if checkPlacement {
			for i, group := range plan.groups {
				var regions []*core.RegionInfo
				scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
				for ctx.Err() == nil {
					region := scan.next(c.cluster)
					if region == nil {
						break
					}
					regions = append(regions, region)
				}
				plan.input.Groups[i].CheckTarget = c.balanceTargetCheck(ctx, plan.stores, regions)
			}
		}
		result := balanceplan.Plan(&plan.input, balanceplan.Options{
			Seed:   snapshot.Version,
			Cancel: func() bool { return ctx.Err() != nil },
		})
		res.planner = result
		res.retry = res.retry || result.TemporarilyBlocked || result.PlacementLimited || result.Stop == balanceplan.StopBudget
		res.rounds++
		if counter := affinityBalancePlannerStops[result.Stop]; counter != nil {
			counter.Inc()
		}
		res.work += result.Work
		res.visits += result.Visits
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if !result.Score.Less(result.Baseline) {
			return res, nil
		}
		targets := plan.changedTargets(&result)
		failed := false
		learnerConflict := false
		for _, target := range targets {
			group := snapshot.Group(target.ID)
			status, learners, failure, retry := c.validateBalancePlanTarget(ctx, snapshot, group, target)
			res.retry = res.retry || retry
			if err := ctx.Err(); err != nil {
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
				learnerConflict = true
				exclude[target.ID] = append(exclude[target.ID], learners...)
			case (status == balanceRejected || status == balanceTemporarilyBlocked) && failure == balanceTargetLocation && !withPlacement:
				checkPlacement = true
			case status == balanceUnready && failure == balanceTargetRegionUnready:
				pinned[target.ID] = balanceGroupUnready
				res.retry = true
				if !checkedUnready {
					c.pinUnreadyBalanceGroups(ctx, snapshot, plan.groups, pinned)
					checkedUnready = true
				}
			default:
				pinned[target.ID] = balanceGroupUnsupported
				if status != balanceRejected {
					pinned[target.ID] = balanceGroupUnready
				}
			}
			failed = true
		}
		// A learner store of any known Region cannot hold their shared voter
		// target. Learn these group constraints together, avoiding one newly
		// conflicting group per round. They reset with this invocation.
		if (learnerConflict || checkPlacement) && !checkedLearners {
			for _, group := range plan.groups {
				scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
				for ctx.Err() == nil {
					region := scan.next(c.cluster)
					if region == nil {
						break
					}
					for _, peer := range region.GetLearners() {
						if !slices.Contains(exclude[group.ID], peer.GetStoreId()) {
							exclude[group.ID] = append(exclude[group.ID], peer.GetStoreId())
						}
					}
				}
			}
			checkedLearners = true
		}
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if !failed {
			res.targets = targets
			return res, nil
		}
	}
	res.retry = true
	return res, nil
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
		case errors.Is(err, affinity.ErrTargetWriterUnsupported):
			return written, balanceCommitUnsupported, nil
		default:
			return written, balanceCommitError, err
		}
	}
	if len(written) == 0 {
		return written, balanceCommitAllConflicted, nil
	}
	return written, "", nil
}

// pinUnreadyBalanceGroups learns current Region constraints in one pass,
// so groups that cannot move do not consume separate validation rounds.
func (c *AffinityChecker) pinUnreadyBalanceGroups(ctx context.Context, snapshot *affinity.BalanceSnapshot,
	groups []*affinity.GroupState, pinned map[string]balancePlanGroupKind) {
	for _, group := range groups {
		if _, ok := pinned[group.ID]; ok {
			continue
		}
		scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
		for ctx.Err() == nil {
			region := scan.next(c.cluster)
			if region == nil {
				break
			}
			if _, status := c.validateBalanceCurrentRegion(group, region); status != balanceAllowed {
				pinned[group.ID] = balanceGroupUnready
				break
			}
		}
	}
}

// validateBalancePlanTarget checks the store actions of a changed group and,
// on every known Region of its ranges, the current Region and the Region moved to
// the target. On a rejection it also returns the target voters that hold a
// learner of the Region. The last result preserves any observed transient
// constraint even if a definite rejection wins the aggregate status.
func (c *AffinityChecker) validateBalancePlanTarget(ctx context.Context, snapshot *affinity.BalanceSnapshot, group *affinity.GroupState,
	target *affinity.Group) (balanceStatus, []uint64, balanceTargetFailure, bool) {
	status, failure := c.balanceTargetExecutionStatus(&group.Group, target), balanceTargetValid
	retry := status == balanceTemporarilyBlocked
	if status != balanceAllowed {
		failure = balanceTargetStoreAction
		if status == balanceRejected {
			return status, nil, failure, retry
		}
	}
	var learners []uint64
	scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
	for ctx.Err() == nil {
		region := scan.next(c.cluster)
		if region == nil {
			return status, learners, failure, retry
		}
		fit, currentStatus := c.validateBalanceCurrentRegion(group, region)
		if currentStatus != balanceAllowed {
			retry = true
			if status == balanceAllowed {
				status, failure = currentStatus, balanceTargetRegionUnready
			}
			continue
		}
		targetStatus, targetFailure := c.validateBalanceTarget(target, region, fit)
		retry = retry || targetStatus == balanceTemporarilyBlocked
		if targetFailure == balanceTargetLearnerConflict {
			for _, peer := range region.GetLearners() {
				if id := peer.GetStoreId(); slices.Contains(target.VoterStoreIDs, id) && !slices.Contains(learners, id) {
					learners = append(learners, id)
				}
			}
		}
		// A temporary failure must not hide another Region's definite rejection.
		// Within rejected results prefer failures that replacement hints cannot fix.
		if targetStatus == balanceRejected && (status != balanceRejected || failure == balanceTargetLocation) ||
			targetStatus != balanceAllowed && status == balanceAllowed {
			status, failure = targetStatus, targetFailure
		}
	}
	return balanceUnready, learners, balanceTargetRegionUnready, true
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

// balanceTargetCheck reuses placement feedback for all known Regions
// after a placement rejection. Every new complete target is
// still validated on all known Regions before it is returned for commit.
func (c *AffinityChecker) balanceTargetCheck(ctx context.Context, storeIDs []uint64, regions []*core.RegionInfo) func([]int, int, int64) balanceplan.TargetFeedback {
	index := make(map[uint64]int, len(storeIDs))
	for i, id := range storeIDs {
		index[id] = i
	}
	currentFits := make([]*placement.RegionFit, len(regions))
	for i, region := range regions {
		if ctx.Err() != nil {
			break
		}
		currentFits[i] = c.ruleManager.FitRegion(c.cluster, region)
	}
	return func(voters []int, leader int, workLimit int64) balanceplan.TargetFeedback {
		stores := make([]uint64, len(voters))
		for i, s := range voters {
			stores[i] = storeIDs[s]
		}
		feedback := balanceplan.TargetFeedback{}
		for i, region := range regions {
			if ctx.Err() != nil || feedback.Work >= workLimit {
				feedback.Status = balanceplan.TargetRejected
				return feedback
			}
			virtual := cloneRegionWithReplacePeerStores(region, storeIDs[leader], stores...)
			feedback.Work += int64(len(storeIDs))
			if virtual == nil {
				feedback.Status = balanceplan.TargetRejected
				return feedback
			}
			fit := c.ruleManager.FitRegionWithoutCache(c.cluster, virtual)
			feedback.Work += int64(len(storeIDs) * len(fit.RuleFits))
			if !placementNotWorse(currentFits[i], fit) {
				feedback.Status = balanceplan.TargetRejected
				return feedback
			}
			d := c.regionPlacementFeedback(virtual, fit)
			feedback.Work += int64(len(storeIDs) * d.scans)
			switch d.status {
			case affinityPlacementReady:
				continue
			case affinityPlacementTemporary:
				if feedback.Status == balanceplan.TargetSatisfied {
					feedback.Status = balanceplan.TargetTemporarilyBlocked
				}
			default:
				feedback.Status = balanceplan.TargetRejected
				for _, replacement := range d.replacements {
					if old, ok := index[replacement.old]; ok {
						r := balanceplan.StoreReplacement{Old: old}
						for _, id := range replacement.candidates {
							if next, ok := index[id]; ok {
								r.Candidates = append(r.Candidates, next)
							}
						}
						feedback.Replace = append(feedback.Replace, r)
					}
				}
			}
		}
		return feedback
	}
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
func (c *AffinityChecker) balanceRuleOf(group *affinity.GroupState, ranges affinity.GroupKeyRanges, rules []*placement.Rule) ([]int, bool) {
	ruleOf := make([]int, len(group.VoterStoreIDs))
	if len(rules) == 1 {
		return ruleOf, true
	}
	scan := newBalanceRegionScan(ranges)
	region := scan.next(c.cluster)
	if region == nil {
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
	// These fields contain only strings, integers, booleans and slices, so
	// JSON encoding cannot fail and preserves every field boundary.
	encoded, _ := json.Marshal(struct {
		Role             placement.PeerRoleType
		IsWitness        bool
		Count            int
		LocationLabels   []string
		IsolationLevel   string
		LabelConstraints []placement.LabelConstraint
	}{r.Role, r.IsWitness, r.Count, r.LocationLabels, r.IsolationLevel, r.LabelConstraints})
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
	if r.IsWitness {
		rc.rules[key] = nil
		return nil, false
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
		if leader == group.LeaderStoreID && slices.Equal(voters, group.VoterStoreIDs) {
			continue
		}
		target := group.Group
		target.LeaderStoreID, target.VoterStoreIDs = leader, voters
		targets = append(targets, &target)
	}
	return targets
}
