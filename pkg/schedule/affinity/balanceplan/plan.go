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

// Package balanceplan plans balanced leader/voter targets for the affinity
// groups of one table. It is a pure computation over an abstract input: it
// never reads the cluster, takes metadata locks or writes storage, and it
// does not modify its input.
//
// It targets tables whose partitions share their placement rules, the usual
// case for affinity partition tables. When partitions may use different
// stores, the plan is still legal and never worse than the input, but it may
// stop at a local optimum that only several coordinated moves would leave.
package balanceplan

import (
	"math/rand/v2"
	"slices"
)

// Role is the placement role of a voter rule.
type Role uint8

const (
	// RoleVoter places voters, any of which may lead.
	RoleVoter Role = iota
	// RoleLeader places exactly the leader.
	RoleLeader
	// RoleFollower places voters that never lead.
	RoleFollower
)

// Rule is the abstract domain of one voter rule. Groups with the same
// placement rules share one Rule.
type Rule struct {
	Role  Role
	Count int
	// Stores lists the stores where the rule may add a new peer.
	Stores []int
	// Location[s] is the location path of store s for the rule's location
	// labels. Values are interned per level and every path has equal length.
	// A rule without location labels uses empty paths.
	Location [][]int32

	// Derived by the planner on its private copy.
	level    int     // first level where candidate stores differ
	domainOf []int32 // store -> domain index at level
	domains  int
}

// Group is one group whose target may change. Voters/Leader is its current
// target; RuleOf[i] is the rule index that Voters[i] fits.
type Group struct {
	Rules  []*Rule
	Voters []int
	RuleOf []int
	Leader int
	// CanLead[s] reports whether the leader may be moved to store s.
	CanLead []bool
	// Forced voters cannot be removed; a pinned leader cannot move.
	Forced       []int
	LeaderPinned bool
	// Exclude lists stores this group must not use, such as stores that hold
	// one of its learners.
	Exclude []int
}

// Input describes one table.
type Input struct {
	Stores int
	// BaseVoters and BaseLeaders count every target of this table that the
	// plan keeps: fixed, converging, unavailable and unsupported groups.
	BaseVoters, BaseLeaders []int
	Groups                  []Group
	// OtherVoters and OtherLeaders count the targets of all other tables.
	// They only break ties, so that tables do not pile onto the same stores;
	// nil means zero.
	OtherVoters, OtherLeaders []int
}

// Options bounds the search. Zero values select the defaults.
type Options struct {
	Sweeps int
	// WorkBudget limits candidate and reversion load entries processed by the
	// whole plan, including entries restored after a rejected reversion.
	WorkBudget int64
	Seed       uint64
	// Cancel is checked before every group visit and reversion trial.
	Cancel func() bool
}

// Default search limits. The work budget is shared by greedy candidate scans
// and reversion cleanup.
const (
	DefaultSweeps     = 4
	DefaultWorkBudget = 10_000_000
)

// Score is (sum of squared voter counts, sum of squared leader counts),
// compared voter first.
type Score struct{ Voter, Leader int64 }

// Less reports whether s is strictly better than o.
func (s Score) Less(o Score) bool {
	if s.Voter != o.Voter {
		return s.Voter < o.Voter
	}
	return s.Leader < o.Leader
}

// StopReason explains why the search ended.
type StopReason string

// Stop reasons. Only StopBound proves that no better layout exists.
const (
	StopNoGroups StopReason = "no_groups"
	StopBound    StopReason = "lower_bound"
	StopGreedy   StopReason = "greedy"
	StopBudget   StopReason = "budget"
	StopCanceled StopReason = "canceled"
)

// Result is the planned layout in input group order. Groups the planner
// cannot express keep their input target.
type Result struct {
	Voters                       [][]int
	Leaders                      []int
	Baseline, Fast, Score, Bound Score
	Stop                         StopReason
	Visits                       int
	Work                         int64
	Changed                      int
}

type groupState struct {
	*Group
	index   int   // position in Input.Groups
	cur     []int // working target
	curRule []int
	lead    int
	startV  []int // target at the start of the current greedy call
	startLd int
	iso     []*isoTarget // per rule: isolation every target must reach
}

type planner struct {
	in      *Input
	opt     Options
	n       int
	gs      []groupState
	v, l    []int
	baseV   []int // counts of everything the plan keeps
	baseL   []int
	otherV  []int
	otherL  []int
	work    int64
	visits  int
	stop    StopReason
	pk      picker
	parts   [maxDepth]partScratch
	results [maxDepth + 1][2][]choice
	candBuf []int
	ruleIso map[*Rule]*isoTarget
}

// Plan returns a layout that is never worse than the input.
func Plan(in *Input, opt Options) Result {
	if opt.Sweeps <= 0 {
		opt.Sweeps = DefaultSweeps
	}
	if opt.WorkBudget <= 0 {
		opt.WorkBudget = DefaultWorkBudget
	}
	p := newPlanner(in, opt)
	res := Result{Baseline: p.score()}
	if len(p.gs) == 0 {
		res.Fast, res.Score, res.Bound, res.Stop = res.Baseline, res.Baseline, res.Baseline, StopNoGroups
		res.Voters, res.Leaders = p.expand(nil, nil)
		return res
	}
	origV, origL := p.layout()
	res.Bound = p.lowerBound()
	if res.Baseline == res.Bound {
		// Both parts already attain their lower bounds: nothing can improve.
		res.Fast, res.Score, res.Stop = res.Baseline, res.Baseline, StopBound
		res.Voters, res.Leaders = p.expand(origV, origL)
		return res
	}
	fastV, fastL, fast := p.greedy(opt.Seed)
	res.Fast = fast
	res.Stop = StopGreedy
	if fast == res.Bound {
		res.Stop = StopBound
	}
	res.Score = p.revert(origV, origL, fastV, fastL)
	if p.stop != "" {
		res.Stop = p.stop
	}
	res.Voters, res.Leaders = p.expand(fastV, fastL)
	res.Visits, res.Work = p.visits, p.work
	for i := range fastV {
		if fastL[i] != origL[i] || !sameSet(fastV[i], origV[i]) {
			res.Changed++
		}
	}
	return res
}

// expand maps planned targets back to input order; groups that were not
// planned keep their input target.
func (p *planner) expand(voters [][]int, leaders []int) ([][]int, []int) {
	outV, outL := make([][]int, len(p.in.Groups)), make([]int, len(p.in.Groups))
	for i := range p.in.Groups {
		outV[i], outL[i] = slices.Clone(p.in.Groups[i].Voters), p.in.Groups[i].Leader
	}
	for i := range voters {
		outV[p.gs[i].index], outL[p.gs[i].index] = voters[i], leaders[i]
	}
	return outV, outL
}

// validRule reports whether the planner can model r exactly.
func validRule(r *Rule, stores int) bool {
	if r == nil || r.Count <= 0 || r.Count > maxCount || r.depth() > maxDepth {
		return false
	}
	if d := r.depth(); d > 0 {
		if len(r.Location) < stores {
			return false
		}
		for _, path := range r.Location[:stores] {
			if len(path) != d {
				return false
			}
		}
	}
	for _, s := range r.Stores {
		if s < 0 || s >= stores {
			return false
		}
	}
	return true
}

// supported reports whether g's current target matches its rules, so that
// the planner can replace it.
func supported(g *Group, stores int, valid func(*Rule) bool) bool {
	if len(g.Rules) == 0 || !slices.Contains(g.Voters, g.Leader) || len(g.RuleOf) != 0 && len(g.RuleOf) != len(g.Voters) {
		return false
	}
	counts := make([]int, len(g.Rules))
	seen := make(map[int]bool, len(g.Voters))
	for k, s := range g.Voters {
		ri := ruleIndex(g, k)
		if s < 0 || s >= stores || ri < 0 || ri >= len(g.Rules) || seen[s] {
			return false
		}
		seen[s] = true
		counts[ri]++
	}
	for ri, r := range g.Rules {
		if !valid(r) || counts[ri] != r.Count {
			return false
		}
	}
	return true
}

func newPlanner(in *Input, opt Options) *planner {
	p := &planner{in: in, opt: opt, n: in.Stores, v: make([]int, in.Stores), l: make([]int, in.Stores)}
	copy(p.v, in.BaseVoters)
	copy(p.l, in.BaseLeaders)
	if len(in.OtherVoters) >= p.n {
		p.otherV = in.OtherVoters
	}
	if len(in.OtherLeaders) >= p.n {
		p.otherL = in.OtherLeaders
	}
	// Work on private rule copies: preparation derives fields in place.
	valid := map[*Rule]bool{}
	isValid := func(r *Rule) bool {
		ok, seen := valid[r]
		if !seen {
			ok = validRule(r, in.Stores)
			valid[r] = ok
		}
		return ok
	}
	copies := map[*Rule]*Rule{}
	for i := range in.Groups {
		g := &in.Groups[i]
		if !supported(g, in.Stores, isValid) {
			// Kept as is and counted like a fixed group.
			for _, s := range g.Voters {
				if s >= 0 && s < p.n {
					p.v[s]++
				}
			}
			if g.Leader >= 0 && g.Leader < p.n {
				p.l[g.Leader]++
			}
			continue
		}
		gc := *g
		gc.Rules = make([]*Rule, len(g.Rules))
		for ri, r := range g.Rules {
			if copies[r] == nil {
				c := *r
				c.Stores = slices.Clone(r.Stores)
				copies[r] = &c
			}
			gc.Rules[ri] = copies[r]
		}
		p.gs = append(p.gs, groupState{Group: &gc, index: i})
	}
	p.baseV, p.baseL = slices.Clone(p.v), slices.Clone(p.l)
	p.prepareRules()
	for i := range p.gs {
		s := &p.gs[i]
		g := s.Group
		s.cur, s.curRule, s.lead = slices.Clone(g.Voters), slices.Clone(g.RuleOf), g.Leader
		if s.curRule == nil {
			s.curRule = make([]int, len(s.cur))
		}
		for _, v := range s.cur {
			p.v[v]++
		}
		p.l[s.lead]++
	}
	return p
}

// isMember reports whether s is one of the rule's shared stores, which are
// kept sorted by prepareRules.
func (r *Rule) isMember(s int) bool {
	_, ok := slices.BinarySearch(r.Stores, s)
	return ok
}

// prepareRules derives each rule's isolation level and domain index once.
func (p *planner) prepareRules() {
	members := map[*Rule][]int{}
	for gi := range p.gs {
		g := p.gs[gi].Group
		for _, r := range g.Rules {
			if _, ok := members[r]; !ok {
				members[r] = slices.Clone(r.Stores)
			}
		}
		for k, v := range g.Voters {
			r := g.Rules[ruleIndex(g, k)]
			members[r] = append(members[r], v)
		}
	}
	for r, stores := range members {
		slices.Sort(r.Stores)
		r.Stores = slices.Compact(r.Stores)
		depth := r.depth()
		r.level = depth
		for lv := range depth {
			if !sameValue(r, stores, lv) {
				r.level = lv
				break
			}
		}
		r.domainOf, r.domains = make([]int32, p.n), p.n
		if r.level == depth {
			for s := range p.n {
				r.domainOf[s] = int32(s)
			}
			continue
		}
		values := make([]int32, p.n)
		for s := range p.n {
			values[s] = r.Location[s][r.level]
		}
		slices.Sort(values)
		values = slices.Compact(values)
		r.domains = len(values)
		for s := range p.n {
			i, _ := slices.BinarySearch(values, r.Location[s][r.level])
			r.domainOf[s] = int32(i)
		}
	}
}

func ruleIndex(g *Group, k int) int {
	if g.RuleOf == nil {
		return 0
	}
	return g.RuleOf[k]
}

// isForced reports whether s must stay a voter of g.
func (g *groupState) isForced(s int) bool {
	return slices.Contains(g.Forced, s) || g.LeaderPinned && s == g.Leader
}

func (*planner) canLead(g *groupState, s int) bool {
	if s == g.Leader {
		return true
	}
	return !g.LeaderPinned && s < len(g.CanLead) && g.CanLead[s]
}

func (p *planner) score() Score {
	var out Score
	for s := range p.n {
		out.Voter += int64(p.v[s]) * int64(p.v[s])
		out.Leader += int64(p.l[s]) * int64(p.l[s])
	}
	return out
}

func (p *planner) layout() ([][]int, []int) {
	voters, leaders := make([][]int, len(p.gs)), make([]int, len(p.gs))
	for i := range p.gs {
		voters[i], leaders[i] = slices.Clone(p.gs[i].cur), p.gs[i].lead
	}
	return voters, leaders
}

func (p *planner) setLayout(voters [][]int, leaders []int, rules [][]int) {
	for i := range p.gs {
		g := &p.gs[i]
		for _, s := range g.cur {
			p.v[s]--
		}
		p.l[g.lead]--
		g.cur, g.lead = slices.Clone(voters[i]), leaders[i]
		if rules != nil {
			g.curRule = slices.Clone(rules[i])
		}
		for _, s := range g.cur {
			p.v[s]++
		}
		p.l[g.lead]++
	}
}

func (p *planner) rules() [][]int {
	out := make([][]int, len(p.gs))
	for i := range p.gs {
		out[i] = slices.Clone(p.gs[i].curRule)
	}
	return out
}

// stopped reports cancellation or an exhausted work budget.
func (p *planner) stopped() bool {
	if p.stop == "" {
		if p.opt.Cancel != nil && p.opt.Cancel() {
			p.stop = StopCanceled
		} else if p.work >= p.opt.WorkBudget {
			p.stop = StopBudget
		}
	}
	return p.stop != ""
}

// greedy runs the source-ordered complete-target sweeps from the working
// layout. It returns the best layout among the start and every sweep end and
// leaves that layout installed.
// Groups with different candidate stores may stop at a local optimum;
// see #11502.
func (p *planner) greedy(seed uint64) ([][]int, []int, Score) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	for i := range p.gs {
		g := &p.gs[i]
		g.startV = append(g.startV[:0], g.cur...)
		g.startLd = g.lead
	}
	best := p.score()
	bestV, bestL := p.layout()
	bestR := p.rules()
	members := make([][]int, p.n)
	pending := make([]bool, len(p.gs))
	order := make([]int, len(p.gs))
	for range p.opt.Sweeps {
		moved := false
		for i := range order {
			order[i], pending[i] = i, true
		}
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		for s := range members {
			members[s] = members[s][:0]
		}
		for _, i := range order {
			g := &p.gs[i]
			for _, s := range g.cur {
				if !g.isForced(s) {
					members[s] = append(members[s], i)
				}
			}
		}
		left := len(p.gs)
		for left > 0 && !p.stopped() {
			// Visit from the store with the most voters that still lists a
			// pending group; ties go to the lower index. Up to the target
			// scale of 300 stores a linear scan costs about the same as a
			// heap and keeps no invariant. It grows with the store count: a
			// 1000-group sparse table on 3000 stores plans in 2.9 s with the
			// scan and 0.47 s with a lazy heap, with identical results.
			s := -1
			for t := range members {
				if len(members[t]) > 0 && (s < 0 || p.v[t] > p.v[s]) {
					s = t
				}
			}
			if s < 0 {
				break
			}
			m := members[s]
			for len(m) > 0 && !pending[m[len(m)-1]] {
				m = m[:len(m)-1]
			}
			if len(m) == 0 {
				members[s] = m
				continue
			}
			i := m[len(m)-1]
			members[s] = m[:len(m)-1]
			pending[i], left = false, left-1
			if p.visit(&p.gs[i]) {
				moved = true
			}
		}
		// Groups that cannot move a voter may still move their leader.
		rest := make([]int, 0, left)
		for i := range p.gs {
			if pending[i] {
				rest = append(rest, i)
			}
		}
		slices.SortFunc(rest, func(a, b int) int {
			la, lb := p.l[p.gs[a].lead], p.l[p.gs[b].lead]
			if la != lb {
				return lb - la
			}
			return a - b
		})
		for _, i := range rest {
			if p.stopped() {
				break
			}
			if p.visitLeader(&p.gs[i]) {
				moved = true
			}
		}
		if value := p.score(); value.Less(best) {
			best = value
			bestV, bestL = p.layout()
			bestR = p.rules()
		}
		// Choices depend only on counts, so a sweep without any change
		// would repeat itself in every later sweep.
		if p.stop != "" || !moved {
			break
		}
	}
	p.setLayout(bestV, bestL, bestR)
	return bestV, bestL, best
}

// visit replaces one group's target and reports a change.
func (p *planner) visit(g *groupState) bool {
	for _, s := range g.cur {
		p.v[s]--
	}
	p.l[g.lead]--
	t, r, leader, ok := p.choose(g)
	changed := false
	if ok {
		changed = leader != g.lead || !sameSet(t, g.cur)
		g.cur, g.curRule, g.lead = t, r, leader
	}
	for _, s := range g.cur {
		p.v[s]++
	}
	p.l[g.lead]++
	if !ok {
		return p.visitLeader(g)
	}
	p.visits++
	return changed
}

// visitLeader keeps the voters and picks the leader by (count, keep, count in
// other tables, ID).
func (p *planner) visitLeader(g *groupState) bool {
	p.visits++
	p.work += int64(len(g.cur))
	if g.LeaderPinned {
		return false
	}
	old := g.lead
	p.l[old]--
	key := func(s int) [4]int {
		moved, other := 0, 0
		if s != old {
			moved = 1
		}
		if p.otherL != nil {
			other = p.otherL[s]
		}
		return [4]int{p.l[s], moved, other, s}
	}
	best := old
	hosts := p.leaderHosts(g)
	for k, s := range g.cur {
		ri := g.curRule[k]
		if !slices.Contains(hosts, ri) || !p.leaderCapable(g, g.Rules[ri], s) {
			continue
		}
		if a, b := key(s), key(best); slices.Compare(a[:], b[:]) < 0 {
			best = s
		}
	}
	g.lead = best
	p.l[best]++
	return best != old
}

// revert restores the original target of every changed group whose change is
// not needed for the planned score, so fewer groups are rewritten and moved.
// The search breaks ties by moving, which helps it cross plateaus but leaves
// neutral changes behind; re-planning a mostly balanced 128-group table
// rewrites 39 groups with this pass and 75 without it. It repeats until a pass
// reverts nothing; every other pass reverts at least one group, so it ends.
// Trials share the search's cancellation and work budget. A cutoff keeps the
// last accepted layout, whose score is never worse than the saved best layout.
func (p *planner) revert(origV [][]int, origL []int, voters [][]int, leaders []int) Score {
	p.setLayout(voters, leaders, nil)
	cur := p.score()
	if p.stopped() {
		return cur
	}
	for reverted := true; reverted; {
		reverted = false
		for i := range p.gs {
			if p.stopped() {
				return cur
			}
			g := &p.gs[i]
			if g.lead == origL[i] && sameSet(g.cur, origV[i]) {
				continue
			}
			oldV, oldL := g.cur, g.lead
			// move processes both voter targets and the two leader entries.
			work := int64(len(oldV) + len(origV[i]) + 2)
			p.work += work
			p.visits++
			d := p.move(g, slices.Clone(origV[i]), origL[i])
			if value := (Score{cur.Voter + d.Voter, cur.Leader + d.Leader}); !cur.Less(value) {
				cur, reverted = value, true
				voters[i], leaders[i] = slices.Clone(origV[i]), origL[i]
				continue
			}
			p.move(g, oldV, oldL)
			p.work += work
		}
	}
	return cur
}

// move installs a target for g and returns the exact score change.
func (p *planner) move(g *groupState, voters []int, leader int) Score {
	var d Score
	for _, s := range g.cur {
		d.Voter -= int64(2*p.v[s] - 1)
		p.v[s]--
	}
	d.Leader -= int64(2*p.l[g.lead] - 1)
	p.l[g.lead]--
	g.cur, g.lead = voters, leader
	for _, s := range g.cur {
		d.Voter += int64(2*p.v[s] + 1)
		p.v[s]++
	}
	d.Leader += int64(2*p.l[g.lead] + 1)
	p.l[g.lead]++
	return d
}

func sameSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !slices.Contains(b, x) {
			return false
		}
	}
	return true
}
