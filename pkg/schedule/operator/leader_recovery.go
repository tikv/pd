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

package operator

import (
	"cmp"
	"crypto/rand"
	"math"
	"slices"
	"sync/atomic"

	"github.com/google/btree"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// RecoveryRound is the orchestrator's fencing token. Epoch changes when PD or
// scheduling-service leadership changes. IDs increase within an epoch.
type RecoveryRound struct {
	Epoch    string `json:"epoch"`
	ID       uint64 `json:"round,string"`
	Finished bool   `json:"finished"`
}

type departure struct {
	round, id, home, peer, term, version, successor uint64
	process                                         int64
	rate                                            float64
}

type plannedDeparture struct {
	departure
	hadOrigin bool // an evicted or spent origin must not be replaced by this temporary host
	command   atomic.Uint32
}

const (
	drainIssued      uint32 = iota + 1
	drainReplacedHot        // a confirmation could belong to the already issued hot command
)

func (d departure) matches(r *core.RegionInfo, home *core.StoreInfo) bool {
	if r == nil || !home.RestartedFrom(d.process) || r.GetID() != d.id || r.GetTerm() <= 1 ||
		r.GetTerm()-1 != d.term || r.GetRegionEpoch().GetVersion() != d.version || r.GetLeader().GetStoreId() != d.successor {
		return false
	}
	p := r.GetStoreVoter(d.home)
	return p != nil && p.GetId() == d.peer && !p.GetIsWitness() && core.IsVoterOrIncomingVoter(p)
}

func (d departure) stillLeads(r *core.RegionInfo) bool {
	return r != nil && r.GetLeader().GetId() == d.peer && r.GetLeader().GetStoreId() == d.home && r.GetTerm() == d.term && r.GetRegionEpoch().GetVersion() == d.version
}

// Departing read rate bounds the reusable read rate from above.
func byDirection(a, b departure) bool {
	return cmp.Or(cmp.Compare(a.home, b.home), cmp.Compare(a.successor, b.successor), cmp.Compare(b.rate, a.rate), cmp.Compare(a.id, b.id)) < 0
}

type leaderRecovery struct {
	syncutil.Mutex
	active atomic.Bool
	RecoveryRound
	roundRecords
}

type transferSample struct {
	rate float64
	term uint64
}

const maxRecoveryRegions = 8192

// Only the exact confirming heartbeat can carry the cost to another transfer.
type handoffReport struct {
	term, peer, store, version, confVer, start, end uint64
}

func identifyHandoff(r *core.RegionInfo) handoffReport {
	e, i := r.GetRegionEpoch(), r.GetInterval()
	return handoffReport{r.GetTerm(), r.GetLeader().GetId(), r.GetLeader().GetStoreId(), e.GetVersion(), e.GetConfVer(), i.GetStartTimestamp(), i.GetEndTimestamp()}
}

type handoffCost struct {
	report handoffReport
	rate   float64
}

type roundRecords struct {
	handoffs cache.Cache // Region ID -> cost on its last confirmed long heartbeat
	pending  map[*Operator]transferSample
	draining map[uint64]int64
	yielded  map[uint64]bool
	history  map[uint64]departure
	coldest  *btree.BTreeG[departure]
	index    *btree.BTreeG[departure]
	recent   map[uint64]map[uint64]readBounds // Store -> last reported end -> change still possibly absent
}

// CurrentRecoveryRound returns the active scheduler incarnation and latest round.
func (oc *Controller) CurrentRecoveryRound() RecoveryRound {
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	if s.Epoch == "" {
		s.Epoch = rand.Text()
	}
	return s.RecoveryRound
}

// UpdateRecoveryRound declares a planned drain of a specific TiKV process or
// closes a round. A repeated finish also fences a delayed start for that ID.
func (oc *Controller) UpdateRecoveryRound(round RecoveryRound, store uint64, process int64) error {
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	if round.ID == 0 || round.Epoch == "" || round.Epoch != s.Epoch || round.ID < s.ID || round.ID == s.ID && s.Finished && !round.Finished {
		return errors.New("stale recovery round")
	}
	if round.Finished && s.active.Load() && round.ID != s.ID {
		return errors.New("finish does not match the active recovery round")
	}
	if round.Finished && (store != 0 || process != 0) || !round.Finished && (store == 0 || process <= 0) {
		return errors.New("specify a draining store and process, or finish the round")
	}
	if !round.Finished {
		if round.ID == s.ID && s.draining[store] == process {
			return nil
		}
		st := oc.cluster.GetStore(store)
		if st == nil || st.GetMeta().GetStartTimestamp() != process || round.ID == s.ID && s.draining[store] != 0 {
			return errors.New("draining store process changed")
		}
	}
	if round.ID != s.ID || round.Finished {
		s.RecoveryRound = round
		s.roundRecords = roundRecords{}
		s.active.Store(!round.Finished)
		if round.Finished {
			return nil
		}
		s.roundRecords = roundRecords{
			handoffs: cache.NewCache(maxRecoveryRegions, cache.LRUCache),
			draining: make(map[uint64]int64), yielded: make(map[uint64]bool),
			history: make(map[uint64]departure),
			index:   btree.NewG(16, byDirection),
			coldest: btree.NewG(16, func(a, b departure) bool { return cmp.Or(cmp.Compare(a.rate, b.rate), cmp.Compare(a.id, b.id)) < 0 }),
			recent:  make(map[uint64]map[uint64]readBounds), pending: make(map[*Operator]transferSample),
		}
		oc.operators.Range(func(_, v any) bool { oc.trackTransfer(v.(*Operator)); return true })
	}
	s.draining[store] = process
	return nil
}

func (s *leaderRecovery) forget(id uint64) {
	if d, ok := s.history[id]; ok {
		delete(s.history, id)
		s.coldest.Delete(d)
		s.index.Delete(d)
	}
}

func (s *leaderRecovery) consume(id uint64) {
	if s.history[id].id != 0 {
		s.forget(id)
		s.history[id] = departure{}
	}
}

// Keep the first retained origin. A Store which temporarily hosted a Region
// during this roll must not learn it back from the return trip.
func (s *leaderRecovery) remember(d departure) {
	if _, known := s.history[d.id]; known {
		return
	}
	if len(s.history) == maxRecoveryRegions {
		cold, ok := s.coldest.Min()
		if !ok || d.rate <= cold.rate {
			return
		}
		s.forget(cold.id)
	}
	s.history[d.id] = d
	s.coldest.ReplaceOrInsert(d)
	s.index.ReplaceOrInsert(d)
}

// BestReturn finds the largest reusable read rate for one ordinary balance slot.
// The default wins ties. The callback checks the scheduler's live placement and
// admission rules and retains the operator only when it accepts a better Region.
func (oc *Controller) BestReturn(c sche.SchedulerCluster, source, target *core.StoreInfo, ordinary *core.RegionInfo, batch []*Operator, accept func(*core.RegionInfo) bool) *core.RegionInfo {
	s := &oc.recovery
	if !s.active.Load() {
		return ordinary
	}
	s.Lock()
	if !s.active.Load() || s.index.Len() == 0 || !target.RestartedFrom(s.draining[target.GetID()]) {
		s.Unlock()
		return ordinary
	}
	cost := s.readCostBounds(ordinary)
	loads, ok := oc.compareReads(target, source, &recoveryChoice{extra: batch})
	if !ok || math.IsInf(cost.high, 0) {
		s.Unlock()
		return ordinary
	}
	best := 0.0
	if d, found := s.history[ordinary.GetID()]; found && d.home == target.GetID() && d.successor == source.GetID() && d.matches(ordinary, target) {
		best = min(d.rate, cost.high)
	}
	index := s.index.Clone()
	s.Unlock()

	type candidate struct {
		region *core.RegionInfo
		score  float64
	}
	var candidates []candidate
	first := departure{home: target.GetID(), successor: source.GetID(), rate: math.Inf(1)}
	index.AscendGreaterOrEqual(first, func(d departure) bool {
		if d.home != first.home || d.successor != first.successor || d.rate <= best {
			return false
		}
		r := oc.cluster.GetRegion(d.id)
		if !d.matches(r, target) {
			return true
		}
		rate, valid := r.ReadKeysPerSecond()
		score := min(d.rate, rate)
		if valid && score > best && loads.allowsReturn(cost, rate) {
			candidates = append(candidates, candidate{r, score})
		}
		return true
	})
	if len(candidates) == 0 {
		return ordinary
	}
	slices.SortStableFunc(candidates, func(a, b candidate) int { return cmp.Compare(b.score, a.score) })
	regions := make([]*core.RegionInfo, len(candidates))
	for i, candidate := range candidates {
		regions[i] = candidate.region
	}
	for i, allowed := range c.LeaderReturnEligibility(regions, c.GetSchedulerConfig().GetHotRegionCacheHitsThreshold()) {
		if allowed && accept(regions[i]) {
			return regions[i]
		}
	}
	return ordinary
}

type readBounds struct{ low, high float64 }

func (b *readBounds) add(delta float64) {
	b.low += min(0, delta)
	b.high += max(0, delta)
}

// leaderReadChanges includes compound moves and all alternate receivers. A
// confirmed single transfer can settle on its actual Leader immediately.
func (op *Operator) leaderReadChanges(rate float64, actual uint64, apply func(uint64, float64)) {
	for _, step := range op.steps {
		t, ok := step.(TransferLeader)
		if !ok {
			continue
		}
		apply(t.FromStore, -rate)
		if actual != 0 && len(op.steps) == 1 {
			apply(actual, rate)
			continue
		}
		apply(t.ToStore, rate)
		for _, id := range t.ToStores {
			if id != t.ToStore {
				apply(id, rate)
			}
		}
	}
}

func readCost(r *core.RegionInfo) float64 {
	if r != nil {
		if rate, ok := r.ReadKeysPerSecond(); ok {
			return rate
		}
	}
	return math.Inf(1)
}

// readCostBounds keeps a measured rate distinct from a confirmed handoff's
// upper bound. Optional return candidates still require a measured rate.
func (s *leaderRecovery) readCostBounds(r *core.RegionInfo) readBounds {
	if rate := readCost(r); !math.IsInf(rate, 1) {
		return readBounds{rate, rate}
	}
	if bound, valid := r.LongIntervalReadBound(); valid {
		if h, ok := s.handoffs.Peek(r.GetID()); ok {
			previous := h.(handoffCost)
			if previous.report == identifyHandoff(r) {
				return readBounds{high: max(previous.rate, bound)}
			}
		}
	}
	return readBounds{high: math.Inf(1)}
}

// A new Leader's first report can include its time as a follower. For a
// confirmed direct transfer, retain both the admitted rate and the greatest
// read rate the reported counter could cover in a valid reporting window.
func (v transferSample) handoffBound(op *Operator, r *core.RegionInfo) (float64, bool) {
	bound, ok := r.LongIntervalReadBound()
	if !ok || r.GetTerm() <= 1 || r.GetTerm()-1 != v.term {
		return 0, false
	}
	step, ok := primary(op)
	e, initial := r.GetRegionEpoch(), op.RegionEpoch()
	if !ok || step.FromStore == r.GetLeader().GetStoreId() || !step.IsFinish(r) || e == nil || initial == nil ||
		e.GetVersion() != initial.GetVersion() || e.GetConfVer() != initial.GetConfVer() {
		return 0, false
	}
	return max(v.rate, bound), true
}

type recoveryChoice struct {
	siblings [2]*Operator // the two independent Raft commands in an exchange
	partner  *departure   // original Leader of the other eviction

	round    uint64
	cluster  sche.SchedulerCluster
	home     departure
	ordinary uint64 // balance: default Region; eviction: Region exchanged for home
	other    uint64 // peer Store trading reads with home

	extra []*Operator // remaining plans in the scheduling batch
	sent  [2]bool     // protected by leaderRecovery.Mutex
}

// loadBounds includes live operators, even when they predate this round, plus
// the signed ranges from transfers a Store's measurement may not yet include.
func (oc *Controller) loadBounds(st *core.StoreInfo, p *recoveryChoice) (readBounds, bool) {
	stats := st.ReadStats()
	w := st.GetLeaderWeight()
	if stats == nil || !(w > 0 && w <= math.MaxFloat64) {
		return readBounds{}, false
	}
	i := stats.GetInterval()
	rate := float64(stats.GetKeysRead()) / float64(i.GetEndTimestamp()-i.GetStartTimestamp())
	b := readBounds{rate, rate}
	for end, v := range oc.recovery.recent[st.GetID()] {
		if i.GetStartTimestamp() > end {
			delete(oc.recovery.recent[st.GetID()], end)
			continue
		}
		b.low += v.low
		b.high += v.high
	}
	add := func(op *Operator, sample transferSample) {
		if op.recovery == p {
			return
		}
		r := oc.cluster.GetRegion(op.RegionID())
		rate, ok := sample.handoffBound(op, r)
		if !ok {
			rate = max(sample.rate, oc.recovery.readCostBounds(r).high)
		}
		op.leaderReadChanges(rate, 0, func(id uint64, delta float64) {
			if id == st.GetID() {
				b.add(delta)
			}
		})
	}
	for op, sample := range oc.recovery.pending {
		add(op, sample)
	}
	for _, op := range p.extra {
		if _, tracked := oc.recovery.pending[op]; !tracked && op.Status() == CREATED {
			add(op, transferSample{})
		}
	}
	// Unmeasured outgoing work can reduce the load to zero. Unmeasured
	// incoming work leaves the upper bound open, but the lower bound survives.
	b = readBounds{max(0, b.low) / w, b.high / w}
	return b, b.low <= math.MaxFloat64 && b.low <= b.high
}

func eligibleReturnTarget(c sche.SchedulerCluster, r *core.RegionInfo, to uint64) bool {
	if r == nil || len(r.GetDownPeers()) != 0 || len(r.GetPendingPeers()) != 0 {
		return false
	}
	if l := c.GetRegionLabeler(); l != nil && l.ScheduleDisabled(r) {
		return false
	}
	peer := r.GetStoreVoter(to)
	return peer != nil && !peer.GetIsWitness() && IsAllowedLeaderTarget(c, r, peer)
}

func eligibleReturn(c sche.SchedulerCluster, r *core.RegionInfo, to uint64) bool {
	if !eligibleReturnTarget(c, r, to) {
		return false
	}
	mask := c.LeaderReturnEligibility([]*core.RegionInfo{r}, c.GetSchedulerConfig().GetHotRegionCacheHitsThreshold())
	return len(mask) == 1 && mask[0]
}

// fits compares the same source/target pair under the default and proposed
// allocations. Differences of squared gaps are linear in the starting gap,
// so checking both endpoints covers any completion order of pending moves.
func (oc *Controller) fits(p *recoveryChoice) bool {
	s := &oc.recovery
	if !s.active.Load() || p.round != s.ID {
		return false
	}
	home, other := oc.cluster.GetStore(p.home.home), oc.cluster.GetStore(p.other)
	r, a := oc.cluster.GetRegion(p.home.id), oc.cluster.GetRegion(p.ordinary)
	if home == nil || other == nil || r == nil || a == nil || !home.RestartedFrom(p.home.process) {
		return false
	}
	bRate := readCost(r)
	if bRate <= 0 || math.IsInf(bRate, 0) {
		return false
	}
	if !p.sent[0] && (!p.home.matches(r, home) || !p.sent[1] && s.history[p.home.id] != p.home) {
		return false
	}
	if p.partner == nil {
		labels := p.cluster.GetRegionLabeler()
		if a.GetLeader().GetStoreId() != other.GetID() || oc.GetOperator(a.GetID()) != nil || labels != nil && labels.ScheduleDisabled(a) {
			return false
		}
	}
	if p.partner != nil {
		if d, known := s.history[p.ordinary]; known && d.id == 0 {
			return false
		}
		for _, id := range []uint64{p.home.id, p.ordinary} {
			if op := oc.GetOperator(id); op != nil && op.recovery != p {
				return false
			}
		}
		from := oc.cluster.GetStore(p.home.successor)
		if from == nil || from.GetMeta().GetStartTimestamp() != s.draining[p.home.successor] {
			return false
		}
	}
	if p.partner != nil && !p.sent[1] && !p.partner.stillLeads(a) {
		return false
	}
	loads, ok := oc.compareReads(home, other, p)
	if !ok {
		return false
	}
	if p.partner != nil {
		return loads.allowsExchange(readCost(a), bRate, p)
	}
	return loads.allowsReturn(s.readCostBounds(a), bRate)
}

// The same two Store bounds apply to every candidate for one balance slot.
// They are taken again against live operators before first dispatch.
type readComparison struct {
	lo, hi, homeWeight, otherWeight float64
	homeYielded, otherYielded       bool
}

func (oc *Controller) compareReads(home, other *core.StoreInfo, p *recoveryChoice) (readComparison, bool) {
	h, ok := oc.loadBounds(home, p)
	if !ok {
		return readComparison{}, false
	}
	o, ok := oc.loadBounds(other, p)
	if !ok {
		return readComparison{}, false
	}
	return readComparison{
		lo: o.low - h.high, hi: o.high - h.low,
		homeWeight: home.GetLeaderWeight(), otherWeight: other.GetLeaderWeight(),
		homeYielded: oc.recovery.yielded[home.GetID()], otherYielded: oc.recovery.yielded[other.GetID()],
	}, true
}

// The default rate closest to a balanced split is the hardest to beat.
// At each endpoint of the possible Store gaps, compare against that rate
// and leaving the Leader in place. For each fixed default the difference
// of squared gaps is affine, so these endpoints cover every interior gap.
func (c readComparison) allowsReturn(ordinary readBounds, rate float64) bool {
	if c.homeYielded && rate > ordinary.low || c.otherYielded && rate < ordinary.high {
		return false
	}
	scale := 1/c.homeWeight + 1/c.otherWeight
	low, high, candidate := ordinary.low*scale, ordinary.high*scale, rate*scale
	if math.IsInf(high, 1) || !noWiderGap(c.lo, c.hi, 0, candidate) {
		return false
	}
	for _, gap := range [...]float64{c.lo, c.hi} {
		best := min(max(low, gap), high)
		if !noWiderGap(gap, gap, best, candidate) {
			return false
		}
	}
	return true
}

// Either eviction can complete first or fail independently.
func (c readComparison) allowsExchange(aRate, bRate float64, p *recoveryChoice) bool {
	lo, hi := c.lo, c.hi
	if c.homeYielded && bRate > 0 || c.otherYielded && aRate > 0 {
		return false
	}
	bHome, bOther := bRate/c.homeWeight, bRate/c.otherWeight
	aHome, aOther := aRate/c.homeWeight, aRate/c.otherWeight
	if p.sent[0] {
		hi += bHome
	}
	if p.sent[1] {
		lo -= aOther
	}
	return noWiderGap(lo, hi, -bOther, bHome) && noWiderGap(lo, hi, aHome, -aOther) &&
		noWiderGap(lo, hi, aHome-bOther, bHome-aOther)
}

func noWiderGap(lo, hi, ordinary, candidate float64) bool {
	if !(lo <= hi && math.Abs(ordinary) <= math.MaxFloat64 && math.Abs(candidate) <= math.MaxFloat64) {
		return false
	}
	if ordinary == candidate {
		return true
	}
	// The midpoint separates the gaps that prefer one allocation from those
	// that prefer the other. Only the endpoint on the ordinary side matters.
	if candidate < ordinary {
		lo, ordinary, candidate = -hi, -ordinary, -candidate
	}
	if lo >= candidate {
		return true
	}
	if lo <= ordinary {
		return false
	}
	left, right := lo-ordinary, candidate-lo
	if left != right {
		return left > right
	}
	// Recover each subtraction's rounding error when distinct distances round
	// to a tie. FastTwoSum needs the larger-magnitude summand first.
	residual := func(a, b float64) float64 {
		if math.Abs(a) < math.Abs(b) {
			a, b = b, a
		}
		return b - (left - a)
	}
	return residual(lo, -ordinary) >= residual(candidate, -lo)
}

// PreferReturn checks the actual ordinary candidate and attaches its comparison
// to the single final operator. Only its Region enters normal admission.
func (oc *Controller) PreferReturn(c sche.SchedulerCluster, op *Operator, ordinary uint64, extra []*Operator) bool {
	step, ok := primary(op)
	if !ok || !eligibleReturn(c, oc.cluster.GetRegion(op.RegionID()), step.ToStore) {
		return false
	}
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	d, ok := s.history[op.RegionID()]
	if !ok || step.ToStore != d.home || step.FromStore != d.successor {
		return false
	}
	p := &recoveryChoice{round: s.ID, cluster: c, home: d, ordinary: ordinary, other: d.successor, extra: slices.Clone(extra)}
	if !oc.fits(p) {
		return false
	}
	op.recovery = p
	op.SetAdditionalInfo("cache-return", "balance")
	return true
}

func primary(op *Operator) (TransferLeader, bool) {
	if op == nil || len(op.steps) != 1 {
		return TransferLeader{}, false
	}
	t, ok := op.steps[0].(TransferLeader)
	return t, ok
}

// PlanEvictions tags planned departures and exchanges destinations only within
// this batch. Preserve both defaults unless both replacement operators build.
func (oc *Controller) PlanEvictions(c sche.SchedulerCluster, ops []*Operator) {
	s := &oc.recovery
	if !s.active.Load() {
		return
	}
	s.Lock()
	round := s.ID
	former := make(map[uint64]departure, len(ops))
	for _, op := range ops {
		t, ok := primary(op)
		if !ok {
			continue
		}
		from := oc.cluster.GetStore(t.FromStore)
		r := oc.cluster.GetRegion(op.RegionID())
		if from == nil || r == nil || s.draining[t.FromStore] <= 0 || s.draining[t.FromStore] != from.GetMeta().GetStartTimestamp() || r.GetLeader().GetStoreId() != t.FromStore {
			continue
		}
		origin, known := s.history[r.GetID()]
		rate := readCost(r)
		if (!known && math.IsInf(rate, 1)) || r.GetTerm() == 0 || int64(r.GetInterval().GetEndTimestamp()) < s.draining[t.FromStore] {
			continue
		}
		op.drain = &plannedDeparture{
			departure: departure{round: s.ID, id: r.GetID(), home: t.FromStore, peer: r.GetLeader().GetId(), process: s.draining[t.FromStore], term: r.GetTerm(), version: r.GetRegionEpoch().GetVersion(), rate: rate},
			hadOrigin: known,
		}
		if known && !math.IsInf(rate, 1) && origin.matches(r, oc.cluster.GetStore(origin.home)) {
			former[r.GetID()] = origin
		}
	}
	s.Unlock()
	// Try the highest reusable read rate first. Both members must be movable
	// under the same hot-cache observation.
	var homes []int
	for i, op := range ops {
		if d, ok := former[op.RegionID()]; ok && min(d.rate, op.drain.rate) > 0 {
			homes = append(homes, i)
		}
	}
	volume := func(i int) float64 { return min(ops[i].drain.rate, former[ops[i].RegionID()].rate) }
	slices.SortStableFunc(homes, func(i, j int) int { return cmp.Compare(volume(j), volume(i)) })
	if len(homes) == 0 {
		return
	}
	regions := make([]*core.RegionInfo, len(ops))
	for i, op := range ops {
		if op.drain != nil {
			regions[i] = oc.cluster.GetRegion(op.RegionID())
		}
	}
	allowed := c.LeaderReturnEligibility(regions, c.GetSchedulerConfig().GetHotRegionCacheHitsThreshold())
	if len(allowed) != len(ops) {
		return
	}
	loads := make(map[[2]uint64]readComparison)
	for _, i := range homes {
		op := ops[i]
		t, _ := primary(op)
		d := former[op.RegionID()]
		if !allowed[i] || t.ToStore == d.home || !slices.Contains(t.ToStores, d.home) || !eligibleReturnTarget(c, regions[i], d.home) {
			continue
		}
		// All other commands in this batch might fail. If that outcome alone
		// rules out an exchange, building its full pending plan is unnecessary.
		pair := [2]uint64{d.home, t.ToStore}
		base, known := loads[pair]
		if !known {
			s.Lock()
			home, other := oc.cluster.GetStore(d.home), oc.cluster.GetStore(t.ToStore)
			if home != nil && other != nil && s.active.Load() && s.ID == round {
				base, _ = oc.compareReads(home, other, &recoveryChoice{})
			}
			s.Unlock()
			loads[pair] = base
		}
		if base.homeWeight == 0 || base.otherWeight == 0 {
			continue
		}
		for j, other := range ops {
			u, ok := primary(other)
			if !ok || other.drain == nil || t.FromStore != u.FromStore || u.ToStore != d.home || !slices.Contains(u.ToStores, t.ToStore) {
				continue
			}
			if _, exists := former[other.RegionID()]; exists {
				continue
			}
			p := &recoveryChoice{round: round, cluster: c, home: d, ordinary: other.RegionID(), other: t.ToStore, partner: &other.drain.departure}
			if !allowed[j] || !base.allowsExchange(other.drain.rate, op.drain.rate, p) || !eligibleReturnTarget(c, regions[j], t.ToStore) {
				continue
			}
			for k, rest := range ops {
				if k != i && k != j {
					p.extra = append(p.extra, rest)
				}
			}
			s.Lock()
			fits := oc.fits(p)
			s.Unlock()
			if !fits {
				continue
			}
			a, b := p.redirectEviction(op, d.home), p.redirectEviction(other, t.ToStore)
			if a != nil && b != nil {
				p.siblings = [2]*Operator{a, b}
				ops[i], ops[j] = a, b
				return
			}
		}
	}
}

func (p *recoveryChoice) redirectEviction(old *Operator, to uint64) *Operator {
	r := p.cluster.GetRegion(old.RegionID())
	if r == nil {
		return nil
	}
	op, err := CreateTransferLeaderOperator(old.Desc(), p.cluster, r, to, nil, OpLeader)
	if err != nil {
		return nil
	}
	// A cache-directed exchange has normal Leader priority. A hot operator
	// can therefore win regardless of which is published first.
	op.Counters = old.Counters
	op.recovery, op.drain = p, old.drain
	op.SetAdditionalInfo("cache-return", "evict")
	return op
}

func (oc *Controller) observeRecovery(op *Operator) {
	s := &oc.recovery
	if !s.active.Load() {
		return
	}
	s.Lock()
	defer s.Unlock()
	if s.active.Load() {
		oc.trackTransfer(op)
	}
}

func (oc *Controller) trackTransfer(op *Operator) {
	s := &oc.recovery
	if op.IsEnd() {
		return
	}
	if op.Kind()&OpHotRegion != 0 {
		s.consume(op.RegionID())
	}
	for _, step := range op.steps {
		switch t := step.(type) {
		case TransferLeader:
			if _, ok := s.pending[op]; !ok {
				r := oc.cluster.GetRegion(op.RegionID())
				sample := transferSample{rate: s.readCostBounds(r).high}
				if r != nil && r.GetLeader().GetStoreId() == t.FromStore {
					sample.term = r.GetTerm()
				}
				s.pending[op] = sample
			}
			if op.Kind()&OpHotRegion != 0 {
				s.yielded[t.FromStore] = true
			}
		case RemovePeer:
			if op.Kind()&OpHotRegion != 0 {
				s.yielded[t.FromStore] = true
			}
		}
	}
}

func (oc *Controller) canSendRecovery(op *Operator, region *core.RegionInfo) bool {
	p := op.recovery
	if p == nil {
		return true
	}
	s := &oc.recovery
	slot, to := 0, p.home.home
	if p.partner != nil && op.RegionID() == p.ordinary {
		slot, to = 1, p.other
	}
	// Dispatch can receive a newer Region heartbeat than the scheduling cache.
	// Never resend a cache choice across an intervening election or split.
	if slot == 0 && !p.home.matches(region, oc.cluster.GetStore(to)) || slot == 1 && !p.partner.stillLeads(region) {
		return false
	}
	s.Lock()
	if p.sent[slot] {
		s.Unlock()
		return true
	}
	checkSibling := p.partner != nil && !p.sent[1-slot]
	s.Unlock()
	eligible := eligibleReturn(p.cluster, region, to)
	if checkSibling {
		other := p.siblings[1-slot]
		step, _ := primary(other)
		eligible = eligible && eligibleReturn(p.cluster, oc.cluster.GetRegion(other.RegionID()), step.ToStore)
	}
	s.Lock()
	defer s.Unlock()
	if p.sent[slot] {
		return true
	}
	if p.partner != nil {
		other := p.siblings[1-slot]
		if other.IsEnd() && other.Status() != SUCCESS {
			return false
		}
	}
	if !eligible || !oc.fits(p) || op.IsEnd() {
		return false
	}
	if !p.sent[0] && !p.sent[1] {
		// The first leg claims the origin for both commands, whichever sends
		// first. Its spent ID stays in the same bounded history until finish.
		s.consume(p.home.id)
	}
	p.sent[slot] = true
	return true
}

func (oc *Controller) finishRecovery(op *Operator, r *core.RegionInfo) {
	s := &oc.recovery
	if !s.active.Load() {
		return
	}
	s.Lock()
	defer s.Unlock()
	admitted, tracked := s.pending[op]
	if !tracked {
		return
	}
	delete(s.pending, op)
	if !op.HasStarted() {
		return
	}
	if r == nil {
		r = oc.cluster.GetRegion(op.RegionID())
	}
	actual := uint64(0)
	if r != nil && op.Status() == SUCCESS && op.steps[len(op.steps)-1].IsFinish(r) {
		actual = r.GetLeader().GetStoreId()
	}
	rate := max(admitted.rate, s.readCostBounds(r).high)
	if bound, ok := admitted.handoffBound(op, r); actual != 0 && ok {
		rate = bound
		s.handoffs.Put(r.GetID(), handoffCost{identifyHandoff(r), rate})
	}
	op.leaderReadChanges(rate, actual, func(id uint64, delta float64) {
		st := oc.cluster.GetStore(id)
		if st == nil {
			return
		}
		interval := st.GetStoreStats().GetInterval()
		buckets := s.recent[id]
		if buckets == nil {
			buckets = make(map[uint64]readBounds)
			s.recent[id] = buckets
		}
		for end := range buckets {
			if end < interval.GetStartTimestamp() {
				delete(buckets, end)
			}
		}
		end := interval.GetEndTimestamp()
		if id == actual {
			end = max(end, r.GetInterval().GetEndTimestamp())
		}
		b := buckets[end]
		b.add(delta)
		buckets[end] = b
	})
	d := op.drain
	if actual == 0 || d == nil || op.recovery != nil || d.command.Load() != drainIssued || s.ID != d.round || actual == d.home ||
		r.GetTerm() <= 1 || r.GetTerm()-1 != d.term || r.GetRegionEpoch().GetVersion() != d.version {
		return
	}
	// A late completion for a replaced process cannot establish a new origin.
	from := oc.cluster.GetStore(d.home)
	if from == nil || from.GetMeta().GetStartTimestamp() != d.process {
		return
	}
	h := d.departure
	if origin, known := s.history[d.id]; known {
		// Follow only adjacent, confirmed planned drains. Any intervening
		// Leader change through another scheduler breaks this chain.
		if d.term <= 1 || origin.term != d.term-1 || origin.successor != d.home || origin.version != d.version || actual == origin.home {
			return
		}
		s.forget(d.id)
		h = origin
		h.term = d.term
	} else if d.hadOrigin {
		return
	}
	h.successor = actual
	s.remember(h)
}
