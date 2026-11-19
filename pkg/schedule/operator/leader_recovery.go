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

type storePair struct{ from, to uint64 }

// Order each Store pair by its departing read rate; the cursor makes zero-read
// candidates eligible on subsequent passes without building a per-restart queue.
func byDirection(a, b departure) bool {
	return cmp.Or(cmp.Compare(a.home, b.home), cmp.Compare(a.successor, b.successor), cmp.Compare(b.rate, a.rate), cmp.Compare(a.id, b.id)) < 0
}

type leaderRecovery struct {
	syncutil.Mutex
	active atomic.Bool
	RecoveryRound
	roundRecords
}

type roundRecords struct {
	pending  map[*Operator]float64
	draining map[uint64]int64
	yielded  map[uint64]bool
	history  map[uint64]departure
	coldest  *btree.BTreeG[departure]
	index    *btree.BTreeG[departure]
	cursor   map[storePair]departure
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
			draining: make(map[uint64]int64), yielded: make(map[uint64]bool),
			history: make(map[uint64]departure), cursor: make(map[storePair]departure),
			index:   btree.NewG(16, byDirection),
			coldest: btree.NewG(16, func(a, b departure) bool { return cmp.Or(cmp.Compare(a.rate, b.rate), cmp.Compare(a.id, b.id)) < 0 }),
			recent:  make(map[uint64]map[uint64]readBounds), pending: make(map[*Operator]float64),
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
		delete(s.cursor, storePair{d.successor, d.home})
	}
}

// Keep the first retained origin. A Store which temporarily hosted a Region
// during this roll must not learn it back from the return trip.
func (s *leaderRecovery) remember(d departure) {
	if _, known := s.history[d.id]; known {
		return
	}
	const capacity = 8192
	if len(s.history) == capacity {
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

// ReturnCandidates revisits a bounded slice of the recorded handovers for this
// direction. Missing and idle samples stay in the rotation for later passes.
func (oc *Controller) ReturnCandidates(source, target *core.StoreInfo) []*core.RegionInfo {
	s := &oc.recovery
	if !s.active.Load() {
		return nil
	}
	s.Lock()
	defer s.Unlock()
	if !target.RestartedFrom(s.draining[target.GetID()]) {
		return nil
	}
	pair := storePair{source.GetID(), target.GetID()}
	cursor, continuing := s.cursor[pair]
	if !continuing {
		cursor = departure{home: pair.to, successor: pair.from, rate: math.Inf(1)}
	}
	var candidates []*core.RegionInfo
	visits := 0
	s.index.AscendGreaterOrEqual(cursor, func(d departure) bool {
		if d.home != pair.to || d.successor != pair.from {
			return false
		}
		if continuing && d == cursor {
			return true
		}
		r := oc.cluster.GetRegion(d.id)
		if d.rate > 0 && d.matches(r, target) {
			if rate, ok := r.ReadKeysPerSecond(); ok && rate > 0 {
				candidates = append(candidates, r)
			}
		}
		visits++
		if visits == 10 {
			s.cursor[pair] = d
			return false
		}
		return true
	})
	if visits < 10 {
		delete(s.cursor, pair)
	}
	slices.SortStableFunc(candidates, func(a, b *core.RegionInfo) int {
		ar, _ := a.ReadKeysPerSecond()
		br, _ := b.ReadKeysPerSecond()
		return cmp.Compare(min(s.history[b.GetID()].rate, br), min(s.history[a.GetID()].rate, ar))
	})
	return candidates
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
	if stats == nil || st.GetLeaderWeight() <= 0 {
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
	add := func(op *Operator, rate float64) {
		if op.recovery == p {
			return
		}
		op.leaderReadChanges(max(rate, readCost(oc.cluster.GetRegion(op.RegionID()))), 0, func(id uint64, delta float64) {
			if id == st.GetID() {
				b.add(delta)
			}
		})
	}
	for op, rate := range oc.recovery.pending {
		add(op, rate)
	}
	for _, op := range p.extra {
		if _, tracked := oc.recovery.pending[op]; !tracked && op.Status() == CREATED {
			add(op, 0)
		}
	}
	w := st.GetLeaderWeight()
	return readBounds{max(0, b.low) / w, b.high / w}, !math.IsInf(b.low, 0) && !math.IsInf(b.high, 0)
}

func eligibleReturn(c sche.SchedulerCluster, r *core.RegionInfo, to uint64) bool {
	if r == nil || len(r.GetDownPeers()) != 0 || len(r.GetPendingPeers()) != 0 {
		return false
	}
	if l := c.GetRegionLabeler(); l != nil && l.ScheduleDisabled(r) {
		return false
	}
	peer := r.GetStoreVoter(to)
	return peer != nil && !peer.GetIsWitness() && IsAllowedLeaderTarget(c, r, peer) &&
		c.AllowsLeaderReturn(r, c.GetSchedulerConfig().GetHotRegionCacheHitsThreshold())
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
	bRate, aRate := readCost(r), readCost(a)
	if bRate <= 0 || math.IsInf(bRate, 0) || math.IsInf(aRate, 0) {
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
		for _, id := range []uint64{p.home.id, p.ordinary} {
			if op := oc.GetOperator(id); op != nil && op.Kind()&OpHotRegion != 0 {
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
	h, ok := oc.loadBounds(home, p)
	if !ok {
		return false
	}
	o, ok := oc.loadBounds(other, p)
	if !ok {
		return false
	}
	delta := bRate - aRate
	if s.yielded[home.GetID()] && delta > 0 || s.yielded[other.GetID()] && delta < 0 {
		return false
	}
	scale := 1/home.GetLeaderWeight() + 1/other.GetLeaderWeight()
	ordinary, candidate := aRate*scale, bRate*scale
	lo, hi := o.low-h.high, o.high-h.low
	if p.partner != nil {
		// Either half may already appear in the Store samples when its sibling
		// reaches first dispatch. Bound the original load in both cases.
		if p.sent[0] {
			hi += bRate / home.GetLeaderWeight()
		}
		if p.sent[1] {
			lo -= aRate / other.GetLeaderWeight()
		}
		ordinary = aRate/home.GetLeaderWeight() - bRate/other.GetLeaderWeight()
		candidate = bRate/home.GetLeaderWeight() - aRate/other.GetLeaderWeight()
	}
	for _, gap := range []float64{lo, hi} {
		after := math.Abs(gap - candidate)
		if after > math.Abs(gap-ordinary) || p.partner == nil && after > math.Abs(gap) {
			return false
		}
	}
	return true
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
		rate, valid := r.ReadKeysPerSecond()
		if !valid || r.GetTerm() == 0 || int64(r.GetInterval().GetEndTimestamp()) < s.draining[t.FromStore] {
			continue
		}
		op.drain = &departure{round: s.ID, id: r.GetID(), home: t.FromStore, peer: r.GetLeader().GetId(), process: s.draining[t.FromStore], term: r.GetTerm(), version: r.GetRegionEpoch().GetVersion(), rate: rate}
		if d, ok := s.history[r.GetID()]; ok && d.matches(r, oc.cluster.GetStore(d.home)) {
			former[r.GetID()] = d
		}
	}
	s.Unlock()
	// Start with the largest reusable working set. Bound expensive load and
	// placement checks by the actual batch length, including multi-store drains.
	var homes []int
	for i, op := range ops {
		if d, ok := former[op.RegionID()]; ok && min(d.rate, op.drain.rate) > 0 {
			homes = append(homes, i)
		}
	}
	volume := func(i int) float64 { return min(ops[i].drain.rate, former[ops[i].RegionID()].rate) }
	slices.SortStableFunc(homes, func(i, j int) int { return cmp.Compare(volume(j), volume(i)) })
	checks := len(ops)
	for _, i := range homes {
		op := ops[i]
		t, _ := primary(op)
		d := former[op.RegionID()]
		if t.ToStore == d.home || !slices.Contains(t.ToStores, d.home) || !eligibleReturn(c, oc.cluster.GetRegion(d.id), d.home) {
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
			if checks == 0 {
				return
			}
			checks--
			if !eligibleReturn(c, oc.cluster.GetRegion(other.RegionID()), t.ToStore) {
				continue
			}
			p := &recoveryChoice{round: round, cluster: c, home: d, ordinary: other.RegionID(), other: t.ToStore, partner: other.drain}
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
	op.SetPriorityLevel(old.GetPriorityLevel())
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
	for _, step := range op.steps {
		switch t := step.(type) {
		case TransferLeader:
			if _, ok := s.pending[op]; !ok {
				s.pending[op] = readCost(oc.cluster.GetRegion(op.RegionID()))
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
		s.forget(p.home.id)
		// The first leg claims the origin for both commands, whichever sends
		// first. Its spent ID stays in the same bounded history until finish.
		s.history[p.home.id] = departure{}
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
	op.leaderReadChanges(max(admitted, readCost(r)), actual, func(id uint64, delta float64) {
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
	if actual == 0 || d == nil || op.recovery != nil || s.ID != d.round || actual == d.home ||
		r.GetTerm() <= 1 || r.GetTerm()-1 != d.term || r.GetRegionEpoch().GetVersion() != d.version {
		return
	}
	// A late completion for a replaced process cannot establish a new origin.
	from := oc.cluster.GetStore(d.home)
	if from != nil && from.GetMeta().GetStartTimestamp() == d.process {
		h := *d
		h.successor = actual
		s.remember(h)
	}
}
