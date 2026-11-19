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
	"slices"
	"time"

	"github.com/google/btree"

	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// A departure is inserted only on the successful heartbeat for a planned
// evacuation. Peer ID and term fence replacements, splits and intervening elections.
type departure struct {
	core.FormerLeader
	successor uint64
	rate      float64
}

func (d departure) matches(r *core.RegionInfo, store *core.StoreInfo) bool {
	return r != nil && r.GetLeader().GetStoreId() == d.successor && d.CanReturn(r, store)
}

type returnCandidate struct {
	id     uint64
	volume float64
}

type sourceQueue struct {
	regions []returnCandidate
	next    int
}
type evacuation struct {
	id         uint64
	inflight   int // verified returns to this Store awaiting completion
	start      int64
	last       time.Time
	yielded    bool   // the hot scheduler moved load away
	lastReturn uint64 // Store heartbeat end observed at the last confirmed return
	regions    map[uint64]departure
	queues     map[uint64]*sourceQueue
	// Bounds on net deviations from the default if pending changes fail or succeed.
	minDelta, maxDelta map[uint64]int
}

type recoveryChoice struct {
	process  int64 // restarted balance target at selection
	window   *evacuation
	original uint64
	targets  []uint64
	batch    int
	fallback *Operator
}

type recoveryTransfer struct {
	from, to   uint64
	window     *evacuation // the recovery which authorized this command
	drain      *evacuation
	targetHome *evacuation // receiver generation at admission
	returning  bool        // a verified former leader on the primary receiver
	sent       bool
	original   uint64 // nonzero only for a voluntary destination change
	move       departure
}

// Keep finished transfers through the first mixed heartbeat interval. Both
// interval boundaries below come from the same Store, independent of PD's clock.
type readAdjustment struct {
	active    float64
	operators int
	completed map[uint64]float64
	last      time.Time
}

type leaderRecovery struct {
	syncutil.Mutex
	homes     map[uint64]*evacuation
	coldest   *btree.BTreeG[departure]
	reads     map[uint64]*readAdjustment
	lastPrune time.Time
}

func (s *leaderRecovery) active(store *core.StoreInfo) *evacuation {
	if store == nil {
		return nil
	}
	h := s.homes[store.GetID()]
	if h == nil || h.regions == nil || !store.RestartedFrom(h.start) || time.Since(h.last) > core.LeaderReturnWindow {
		return nil
	}
	// The first interval straddling a transfer may already include part of it.
	// A later full interval with no more returns ends the preference permanently.
	if h.inflight == 0 && h.lastReturn != 0 && store.ReadStats().GetInterval().GetStartTimestamp() > h.lastReturn {
		s.release(h)
		return nil
	}
	return h
}

func (s *leaderRecovery) forget(h *evacuation, id uint64) {
	if d, ok := h.regions[id]; ok {
		s.coldest.Delete(d)
		delete(h.regions, id)
	}
}

func (s *leaderRecovery) release(h *evacuation) {
	for _, d := range h.regions {
		s.coldest.Delete(d)
	}
	h.regions, h.queues = nil, nil
}

func (s *leaderRecovery) forgetStore(id uint64) {
	if h := s.homes[id]; h != nil {
		s.release(h)
		delete(s.homes, id)
	}
}

func (s *leaderRecovery) prune(c *core.BasicCluster) {
	if time.Since(s.lastPrune) < time.Minute {
		return
	}
	for id, h := range s.homes {
		if time.Since(h.last) > core.LeaderReturnWindow {
			if st := c.GetStore(id); st != nil && st.RestartedFrom(h.start) {
				continue
			}
			s.forgetStore(id)
		}
	}
	for id, a := range s.reads {
		if a.operators == 0 && time.Since(a.last) > core.LeaderReturnWindow {
			delete(s.reads, id)
		}
	}
	s.lastPrune = time.Now()
}

func (s *leaderRecovery) draining(store *core.StoreInfo) *evacuation {
	id, start := store.GetID(), store.GetMeta().GetStartTimestamp()
	h := s.homes[id]
	if h == nil || h.start != start || time.Since(h.last) > core.LeaderReturnWindow {
		s.forgetStore(id)
		if s.homes == nil {
			s.homes = make(map[uint64]*evacuation)
		}
		h = &evacuation{id: id, start: start, regions: make(map[uint64]departure), minDelta: make(map[uint64]int), maxDelta: make(map[uint64]int)}
		s.homes[id] = h
	}
	h.last = time.Now()
	return h
}

// Bound all concurrent evacuations together. On overflow, retain the
// busiest departing Regions across the whole controller.
func (s *leaderRecovery) remember(h *evacuation, m departure) {
	const maxRegions = 8192
	if h.regions == nil || h.queues != nil {
		return
	}
	if s.coldest == nil {
		s.coldest = btree.NewG(16, func(a, b departure) bool {
			return cmp.Or(cmp.Compare(a.rate, b.rate), cmp.Compare(a.StoreID, b.StoreID), cmp.Compare(a.RegionID, b.RegionID)) < 0
		})
	}
	s.forget(h, m.RegionID)
	h.regions[m.RegionID] = m
	s.coldest.ReplaceOrInsert(m)
	if s.coldest.Len() > maxRegions {
		old, _ := s.coldest.DeleteMin()
		delete(s.homes[old.StoreID].regions, old.RegionID)
	}
}

// FormerLeaders tries up to ten planned departures and offers the first one with headroom.
// Candidates are ranked once per recovery by reusable read volume.
func (oc *Controller) FormerLeaders(source, target *core.StoreInfo, ordinary *core.RegionInfo) ([]*core.RegionInfo, bool) {
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	h := s.active(target)
	if h == nil {
		old := s.homes[target.GetID()]
		return nil, old != nil && target.RestartedFrom(old.start)
	}
	if h.queues == nil {
		h.rank(oc.cluster, target)
	}
	if rate, ok := ordinary.ReadKeysPerSecond(); ok && rate == 0 && target.GetLeaderCount() > 0 {
		a, _ := s.load(source)
		b, _ := s.load(target)
		if b*float64(source.GetLeaderCount()) > a*float64(target.GetLeaderCount()) {
			return nil, true
		}
	}
	q := h.queues[source.GetID()]
	if q == nil {
		return nil, true
	}
	for range min(10, len(q.regions)) {
		id := q.regions[q.next].id
		q.next = (q.next + 1) % len(q.regions)
		r := oc.cluster.GetRegion(id)
		if r != nil && oc.GetOperator(id) == nil && len(r.GetPendingPeers()) == 0 && len(r.GetDownPeers()) == 0 && s.fitsReturn(r, source, target) {
			return []*core.RegionInfo{r}, true
		}
	}
	return nil, true
}

func (h *evacuation) rank(cluster *core.BasicCluster, target *core.StoreInfo) {
	h.queues = make(map[uint64]*sourceQueue)
	for _, d := range h.regions {
		r := cluster.GetRegion(d.RegionID)
		if !d.matches(r, target) {
			continue
		}
		rate, ok := r.ReadKeysPerSecond()
		if !ok || rate <= 0 {
			continue
		}
		q := h.queues[d.successor]
		if q == nil {
			q = &sourceQueue{}
			h.queues[d.successor] = q
		}
		q.regions = append(q.regions, returnCandidate{d.RegionID, min(d.rate, rate)})
	}
	for _, q := range h.queues {
		slices.SortFunc(q.regions, func(a, b returnCandidate) int {
			return cmp.Or(cmp.Compare(b.volume, a.volume), cmp.Compare(a.id, b.id))
		})
	}
}

func (s *leaderRecovery) load(store *core.StoreInfo) (float64, bool) {
	stats := store.ReadStats()
	if stats == nil || store.GetLeaderWeight() <= 0 {
		return 0, false
	}
	i := stats.GetInterval()
	load := float64(stats.GetKeysRead()) / float64(i.GetEndTimestamp()-i.GetStartTimestamp())
	if change := s.reads[store.GetID()]; change != nil {
		load += change.active
		for stamp, rate := range change.completed {
			if stamp < i.GetStartTimestamp() {
				delete(change.completed, stamp)
			} else {
				load += rate
			}
		}
	}
	return max(0, load), true
}

func (s *leaderRecovery) adjustment(id uint64) *readAdjustment {
	if s.reads == nil {
		s.reads = make(map[uint64]*readAdjustment)
	}
	a := s.reads[id]
	if a == nil {
		a = &readAdjustment{completed: make(map[uint64]float64)}
		s.reads[id] = a
	}
	a.last = time.Now()
	return a
}

func (s *leaderRecovery) adjustPending(id uint64, rate float64, count int) {
	if rate == 0 {
		return
	}
	a := s.adjustment(id)
	a.operators += count
	a.active += rate
	if a.operators == 0 {
		a.active = 0
	}
}

func (s *leaderRecovery) settle(store *core.StoreInfo, rate float64) {
	if rate == 0 || store == nil {
		return
	}
	a := s.adjustment(store.GetID())
	interval := store.GetStoreStats().GetInterval()
	for stamp := range a.completed {
		if stamp < interval.GetStartTimestamp() {
			delete(a.completed, stamp)
		}
	}
	a.completed[interval.GetEndTimestamp()] += rate
}

func (s *leaderRecovery) fitsReturn(region *core.RegionInfo, from, to *core.StoreInfo) bool {
	h := s.active(to)
	if h == nil || from == nil {
		return false
	}
	d, ok := h.regions[region.GetID()]
	if !ok || !d.matches(region, to) {
		return false
	}
	rate, valid := region.ReadKeysPerSecond()
	a, aOK := s.load(from)
	b, bOK := s.load(to)
	return valid && rate > 0 && aOK && bOK && (b+rate)/to.GetLeaderWeight() <= (a-rate)/from.GetLeaderWeight()
}

// MarkLeaderReturn requests a final check when the operator is admitted.
func (op *Operator) MarkLeaderReturn(target *core.StoreInfo, fallback *Operator) {
	op.recovery = &recoveryChoice{process: target.GetMeta().GetStartTimestamp(), fallback: fallback}
	op.SetAdditionalInfo("cache-return", "balance")
}

func (s *leaderRecovery) evictionTarget(c *core.BasicCluster, r *core.RegionInfo, original uint64, targets []uint64, batch int) (uint64, *evacuation, bool) {
	src := c.GetStore(r.GetLeader().GetStoreId())
	rate, valid := r.ReadKeysPerSecond()
	total, ok := s.load(src)
	if len(targets) < 2 || !ok || !valid {
		return original, nil, false
	}
	type receiver struct {
		id            uint64
		before, after float64 // read load per unit of Leader weight
	}
	var home *evacuation
	var weights float64
	receivers := make([]receiver, 0, len(targets))
	for _, id := range targets {
		store := c.GetStore(id)
		load, ok := s.load(store)
		if !ok {
			return original, nil, false
		}
		if h := s.homes[id]; h != nil && h.yielded && store.RestartedFrom(h.start) {
			return original, nil, false
		}
		if h := s.active(store); h != nil && h.lastReturn != 0 && len(h.regions) != 0 {
			if home != nil { // Multiple recovering peers have conflicting preferences.
				return original, nil, false
			}
			home = h
		}
		weight := store.GetLeaderWeight()
		receivers = append(receivers, receiver{id, load / weight, (load + rate) / weight})
		total += load
		weights += weight
	}
	if home == nil {
		return original, nil, false
	}
	wantsHome := home.regions[r.GetID()].matches(r, c.GetStore(home.id))
	// Prefer cache locality, then the default target, then the lightest peer.
	// A change is voluntary only when the original target also fits.
	var best *receiver
	bestRank := -1
	originalFits := false
	for i := range receivers {
		t := &receivers[i]
		if t.after > total/weights {
			continue
		}
		rank := 0
		if (t.id == home.id) == wantsHome {
			rank = 2
		}
		if t.id == original {
			originalFits = true
			rank++
		}
		if rank > bestRank || rank == bestRank && t.before < best.before {
			best, bestRank = t, rank
		}
	}
	if best == nil || best.id == original {
		return original, nil, false
	}
	if h := s.homes[src.GetID()]; originalFits && h != nil && h.start == src.GetMeta().GetStartTimestamp() {
		if h.maxDelta[best.id] >= batch || h.minDelta[original] <= -batch {
			return original, nil, false
		}
	}
	return best.id, home, originalFits
}

// PreferEvictionTarget preserves the urgent fallback and considers only peers that
// would be legal primary targets. The builder's multi-target list can be broader.
func (oc *Controller) PreferEvictionTarget(cluster sche.SharedCluster, region *core.RegionInfo, original uint64, targets []uint64, batch int, fallback *Operator) *Operator {
	if len(region.GetPendingPeers()) != 0 || len(region.GetDownPeers()) != 0 {
		return fallback
	}
	eligible := make([]uint64, 0, len(targets))
	for _, id := range targets {
		if peer := region.GetStoreVoter(id); peer != nil && IsAllowedLeaderTarget(cluster, region, peer) {
			eligible = append(eligible, id)
		}
	}
	if !slices.Contains(eligible, original) {
		return fallback
	}
	s := &oc.recovery
	s.Lock()
	id, home, _ := s.evictionTarget(oc.cluster, region, original, eligible, batch)
	s.Unlock()
	if id == original {
		return fallback
	}
	op, err := CreateTransferLeaderOperator(fallback.Desc(), cluster, region, id, eligible, OpLeader)
	if err != nil {
		return fallback
	}
	op.SetPriorityLevel(fallback.GetPriorityLevel())
	op.Counters = fallback.Counters
	op.recovery = &recoveryChoice{window: home, original: original, targets: eligible, batch: batch, fallback: fallback}
	op.SetAdditionalInfo("cache-return", "evict")
	return op
}

func (op *Operator) tracksLeaderRecovery() bool {
	return op.recovery != nil || op.Kind()&OpHotRegion != 0 || op.Desc() == types.EvictLeaderScheduler.String() || op.Desc() == types.BalanceLeaderScheduler.String()
}

func (oc *Controller) admitRecovery(op *Operator) bool {
	if !op.tracksLeaderRecovery() {
		return true
	}
	if oc.cluster == nil {
		return op.recovery == nil
	}
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	if op.IsEnd() {
		return false
	}
	if len(s.homes) > 0 || len(s.reads) > 0 {
		s.prune(oc.cluster)
	}
	evict := op.Desc() == types.EvictLeaderScheduler.String()
	if !evict && len(s.homes) == 0 {
		return op.recovery == nil
	}
	if op.Kind()&OpHotRegion != 0 {
		for _, step := range op.steps {
			var id uint64
			switch m := step.(type) {
			case TransferLeader:
				id = m.FromStore
			case RemovePeer:
				id = m.FromStore
			}
			store := oc.cluster.GetStore(id)
			if h := s.homes[id]; h != nil && store != nil && store.RestartedFrom(h.start) {
				s.release(h)
				h.yielded = true
			}
		}
		// A single-step inbound hot transfer also consumes receiver headroom.
	}
	if len(op.steps) != 1 {
		return op.recovery == nil
	}
	step, ok := op.steps[0].(TransferLeader)
	r := oc.cluster.GetRegion(op.RegionID())
	if !ok || r == nil || r.GetLeader().GetStoreId() != step.FromStore {
		return op.recovery == nil
	}
	from, to := oc.cluster.GetStore(step.FromStore), oc.cluster.GetStore(step.ToStore)
	if op.recovery != nil && (len(r.GetPendingPeers()) != 0 || len(r.GetDownPeers()) != 0) {
		return false
	}
	if from == nil || to == nil {
		return op.recovery == nil
	}
	m := &recoveryTransfer{from: step.FromStore, to: step.ToStore}
	m.targetHome = s.active(to)
	m.returning = m.targetHome != nil && m.targetHome.regions[r.GetID()].matches(r, to)
	rate, _ := r.ReadKeysPerSecond()
	m.move = departure{FormerLeader: r.FormerLeader(), rate: rate}
	if evict {
		h := s.draining(from)
		m.drain = h
		if choice := op.recovery; choice != nil {
			id, home, voluntary := s.evictionTarget(oc.cluster, r, choice.original, choice.targets, choice.batch)
			if id != step.ToStore || choice.window != home {
				return false
			}
			m.window = home
			if voluntary {
				m.original = choice.original
				h.maxDelta[m.to]++
				h.minDelta[m.original]--
			}
		}
	} else if op.recovery != nil {
		if op.recovery.process != to.GetMeta().GetStartTimestamp() || !s.fitsReturn(r, from, to) {
			return false
		}
		m.window = m.targetHome
		s.forget(s.homes[to.GetID()], r.GetID())
	} else if s.active(from) == nil && s.active(to) == nil {
		return true
	}
	// Even a transfer without a read measurement can be the first Leader back.
	if m.returning {
		m.targetHome.inflight++
	}
	op.transfer = m
	s.adjustPending(m.from, -m.move.rate, 1)
	s.adjustPending(m.to, m.move.rate, 1)
	return true
}

// Claim the first dispatch under the same lock that closes the window on hot
// feedback. A retransmission of an already issued command may finish normally.
func (oc *Controller) canSendRecovery(op *Operator) bool {
	if op.recovery == nil {
		return true
	}
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	m := op.transfer
	if m == nil {
		return false
	}
	if !m.sent && (m.window == nil || s.active(oc.cluster.GetStore(m.window.id)) != m.window) {
		return false
	}
	m.sent = true
	return true
}

func (oc *Controller) finishRecovery(op *Operator, r *core.RegionInfo) {
	if !op.tracksLeaderRecovery() {
		return
	}
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	m := op.transfer
	if m == nil {
		return
	}
	h := s.homes[m.from]
	success := r != nil && op.Status() == SUCCESS && op.steps[0].IsFinish(r)
	direct := success && r.GetTerm() > 1 && r.GetTerm()-1 == m.move.Term && r.GetRegionEpoch().GetVersion() == m.move.Version
	if h != nil && h == m.drain {
		if m.original != 0 {
			if success && r.GetLeader().GetStoreId() == m.to {
				h.minDelta[m.to]++
				h.maxDelta[m.original]--
			} else {
				h.maxDelta[m.to]--
				h.minDelta[m.original]++
			}
		}
		if direct && m.move.rate > 0 && int64(m.move.ReportEnd) >= h.start {
			m.move.successor = r.GetLeader().GetStoreId()
			s.remember(h, m.move)
		}
	}
	op.transfer = nil
	s.adjustPending(m.from, m.move.rate, -1)
	s.adjustPending(m.to, -m.move.rate, -1)
	if success {
		s.settle(oc.cluster.GetStore(m.from), -m.move.rate)
		target := oc.cluster.GetStore(r.GetLeader().GetStoreId())
		s.settle(target, m.move.rate)
		if h := s.active(target); direct && h != nil && h == m.targetHome && (m.returning || h.lastReturn == 0 && op.Desc() == types.BalanceLeaderScheduler.String()) {
			// Use the receiver's clock. Ignore samples from its old process.
			if end := target.ReadStats().GetInterval().GetEndTimestamp(); int64(end) >= target.GetMeta().GetStartTimestamp() {
				h.lastReturn = max(h.lastReturn, end)
			}
		}
	}
	if m.returning {
		m.targetHome.inflight--
	}
}
