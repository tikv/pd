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
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const recoveryWindow = 10 * time.Minute

// A departure is inserted only on the successful heartbeat for a planned
// evacuation. Peer ID and term fence replacements, splits and intervening elections.
type departure struct {
	home, region, peer, version, term, successor uint64
	rate                                         float64
	report                                       uint64
}

func (d departure) matches(r *core.RegionInfo, store *core.StoreInfo) bool {
	if store == nil || r == nil || d.region != r.GetID() || r.GetTerm() <= 1 || r.GetTerm()-1 != d.term ||
		r.GetRegionEpoch().GetVersion() != d.version || r.GetLeader().GetStoreId() != d.successor {
		return false
	}
	start := store.GetMeta().GetStartTimestamp()
	if start <= 0 || d.report >= uint64(start) || int64(d.report) < start-int64(recoveryWindow/time.Second) {
		return false
	}
	peer := r.GetStoreVoter(store.GetID())
	return peer != nil && peer.GetId() == d.peer && core.IsVoterOrIncomingVoter(peer) && !peer.GetIsWitness()
}

type sourceQueue struct {
	regions []uint64
	next    int
}
type evacuation struct {
	start   int64
	last    time.Time
	closed  bool
	regions map[uint64]departure
	queues  map[uint64]*sourceQueue
	delta   map[uint64]*destinationDelta
}

// Pending changes in opposite directions cannot cancel each other until they
// succeed. Either operator might fail or finish at a different target.
type destinationDelta struct{ completed, adding, removing int }

func (h *evacuation) receiver(id uint64) *destinationDelta {
	d := h.delta[id]
	if d == nil {
		d = &destinationDelta{}
		h.delta[id] = d
	}
	return d
}

type recoveryChoice struct {
	home, original uint64
	targets        []uint64
	batch          int
	fallback       *Operator
}

type recoveryTransfer struct {
	from, to uint64
	window   uint64 // recovering Store whose preference selected this transfer
	sent     bool
	start    int64
	original uint64 // nonzero only for a voluntary destination change
	move     departure
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
	pending   map[*Operator]*recoveryTransfer
	coldest   *btree.BTreeG[departure]
	reads     map[uint64]*readAdjustment
	lastPrune time.Time
}

func (s *leaderRecovery) active(store *core.StoreInfo) *evacuation {
	if store == nil {
		return nil
	}
	h := s.homes[store.GetID()]
	if h == nil || h.closed || !store.RestartedFrom(h.start) || time.Since(h.last) > recoveryWindow {
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

func (s *leaderRecovery) forgetStore(id uint64) {
	if h := s.homes[id]; h != nil {
		for k := range h.regions {
			s.forget(h, k)
		}
		delete(s.homes, id)
	}
}

func (s *leaderRecovery) prune() {
	if time.Since(s.lastPrune) < time.Minute {
		return
	}
	for id, h := range s.homes {
		if time.Since(h.last) > recoveryWindow {
			s.forgetStore(id)
		}
	}
	for id, a := range s.reads {
		if a.operators == 0 && time.Since(a.last) > recoveryWindow {
			delete(s.reads, id)
		}
	}
	s.lastPrune = time.Now()
}

func (s *leaderRecovery) draining(store *core.StoreInfo) *evacuation {
	id, start := store.GetID(), store.GetMeta().GetStartTimestamp()
	h := s.homes[id]
	if h == nil || h.start != start || time.Since(h.last) > recoveryWindow {
		s.forgetStore(id)
		if s.homes == nil {
			s.homes = make(map[uint64]*evacuation)
		}
		h = &evacuation{start: start, regions: make(map[uint64]departure), delta: make(map[uint64]*destinationDelta)}
		s.homes[id] = h
	}
	h.last = time.Now()
	return h
}

// Bound all concurrent evacuations together. On overflow, retain the
// busiest departing Regions across the whole controller.
func (s *leaderRecovery) remember(h *evacuation, m departure) {
	const maxRegions = 8192
	if h.queues != nil {
		return
	}
	if s.coldest == nil {
		s.coldest = btree.NewG(16, func(a, b departure) bool {
			return cmp.Or(cmp.Compare(a.rate, b.rate), cmp.Compare(a.home, b.home), cmp.Compare(a.region, b.region)) < 0
		})
	}
	s.forget(h, m.region)
	h.regions[m.region] = m
	s.coldest.ReplaceOrInsert(m)
	if s.coldest.Len() > maxRegions {
		old, _ := s.coldest.DeleteMin()
		delete(s.homes[old.home].regions, old.region)
	}
}

// FormerLeaders tries up to ten planned departures and offers the first one with headroom.
// Candidates are ranked once per recovery by reusable fraction, then volume.
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
		id := q.regions[q.next]
		q.next = (q.next + 1) % len(q.regions)
		r := oc.cluster.GetRegion(id)
		if r != nil && oc.GetOperator(id) == nil && len(r.GetPendingPeers()) == 0 && len(r.GetDownPeers()) == 0 && s.fitsReturn(r, source, target) {
			return []*core.RegionInfo{r}, true
		}
	}
	return nil, true
}

func (h *evacuation) rank(cluster *core.BasicCluster, target *core.StoreInfo) {
	type candidate struct {
		id               uint64
		fraction, volume float64
	}
	groups := make(map[uint64][]candidate)
	for _, d := range h.regions {
		r := cluster.GetRegion(d.region)
		if !d.matches(r, target) {
			continue
		}
		rate, ok := r.ReadKeysPerSecond()
		if !ok || rate <= 0 {
			continue
		}
		volume := min(d.rate, rate)
		groups[d.successor] = append(groups[d.successor], candidate{d.region, volume / rate, volume})
	}
	h.queues = make(map[uint64]*sourceQueue, len(groups))
	for sid, group := range groups {
		slices.SortFunc(group, func(a, b candidate) int {
			return cmp.Or(cmp.Compare(b.fraction, a.fraction), cmp.Compare(b.volume, a.volume), cmp.Compare(a.id, b.id))
		})
		q := &sourceQueue{}
		for _, c := range group {
			q.regions = append(q.regions, c.id)
		}
		h.queues[sid] = q
	}
}

func (s *leaderRecovery) load(store *core.StoreInfo) (float64, bool) {
	if store == nil || store.GetLeaderWeight() <= 0 {
		return 0, false
	}
	stats := store.GetStoreStats()
	i := stats.GetInterval()
	end, start := i.GetEndTimestamp(), i.GetStartTimestamp()
	if end <= start || end-start > 300 || time.Since(store.GetLastHeartbeatTS()) > time.Minute {
		return 0, false
	}
	load := float64(stats.GetKeysRead()) / float64(end-start)
	if change := s.reads[store.GetID()]; change != nil {
		load += change.active
		for stamp, rate := range change.completed {
			if stamp < start {
				delete(change.completed, stamp)
			} else {
				load += rate
			}
		}
	}
	return max(0, load), true
}

func (s *leaderRecovery) adjustPending(id uint64, rate float64, count int) {
	if rate == 0 {
		return
	}
	if s.reads == nil {
		s.reads = make(map[uint64]*readAdjustment)
	}
	a := s.reads[id]
	if a == nil {
		a = &readAdjustment{completed: make(map[uint64]float64)}
		s.reads[id] = a
	}
	a.operators += count
	a.active += rate
	if a.operators == 0 {
		a.active = 0
	}
	a.last = time.Now()
}

func (s *leaderRecovery) settle(store *core.StoreInfo, rate float64) {
	if rate == 0 || store == nil {
		return
	}
	id := store.GetID()
	a := s.reads[id]
	if a == nil { // A multi-target evacuation finished on a different Store.
		a = &readAdjustment{completed: make(map[uint64]float64)}
		s.reads[id] = a
	}
	interval := store.GetStoreStats().GetInterval()
	for stamp := range a.completed {
		if stamp < interval.GetStartTimestamp() {
			delete(a.completed, stamp)
		}
	}
	a.completed[interval.GetEndTimestamp()] += rate
	a.last = time.Now()
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
func (op *Operator) MarkLeaderReturn(home uint64, fallback *Operator) {
	op.recovery = &recoveryChoice{home: home, fallback: fallback}
	op.SetAdditionalInfo("cache-return", "balance")
}

func (s *leaderRecovery) evictionTarget(c *core.BasicCluster, r *core.RegionInfo, original uint64, targets []uint64, batch int) (uint64, bool) {
	if len(targets) < 2 {
		return original, false
	}
	var home uint64
	for _, id := range targets {
		store := c.GetStore(id)
		if h := s.homes[id]; h != nil && h.closed && store != nil && store.RestartedFrom(h.start) {
			return original, false
		}
		if h := s.active(store); h != nil {
			if home != 0 {
				return original, false
			} // overlapping recovery windows
			home = id
		}
	}
	if home == 0 {
		return original, false
	}
	src := c.GetStore(r.GetLeader().GetStoreId())
	rate, valid := r.ReadKeysPerSecond()
	total, ok := s.load(src)
	if !ok || !valid {
		return original, false
	}
	type receiver struct {
		id           uint64
		load, weight float64
	}
	var weights float64
	destinations := make([]receiver, 0, len(targets))
	for _, id := range targets {
		store := c.GetStore(id)
		load, ok := s.load(store)
		if !ok {
			return original, false
		}
		destinations = append(destinations, receiver{id, load, store.GetLeaderWeight()})
		total += load
		weights += store.GetLeaderWeight()
	}
	former := s.homes[home].regions[r.GetID()]
	wantsHome := former.matches(r, c.GetStore(home))
	perWeight := total / weights
	var first, second *receiver
	for i := range destinations {
		t := &destinations[i]
		if (t.load+rate)/t.weight > perWeight {
			continue
		}
		if (t.id == home) == wantsHome {
			if t.id == original {
				return original, false
			}
			if first == nil || t.load/t.weight < first.load/first.weight {
				first = t
			}
		} else if t.id == original || second == nil || second.id != original && t.load/t.weight < second.load/second.weight {
			second = t
		}
	}
	if first == nil {
		if second == nil {
			return original, false
		}
		return second.id, false
	}
	if second == nil || second.id != original {
		return first.id, false
	}
	if h := s.homes[src.GetID()]; h != nil && h.start == src.GetMeta().GetStartTimestamp() {
		to, from := h.receiver(first.id), h.receiver(original)
		if to.completed+to.adding >= batch || from.completed-from.removing <= -batch {
			return original, false
		}
	}
	return first.id, true
}

// EvictionTarget prefers a recovering former leader among legal targets.
// The batch bounds each receiver's net voluntary deviations from the default.
func (oc *Controller) EvictionTarget(r *core.RegionInfo, original uint64, targets []uint64, batch int) uint64 {
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	id, _ := s.evictionTarget(oc.cluster, r, original, targets, batch)
	return id
}

// MarkEvictionChoice preserves the original urgent operator if read headroom
// or the batch allowance changes while the preferred operator waits.
func (op *Operator) MarkEvictionChoice(original uint64, targets []uint64, batch int, fallback *Operator) {
	op.recovery = &recoveryChoice{original: original, targets: slices.Clone(targets), batch: batch, fallback: fallback}
	op.SetAdditionalInfo("cache-return", "evict")
}

func (oc *Controller) admitRecovery(op *Operator) bool {
	evict := op.Desc() == types.EvictLeaderScheduler.String()
	hot := op.Kind()&OpHotRegion != 0
	if !evict && !hot && op.recovery == nil && op.Desc() != types.BalanceLeaderScheduler.String() {
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
		s.prune()
	}
	if !evict && len(s.homes) == 0 {
		return op.recovery == nil
	}
	if hot {
		for _, step := range op.steps {
			var id uint64
			switch m := step.(type) {
			case TransferLeader:
				id = m.FromStore
			case RemovePeer:
				id = m.FromStore
			}
			if h := s.active(oc.cluster.GetStore(id)); h != nil {
				h.closed = true
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
	m := &recoveryTransfer{from: step.FromStore, to: step.ToStore, start: from.GetMeta().GetStartTimestamp()}
	rate, valid := r.ReadKeysPerSecond()
	m.move = departure{home: m.from, region: r.GetID(), peer: r.GetLeader().GetId(), version: r.GetRegionEpoch().GetVersion(), term: r.GetTerm(), report: r.GetInterval().GetEndTimestamp(), rate: rate}
	if evict {
		h := s.draining(from)
		if choice := op.recovery; choice != nil {
			id, voluntary := s.evictionTarget(oc.cluster, r, choice.original, choice.targets, choice.batch)
			if id != step.ToStore {
				return false
			}
			for _, id := range choice.targets {
				if s.active(oc.cluster.GetStore(id)) != nil {
					m.window = id
					break
				}
			}
			if voluntary {
				m.original = choice.original
				h.receiver(m.to).adding++
				h.receiver(m.original).removing++
			}
		}
	} else if op.recovery != nil {
		if op.recovery.home != to.GetID() || !s.fitsReturn(r, from, to) {
			return false
		}
		m.window = to.GetID()
		s.forget(s.homes[to.GetID()], r.GetID())
	} else if s.active(from) == nil && s.active(to) == nil {
		return true
	}
	if s.pending == nil {
		s.pending = make(map[*Operator]*recoveryTransfer)
	}
	// Even an unknown rate can carry a voluntary count reservation.
	if valid || m.original != 0 {
		s.pending[op] = m
		s.adjustPending(m.from, -m.move.rate, 1)
		s.adjustPending(m.to, m.move.rate, 1)
	}
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
	m := s.pending[op]
	if m == nil {
		return false
	}
	if !m.sent && s.active(oc.cluster.GetStore(m.window)) == nil {
		return false
	}
	m.sent = true
	return true
}

func (oc *Controller) finishRecovery(op *Operator, r *core.RegionInfo) {
	if op.recovery == nil && op.Kind()&OpHotRegion == 0 && op.Desc() != types.EvictLeaderScheduler.String() && op.Desc() != types.BalanceLeaderScheduler.String() {
		return
	}
	s := &oc.recovery
	s.Lock()
	defer s.Unlock()
	m := s.pending[op]
	if m == nil {
		return
	}
	h := s.homes[m.from]
	success := r != nil && op.Status() == SUCCESS && op.steps[0].IsFinish(r)
	if h != nil && h.start == m.start {
		if m.original != 0 {
			to, original := h.receiver(m.to), h.receiver(m.original)
			to.adding--
			original.removing--
			if success && r.GetLeader().GetStoreId() == m.to {
				to.completed++
				original.completed--
			}
		}
		if success && op.Desc() == types.EvictLeaderScheduler.String() && r.GetTerm() > 1 && r.GetTerm()-1 == m.move.term &&
			r.GetRegionEpoch().GetVersion() == m.move.version && m.move.rate > 0 && int64(m.move.report) >= m.start {
			m.move.successor = r.GetLeader().GetStoreId()
			s.remember(h, m.move)
		}
	}
	delete(s.pending, op)
	s.adjustPending(m.from, m.move.rate, -1)
	s.adjustPending(m.to, -m.move.rate, -1)
	if success {
		s.settle(oc.cluster.GetStore(m.from), -m.move.rate)
		s.settle(oc.cluster.GetStore(r.GetLeader().GetStoreId()), m.move.rate)
	}
}
