// Copyright 2019 TiKV Project Authors.
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
	"math"
	"slices"
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

const replicaBaseScore = 100

// RegionFit is the result of fitting a region's peers to rule list.
// All peers are divided into corresponding rules according to the matching
// rules, and the remaining Peers are placed in the OrphanPeers list.
type RegionFit struct {
	RuleFits     []*RuleFit     `json:"rule-fits"`
	OrphanPeers  []*metapb.Peer `json:"orphan-peers"`
	regionStores []*core.StoreInfo
	rules        []*Rule
}

// Replace return true if the replacement store is fit all constraints and isolation score is not less than the origin.
func (f *RegionFit) Replace(srcStoreID uint64, dstStore *core.StoreInfo) bool {
	return f.checkReplacement(srcStoreID, dstStore, f.getRuleFitByStoreID(srcStoreID), nil)
}

// NewReplacementChecker reuses source isolation statistics across candidates.
// The fit and its stores must remain immutable for the checker's lifetime.
// Build a new checker after changing the planned layout or metadata snapshot.
func (f *RegionFit) NewReplacementChecker(srcStoreID uint64) func(*core.StoreInfo) bool {
	fit := f.getRuleFitByStoreID(srcStoreID)
	var isolation *isolationReplacement
	if fit != nil {
		if labels := fit.Rule.isolationLabels(); labels != nil {
			stats := newIsolationReplacement(fit, labels, srcStoreID)
			isolation = &stats
		}
	}
	return func(dstStore *core.StoreInfo) bool {
		return f.checkReplacement(srcStoreID, dstStore, fit, isolation)
	}
}

func (f *RegionFit) checkReplacement(srcStoreID uint64, dstStore *core.StoreInfo, fit *RuleFit, isolation *isolationReplacement) bool {
	if dstStore == nil {
		return false
	}
	// check the target store is fit all constraints.
	if fit == nil {
		return false
	}
	// the members of the rule are same, it shouldn't check the score.
	// it is used to transfer role like transfer leader.
	if fit.contain(dstStore.GetID()) {
		return true
	}

	// it should be allowed to exchange role if both the source and target's rule role is same.
	// e.g. transfer leader from store 1 to store 2, and store-1 and store-2 are both voter.
	targetFit := f.getRuleFitByStoreID(dstStore.GetID())
	if targetFit != nil && targetFit.Rule.Role == fit.Rule.Role {
		return true
	}

	// the target store should be fit all constraints.
	if !MatchLabelConstraints(dstStore, fit.Rule.LabelConstraints) {
		return false
	}

	if isolation == nil {
		if labels := fit.Rule.isolationLabels(); labels != nil {
			stats := newIsolationReplacement(fit, labels, srcStoreID)
			isolation = &stats
		}
	}
	if isolation != nil && !isolation.allow(dstStore) {
		return false
	}

	score := isolationStoreScore(srcStoreID, dstStore, fit.Stores, fit.Rule.LocationLabels)
	// restore the source store.
	return fit.IsolationScore <= score
}

func (f *RegionFit) getRuleFitByStoreID(storeID uint64) *RuleFit {
	for _, rf := range f.RuleFits {
		for _, p := range rf.Peers {
			if p.GetStoreId() == storeID {
				return rf
			}
		}
	}
	return nil
}

// IsSatisfied returns if the rules are properly satisfied.
// It means all Rules are fulfilled and there is no orphan peers.
func (f *RegionFit) IsSatisfied() bool {
	if len(f.RuleFits) == 0 {
		return false
	}
	for _, r := range f.RuleFits {
		if !r.IsSatisfied() {
			return false
		}
	}
	return len(f.OrphanPeers) == 0
}

// ExtraCount return the extra count.
func (f *RegionFit) ExtraCount() int {
	desired := 0
	for _, r := range f.RuleFits {
		desired += r.Rule.Count
	}
	return len(f.regionStores) - desired
}

// GetRuleFit returns the RuleFit that contains the peer.
func (f *RegionFit) GetRuleFit(peerID uint64) *RuleFit {
	for _, rf := range f.RuleFits {
		for _, p := range rf.Peers {
			if p.GetId() == peerID {
				return rf
			}
		}
	}
	return nil
}

// GetRegionStores returns region's stores
func (f *RegionFit) GetRegionStores() []*core.StoreInfo {
	return f.regionStores
}

// GetRules returns the rules that are used to fit the region.
func (f *RegionFit) GetRules() []*Rule {
	return f.rules
}

// RuleFit is the result of fitting status of a Rule.
type RuleFit struct {
	Rule *Rule `json:"rule"`
	// Peers of the Region that are divided to this Rule.
	Peers []*metapb.Peer `json:"peers"`
	// PeersWithDifferentRole is subset of `Peers`. It contains all Peers that have
	// different Role from configuration (the Role can be migrated to target role
	// by scheduling).
	PeersWithDifferentRole []*metapb.Peer `json:"peers-different-role"`
	// IsolationScore indicates at which level of labeling these Peers are
	// isolated. A larger value is better.
	IsolationScore float64 `json:"isolation-score"`
	WitnessScore   int     `json:"witness-score"`
	// Stores is the stores that the peers are placed in.
	Stores []*core.StoreInfo `json:"-"`
}

// IsSatisfied returns if the rule is properly satisfied.
func (f *RuleFit) IsSatisfied() bool {
	return len(f.Peers) == f.Rule.Count && len(f.PeersWithDifferentRole) == 0
}

// isolationLabels returns the location labels up to the configured isolation
// level. Rule validation accepts a level that is absent from LocationLabels;
// treat it as unset instead of rejecting every scheduling change for the rule.
func (r *Rule) isolationLabels() []string {
	if r.IsolationLevel == "" {
		return nil
	}
	level := slices.Index(r.LocationLabels, r.IsolationLevel)
	if level < 0 {
		return nil
	}
	return r.LocationLabels[:level+1]
}

func hasIsolationLevel(rules []*Rule) bool {
	for _, rule := range rules {
		if rule.isolationLabels() != nil {
			return true
		}
	}
	return false
}

// HasIsolationLevel reports whether this fit has an effective isolation level.
func (f *RegionFit) HasIsolationLevel() bool {
	return hasIsolationLevel(f.rules)
}

// IsIsolationSatisfied reports whether each pair of stores is separated at or
// above the rule's configured isolation level. It does not impose a default
// level or compare the weighted isolation score.
func (f *RuleFit) IsIsolationSatisfied() bool {
	labels := f.Rule.isolationLabels()
	if labels == nil {
		return true
	}
	for i, store := range f.Stores {
		if store == nil {
			return false
		}
		for _, other := range f.Stores[i+1:] {
			if other == nil || store.CompareLocation(other, labels) < 0 {
				return false
			}
		}
	}
	return true
}

// IsIsolationPreserved checks a fixed-size move against the configured isolation
// level. It permits incremental repair, but neither the number of conflicting
// pairs nor the largest set of peers not separated from one peer may increase.
// Missing labels use CompareLocation's conservative pairwise semantics; they
// are not grouped under a synthetic host name. Different rules are independent.
func (f *RuleFit) IsIsolationPreserved(target *RuleFit) bool {
	labels := f.Rule.isolationLabels()
	if labels == nil {
		return true
	}
	if target == nil || len(f.Stores) != len(target.Stores) {
		return false
	}
	beforePairs, beforeMax, beforeOK := isolationConflicts(f.Stores, labels)
	afterPairs, afterMax, afterOK := isolationConflicts(target.Stores, labels)
	return beforeOK && afterOK && afterPairs <= beforePairs && afterMax <= beforeMax
}

func isolationConflicts(stores []*core.StoreInfo, labels []string) (pairs, maxPeers int, valid bool) {
	counts := make([]int, len(stores))
	for i, store := range stores {
		if store == nil {
			return 0, 0, false
		}
		counts[i]++
		for j := i + 1; j < len(stores); j++ {
			if stores[j] == nil {
				return 0, 0, false
			}
			if store.CompareLocation(stores[j], labels) < 0 {
				pairs++
				counts[i]++
				counts[j]++
			}
		}
	}
	for _, count := range counts {
		maxPeers = max(maxPeers, count)
	}
	return pairs, maxPeers, true
}

// IsIsolationPreserved checks complete fits made with the same effective rules.
// It is for fixed-size scheduling, not replica-count-changing repairs.
func (f *RegionFit) IsIsolationPreserved(target *RegionFit) bool {
	return f.preservesIsolation(target, true)
}

// preservesIsolation optionally keeps the weighted score check. Leader
// transfers skip it: they move no peer, so a lower score only reflects how
// peers are assigned to rules, not a change in replica locations.
func (f *RegionFit) preservesIsolation(target *RegionFit, checkScore bool) bool {
	if target == nil || len(f.RuleFits) != len(target.RuleFits) {
		return false
	}
	for i, before := range f.RuleFits {
		after := target.RuleFits[i]
		if before.Rule != after.Rule || len(before.Peers) != len(after.Peers) ||
			(checkScore && after.IsolationScore < before.IsolationScore) || !before.IsIsolationPreserved(after) {
			return false
		}
	}
	return true
}

func (f *RuleFit) contain(storeID uint64) bool {
	for _, p := range f.Peers {
		if p.GetStoreId() == storeID {
			return true
		}
	}
	return false
}

func compareRuleFit(a, b *RuleFit) int {
	switch {
	case len(a.Peers) < len(b.Peers):
		return -1
	case len(a.Peers) > len(b.Peers):
		return 1
	case len(a.PeersWithDifferentRole) > len(b.PeersWithDifferentRole):
		return -1
	case len(a.PeersWithDifferentRole) < len(b.PeersWithDifferentRole):
		return 1
	case a.IsolationScore < b.IsolationScore:
		return -1
	case a.IsolationScore > b.IsolationScore:
		return 1
	case a.WitnessScore > b.WitnessScore:
		return -1
	case a.WitnessScore < b.WitnessScore:
		return 1
	default:
		return 0
	}
}

// StoreSet represents the store container.
type StoreSet interface {
	GetStores() []*core.StoreInfo
	GetStore(id uint64) *core.StoreInfo
}

// FitRegionWithRules fits a layout with an explicit effective-rule snapshot.
// It neither reads nor writes the real-region fit cache. Callers comparing two
// planned layouts must supply the same rules and store metadata to both calls.
func FitRegionWithRules(stores StoreSet, region *core.RegionInfo, rules []*Rule, supportWitness bool) *RegionFit {
	regionStores := getStoresByRegion(stores, region)
	fit := fitRegion(regionStores, region, rules, supportWitness)
	fit.regionStores = regionStores
	fit.rules = rules
	return fit
}

// fitRegion tries to fit peers of a region to the rules.
func fitRegion(stores []*core.StoreInfo, region *core.RegionInfo, rules []*Rule, supportWitness bool) *RegionFit {
	w := newFitWorker(stores, region, rules, supportWitness)
	w.run()
	return &w.bestFit
}

type fitWorker struct {
	stores         []*core.StoreInfo
	bestFit        RegionFit  // update during execution
	peers          []*fitPeer // p.selected is updated during execution.
	rules          []*Rule
	supportWitness bool
	needIsolation  bool
	exit           bool
}

func newFitPeer(stores []*core.StoreInfo, region *core.RegionInfo, fitPeers []*metapb.Peer) []*fitPeer {
	peers := make([]*fitPeer, len(fitPeers))
	for i, p := range fitPeers {
		peer := &fitPeer{
			Peer:     p,
			store:    getStoreByID(stores, p.GetStoreId()),
			isLeader: region.GetLeader().GetId() == p.GetId(),
		}
		peers[i] = peer
	}
	return peers
}

func newFitWorker(stores []*core.StoreInfo, region *core.RegionInfo, rules []*Rule, supportWitness bool) *fitWorker {
	peers := newFitPeer(stores, region, region.GetPeers())
	// Sort peers to keep the match result deterministic.
	sort.Slice(peers, func(i, j int) bool {
		// Put healthy peers in front of priority to fit healthy peers.
		si, sj := stateScore(region, peers[i].GetId()), stateScore(region, peers[j].GetId())
		return si > sj || (si == sj && peers[i].GetId() < peers[j].GetId())
	})
	return &fitWorker{
		stores:         stores,
		bestFit:        RegionFit{RuleFits: make([]*RuleFit, len(rules))},
		peers:          peers,
		needIsolation:  needIsolation(rules),
		rules:          rules,
		supportWitness: supportWitness,
	}
}

func (w *fitWorker) run() {
	w.fitRule(0)
	w.updateOrphanPeers(0) // All peers go to orphanList when RuleList is empty.
}

// Pick the most suitable peer combination for the rule.
// Index specifies the position of the rule.
// returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) fitRule(index int) bool {
	if w.exit {
		return false
	}
	if index >= len(w.rules) {
		// If there is no isolation level and we already find one solution, we can early exit searching instead of
		// searching the whole cases.
		if !w.needIsolation && w.bestFit.IsSatisfied() {
			w.exit = true
		}
		return false
	}

	var candidates []*fitPeer
	if checkRule(w.rules[index], w.stores) {
		// Only consider stores:
		// 1. Match label constraints
		// 2. Role match, or can match after transformed.
		// 3. Don't select leader as witness.
		// 4. Not selected by other rules.
		for _, p := range w.peers {
			if !p.selected && MatchLabelConstraints(p.store, w.rules[index].LabelConstraints) && (!p.isLeader || !w.supportWitness || !w.rules[index].IsWitness) {
				candidates = append(candidates, p)
			}
		}
	}

	count := min(len(candidates), w.rules[index].Count)

	return w.fixRuleWithCandidates(candidates, index, count)
}

// Pick the most suitable peer combination for the rule with candidates.
// Returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) fixRuleWithCandidates(candidates []*fitPeer, index int, count int) bool {
	// There is only one combination when every candidate is needed.
	if count == len(candidates) {
		for _, p := range candidates {
			p.selected = true
		}
		better := w.compareBest(candidates, index)
		unSelectPeers(candidates)
		return better
	}
	if len(candidates) >= 8 && w.needIsolation {
		if classes, classCount := w.classifyCandidates(candidates, index, count); classCount > 0 {
			if classCount == 1 {
				return w.fixRuleWithCandidates(candidates[:count], index, count)
			}
			return w.fitEquivalentCandidates(candidates, classes, classCount, index, count)
		}
	}

	// Visit combinations in increasing bitmask order without depending on the
	// machine word size or scanning subsets with a different number of peers.
	indexes := make([]int, count)
	selected := make([]*fitPeer, count)
	for i := range indexes {
		indexes[i] = i
	}

	var better bool
	for {
		for i, candidateIndex := range indexes {
			selected[i] = candidates[candidateIndex]
			selected[i].selected = true
		}
		better = w.compareBest(selected, index) || better
		unSelectPeers(selected)
		if w.exit {
			return better
		}

		// Advance the lowest index that has room, then reset lower indexes.
		i := 0
		for ; i < count; i++ {
			limit := len(candidates)
			if i+1 < count {
				limit = indexes[i+1]
			}
			if indexes[i]+1 < limit {
				break
			}
		}
		if i == count {
			return better
		}
		indexes[i]++
		for j := range i {
			indexes[j] = j
		}
	}
}

type peerClass struct {
	id       int
	previous int // Number of earlier candidates in this class.
}

// classifyCandidates groups peers that are interchangeable for every rule they
// can reach. Keep ordinary replica sets on the existing enumeration path.
func (w *fitWorker) classifyCandidates(candidates []*fitPeer, index, count int) ([]peerClass, int) {
	// Peers outside candidates can still occupy slots in later rules.
	remaining := -count
	for _, peer := range w.peers {
		if !peer.selected {
			remaining++
		}
	}
	// Replacing equivalent peers can change the order of isolation additions.
	// Only prune when all possible sums are exact integers in float64. Leave a
	// conservative margin below 2^53, including for the bound's own rounding.
	pairs := float64(len(w.peers)) * float64(len(w.peers)-1) / 2
	for _, rule := range w.rules[index:] {
		if len(rule.LocationLabels) > 0 && pairs*math.Pow(replicaBaseScore, float64(len(rule.LocationLabels)-1)) >= 1<<52 {
			return nil, 0
		}
	}
	// Limit the class representatives and keep them on the stack. If peers have
	// too many distinct uses, fall back without allocating.
	var representatives [8]*fitPeer
	limit := min(count, len(candidates)-count, len(representatives))
	classCount := 0
	for _, peer := range candidates {
		class := 0
		for ; class < classCount; class++ {
			if w.equivalentPeers(peer, representatives[class], index, remaining) {
				break
			}
		}
		if class == classCount {
			if classCount == limit {
				return nil, 0
			}
			representatives[classCount] = peer
			classCount++
		}
	}
	if classCount == 1 {
		return nil, 1
	}
	if classCount == len(candidates) {
		return nil, 0
	}
	classes := make([]peerClass, len(candidates))
	var sizes [8]int
	for i, peer := range candidates {
		for class := range classCount {
			if w.equivalentPeers(peer, representatives[class], index, remaining) {
				classes[i] = peerClass{id: class, previous: sizes[class]}
				sizes[class]++
				break
			}
		}
	}
	return classes, classCount
}

func (w *fitWorker) equivalentPeers(a, b *fitPeer, index, remaining int) bool {
	if a.store == nil || b.store == nil || a.IsWitness != b.IsWitness {
		return false
	}
	for i, rule := range w.rules[index:] {
		if a.matchRoleStrict(rule.Role) != b.matchRoleStrict(rule.Role) {
			return false
		}
		matches := MatchLabelConstraints(a.store, rule.LabelConstraints)
		if matches != MatchLabelConstraints(b.store, rule.LabelConstraints) {
			return false
		}
		if w.supportWitness && rule.IsWitness && (a.isLeader != b.isLeader || a.store.GetWitnessCount() != b.store.GetWitnessCount()) {
			return false
		}
		for _, label := range rule.LocationLabels {
			if a.store.GetLabelValue(label) != b.store.GetLabelValue(label) {
				return false
			}
		}
		if i > 0 && matches && (!a.isLeader || !w.supportWitness || !rule.IsWitness) {
			// If the exchanged peer survives this rule, the rule must consume
			// Count other peers. Once no peers could remain, neither exchanged
			// peer can reach a later rule, so later distinctions are irrelevant.
			if rule.Count >= remaining {
				return true
			}
			remaining -= rule.Count
		}
	}
	return true
}

// fitEquivalentCandidates visits only combinations containing a prefix of each
// equivalence class. Any omitted combination can exchange a peer for an earlier
// equivalent peer without changing this or any later rule's best scores. The
// prefix is visited first in the original order, preserving deterministic ties.
func (w *fitWorker) fitEquivalentCandidates(candidates []*fitPeer, classes []peerClass, classCount, index, count int) bool {
	selected := make([]*fitPeer, count)
	required := make([]bool, classCount)
	const (
		excludeCandidate = iota
		includeCandidate
		unselectCandidate
		unselectAndResetClass
	)
	type frame struct {
		candidate int
		count     int
		forced    int
		phase     int
	}
	// Walk high candidate indexes first, excluding before including, to retain
	// increasing bitmask order. Use an explicit stack to avoid recursion depth
	// depending on the number of peers.
	stack := make([]frame, 1, len(candidates)+1)
	stack[0] = frame{candidate: len(candidates) - 1, count: count}
	var better bool
	for len(stack) > 0 {
		f := &stack[len(stack)-1]
		if f.phase >= unselectCandidate {
			candidates[f.candidate].selected = false
			if f.phase == unselectAndResetClass {
				required[classes[f.candidate].id] = false
			}
			stack = stack[:len(stack)-1]
			continue
		}
		if w.exit || f.count < f.forced || f.count > f.candidate+1 {
			stack = stack[:len(stack)-1]
			continue
		}
		if f.count == 0 {
			better = w.compareBest(selected, index) || better
			stack = stack[:len(stack)-1]
			continue
		}
		class := classes[f.candidate]
		if f.phase == excludeCandidate && !required[class.id] {
			f.phase = includeCandidate
			stack = append(stack, frame{candidate: f.candidate - 1, count: f.count, forced: f.forced})
			continue
		}
		forced := f.forced - 1
		f.phase = unselectCandidate
		if !required[class.id] {
			forced = f.forced + class.previous
			required[class.id] = true
			f.phase = unselectAndResetClass
		}
		selected[f.count-1] = candidates[f.candidate]
		candidates[f.candidate].selected = true
		stack = append(stack, frame{candidate: f.candidate - 1, count: f.count - 1, forced: forced})
	}
	return better
}

func unSelectPeers(selected []*fitPeer) {
	for _, p := range selected {
		p.selected = false
	}
}

// compareBest checks if the selected peers is better then previous best.
// Returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) compareBest(selected []*fitPeer, index int) bool {
	rf := newRuleFit(w.rules[index], selected, w.supportWitness)
	if best := w.bestFit.RuleFits[index]; best != nil {
		switch compareRuleFit(rf, best) {
		case -1:
			return false
		case 0:
			if w.fitRule(index + 1) {
				w.bestFit.RuleFits[index] = rf
				return true
			}
			return false
		}
		// Only replacing an existing fit can invalidate later results.
		for i := index + 1; i < len(w.rules); i++ {
			w.bestFit.RuleFits[i] = nil
		}
	}
	w.bestFit.RuleFits[index] = rf
	w.fitRule(index + 1)
	w.updateOrphanPeers(index + 1)
	return true
}

// determine the orphanPeers list based on fitPeer.selected flag.
func (w *fitWorker) updateOrphanPeers(index int) {
	if index != len(w.rules) {
		return
	}
	w.bestFit.OrphanPeers = w.bestFit.OrphanPeers[:0]
	for _, p := range w.peers {
		if !p.selected {
			w.bestFit.OrphanPeers = append(w.bestFit.OrphanPeers, p.Peer)
		}
	}
}

func newRuleFit(rule *Rule, peers []*fitPeer, supportWitness bool) *RuleFit {
	rf := &RuleFit{Rule: rule, IsolationScore: isolationScore(peers, rule.LocationLabels), WitnessScore: witnessScore(peers, supportWitness && rule.IsWitness)}
	for _, p := range peers {
		rf.Peers = append(rf.Peers, p.Peer)
		rf.Stores = append(rf.Stores, p.store)
		if !p.matchRoleStrict(rule.Role) ||
			(supportWitness && (p.IsWitness != rule.IsWitness)) ||
			(!supportWitness && p.IsWitness) {
			rf.PeersWithDifferentRole = append(rf.PeersWithDifferentRole, p.Peer)
		}
	}
	return rf
}

type fitPeer struct {
	*metapb.Peer
	store           *core.StoreInfo
	isLeader        bool
	selected        bool
	relaxLeaderRole bool
}

func (p *fitPeer) matchRoleStrict(role PeerRoleType) bool {
	if p.relaxLeaderRole && (role == Leader || role == Follower) {
		return !core.IsLearner(p.Peer)
	}
	switch role {
	case Voter: // Voter matches either Leader or Follower.
		return !core.IsLearner(p.Peer)
	case Leader:
		return p.isLeader
	case Follower:
		return !core.IsLearner(p.Peer) && !p.isLeader
	case Learner:
		return core.IsLearner(p.Peer)
	}
	return false
}

func isolationStoreScore(srcStoreID uint64, dstStore *core.StoreInfo, stores []*core.StoreInfo, labels []string) float64 {
	var score float64
	if len(labels) == 0 || len(stores) <= 1 {
		return 0
	}
	for i := range stores {
		store1 := stores[i]
		if store1.GetID() == srcStoreID {
			store1 = dstStore
		}
		for _, store2 := range stores[i+1:] {
			if store2.GetID() == srcStoreID {
				store2 = dstStore
			}
			if index := store1.CompareLocation(store2, labels); index != -1 {
				score += math.Pow(replicaBaseScore, float64(len(labels)-index-1))
			}
		}
	}
	return score
}

func isolationScore(peers []*fitPeer, labels []string) float64 {
	var score float64
	if len(labels) == 0 || len(peers) <= 1 {
		return 0
	}
	// NOTE: following loop is partially duplicated with `core.DistinctScore`.
	// The reason not to call it directly is that core.DistinctScore only
	// accepts `[]StoreInfo` not `[]*fitPeer` and I don't want alloc slice
	// here because it is kind of hot path.
	// After Go supports generics, we will be enable to do some refactor and
	// reuse `core.DistinctScore`.
	for i, p1 := range peers {
		for _, p2 := range peers[i+1:] {
			if index := p1.store.CompareLocation(p2.store, labels); index != -1 {
				score += math.Pow(replicaBaseScore, float64(len(labels)-index-1))
			}
		}
	}
	return score
}

func needIsolation(rules []*Rule) bool {
	for _, rule := range rules {
		if len(rule.LocationLabels) > 0 {
			return true
		}
	}
	return false
}

func stateScore(region *core.RegionInfo, peerID uint64) int {
	switch {
	case region.GetDownPeer(peerID) != nil:
		return 0
	case region.GetPendingPeer(peerID) != nil:
		return 1
	default:
		return 2
	}
}

func witnessScore(peers []*fitPeer, fitWitness bool) int {
	var score int
	if !fitWitness || len(peers) == 0 {
		return 0
	}
	for _, p := range peers {
		score += p.store.GetWitnessCount()
	}
	return score
}
