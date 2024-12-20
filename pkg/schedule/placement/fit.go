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
	"math/bits"
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
	if dstStore == nil {
		return false
	}
	fit := f.getRuleFitByStoreID(srcStoreID)
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

	score := isolationStoreScore(srcStoreID, dstStore, fit.stores, fit.Rule.LocationLabels)
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
	// stores is the stores that the peers are placed in.
	stores []*core.StoreInfo
}

// IsSatisfied returns if the rule is properly satisfied.
func (f *RuleFit) IsSatisfied() bool {
	return len(f.Peers) == f.Rule.Count && len(f.PeersWithDifferentRole) == 0
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
			if !p.selected && MatchLabelConstraints(p.store, w.rules[index].LabelConstraints) && !(p.isLeader && w.supportWitness && w.rules[index].IsWitness) {
				candidates = append(candidates, p)
			}
		}
	}

	count := w.rules[index].Count
	if len(candidates) < count {
		count = len(candidates)
	}

	return w.fixRuleWithCandidates(candidates, index, count)
}

// Pick the most suitable peer combination for the rule with candidates.
// Returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) fixRuleWithCandidates(candidates []*fitPeer, index int, count int) bool {
	// map the candidates to binary numbers with len(candidates) bits,
	// each bit can be 1 or 0, 1 means a picked candidate
	// the binary numbers with `count` 1 means a choose for the current rule.

	var better bool
	limit := uint(1<<len(candidates) - 1)
	binaryInt := uint(1<<count - 1)
	for ; binaryInt <= limit; binaryInt++ {
		// there should be exactly `count` number in current binary number `binaryInt`
		if bits.OnesCount(binaryInt) != count {
			continue
		}
		selected := pickPeersFromBinaryInt(candidates, binaryInt)
		better = w.compareBest(selected, index) || better
		// reset the selected items to false.
		unSelectPeers(selected)
		if w.exit {
			break
		}
	}
	return better
}

// pickPeersFromBinaryInt picks the candidates with the related index at the position of binary for the `binaryNumber` is `1`.
// binaryNumber = 5, which means the related binary is 101, it will returns {candidates[0],candidates[2]}
// binaryNumber = 6, which means the related binary is 110, it will returns {candidates[1],candidates[2]}
func pickPeersFromBinaryInt(candidates []*fitPeer, binaryNumber uint) []*fitPeer {
	selected := make([]*fitPeer, 0)
	for _, p := range candidates {
		if binaryNumber&1 == 1 {
			p.selected = true
			selected = append(selected, p)
		}
		binaryNumber >>= 1
		if binaryNumber == 0 {
			break
		}
	}
	return selected
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
	cmp := 1
	if best := w.bestFit.RuleFits[index]; best != nil {
		cmp = compareRuleFit(rf, best)
	}

	switch cmp {
	case 1:
		w.bestFit.RuleFits[index] = rf
		// Reset previous result after position index.
		for i := index + 1; i < len(w.rules); i++ {
			w.bestFit.RuleFits[i] = nil
		}
		w.fitRule(index + 1)
		w.updateOrphanPeers(index + 1)
		return true
	case 0:
		if w.fitRule(index + 1) {
			w.bestFit.RuleFits[index] = rf
			return true
		}
	}
	return false
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
		rf.stores = append(rf.stores, p.store)
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
	store    *core.StoreInfo
	isLeader bool
	selected bool
}

func (p *fitPeer) matchRoleStrict(role PeerRoleType) bool {
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
