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
	"sort"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
)

// RegionFit is the result of fitting a region's peers to rule list.
// All peers are divided into corresponding rules according to the matching
// rules, and the remaining Peers are placed in the OrphanPeers list.
type RegionFit struct {
	mu struct {
		sync.RWMutex
		cached bool
	}
	RuleFits     []*RuleFit
	OrphanPeers  []*metapb.Peer
	regionStores []*core.StoreInfo
	rules        []*Rule
}

// SetCached indicates this RegionFit is fetch form cache
func (f *RegionFit) SetCached(cached bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.cached = cached
}

// IsCached indicates whether this result is fetched from caches
func (f *RegionFit) IsCached() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.cached
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

// CompareRegionFit determines the superiority of 2 fits.
// It returns 1 when the first fit result is better.
func CompareRegionFit(a, b *RegionFit) int {
	for i := range a.RuleFits {
		if i >= len(b.RuleFits) {
			break
		}
		if cmp := compareRuleFit(a.RuleFits[i], b.RuleFits[i]); cmp != 0 {
			return cmp
		}
	}
	switch {
	case len(a.OrphanPeers) < len(b.OrphanPeers):
		return 1
	case len(a.OrphanPeers) > len(b.OrphanPeers):
		return -1
	default:
		return 0
	}
}

// RuleFit is the result of fitting status of a Rule.
type RuleFit struct {
	Rule *Rule
	// Peers of the Region that are divided to this Rule.
	Peers []*metapb.Peer
	// PeersWithDifferentRole is subset of `Peers`. It contains all Peers that have
	// different Role from configuration (the Role can be migrated to target role
	// by scheduling).
	PeersWithDifferentRole []*metapb.Peer
	// IsolationScore indicates at which level of labeling these Peers are
	// isolated. A larger value is better.
	IsolationScore float64
}

// IsSatisfied returns if the rule is properly satisfied.
func (f *RuleFit) IsSatisfied() bool {
	return len(f.Peers) == f.Rule.Count && len(f.PeersWithDifferentRole) == 0
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
func fitRegion(stores []*core.StoreInfo, region *core.RegionInfo, rules []*Rule) *RegionFit {
	w := newFitWorker(stores, region, rules)
	w.run()
	return &w.bestFit
}

type fitWorker struct {
	stores        []*core.StoreInfo
	bestFit       RegionFit  // update during execution
	peers         []*fitPeer // p.selected is updated during execution.
	rules         []*Rule
	needIsolation bool
	exit          bool
}

func newFitWorker(stores []*core.StoreInfo, region *core.RegionInfo, rules []*Rule) *fitWorker {
	regionPeers := region.GetPeers()
	peers := make([]*fitPeer, 0, len(regionPeers))
	for _, p := range regionPeers {
		peers = append(peers, &fitPeer{
			Peer:     p,
			store:    getStoreByID(stores, p.GetStoreId()),
			isLeader: region.GetLeader().GetId() == p.GetId(),
		})
	}
	// Sort peers to keep the match result deterministic.
	sort.Slice(peers, func(i, j int) bool {
		// Put healthy peers in front to priority to fit healthy peers.
		si, sj := stateScore(region, peers[i].GetId()), stateScore(region, peers[j].GetId())
		return si > sj || (si == sj && peers[i].GetId() < peers[j].GetId())
	})

	return &fitWorker{
		stores:        stores,
		bestFit:       RegionFit{RuleFits: make([]*RuleFit, len(rules))},
		peers:         peers,
		needIsolation: needIsolation(rules),
		rules:         rules,
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
		// 3. Not selected by other rules.
		for _, p := range w.peers {
			if MatchLabelConstraints(p.store, w.rules[index].LabelConstraints) &&
				!p.selected {
				candidates = append(candidates, p)
			}
		}
	}

	count := w.rules[index].Count
	if len(candidates) < count {
		count = len(candidates)
	}
	return w.enumPeers(candidates, nil, index, count)
}

// Recursively traverses all feasible peer combinations.
// For each combination, call `compareBest` to determine whether it is better
// than the existing option.
// Returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) enumPeers(candidates, selected []*fitPeer, index int, count int) bool {
	if len(selected) == count {
		// We collect enough peers. End recursive.
		return w.compareBest(selected, index)
	}

	var better bool
	for i, p := range candidates {
		p.selected = true
		better = w.enumPeers(candidates[i+1:], append(selected, p), index, count) || better
		p.selected = false
		if w.exit {
			break
		}
	}
	return better
}

// compareBest checks if the selected peers is better then previous best.
// Returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) compareBest(selected []*fitPeer, index int) bool {
	rf := newRuleFit(w.rules[index], selected)
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

func newRuleFit(rule *Rule, peers []*fitPeer) *RuleFit {
	rf := &RuleFit{Rule: rule, IsolationScore: isolationScore(peers, rule.LocationLabels)}
	for _, p := range peers {
		rf.Peers = append(rf.Peers, p.Peer)
		if !p.matchRoleStrict(rule.Role) {
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
	const replicaBaseScore = 100
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
