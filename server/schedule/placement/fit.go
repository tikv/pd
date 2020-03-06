// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/pkg/slice"
	"github.com/pingcap/pd/v4/server/core"
)

// RegionFit is the result of fitting a region's peers to rule list.
// All peers are divided into corresponding rules according to the matching
// rules, and the remaining Peers are placed in the OrphanPeers list.
type RegionFit struct {
	RuleFits    []*RuleFit
	OrphanPeers []*metapb.Peer
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

// CompareRegionFit determines the superiority of 2 fits.
// It returns 1 when the first fit result is better.
func CompareRegionFit(a, b *RegionFit) int {
	for i := range a.RuleFits {
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
	// IsolationLevel indicates at which level of labeling these Peers are
	// isolated. A larger value indicates a higher isolation level.
	IsolationLevel int
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
	case a.IsolationLevel < b.IsolationLevel:
		return -1
	case a.IsolationLevel > b.IsolationLevel:
		return 1
	default:
		return 0
	}
}

// FitRegion tries to fit peers of a region to the rules.
func FitRegion(stores core.StoreSetInformer, region *core.RegionInfo, rules []*Rule) *RegionFit {
	peers := prepareFitPeers(stores, region)

	var regionFit RegionFit
	if len(rules) == 0 {
		return &regionFit
	}
	for _, rule := range rules {
		rf := fitRule(peers, rule)
		regionFit.RuleFits = append(regionFit.RuleFits, rf)
		// Remove selected.
		peers = filterPeersBy(peers, func(p *fitPeer) bool {
			return slice.NoneOf(rf.Peers, func(i int) bool { return rf.Peers[i].Id == p.Peer.Id })
		})
	}
	for _, p := range peers {
		regionFit.OrphanPeers = append(regionFit.OrphanPeers, p.Peer)
	}
	return &regionFit
}

func fitRule(peers []*fitPeer, rule *Rule) *RuleFit {
	// Ignore peers that does not match label constraints, and that cannot be
	// transformed to expected role type.
	peers = filterPeersBy(peers,
		func(p *fitPeer) bool { return MatchLabelConstraints(p.store, rule.LabelConstraints) },
		func(p *fitPeer) bool { return p.matchRoleLoose(rule.Role) })

	if len(peers) <= rule.Count {
		return newRuleFit(rule, peers)
	}

	// TODO: brute force can be improved.
	var best *RuleFit
	iterPeers(peers, rule.Count, func(candidates []*fitPeer) {
		rf := newRuleFit(rule, candidates)
		if best == nil || compareRuleFit(rf, best) > 0 {
			best = rf
		}
	})
	return best
}

func newRuleFit(rule *Rule, peers []*fitPeer) *RuleFit {
	rf := &RuleFit{Rule: rule, IsolationLevel: isolationLevel(peers, rule.LocationLabels)}
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
}

func (p *fitPeer) matchRoleStrict(role PeerRoleType) bool {
	switch role {
	case Voter: // Voter matches either Leader or Follower.
		return !p.IsLearner
	case Leader:
		return p.isLeader
	case Follower:
		return !p.IsLearner && !p.isLeader
	case Learner:
		return p.IsLearner
	}
	return false
}

func (p *fitPeer) matchRoleLoose(role PeerRoleType) bool {
	// non-learner cannot become learner. All other roles can migrate to
	// others by scheduling. For example, Leader->Follower, Learner->Leader
	// are possible, but Voter->Learner is impossible.
	return role != Learner || p.IsLearner
}

func prepareFitPeers(stores core.StoreSetInformer, region *core.RegionInfo) []*fitPeer {
	var peers []*fitPeer
	for _, p := range region.GetPeers() {
		peers = append(peers, &fitPeer{
			Peer:     p,
			store:    stores.GetStore(p.GetStoreId()),
			isLeader: region.GetLeader().GetId() == p.GetId(),
		})
	}
	// Sort peers to keep the match result deterministic.
	sort.Slice(peers, func(i, j int) bool { return peers[i].GetId() < peers[j].GetId() })
	return peers
}

func filterPeersBy(peers []*fitPeer, preds ...func(*fitPeer) bool) (selected []*fitPeer) {
	for _, p := range peers {
		if slice.AllOf(preds, func(i int) bool { return preds[i](p) }) {
			selected = append(selected, p)
		}
	}
	return
}

// Iterate all combinations of select N peers from the list.
func iterPeers(peers []*fitPeer, n int, f func([]*fitPeer)) {
	out := make([]*fitPeer, n)
	iterPeersRecr(peers, 0, out, func() { f(out) })
}

func iterPeersRecr(peers []*fitPeer, index int, out []*fitPeer, f func()) {
	for i := index; i <= len(peers)-len(out); i++ {
		out[0] = peers[i]
		if len(out) > 1 {
			iterPeersRecr(peers, i+1, out[1:], f)
		} else {
			f()
		}
	}
}

func isolationLevel(peers []*fitPeer, labels []string) int {
	if len(labels) == 0 || len(peers) == 0 {
		return 0
	}
	if len(peers) == 1 {
		return len(labels)
	}
	if len(peers) == 2 {
		for l, label := range labels {
			if peers[0].store.GetLabelValue(label) != peers[1].store.GetLabelValue(label) {
				return len(labels) - l
			}
		}
		return 0
	}

	// TODO: brute force can be improved.
	level := len(labels)
	iterPeers(peers, 2, func(pair []*fitPeer) {
		if l := isolationLevel(pair, labels); l < level {
			level = l
		}
	})
	return level
}
