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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

// RegionFit is the result of fitting a region's peers to rule list.
type RegionFit struct {
	RuleFits    []*RuleFit
	OrphanPeers []*metapb.Peer
}

// IsSatisfied returns if the rules are properly satisfied.
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
	Rule              *Rule
	Peers             []*metapb.Peer
	LooseMatchedPeers []*metapb.Peer
	IsolationLevel    int
}

// IsSatisfied returns if the rule is properly satisfied.
func (f *RuleFit) IsSatisfied() bool {
	return len(f.Peers) == f.Rule.Count && len(f.LooseMatchedPeers) == 0
}

func compareRuleFit(a, b *RuleFit) int {
	switch {
	case len(a.Peers) < len(b.Peers):
		return -1
	case len(a.Peers) > len(b.Peers):
		return 1
	case len(a.LooseMatchedPeers) > len(b.LooseMatchedPeers):
		return -1
	case len(a.LooseMatchedPeers) < len(b.LooseMatchedPeers):
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
	return nil
}
