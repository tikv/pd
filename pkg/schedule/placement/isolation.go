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

package placement

import (
	"github.com/tikv/pd/pkg/core"
)

// isolationReplacement retains conflict degrees for one immutable source fit.
// A replacement changes only edges incident to source. Degrees exclude self.
type isolationReplacement struct {
	stores    []*core.StoreInfo
	labels    []string
	degrees   []int
	source    int
	maxDegree int
	valid     bool
}

// newIsolationReplacement requires labels from Rule.isolationLabels.
func newIsolationReplacement(fit *RuleFit, labels []string, source uint64) isolationReplacement {
	c := isolationReplacement{stores: fit.Stores, labels: labels, source: -1}
	c.degrees = make([]int, len(c.stores))
	for i, store := range c.stores {
		if store == nil {
			return c
		}
		if store.GetID() == source {
			c.source = i
		}
		for j := i + 1; j < len(c.stores); j++ {
			if c.stores[j] == nil {
				return c
			}
			if store.CompareLocation(c.stores[j], c.labels) < 0 {
				c.degrees[i]++
				c.degrees[j]++
			}
		}
	}
	for _, degree := range c.degrees {
		c.maxDegree = max(c.maxDegree, degree)
	}
	c.valid = c.source >= 0
	return c
}

func (c *isolationReplacement) allow(target *core.StoreInfo) bool {
	if !c.valid || target == nil {
		return false
	}
	added := 0
	for i, store := range c.stores {
		if i == c.source {
			continue
		}
		if target.CompareLocation(store, c.labels) >= 0 {
			continue
		}
		added++
		// Other edges stay unchanged. Exceeding the removed edge count already
		// proves that the total number of conflicting pairs would increase.
		if added > c.degrees[c.source] {
			return false
		}
		// A new edge can increase this neighbor's degree only when it did not
		// already conflict with source. Removed edges cannot increase a degree.
		if c.degrees[i] == c.maxDegree && c.stores[c.source].CompareLocation(store, c.labels) >= 0 {
			return false
		}
	}
	return true
}

// IsTargetIsolationPreserved fits a complete target against this fit's rules.
// Force may relax leader/follower roles only when ordinary matching fails. It
// still requires every peer to match a rule, including labels and witness state.
// A normally satisfied target must pass isolation without role relaxation.
func (f *RegionFit) IsTargetIsolationPreserved(stores StoreSet, target *core.RegionInfo, supportWitness, forceLeader bool) bool {
	if !f.IsSatisfied() {
		return false
	}
	return f.checkTargetIsolation(getStoresByRegion(stores, target), target, supportWitness, forceLeader)
}

func (f *RegionFit) checkTargetIsolation(stores []*core.StoreInfo, target *core.RegionInfo, supportWitness, forceLeader bool) bool {
	after := fitRegion(stores, target, f.rules, supportWitness)
	if !after.IsSatisfied() && forceLeader {
		worker := newFitWorker(stores, target, f.rules, supportWitness)
		for _, peer := range worker.peers {
			peer.relaxLeaderRole = true
		}
		worker.run()
		after = &worker.bestFit
	}
	return after.IsSatisfied() && f.IsIsolationPreserved(after)
}

// IsFinalIsolationPreserved checks the complete layout of an operator built
// from this fit, including its final leader, which can change rule assignment.
// Only configured isolation levels are compared, by conflicts; candidate
// safeguards keep the weighted score check. Incomplete source fits and rules
// whose peer count changes keep their existing repair behavior.
func (f *RegionFit) IsFinalIsolationPreserved(stores StoreSet, target *core.RegionInfo, supportWitness bool) bool {
	if !f.IsSatisfied() || !hasIsolationLevel(f.rules) {
		return true
	}
	after := fitRegion(getStoresByRegion(stores, target), target, f.rules, supportWitness)
	for i, before := range f.RuleFits {
		if i >= len(after.RuleFits) {
			return true
		}
		rf := after.RuleFits[i]
		if rf == nil || before.Rule != rf.Rule || len(before.Peers) != len(rf.Peers) {
			continue
		}
		if !before.IsIsolationPreserved(rf) {
			return false
		}
	}
	return true
}

// IsLeaderIsolationPreserved checks rule reassignment after a leader transfer.
// With no leader-specific rules, changing leadership cannot change the fit.
// Incomplete source fits keep their existing repair admission behavior.
// A transfer moves no peer, so only rules with an IsolationLevel are compared,
// by conflicts rather than by the weighted score.
func (f *RegionFit) IsLeaderIsolationPreserved(region *core.RegionInfo, leaderStore uint64, supportWitness bool) bool {
	if leaderStore == region.GetLeader().GetStoreId() || !f.IsSatisfied() {
		return true
	}
	leader := region.GetStorePeer(leaderStore)
	if leader == nil {
		return false
	}
	sensitive := false
	for _, rule := range f.rules {
		if rule.Role == Leader || rule.Role == Follower {
			sensitive = true
			break
		}
	}
	if !sensitive || !hasIsolationLevel(f.rules) {
		return true
	}
	after := fitRegion(f.regionStores, region.Clone(core.WithLeader(leader)), f.rules, supportWitness)
	return after.IsSatisfied() && f.isIsolationPreserved(after, false)
}
