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

package scatter

import (
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
)

// moveScatterPeer updates a request-owned clone after a completed selection.
// Address peers by ID so updating one store cannot accidentally move another
// peer in a chain of replacements. The standalone leader is a separate clone.
func moveScatterPeer(view *core.RegionInfo, peerID, storeID uint64) {
	view.GetPeer(peerID).StoreId = storeID
	if view.GetLeader().GetId() == peerID {
		view.GetLeader().StoreId = storeID
	}
}

type scatterStoreSet map[uint64]*core.StoreInfo

// GetStore returns the captured store information.
func (s scatterStoreSet) GetStore(id uint64) *core.StoreInfo { return s[id] }

// GetStores returns the captured stores.
func (s scatterStoreSet) GetStores() []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(s))
	for _, store := range s {
		stores = append(stores, store)
	}
	return stores
}

// scatterPlacementValid checks the actual planned membership and leadership.
// Both fits use the same StoreInfo objects. Rules and labels are checked again
// before returning; this does not freeze metadata during operator execution.
func (r *RegionScatterer) scatterPlacementValid(region *core.RegionInfo, targets map[uint64]*metapb.Peer, leaderStore uint64) bool {
	if len(targets) != len(region.GetPeers()) {
		return false
	}
	roles := make(map[metapb.PeerRole]int)
	stores := make(scatterStoreSet, 2*len(targets))
	originalStores := make([]*core.StoreInfo, 0, len(targets))
	originalVoters := make([]*core.StoreInfo, 0, len(region.GetVoters()))
	targetVoters := make([]*core.StoreInfo, 0, len(region.GetVoters()))
	for _, peer := range region.GetPeers() {
		roles[peer.GetRole()]++
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			return false
		}
		stores[store.GetID()] = store
		originalStores = append(originalStores, store)
		if !core.IsLearner(peer) {
			originalVoters = append(originalVoters, store)
		}
	}
	peers := make([]*metapb.Peer, 0, len(targets))
	targetStores := make([]*core.StoreInfo, 0, len(targets))
	var leader *metapb.Peer
	ids := make([]uint64, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		p := targets[id]
		if p == nil || id != p.GetStoreId() {
			return false
		}
		role := p.GetRole()
		roles[role]--
		if roles[role] < 0 {
			return false
		}
		store := stores[id]
		if store == nil {
			store = r.cluster.GetStore(id)
		}
		if store == nil {
			return false
		}
		stores[id] = store
		targetStores = append(targetStores, store)
		if !core.IsLearner(p) {
			targetVoters = append(targetVoters, store)
		}
		peer := *p
		// IDs are local to this fit; never consume persistent IDs for validation.
		peer.Id = uint64(len(peers) + 1)
		peers = append(peers, &peer)
		if id == leaderStore {
			leader = &peer
		}
	}
	if leader == nil || leader.GetRole() != metapb.PeerRole_Voter {
		return false
	}
	target := region.Clone(core.SetPeers(peers), core.WithLeader(leader))
	conf := r.cluster.GetSharedConfig()
	rulesEnabled := conf.IsPlacementRulesEnabled()
	labels := slices.Clone(conf.GetLocationLabels())
	var rules []*placement.Rule
	if rulesEnabled {
		rm := r.cluster.GetRuleManager()
		before := rm.FitRegionWithoutCache(stores, region)
		after := rm.FitRegionWithoutCache(stores, target)
		rules = before.GetRules()
		if !reflect.DeepEqual(rules, after.GetRules()) || !before.IsSatisfied() || !after.IsSatisfied() {
			return false
		}
		for i, oldFit := range before.RuleFits {
			newFit := after.RuleFits[i]
			if !reflect.DeepEqual(oldFit.Rule, newFit.Rule) || newFit.IsolationScore < oldFit.IsolationScore {
				return false
			}
			rule := oldFit.Rule
			if statistics.IsRegionLabelIsolationSatisfied(oldFit.Stores, rule.LocationLabels, rule.IsolationLevel) &&
				!statistics.IsRegionLabelIsolationSatisfied(newFit.Stores, rule.LocationLabels, rule.IsolationLevel) {
				return false
			}
		}
	} else if scatterDistinctScore(labels, targetStores) < scatterDistinctScore(labels, originalStores) {
		return false
	}
	if len(originalVoters) > 1 {
		for _, hostLabels := range scatterHostLabels(labels, rules) {
			hosts := newScatterHostPlacement(hostLabels, originalVoters)
			if !hosts.allowsPlacement(targetVoters) {
				return false
			}
		}
	}
	for id, store := range stores {
		current := r.cluster.GetStore(id)
		if current == nil || !reflect.DeepEqual(store.GetLabels(), current.GetLabels()) {
			return false
		}
	}
	if rulesEnabled != conf.IsPlacementRulesEnabled() || !slices.Equal(labels, conf.GetLocationLabels()) {
		return false
	}
	return !rulesEnabled || reflect.DeepEqual(rules, r.cluster.GetRuleManager().GetRulesForApplyRegion(region))
}

func scatterDistinctScore(labels []string, stores []*core.StoreInfo) float64 {
	var score float64
	for i, store := range stores {
		score += core.DistinctScore(labels, stores[:i], store)
	}
	return score
}

// scatterHostLabels extracts each applicable hierarchy through its host level.
// A hierarchy without a host label does not declare a host failure domain.
func scatterHostLabels(labels []string, rules []*placement.Rule) [][]string {
	var result [][]string
	add := func(labels []string) {
		for i, label := range labels {
			if !strings.EqualFold(label, "host") {
				continue
			}
			prefix := labels[:i+1]
			for _, existing := range result {
				if slices.Equal(existing, prefix) {
					return
				}
			}
			result = append(result, slices.Clone(prefix))
			return
		}
	}
	if rules == nil {
		add(labels)
	} else {
		for _, rule := range rules {
			if rule.Role != placement.Learner {
				add(rule.LocationLabels)
			}
		}
	}
	return result
}

type scatterHostPlacement struct {
	labels   []string
	hosts    map[uint64]string
	counts   map[string]int
	pairs    int
	maxCount int
	complete bool
}

func scatterHostKey(store *core.StoreInfo, labels []string) (string, bool) {
	if store == nil {
		return "", false
	}
	// With a single host label, its value is already an unambiguous key;
	// avoid building a new string for every candidate.
	if len(labels) == 1 {
		value := strings.ToLower(store.GetLabelValue(labels[0]))
		return value, value != ""
	}
	var buffer [128]byte
	key := buffer[:0]
	for _, label := range labels {
		value := strings.ToLower(store.GetLabelValue(label))
		if value == "" {
			return "", false
		}
		// Length prefixes distinguish paths even when label values contain separators.
		key = strconv.AppendInt(key, int64(len(value)), 10)
		key = append(key, ':')
		key = append(key, value...)
	}
	return string(key), true
}

func newScatterHostPlacement(labels []string, stores []*core.StoreInfo) scatterHostPlacement {
	p := scatterHostPlacement{
		labels: labels, hosts: make(map[uint64]string, len(stores)),
		counts: make(map[string]int, len(stores)), complete: true,
	}
	for _, store := range stores {
		host, ok := scatterHostKey(store, labels)
		p.complete = p.complete && ok
		if store == nil {
			continue
		}
		p.hosts[store.GetID()] = host
		p.pairs += p.counts[host]
		p.counts[host]++
		p.maxCount = max(p.maxCount, p.counts[host])
	}
	return p
}

// allowsMove checks one voter replacement without rescanning the other voters.
// An existing voter store is only reserved by scatter; it does not move the source.
func (p *scatterHostPlacement) allowsMove(source uint64, target *core.StoreInfo) bool {
	if _, exists := p.hosts[target.GetID()]; exists {
		return true
	}
	host, ok := scatterHostKey(target, p.labels)
	if !p.complete || !ok {
		return false
	}
	from, exists := p.hosts[source]
	if !exists {
		return false
	}
	if from == host {
		return true
	}
	// Removing a voter removes count(source)-1 pairs; adding it creates count(target).
	return p.counts[host] < p.counts[from] && p.counts[host]+1 <= p.maxCount
}

func (p *scatterHostPlacement) allowsPlacement(stores []*core.StoreInfo) bool {
	after := newScatterHostPlacement(p.labels, stores)
	if !p.complete || !after.complete {
		// Unknown topology cannot justify new voter destinations. Leader changes
		// and learner moves can still proceed with the same voter stores.
		return maps.Equal(p.hosts, after.hosts)
	}
	return after.pairs <= p.pairs && after.maxCount <= p.maxCount
}
