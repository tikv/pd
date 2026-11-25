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

package diagnostics

import (
	"bytes"
	"context"
	"regexp"
	"slices"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

var partitionGroupID = regexp.MustCompile(`^_tidb_(?:ks(0|[1-9][0-9]*)_)?pt_([1-9][0-9]*)_p([1-9][0-9]*)$`)

type tableKey struct {
	keyspace string
	table    string
}

type tableSpread struct {
	leader *uint64
	voter  *uint64
}

type spreadRole uint8

const (
	leaderRole spreadRole = iota
	voterRole
)

func (c *collector) spreadFor(
	ctx context.Context,
	snapshot *affinity.GroupSamplingSnapshot,
	samples map[string]*regionSample,
) (leader, voter *uint64, err error) {
	key, ok := groupTableKey(snapshot.State.ID)
	if !ok {
		return nil, nil, nil
	}
	if spread, ok := c.spreads[key]; ok {
		return spread.leader, spread.voter, nil
	}
	spread, err := c.buildTableSpread(ctx, key, samples)
	if err != nil {
		return nil, nil, err
	}
	c.spreads[key] = spread
	return spread.leader, spread.voter, nil
}

func groupTableKey(id string) (tableKey, bool) {
	parts := partitionGroupID.FindStringSubmatch(id)
	if parts == nil {
		return tableKey{}, false
	}
	return tableKey{keyspace: parts[1], table: parts[2]}, true
}

func (c *collector) buildTableSpread(
	ctx context.Context,
	key tableKey,
	samples map[string]*regionSample,
) (*tableSpread, error) {
	states := c.manager.GetAllAffinityGroupStates()
	ids := make([]string, 0, len(states))
	for _, state := range states {
		if state == nil {
			continue
		}
		if stateKey, ok := groupTableKey(state.ID); ok && stateKey == key {
			ids = append(ids, state.ID)
		}
	}
	slices.Sort(ids)
	snapshots := make([]*affinity.GroupSamplingSnapshot, 0, len(ids))
	leaderCounts := make(map[uint64]uint64)
	voterCounts := make(map[uint64]uint64)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		snapshot, known := c.spreadSnapshot(id, samples)
		if !known {
			return &tableSpread{}, nil
		}
		snapshots = append(snapshots, snapshot)
		if !hasTarget(snapshot.State) {
			continue
		}
		leaderCounts[snapshot.State.LeaderStoreID]++
		seenVoters := make(map[uint64]struct{}, len(snapshot.State.VoterStoreIDs))
		for _, storeID := range snapshot.State.VoterStoreIDs {
			if _, ok := seenVoters[storeID]; ok {
				continue
			}
			seenVoters[storeID] = struct{}{}
			voterCounts[storeID]++
		}
	}
	if len(snapshots) == 0 {
		return &tableSpread{}, nil
	}
	leaderCandidates, voterCandidates, known, err := c.candidateStores(ctx, snapshots)
	if err != nil {
		return nil, err
	}
	if !known {
		return &tableSpread{}, nil
	}
	return &tableSpread{
		leader: spreadValue(leaderCandidates, leaderCounts),
		voter:  spreadValue(voterCandidates, voterCounts),
	}, nil
}

func (c *collector) spreadSnapshot(
	id string,
	samples map[string]*regionSample,
) (*affinity.GroupSamplingSnapshot, bool) {
	if sample, ok := samples[id]; ok {
		if sample == nil || !sample.stable || sample.snapshot == nil {
			return nil, false
		}
		return sample.snapshot, true
	}
	before := c.manager.GetGroupSamplingSnapshot(id)
	if before == nil {
		return nil, false
	}
	after := c.manager.GetGroupSamplingSnapshot(id)
	if !affinity.SameGroupSampling(before, after) {
		return nil, false
	}
	return before, true
}

func (c *collector) candidateStores(
	ctx context.Context,
	snapshots []*affinity.GroupSamplingSnapshot,
) (leaderCandidates, voterCandidates map[uint64]struct{}, known bool, err error) {
	ruleManager := c.cluster.GetRuleManager()
	if ruleManager == nil {
		return nil, nil, false, nil
	}
	stores := c.cluster.GetStores()
	healthy := map[spreadRole]map[uint64]bool{
		leaderRole: make(map[uint64]bool, len(stores)),
		voterRole:  make(map[uint64]bool, len(stores)),
	}
	for _, store := range stores {
		healthy[leaderRole][store.GetID()] = c.healthyStore(store, leaderRole)
		healthy[voterRole][store.GetID()] = c.healthyStore(store, voterRole)
	}
	leaders := make(map[uint64]struct{})
	voters := make(map[uint64]struct{})
	for _, snapshot := range snapshots {
		for _, keyRange := range snapshot.Ranges.KeyRanges {
			segments := placementSegments(ruleManager, keyRange)
			for _, segment := range segments {
				if err := ctx.Err(); err != nil {
					return nil, nil, false, err
				}
				rules := ruleManager.GetRulesForApplyRange(segment.StartKey, segment.EndKey)
				if len(rules) == 0 {
					return nil, nil, false, nil
				}
				for _, store := range stores {
					storeID := store.GetID()
					if healthy[leaderRole][storeID] && matchesPlacementRole(rules, store, leaderRole) {
						leaders[storeID] = struct{}{}
					}
					if healthy[voterRole][storeID] && matchesPlacementRole(rules, store, voterRole) {
						voters[storeID] = struct{}{}
					}
				}
			}
		}
	}
	return leaders, voters, true, nil
}

func placementSegments(manager *placement.RuleManager, keyRange keyutil.KeyRange) []keyutil.KeyRange {
	splitKeys := manager.GetSplitKeys(keyRange.StartKey, keyRange.EndKey)
	slices.SortFunc(splitKeys, bytes.Compare)
	segments := make([]keyutil.KeyRange, 0, len(splitKeys)+1)
	start := keyRange.StartKey
	for _, split := range splitKeys {
		if bytes.Compare(split, start) <= 0 || len(keyRange.EndKey) > 0 && bytes.Compare(split, keyRange.EndKey) >= 0 {
			continue
		}
		segments = append(segments, keyutil.KeyRange{StartKey: start, EndKey: split})
		start = split
	}
	return append(segments, keyutil.KeyRange{StartKey: start, EndKey: keyRange.EndKey})
}

func matchesPlacementRole(rules []*placement.Rule, store *core.StoreInfo, role spreadRole) bool {
	for _, rule := range rules {
		matchesRole := false
		switch role {
		case leaderRole:
			matchesRole = (rule.Role == placement.Voter || rule.Role == placement.Leader) && !rule.IsWitness
		case voterRole:
			matchesRole = (rule.Role == placement.Voter || rule.Role == placement.Follower || rule.Role == placement.Leader) && !rule.IsWitness
		}
		if matchesRole && placement.MatchLabelConstraints(store, rule.LabelConstraints) {
			return true
		}
	}
	return false
}

func (c *collector) healthyStore(store *core.StoreInfo, role spreadRole) bool {
	if store == nil || store.IsRemoved() || store.IsPhysicallyDestroyed() || store.IsRemoving() || store.IsUnhealthy() ||
		store.IsDisconnected() || store.IsLowSpace(c.cluster.GetSharedConfig().GetLowSpaceRatio()) || store.IsPreparing() {
		return false
	}
	if role == voterRole {
		return true
	}
	return !store.IsBusy() && store.AllowLeaderTransferIn() && !store.EvictedAsSlowStore() &&
		!store.EvictedAsStoppingStore() && !store.IsEvictedAsSlowTrend() &&
		!c.cluster.GetSharedConfig().CheckLabelProperty(config.RejectLeader, store.GetLabels())
}

func spreadValue(candidates map[uint64]struct{}, counts map[uint64]uint64) *uint64 {
	if len(candidates) == 0 {
		return nil
	}
	stores := make(map[uint64]struct{}, len(candidates)+len(counts))
	for storeID := range candidates {
		stores[storeID] = struct{}{}
	}
	for storeID := range counts {
		stores[storeID] = struct{}{}
	}
	var minCount, maxCount uint64
	first := true
	for storeID := range stores {
		count := counts[storeID]
		if first || count < minCount {
			minCount = count
		}
		if first || count > maxCount {
			maxCount = count
		}
		first = false
	}
	spread := maxCount - minCount
	return &spread
}
