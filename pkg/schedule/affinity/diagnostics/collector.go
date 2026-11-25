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
	"encoding/binary"
	"math"
	"slices"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

const observedAtLayout = "2006-01-02T15:04:05.000000Z"

var blockedTexts = map[string]string{
	"no_key_ranges":              "The affinity group has no key ranges.",
	"range_conflict":             "The affinity group has conflicting key ranges.",
	"target_not_determined":      "The affinity group target stores are not determined.",
	"target_store_degraded":      "An affinity group target store is degraded.",
	"target_store_expired":       "An affinity group target store is expired.",
	"coverage_gap":               "The affinity group key ranges have Region coverage gaps.",
	"cross_boundary_region":      "A Region crosses an affinity group or key range boundary.",
	"region_without_leader":      "A Region in the affinity group has no leader.",
	"sampling_changed":           "The affinity group changed while diagnostics were being sampled.",
	"affinity_scheduling_paused": "Affinity scheduling is paused.",
}

type samplingManager interface {
	GetGroupSamplingSnapshot(string) *affinity.GroupSamplingSnapshot
	GetAllAffinityGroupStates() []*affinity.GroupState
}

type collector struct {
	manager    samplingManager
	cluster    sche.SchedulerCluster
	operators  *operator.Controller
	observedAt string
	spreads    map[tableKey]*tableSpread
}

type regionSample struct {
	snapshot      *affinity.GroupSamplingSnapshot
	regions       []*core.RegionInfo
	stable        bool
	coverageGap   bool
	crossBoundary bool
	withoutLeader bool
}

// CollectGroupStates returns affinity group states with point-in-time diagnostics.
func CollectGroupStates(
	ctx context.Context,
	manager *affinity.Manager,
	cluster sche.SchedulerCluster,
	operators *operator.Controller,
	rawIDs []string,
) (map[string]*affinity.GroupState, error) {
	states, err := affinity.CollectGroupStates(manager, rawIDs)
	if err != nil {
		return nil, err
	}
	c := &collector{
		manager:    manager,
		cluster:    cluster,
		operators:  operators,
		observedAt: time.Now().UTC().Format(observedAtLayout),
		spreads:    make(map[tableKey]*tableSpread),
	}
	ids := make([]string, 0, len(states))
	for id := range states {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	samples := make(map[string]*regionSample, len(states))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sample, err := c.sample(ctx, id)
		if err != nil {
			return nil, err
		}
		samples[id] = sample
		state := states[id]
		state.Diagnostics = c.newDiagnostics(sample)
		if err := c.attachOperators(ctx, state.Diagnostics, sample); err != nil {
			return nil, err
		}
	}
	for _, id := range ids {
		sample := samples[id]
		if sample == nil || !sample.stable || sample.snapshot == nil {
			continue
		}
		leaderSpread, voterSpread, err := c.spreadFor(ctx, sample.snapshot, samples)
		if err != nil {
			return nil, err
		}
		states[id].Diagnostics.LeaderSpread = leaderSpread
		states[id].Diagnostics.VoterSpread = voterSpread
	}
	return states, nil
}

func (c *collector) sample(ctx context.Context, id string) (*regionSample, error) {
	for attempt := range 2 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		before := c.manager.GetGroupSamplingSnapshot(id)
		if before == nil {
			return &regionSample{}, nil
		}
		sample, err := c.scan(ctx, before)
		if err != nil {
			return nil, err
		}
		after := c.manager.GetGroupSamplingSnapshot(id)
		if affinity.SameGroupSampling(before, after) {
			sample.stable = true
			return sample, nil
		}
		sample.snapshot = after
		if attempt == 1 {
			return sample, nil
		}
	}
	panic("unreachable")
}

func (c *collector) scan(ctx context.Context, snapshot *affinity.GroupSamplingSnapshot) (*regionSample, error) {
	sample := &regionSample{snapshot: snapshot, regions: make([]*core.RegionInfo, 0)}
	seen := make(map[uint64]struct{})
	labeler := c.cluster.GetRegionLabeler()
	for _, keyRange := range snapshot.Ranges.KeyRanges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		regions := c.cluster.ScanRegions(keyRange.StartKey, keyRange.EndKey, -1)
		if len(regions) == 0 {
			sample.coverageGap = true
			continue
		}
		cursor := keyRange.StartKey
		for _, region := range regions {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if region == nil {
				sample.coverageGap = true
				continue
			}
			start, end := region.GetStartKey(), region.GetEndKey()
			contained := bytes.Compare(start, keyRange.StartKey) >= 0 && endWithin(end, keyRange.EndKey)
			adjacent := bytes.Equal(start, cursor)
			if !contained {
				sample.crossBoundary = true
			}
			if !adjacent {
				if bytes.Compare(start, cursor) < 0 {
					sample.crossBoundary = true
				} else {
					sample.coverageGap = true
				}
			}
			if contained && adjacent {
				if labeler != nil {
					groupID := labeler.GetRegionLabel(region, affinity.AffinityLabelKey)
					if groupID != snapshot.State.ID {
						sample.crossBoundary = true
					}
				}
				if _, ok := seen[region.GetID()]; !ok {
					seen[region.GetID()] = struct{}{}
					sample.regions = append(sample.regions, region)
					if region.GetLeader() == nil {
						sample.withoutLeader = true
					}
				}
				cursor = end
			}
		}
		if !bytes.Equal(cursor, keyRange.EndKey) {
			sample.coverageGap = true
		}
	}
	return sample, nil
}

func endWithin(end, limit []byte) bool {
	return len(limit) == 0 || len(end) > 0 && bytes.Compare(end, limit) <= 0
}

func sampleComplete(sample *regionSample) bool {
	return sample != nil && sample.stable && sample.snapshot != nil &&
		!sample.coverageGap && !sample.crossBoundary
}

func (c *collector) newDiagnostics(sample *regionSample) *affinity.GroupDiagnostics {
	d := &affinity.GroupDiagnostics{
		ObservedAt:       c.observedAt,
		RunningOperators: make([]affinity.OperatorCount, 0),
		BlockedReasons:   make([]affinity.BlockedReason, 0),
	}
	if sampleComplete(sample) {
		d.ActualLeaderStoreIDs, d.ActualVoterStoreIDs = actualLayouts(sample.regions)
		size, pending, sizeKnown, pendingKnown := regionSizes(sample, c.cluster)
		if sizeKnown {
			d.ApproximateSizeMiB = &size
		}
		if pendingKnown {
			d.PendingMoveMiB = &pending
		}
	}
	reasons := make(map[string]struct{})
	if sample == nil || !sample.stable || sample.snapshot == nil {
		reasons["sampling_changed"] = struct{}{}
	} else {
		snapshot := sample.snapshot
		if len(snapshot.Ranges.KeyRanges) == 0 {
			reasons["no_key_ranges"] = struct{}{}
		}
		if snapshot.RangeConflict {
			reasons["range_conflict"] = struct{}{}
		}
		if !hasTarget(snapshot.State) {
			reasons["target_not_determined"] = struct{}{}
		}
		switch snapshot.Availability {
		case "degraded":
			reasons["target_store_degraded"] = struct{}{}
		case "expired":
			reasons["target_store_expired"] = struct{}{}
		}
		if sample.coverageGap {
			reasons["coverage_gap"] = struct{}{}
		}
		if sample.crossBoundary {
			reasons["cross_boundary_region"] = struct{}{}
		}
		if sample.withoutLeader {
			reasons["region_without_leader"] = struct{}{}
		}
		if snapshot.State.Phase != affinity.PhaseStable &&
			(c.cluster.GetSharedConfig().GetAffinityScheduleLimit() == 0 || c.cluster.IsSchedulingHalted()) {
			reasons["affinity_scheduling_paused"] = struct{}{}
		}
	}
	d.BlockedReasons = make([]affinity.BlockedReason, 0, len(reasons))
	for code := range reasons {
		d.BlockedReasons = append(d.BlockedReasons, affinity.BlockedReason{Code: code, Text: blockedTexts[code]})
	}
	slices.SortFunc(d.BlockedReasons, func(a, b affinity.BlockedReason) int {
		if a.Code < b.Code {
			return -1
		}
		if a.Code > b.Code {
			return 1
		}
		return 0
	})
	return d
}

func hasTarget(state *affinity.GroupState) bool {
	return state != nil && state.LeaderStoreID != 0 && len(state.VoterStoreIDs) > 0
}

func actualLayouts(regions []*core.RegionInfo) ([]uint64, [][]uint64) {
	leaderSet := make(map[uint64]struct{})
	combinationSet := make(map[string][]uint64)
	for _, region := range regions {
		if leader := region.GetLeader(); leader != nil {
			leaderSet[leader.GetStoreId()] = struct{}{}
		}
		voters := region.GetVoters()
		ids := make([]uint64, 0, len(voters))
		for _, voter := range voters {
			ids = append(ids, voter.GetStoreId())
		}
		slices.Sort(ids)
		ids = slices.Compact(ids)
		combinationSet[combinationKey(ids)] = ids
	}
	leaders := make([]uint64, 0, len(leaderSet))
	for id := range leaderSet {
		leaders = append(leaders, id)
	}
	slices.Sort(leaders)
	combinations := make([][]uint64, 0, len(combinationSet))
	for _, combination := range combinationSet {
		combinations = append(combinations, combination)
	}
	slices.SortFunc(combinations, slices.Compare)
	return leaders, combinations
}

func combinationKey(ids []uint64) string {
	key := make([]byte, len(ids)*8)
	for i, id := range ids {
		binary.BigEndian.PutUint64(key[i*8:], id)
	}
	return string(key)
}

func regionSizes(sample *regionSample, cluster sche.SchedulerCluster) (size, pending uint64, sizeKnown, pendingKnown bool) {
	state := sample.snapshot.State
	for _, region := range sample.regions {
		regionSize := region.GetApproximateSize()
		if regionSize < 0 {
			return 0, 0, false, false
		}
		var ok bool
		size, ok = addSize(size, uint64(regionSize))
		if !ok {
			return 0, 0, false, false
		}
	}
	if !hasTarget(state) || len(sample.snapshot.Ranges.KeyRanges) == 0 {
		return size, 0, true, false
	}
	ruleManager := cluster.GetRuleManager()
	for _, region := range sample.regions {
		regionSize := region.GetApproximateSize()
		var fit *placement.RegionFit
		if ruleManager != nil {
			fit = ruleManager.FitRegion(cluster, region)
		}
		pendingPeers := make(map[uint64]struct{}, len(region.GetPendingPeers()))
		for _, peer := range region.GetPendingPeers() {
			pendingPeers[peer.GetId()] = struct{}{}
		}
		for _, storeID := range state.VoterStoreIDs {
			peer := region.GetStorePeer(storeID)
			targetWitness, roleKnown := targetIsWitness(cluster, fit, peer, storeID)
			if !roleKnown {
				return size, 0, true, false
			}
			if targetWitness {
				continue
			}
			needsData := peer == nil || peer.GetIsWitness()
			if peer != nil {
				_, isPending := pendingPeers[peer.GetId()]
				needsData = needsData || isPending
			}
			if needsData {
				var ok bool
				pending, ok = addSize(pending, uint64(regionSize))
				if !ok {
					return size, 0, true, false
				}
			}
		}
	}
	return size, pending, true, true
}

func targetIsWitness(
	cluster sche.SchedulerCluster,
	fit *placement.RegionFit,
	peer *metapb.Peer,
	storeID uint64,
) (witness bool, known bool) {
	if fit == nil {
		return peer != nil && peer.GetIsWitness(), true
	}
	if peer != nil {
		if ruleFit := fit.GetRuleFit(peer.GetId()); ruleFit != nil {
			return ruleFit.Rule.IsWitness, true
		}
		return peer.GetIsWitness(), true
	}
	store := cluster.GetStore(storeID)
	if store == nil {
		return false, false
	}
	rules := fit.GetRules()
	if len(rules) == 0 {
		return false, true
	}
	found, witness := false, false
	ambiguousRule := func(rule *placement.Rule) bool {
		if rule.Role == placement.Learner || !placement.MatchLabelConstraints(store, rule.LabelConstraints) {
			return false
		}
		if found && witness != rule.IsWitness {
			return true
		}
		found, witness = true, rule.IsWitness
		return false
	}
	for _, ruleFit := range fit.RuleFits {
		if ruleFit == nil || ruleFit.Rule == nil || len(ruleFit.Peers) >= ruleFit.Rule.Count {
			continue
		}
		if ambiguousRule(ruleFit.Rule) {
			return false, false
		}
	}
	if found {
		return witness, true
	}
	if slices.ContainsFunc(rules, ambiguousRule) {
		return false, false
	}
	if !found {
		return false, false
	}
	return witness, true
}

func addSize(total, size uint64) (uint64, bool) {
	if math.MaxUint64-total < size {
		return 0, false
	}
	return total + size, true
}

func (c *collector) attachOperators(ctx context.Context, d *affinity.GroupDiagnostics, sample *regionSample) error {
	if !sampleComplete(sample) {
		d.RunningOperators = nil
		return nil
	}
	if len(sample.snapshot.Ranges.KeyRanges) == 0 || c.operators == nil {
		d.RunningOperators = make([]affinity.OperatorCount, 0)
		return nil
	}
	counts := make(map[string]uint64)
	for _, region := range sample.regions {
		if err := ctx.Err(); err != nil {
			return err
		}
		op := c.operators.GetOperator(region.GetID())
		if op == nil || op.Status() != operator.STARTED {
			continue
		}
		counts[op.Desc()]++
	}
	d.RunningOperators = make([]affinity.OperatorCount, 0, len(counts))
	for kind, count := range counts {
		d.RunningOperators = append(d.RunningOperators, affinity.OperatorCount{Kind: kind, Count: count})
	}
	slices.SortFunc(d.RunningOperators, func(a, b affinity.OperatorCount) int {
		if a.Kind < b.Kind {
			return -1
		}
		if a.Kind > b.Kind {
			return 1
		}
		return 0
	})
	return nil
}
