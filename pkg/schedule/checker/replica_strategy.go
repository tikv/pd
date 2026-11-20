// Copyright 2020 TiKV Project Authors.
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

package checker

import (
	"slices"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/versioninfo"
)

// ReplicaStrategy collects some utilities to manipulate region peers. It
// exists to allow replica_checker and rule_checker to reuse common logics.
type ReplicaStrategy struct {
	checkerName      string // replica-checker / rule-checker
	cluster          sche.CheckerCluster
	locationLabels   []string
	isolationLevel   string
	region           *core.RegionInfo
	extraFilters     []filter.Filter
	fastFailover     bool
	allowReplacement func(oldStoreID, newStoreID uint64) bool
}

// SelectStoreToAdd returns the store to add a replica to a region.
// `coLocationStores` are the stores used to compare location with target
// store.
// `extraFilters` is used to set up more filters based on the context that
// calling this method.
//
// For example, to select a target store to replace a region's peer, we can use
// the peer list with the peer removed as `coLocationStores`.
// Meanwhile, we need to provide more constraints to ensure that the isolation
// level cannot be reduced after replacement.
func (s *ReplicaStrategy) SelectStoreToAdd(coLocationStores []*core.StoreInfo, extraFilters ...filter.Filter) (uint64, bool) {
	storeID, filterByTempState, _ := s.selectReplicaTarget(coLocationStores, 0, extraFilters...)
	return storeID, filterByTempState
}

func (s *ReplicaStrategy) selectReplicaTarget(coLocationStores []*core.StoreInfo, oldStoreID uint64, extraFilters ...filter.Filter) (storeID uint64, filterByTempState, replacementRejected bool) {
	// The selection process uses a two-stage fashion. The first stage
	// ignores the temporary state of the stores and selects the stores
	// with the highest score according to the location label. The second
	// stage considers all temporary states and capacity factors to select
	// the most suitable target.
	//
	// The reason for it is to prevent the non-optimal replica placement due
	// to the short-term state, resulting in redundant scheduling.
	level := constant.High
	if s.fastFailover {
		level = constant.Urgent
	}
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.checkerName, nil, s.region.GetStoreIDs()),
		filter.NewStorageThresholdFilter(s.checkerName),
		filter.NewSpecialUseFilter(s.checkerName),
		&filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowTemporaryStates: true, OperatorLevel: level},
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores))
	}
	if len(extraFilters) > 0 {
		filters = append(filters, extraFilters...)
	}
	if len(s.extraFilters) > 0 {
		filters = append(filters, s.extraFilters...)
	}

	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	strictStateFilter := &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowFastFailover: s.fastFailover, OperatorLevel: level}
	targetCandidate := filter.NewCandidates(s.cluster.GetStores()).
		FilterTarget(s.cluster.GetCheckerConfig(), nil, nil, filters...).
		KeepTheTopStores(isolationComparer, false) // greater isolation score is better
	if targetCandidate.Len() == 0 {
		return 0, false, false
	}
	targetCandidate.FilterTarget(s.cluster.GetCheckerConfig(), nil, nil, strictStateFilter)
	compare := filter.RegionScoreComparer(s.cluster.GetCheckerConfig())
	target := targetCandidate.PickTheTopStore(compare, true) // less region score is better
	if target == nil {
		return 0, true, false // filter by temporary states
	}
	if oldStoreID == 0 || s.allowReplacement == nil || s.allowReplacement(oldStoreID, target.GetID()) {
		return target.GetID(), false, false
	}
	// Rank the alternatives only when the preferred store has no feasible plan.
	slices.SortStableFunc(targetCandidate.Stores, compare)
	for _, candidate := range targetCandidate.Stores {
		if candidate != target && s.allowReplacement(oldStoreID, candidate.GetID()) {
			return candidate.GetID(), false, false
		}
	}
	return 0, false, true // the caller can recheck on the normal patrol
}

// SelectStoreToFix returns a store to replace down/offline old peer. The location
// placement after scheduling is allowed to be worse than original.
func (s *ReplicaStrategy) SelectStoreToFix(coLocationStores []*core.StoreInfo, old uint64) (uint64, bool) {
	if len(coLocationStores) == 0 {
		return 0, false
	}
	// trick to avoid creating a slice with `old` removed.
	swapStoreToFirst(coLocationStores, old)
	// If the coLocationStores only has one store, no need to remove.
	// Otherwise, the other stores will be filtered.
	if len(coLocationStores) > 1 {
		coLocationStores = coLocationStores[1:]
	}
	storeID, filterByTempState, _ := s.selectReplicaTarget(coLocationStores, old)
	return storeID, filterByTempState
}

// SelectStoreToImprove returns a store to replace oldStore. The location
// placement after scheduling should be better than original.
func (s *ReplicaStrategy) SelectStoreToImprove(coLocationStores []*core.StoreInfo, old uint64) (uint64, bool) {
	storeID, filterByTempState, _ := s.selectImprovementTarget(coLocationStores, old)
	return storeID, filterByTempState
}

func (s *ReplicaStrategy) selectImprovementTarget(coLocationStores []*core.StoreInfo, old uint64) (storeID uint64, filterByTempState, replacementRejected bool) {
	if len(coLocationStores) == 0 {
		return 0, false, false
	}
	// trick to avoid creating a slice with `old` removed.
	swapStoreToFirst(coLocationStores, old)
	oldStore := s.cluster.GetStore(old)
	if oldStore == nil {
		return 0, false, false
	}
	filters := []filter.Filter{
		filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationStores, oldStore),
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores[1:]))
	}
	return s.selectReplicaTarget(coLocationStores[1:], old, filters...)
}

func swapStoreToFirst(stores []*core.StoreInfo, id uint64) {
	for i, s := range stores {
		if s.GetID() == id {
			stores[0], stores[i] = stores[i], stores[0]
			return
		}
	}
}

// SelectStoreToRemove returns the best option to remove from the region.
func (s *ReplicaStrategy) SelectStoreToRemove(coLocationStores []*core.StoreInfo) uint64 {
	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	level := constant.High
	if s.fastFailover {
		level = constant.Urgent
	}
	source := filter.NewCandidates(coLocationStores).
		FilterSource(s.cluster.GetCheckerConfig(), nil, nil, &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, OperatorLevel: level}).
		KeepTheTopStores(isolationComparer, true).
		PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetCheckerConfig()), false)
	if source == nil {
		log.Debug("no removable store", zap.Uint64("region-id", s.region.GetID()))
		return 0
	}
	return source.GetID()
}

func (s *ReplicaStrategy) selectStoreToRemoveWithTempState(coLocationStores []*core.StoreInfo) (source *core.StoreInfo, readySources []*core.StoreInfo, filterByTempState bool) {
	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	level := constant.High
	if s.fastFailover {
		level = constant.Urgent
	}
	sourceCandidate := filter.NewCandidates(coLocationStores).
		FilterSource(s.cluster.GetCheckerConfig(), nil, nil, &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowTemporaryStates: true, OperatorLevel: level}).
		KeepTheTopStores(isolationComparer, true)
	if sourceCandidate.Len() == 0 {
		log.Debug("no removable store", zap.Uint64("region-id", s.region.GetID()))
		return nil, nil, false
	}
	ready := filter.NewCandidates(sourceCandidate.Stores).
		FilterSource(s.cluster.GetCheckerConfig(), nil, nil, &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, OperatorLevel: level})
	compare := filter.RegionScoreComparer(s.cluster.GetCheckerConfig())
	source = ready.PickTheTopStore(compare, false)
	if source != nil {
		return source, ready.Stores, false
	}
	return sourceCandidate.PickTheTopStore(compare, false), nil, true
}

func (s *ReplicaStrategy) getBetterLocation(cluster sche.SharedCluster, region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (oldStoreID, newStoreID uint64, filterByTempState bool) {
	ruleStores := getRuleFitStores(cluster, rf)
	source, readySources, sourceFilterByTempState := s.selectStoreToRemoveWithTempState(ruleStores)
	if source == nil {
		return 0, 0, false
	}
	oldStoreID = source.GetID()
	oldStore := cluster.GetStore(oldStoreID)
	if oldStore == nil {
		return 0, 0, false
	}
	regionStores := cluster.GetRegionStores(region)
	newStoreID, filterByTempState, replacementRejected := s.selectBetterLocationTarget(regionStores, fit, rf, oldStore)
	if sourceFilterByTempState {
		return 0, 0, newStoreID != 0 || filterByTempState
	}
	if !replacementRejected {
		return
	}

	// Keep the original isolation tier and source order. A move from a
	// different source can preserve leadership when moving the first cannot.
	compare := filter.RegionScoreComparer(s.cluster.GetCheckerConfig())
	slices.SortStableFunc(readySources, func(a, b *core.StoreInfo) int { return compare(b, a) })
	for _, candidate := range readySources {
		if candidate == source {
			continue
		}
		store := cluster.GetStore(candidate.GetID())
		if store == nil {
			continue
		}
		if storeID, _, _ := s.selectBetterLocationTarget(regionStores, fit, rf, store); storeID != 0 {
			return store.GetID(), storeID, false
		}
	}
	return
}

func (s *ReplicaStrategy) selectBetterLocationTarget(regionStores []*core.StoreInfo, fit *placement.RegionFit, rf *placement.RuleFit, oldStore *core.StoreInfo) (storeID uint64, filterByTempState, replacementRejected bool) {
	var coLocationStores []*core.StoreInfo
	for _, store := range regionStores {
		if store.GetLabelValue(core.EngineKey) != oldStore.GetLabelValue(core.EngineKey) {
			continue
		}
		for _, r := range fit.GetRules() {
			if r.Role != rf.Rule.Role {
				continue
			}
			if placement.MatchLabelConstraints(store, r.LabelConstraints) {
				coLocationStores = append(coLocationStores, store)
				break
			}
		}
	}
	return s.selectImprovementTarget(coLocationStores, oldStore.GetID())
}

func isWitnessEnabled(cluster sche.CheckerCluster) bool {
	config := cluster.GetCheckerConfig()
	return versioninfo.IsFeatureSupported(config.GetClusterVersion(), versioninfo.SwitchWitness) && config.IsWitnessAllowed()
}

func getRuleFitStores(cluster sche.SharedCluster, rf *placement.RuleFit) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(rf.Peers))
	for _, p := range rf.Peers {
		if s := cluster.GetStore(p.GetStoreId()); s != nil {
			stores = append(stores, s)
		}
	}
	return stores
}
