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
	checkerName    string // replica-checker / rule-checker
	cluster        sche.CheckerCluster
	locationLabels []string
	isolationLevel string
	region         *core.RegionInfo
	extraFilters   []filter.Filter
	fastFailover   bool
}

func (s *ReplicaStrategy) operatorLevel() constant.PriorityLevel {
	if s.fastFailover {
		return constant.Urgent
	}
	return constant.High
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
	// The selection process uses a two-stage fashion. The first stage
	// ignores the temporary state of the stores and selects the stores
	// with the highest score according to the location label. The second
	// stage considers all temporary states and capacity factors to select
	// the most suitable target.
	//
	// The reason for it is to prevent the non-optimal replica placement due
	// to the short-term state, resulting in redundant scheduling.
	level := s.operatorLevel()
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
		return 0, false
	}
	target := targetCandidate.FilterTarget(s.cluster.GetCheckerConfig(), nil, nil, strictStateFilter).
		PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetCheckerConfig()), true) // less region score is better
	if target == nil {
		return 0, true // filter by temporary states
	}
	return target.GetID(), false
}

type storeCandidateSet struct {
	stores     []*core.StoreInfo
	filters    []filter.Filter
	next       int
	candidates []*core.StoreInfo
}

// newStoreCandidateSet creates a lazily evaluated candidate set. Static Store
// filters are evaluated at most once per Rule, and scanning stops as soon as a
// Region finds a usable Store.
func (s *ReplicaStrategy) newStoreCandidateSet(stores []*core.StoreInfo) *storeCandidateSet {
	filters := []filter.Filter{
		filter.NewStorageThresholdFilter(s.checkerName),
		filter.NewSpecialUseFilter(s.checkerName),
		&filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowTemporaryStates: true, OperatorLevel: s.operatorLevel()},
	}
	filters = append(filters, s.extraFilters...)
	return &storeCandidateSet{stores: stores, filters: filters}
}

// hasStoreToAdd checks whether at least one Store can satisfy the Region-
// specific exclusion and isolation constraints.
func (s *ReplicaStrategy) hasStoreToAdd(
	candidateSet *storeCandidateSet,
	coLocationStores []*core.StoreInfo,
	extraFilters ...filter.Filter,
) bool {
	conf := s.cluster.GetCheckerConfig()
	var isolationFilter filter.Filter
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		isolationFilter = filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores)
	}
	matchesRegion := func(store *core.StoreInfo) bool {
		if s.region.GetStorePeer(store.GetID()) != nil ||
			(isolationFilter != nil && !isolationFilter.Target(conf, store).IsOK()) {
			return false
		}
		for _, extraFilter := range extraFilters {
			if !extraFilter.Target(conf, store).IsOK() {
				return false
			}
		}
		return true
	}
	for _, store := range candidateSet.candidates {
		if matchesRegion(store) {
			return true
		}
	}
	for candidateSet.next < len(candidateSet.stores) {
		store := candidateSet.stores[candidateSet.next]
		candidateSet.next++
		matched := true
		for _, storeFilter := range candidateSet.filters {
			if !storeFilter.Target(conf, store).IsOK() {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		candidateSet.candidates = append(candidateSet.candidates, store)
		if matchesRegion(store) {
			return true
		}
	}
	return false
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
	return s.SelectStoreToAdd(coLocationStores)
}

func (s *ReplicaStrategy) hasStoreToFix(candidateSet *storeCandidateSet, coLocationStores []*core.StoreInfo, old uint64) bool {
	if len(coLocationStores) == 0 {
		return false
	}
	swapStoreToFirst(coLocationStores, old)
	if len(coLocationStores) > 1 {
		coLocationStores = coLocationStores[1:]
	}
	return s.hasStoreToAdd(candidateSet, coLocationStores)
}

func collectCoLocationStores(cluster sche.SharedCluster, region *core.RegionInfo, fit *placement.RegionFit, ruleFit *placement.RuleFit, oldStore *core.StoreInfo) []*core.StoreInfo {
	var stores []*core.StoreInfo
	for _, store := range cluster.GetRegionStores(region) {
		if store.GetLabelValue(core.EngineKey) != oldStore.GetLabelValue(core.EngineKey) {
			continue
		}
		for _, rule := range fit.GetRules() {
			if rule.Role == ruleFit.Rule.Role && placement.MatchLabelConstraints(store, rule.LabelConstraints) {
				stores = append(stores, store)
				break
			}
		}
	}
	return stores
}

func (s *ReplicaStrategy) hasBetterLocation(
	candidateSet *storeCandidateSet,
	cluster sche.SharedCluster,
	region *core.RegionInfo,
	fit *placement.RegionFit,
	ruleFit *placement.RuleFit,
) bool {
	oldStoreID := s.selectStoreToRemoveForState(ruleFit.Stores)
	if oldStoreID == 0 {
		return false
	}
	oldStore := cluster.GetStore(oldStoreID)
	if oldStore == nil {
		return false
	}
	coLocationStores := collectCoLocationStores(cluster, region, fit, ruleFit, oldStore)
	if len(coLocationStores) == 0 {
		return false
	}
	swapStoreToFirst(coLocationStores, oldStoreID)
	locationImprover := filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationStores, oldStore)
	return s.hasStoreToAdd(candidateSet, coLocationStores[1:], locationImprover)
}

// SelectStoreToImprove returns a store to replace oldStore. The location
// placement after scheduling should be better than original.
func (s *ReplicaStrategy) SelectStoreToImprove(coLocationStores []*core.StoreInfo, old uint64) (uint64, bool) {
	if len(coLocationStores) == 0 {
		return 0, false
	}
	// trick to avoid creating a slice with `old` removed.
	swapStoreToFirst(coLocationStores, old)
	oldStore := s.cluster.GetStore(old)
	if oldStore == nil {
		return 0, false
	}
	filters := []filter.Filter{
		filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationStores, oldStore),
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores[1:]))
	}
	return s.SelectStoreToAdd(coLocationStores[1:], filters...)
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
	source := filter.NewCandidates(coLocationStores).
		FilterSource(s.cluster.GetCheckerConfig(), nil, nil, &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, OperatorLevel: s.operatorLevel()}).
		KeepTheTopStores(isolationComparer, true).
		PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetCheckerConfig()), false)
	if source == nil {
		log.Debug("no removable store", zap.Uint64("region-id", s.region.GetID()))
		return 0
	}
	return source.GetID()
}

func (s *ReplicaStrategy) selectStoreToRemoveWithTempState(coLocationStores []*core.StoreInfo) (uint64, bool) {
	return s.pickStoreToRemove(coLocationStores, true)
}

func (s *ReplicaStrategy) selectStoreToRemoveForState(coLocationStores []*core.StoreInfo) uint64 {
	storeID, _ := s.pickStoreToRemove(coLocationStores, false)
	return storeID
}

func (s *ReplicaStrategy) pickStoreToRemove(coLocationStores []*core.StoreInfo, recordMetrics bool) (uint64, bool) {
	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	level := s.operatorLevel()
	conf := s.cluster.GetCheckerConfig()
	filterSources := func(stores []*core.StoreInfo, storeFilter filter.Filter) []*core.StoreInfo {
		if recordMetrics {
			return filter.NewCandidates(stores).FilterSource(conf, nil, nil, storeFilter).Stores
		}
		selected := make([]*core.StoreInfo, 0, len(stores))
		for _, store := range stores {
			if storeFilter.Source(conf, store).IsOK() {
				selected = append(selected, store)
			}
		}
		return selected
	}
	sourceCandidate := filter.NewCandidates(filterSources(
		coLocationStores,
		&filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowTemporaryStates: true, OperatorLevel: level},
	)).
		KeepTheTopStores(isolationComparer, true)
	if sourceCandidate.Len() == 0 {
		log.Debug("no removable store", zap.Uint64("region-id", s.region.GetID()))
		return 0, false
	}
	source := filter.NewCandidates(filterSources(
		sourceCandidate.Stores,
		&filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, OperatorLevel: level},
	)).
		PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetCheckerConfig()), false)
	if source != nil {
		return source.GetID(), false
	}
	source = sourceCandidate.PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetCheckerConfig()), false)
	return source.GetID(), true
}

func (s *ReplicaStrategy) getBetterLocation(cluster sche.SharedCluster, region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (oldStoreID, newStoreID uint64, filterByTempState bool) {
	ruleStores := getRuleFitStores(cluster, rf)
	oldStoreID, sourceFilterByTempState := s.selectStoreToRemoveWithTempState(ruleStores)
	if oldStoreID == 0 {
		return 0, 0, false
	}
	oldStore := cluster.GetStore(oldStoreID)
	if oldStore == nil {
		return 0, 0, false
	}
	coLocationStores := collectCoLocationStores(cluster, region, fit, rf, oldStore)
	newStoreID, filterByTempState = s.SelectStoreToImprove(coLocationStores, oldStoreID)
	if sourceFilterByTempState {
		if newStoreID != 0 || filterByTempState {
			return 0, 0, true
		}
		return 0, 0, false
	}
	return
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
