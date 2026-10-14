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
	"context"
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
	checkerName    string // replica-checker / rule-checker
	cluster        sche.CheckerCluster
	locationLabels []string
	isolationLevel string
	region         *core.RegionInfo
	extraFilters   []filter.Filter
	fastFailover   bool
}

type storeSelectionProgress struct {
	storeSnapshot     []*core.StoreInfo
	sourceStoreID     uint64
	storeIndex        int
	targetIndex       int
	topCandidates     []*core.StoreInfo
	selectionComplete bool
	bestTarget        *core.StoreInfo
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
	target, temporary, done, _, _ := s.selectStoreToAddProgress(context.Background(), coLocationStores, extraFilters, s.cluster.GetStores(), nil, 0)
	if !done {
		return 0, false
	}
	return target, temporary
}

func (s *ReplicaStrategy) selectStoreToAddProgress(ctx context.Context, coLocationStores []*core.StoreInfo,
	extraFilters []filter.Filter, storeSnapshot []*core.StoreInfo, progress *storeSelectionProgress, budget int) (targetID uint64, temporary, done bool, checks int, next *storeSelectionProgress) {
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
	filters = append(filters, extraFilters...)
	filters = append(filters, s.extraFilters...)
	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	strictStateFilter := &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowFastFailover: s.fastFailover, OperatorLevel: level}
	strictFilters := append(slices.Clone(filters), strictStateFilter)
	if progress == nil {
		progress = &storeSelectionProgress{storeSnapshot: slices.Clone(storeSnapshot)}
	}
	for !progress.selectionComplete && progress.storeIndex < len(progress.storeSnapshot) {
		if ctx.Err() != nil || (budget > 0 && checks >= budget) {
			return 0, false, false, checks, progress
		}
		store := progress.storeSnapshot[progress.storeIndex]
		progress.storeIndex++
		checks++
		if store == nil {
			continue
		}
		candidate := filter.NewCandidates([]*core.StoreInfo{store}).
			FilterTarget(s.cluster.GetCheckerConfig(), nil, nil, filters...)
		if candidate.Len() > 0 {
			if len(progress.topCandidates) == 0 {
				progress.topCandidates = append(progress.topCandidates, store)
			} else {
				compare := isolationComparer(progress.topCandidates[0], store)
				if compare == 0 {
					progress.topCandidates = append(progress.topCandidates, store)
				} else if compare < 0 {
					progress.topCandidates = []*core.StoreInfo{store}
				}
			}
		}
	}
	if progress.storeIndex >= len(progress.storeSnapshot) {
		progress.selectionComplete = true
	}
	for progress.targetIndex < len(progress.topCandidates) {
		if ctx.Err() != nil || (budget > 0 && checks >= budget) {
			return 0, false, false, checks, progress
		}
		store := progress.topCandidates[progress.targetIndex]
		progress.targetIndex++
		checks++
		current := s.cluster.GetStore(store.GetID())
		if current == nil {
			continue
		}
		candidate := filter.NewCandidates([]*core.StoreInfo{current}).
			FilterTarget(s.cluster.GetCheckerConfig(), nil, nil, strictFilters...)
		if candidate.Len() > 0 && (progress.bestTarget == nil || filter.RegionScoreComparer(s.cluster.GetCheckerConfig())(progress.bestTarget, current) > 0) {
			progress.bestTarget = current
		}
	}
	if len(progress.topCandidates) == 0 {
		return 0, false, true, checks, nil
	}
	if progress.bestTarget == nil {
		return 0, true, true, checks, nil
	}
	return progress.bestTarget.GetID(), false, true, checks, nil
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

// SelectStoreToImprove returns a store to replace oldStore. The location
// placement after scheduling should be better than original.
func (s *ReplicaStrategy) SelectStoreToImprove(coLocationStores []*core.StoreInfo, old uint64) (uint64, bool) {
	target, temporary, done, _, _ := s.selectStoreToImproveProgress(context.Background(), coLocationStores, old, s.cluster.GetStores(), nil, 0)
	if !done {
		return 0, false
	}
	return target, temporary
}

func (s *ReplicaStrategy) selectStoreToImproveProgress(ctx context.Context, coLocationStores []*core.StoreInfo, old uint64,
	storeSnapshot []*core.StoreInfo, cursor *storeSelectionProgress, budget int) (targetID uint64, temporary, done bool, checks int, next *storeSelectionProgress) {
	if len(coLocationStores) == 0 {
		return 0, false, true, 0, nil
	}
	// trick to avoid creating a slice with `old` removed.
	swapStoreToFirst(coLocationStores, old)
	oldStore := s.cluster.GetStore(old)
	if oldStore == nil {
		return 0, false, true, 0, nil
	}
	filters := []filter.Filter{
		filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationStores, oldStore),
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores[1:]))
	}
	return s.selectStoreToAddProgress(ctx, coLocationStores[1:], filters, storeSnapshot, cursor, budget)
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

func (s *ReplicaStrategy) selectStoreToRemoveWithTempState(coLocationStores []*core.StoreInfo) (uint64, bool) {
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
		return 0, false
	}
	source := filter.NewCandidates(sourceCandidate.Stores).
		FilterSource(s.cluster.GetCheckerConfig(), nil, nil, &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, OperatorLevel: level}).
		PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetCheckerConfig()), false)
	if source != nil {
		return source.GetID(), false
	}
	source = sourceCandidate.PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetCheckerConfig()), false)
	return source.GetID(), true
}

func (s *ReplicaStrategy) getBetterLocation(cluster sche.SharedCluster, region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (oldStoreID, newStoreID uint64, filterByTempState bool) {
	oldStoreID, newStoreID, filterByTempState, _, _, _, _ = s.getBetterLocationProgress(context.Background(), cluster, region, fit, rf,
		cluster.GetStores(), nil, 0)
	return
}

func (s *ReplicaStrategy) getBetterLocationProgress(ctx context.Context, cluster sche.SharedCluster, region *core.RegionInfo,
	fit *placement.RegionFit, rf *placement.RuleFit, storeSnapshot []*core.StoreInfo, cursor *storeSelectionProgress, budget int) (oldStoreID, newStoreID uint64,
	filterByTempState, done bool, checks int, next *storeSelectionProgress, temporarySource bool) {
	ruleStores := getRuleFitStores(cluster, rf)
	oldStoreID, sourceFilterByTempState := s.selectStoreToRemoveWithTempState(ruleStores)
	if oldStoreID == 0 {
		return 0, 0, false, true, 0, nil, sourceFilterByTempState
	}
	oldStore := cluster.GetStore(oldStoreID)
	if oldStore == nil {
		return 0, 0, false, true, 0, nil, sourceFilterByTempState
	}
	if cursor != nil && cursor.sourceStoreID != 0 && cursor.sourceStoreID != oldStoreID {
		cursor = nil
	}
	var coLocationStores []*core.StoreInfo
	regionStores := cluster.GetRegionStores(region)
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
	newStoreID, filterByTempState, done, checks, next = s.selectStoreToImproveProgress(ctx, coLocationStores, oldStoreID, storeSnapshot, cursor, budget)
	if next != nil {
		next.sourceStoreID = oldStoreID
	}
	if !done {
		return oldStoreID, 0, false, false, checks, next, sourceFilterByTempState
	}
	if sourceFilterByTempState {
		if newStoreID != 0 || filterByTempState {
			return 0, 0, true, true, checks, nil, true
		}
		return 0, 0, false, true, checks, nil, true
	}
	return oldStoreID, newStoreID, filterByTempState, true, checks, nil, false
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
