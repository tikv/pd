// Copyright 2016 PingCAP, Inc.
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

package selector

import (
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/opt"
)

// ReplicaSelector selects source/target store candidates based on their
// distinct scores based on a region's peer stores.
type ReplicaSelector struct {
	regionStores []*core.StoreInfo
	labels       []string
	filters      []filter.Filter
}

// NewReplicaSelector creates a ReplicaSelector instance.
func NewReplicaSelector(regionStores []*core.StoreInfo, labels []string, filters ...filter.Filter) *ReplicaSelector {
	return &ReplicaSelector{
		regionStores: regionStores,
		labels:       labels,
		filters:      filters,
	}
}

// SelectSource selects the store that can pass all filters and has the minimal
// distinct score.
func (s *ReplicaSelector) SelectSource(opt opt.Options, stores []*core.StoreInfo) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		score := core.DistinctScore(s.labels, s.regionStores, store)
		if best == nil || compareStoreScore(opt, store, score, best, bestScore) < 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || !filter.Source(opt, best, s.filters) {
		return nil
	}
	return best
}

// SelectTarget selects the store that can pass all filters and has the maximal
// distinct score.
func (s *ReplicaSelector) SelectTarget(opt opt.Options, stores []*core.StoreInfo, filters ...filter.Filter) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		if !filter.Target(opt, store, filters) {
			continue
		}
		score := core.DistinctScore(s.labels, s.regionStores, store)
		if best == nil || compareStoreScore(opt, store, score, best, bestScore) > 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || !filter.Target(opt, best, s.filters) {
		return nil
	}
	return best
}

// compareStoreScore compares which store is better for replication.
// Returns 0 if store A is as good as store B.
// Returns 1 if store A is better than store B.
// Returns -1 if store B is better than store A.
func compareStoreScore(opt opt.Options, storeA *core.StoreInfo, scoreA float64, storeB *core.StoreInfo, scoreB float64) int {
	// The store with higher score is better.
	if scoreA > scoreB {
		return 1
	}
	if scoreA < scoreB {
		return -1
	}
	// The store with lower region score is better.
	if storeA.RegionScore(opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) <
		storeB.RegionScore(opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) {
		return 1
	}
	if storeA.RegionScore(opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) >
		storeB.RegionScore(opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) {
		return -1
	}
	return 0
}
