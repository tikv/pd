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

package server

import "github.com/pingcap/pd/server/core"

// Filter is an interface to filter source and target store.
type Filter interface {
	// Return true if the store should not be used as a source store.
	FilterSource(store *core.StoreInfo) bool
	// Return true if the store should not be used as a target store.
	FilterTarget(store *core.StoreInfo) bool
}

func filterSource(store *core.StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterSource(store) {
			return true
		}
	}
	return false
}

func filterTarget(store *core.StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterTarget(store) {
			return true
		}
	}
	return false
}

type excludedFilter struct {
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

func newExcludedFilter(sources, targets map[uint64]struct{}) *excludedFilter {
	return &excludedFilter{
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) FilterSource(store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetId()]
	return ok
}

func (f *excludedFilter) FilterTarget(store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetId()]
	return ok
}

type blockFilter struct{}

func newBlockFilter() *blockFilter {
	return &blockFilter{}
}

func (f *blockFilter) FilterSource(store *core.StoreInfo) bool {
	return store.IsBlocked()
}

func (f *blockFilter) FilterTarget(store *core.StoreInfo) bool {
	return store.IsBlocked()
}

type cacheFilter struct {
	cache *idCache
}

func newCacheFilter(cache *idCache) *cacheFilter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) FilterSource(store *core.StoreInfo) bool {
	return f.cache.get(store.GetId())
}

func (f *cacheFilter) FilterTarget(store *core.StoreInfo) bool {
	return false
}

type stateFilter struct {
	opt *scheduleOption
}

func newStateFilter(opt *scheduleOption) *stateFilter {
	return &stateFilter{opt: opt}
}

func (f *stateFilter) filter(store *core.StoreInfo) bool {
	return !store.IsUp()
}

func (f *stateFilter) FilterSource(store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.filter(store)
}

type healthFilter struct {
	opt *scheduleOption
}

func newHealthFilter(opt *scheduleOption) *healthFilter {
	return &healthFilter{opt: opt}
}

func (f *healthFilter) filter(store *core.StoreInfo) bool {
	if store.Stats.GetIsBusy() {
		return true
	}
	return store.DownTime() > f.opt.GetMaxStoreDownTime()
}

func (f *healthFilter) FilterSource(store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *healthFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.filter(store)
}

type snapshotCountFilter struct {
	opt *scheduleOption
}

func newSnapshotCountFilter(opt *scheduleOption) *snapshotCountFilter {
	return &snapshotCountFilter{opt: opt}
}

func (f *snapshotCountFilter) filter(store *core.StoreInfo) bool {
	return uint64(store.Stats.GetSendingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.Stats.GetReceivingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.Stats.GetApplyingSnapCount()) > f.opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) FilterSource(store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *snapshotCountFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.filter(store)
}

// storageThresholdFilter ensures that we will not use an almost full store as a target.
type storageThresholdFilter struct{}

const storageAvailableRatioThreshold = 0.2

func newStorageThresholdFilter(opt *scheduleOption) *storageThresholdFilter {
	return &storageThresholdFilter{}
}

func (f *storageThresholdFilter) FilterSource(store *core.StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(store *core.StoreInfo) bool {
	return store.AvailableRatio() < storageAvailableRatioThreshold
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	rep       *Replication
	stores    []*core.StoreInfo
	safeScore float64
}

func newDistinctScoreFilter(rep *Replication, stores []*core.StoreInfo, source *core.StoreInfo) *distinctScoreFilter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetId() == source.GetId() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		rep:       rep,
		stores:    newStores,
		safeScore: rep.GetDistinctScore(newStores, source),
	}
}

func (f *distinctScoreFilter) FilterSource(store *core.StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.rep.GetDistinctScore(f.stores, store) < f.safeScore
}
