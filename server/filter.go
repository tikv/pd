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

// Filter is an interface to filter source and target store.
type Filter interface {
	// Return true if the store should not be used as a source store.
	FilterSource(store *storeInfo) bool
	// Return true if the store should not be used as a target store.
	FilterTarget(store *storeInfo) bool
}

func filterSource(store *storeInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterSource(store) {
			return true
		}
	}
	return false
}

func filterTarget(store *storeInfo, filters []Filter) bool {
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

func (f *excludedFilter) FilterSource(store *storeInfo) bool {
	_, ok := f.sources[store.GetId()]
	return ok
}

func (f *excludedFilter) FilterTarget(store *storeInfo) bool {
	_, ok := f.targets[store.GetId()]
	return ok
}

type blockFilter struct{}

func newBlockFilter() *blockFilter {
	return &blockFilter{}
}

func (f *blockFilter) FilterSource(store *storeInfo) bool {
	return store.isBlocked()
}

func (f *blockFilter) FilterTarget(store *storeInfo) bool {
	return store.isBlocked()
}

type cacheFilter struct {
	cache *idCache
}

func newCacheFilter(cache *idCache) *cacheFilter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) FilterSource(store *storeInfo) bool {
	return f.cache.get(store.GetId())
}

func (f *cacheFilter) FilterTarget(store *storeInfo) bool {
	return false
}

type stateFilter struct {
	opt *scheduleOption
}

func newStateFilter(opt *scheduleOption) *stateFilter {
	return &stateFilter{opt: opt}
}

func (f *stateFilter) filter(store *storeInfo) bool {
	return !store.isUp()
}

func (f *stateFilter) FilterSource(store *storeInfo) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *storeInfo) bool {
	return f.filter(store)
}

type healthFilter struct {
	opt *scheduleOption
}

func newHealthFilter(opt *scheduleOption) *healthFilter {
	return &healthFilter{opt: opt}
}

func (f *healthFilter) filter(store *storeInfo) bool {
	if store.stats.GetIsBusy() {
		return true
	}
	return store.downTime() > f.opt.GetMaxStoreDownTime()
}

func (f *healthFilter) FilterSource(store *storeInfo) bool {
	return f.filter(store)
}

func (f *healthFilter) FilterTarget(store *storeInfo) bool {
	return f.filter(store)
}

type snapshotCountFilter struct {
	opt *scheduleOption
}

func newSnapshotCountFilter(opt *scheduleOption) *snapshotCountFilter {
	return &snapshotCountFilter{opt: opt}
}

func (f *snapshotCountFilter) filter(store *storeInfo) bool {
	return uint64(store.stats.GetSendingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.stats.GetReceivingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.stats.GetApplyingSnapCount()) > f.opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) FilterSource(store *storeInfo) bool {
	return f.filter(store)
}

func (f *snapshotCountFilter) FilterTarget(store *storeInfo) bool {
	return f.filter(store)
}

// storageThresholdFilter ensures that we will not use an almost full store as a target.
type storageThresholdFilter struct{}

const storageRatioThreshold = 0.8

func newStorageThresholdFilter(opt *scheduleOption) *storageThresholdFilter {
	return &storageThresholdFilter{}
}

func (f *storageThresholdFilter) FilterSource(store *storeInfo) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(store *storeInfo) bool {
	return store.storageRatio() > storageRatioThreshold
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	rep       *Replication
	stores    []*storeInfo
	safeScore float64
}

func newDistinctScoreFilter(rep *Replication, stores []*storeInfo, source *storeInfo) *distinctScoreFilter {
	newStores := make([]*storeInfo, 0, len(stores)-1)
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

func (f *distinctScoreFilter) FilterSource(store *storeInfo) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(store *storeInfo) bool {
	return f.rep.GetDistinctScore(f.stores, store) < f.safeScore
}
