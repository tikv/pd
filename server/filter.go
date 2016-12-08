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

type stateFilter struct {
	opt *scheduleOption
}

func newStateFilter(opt *scheduleOption) *stateFilter {
	return &stateFilter{opt: opt}
}

func (f *stateFilter) filter(store *storeInfo) bool {
	if !store.isUp() {
		return true
	}
	return store.downTime() > f.opt.GetMaxStoreDownTime()
}

func (f *stateFilter) FilterSource(store *storeInfo) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *storeInfo) bool {
	return f.filter(store)
}

type regionCountFilter struct {
	opt *scheduleOption
}

func newRegionCountFilter(opt *scheduleOption) *regionCountFilter {
	return &regionCountFilter{opt: opt}
}

func (f *regionCountFilter) FilterSource(store *storeInfo) bool {
	return uint64(store.stats.RegionCount) < f.opt.GetMinRegionCount()
}

func (f *regionCountFilter) FilterTarget(store *storeInfo) bool {
	return false
}

type leaderCountFilter struct {
	opt *scheduleOption
}

func newLeaderCountFilter(opt *scheduleOption) *leaderCountFilter {
	return &leaderCountFilter{opt: opt}
}

func (f *leaderCountFilter) FilterSource(store *storeInfo) bool {
	return uint64(store.stats.LeaderRegionCount) < f.opt.GetMinLeaderCount()
}

func (f *leaderCountFilter) FilterTarget(store *storeInfo) bool {
	return false
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

type constraintFilter struct {
	source *Constraint
	target *Constraint
}

func newConstraintFilter(source, target *Constraint) *constraintFilter {
	return &constraintFilter{
		source: source,
		target: target,
	}
}

func (f *constraintFilter) FilterSource(store *storeInfo) bool {
	return f.source != nil && !f.source.Match(store)
}

func (f *constraintFilter) FilterTarget(store *storeInfo) bool {
	return f.target != nil && !f.target.Match(store)
}
