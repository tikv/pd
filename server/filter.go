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
	cfg *BalanceConfig
}

func newStateFilter(cfg *BalanceConfig) *stateFilter {
	return &stateFilter{cfg: cfg}
}

func (sf *stateFilter) filterBadStore(store *storeInfo) bool {
	if !store.isUp() {
		return true
	}
	if store.downTime() >= sf.cfg.MaxStoreDownDuration.Duration {
		// The store is considered to be down.
		return true
	}
	return false
}

func (sf *stateFilter) FilterSource(store *storeInfo) bool {
	return sf.filterBadStore(store)
}

func (sf *stateFilter) FilterTarget(store *storeInfo) bool {
	return sf.filterBadStore(store)
}

type capacityFilter struct {
	cfg *BalanceConfig
}

func newCapacityFilter(cfg *BalanceConfig) *capacityFilter {
	return &capacityFilter{cfg: cfg}
}

func (cf *capacityFilter) FilterSource(store *storeInfo) bool {
	return store.usedRatio() <= cf.cfg.MinCapacityUsedRatio
}

func (cf *capacityFilter) FilterTarget(store *storeInfo) bool {
	return store.usedRatio() >= cf.cfg.MaxCapacityUsedRatio
}

type snapCountFilter struct {
	cfg *BalanceConfig
}

func newSnapCountFilter(cfg *BalanceConfig) *snapCountFilter {
	return &snapCountFilter{cfg: cfg}
}

func (sf *snapCountFilter) FilterSource(store *storeInfo) bool {
	return uint64(store.stats.GetSendingSnapCount()) > sf.cfg.MaxSendingSnapCount
}

func (sf *snapCountFilter) FilterTarget(store *storeInfo) bool {
	return uint64(store.stats.GetReceivingSnapCount()) > sf.cfg.MaxReceivingSnapCount ||
		uint64(store.stats.GetApplyingSnapCount()) > sf.cfg.MaxApplyingSnapCount
}

type leaderCountFilter struct {
	cfg *BalanceConfig
}

func newLeaderCountFilter(cfg *BalanceConfig) *leaderCountFilter {
	return &leaderCountFilter{cfg: cfg}
}

func (lf *leaderCountFilter) FilterSource(store *storeInfo) bool {
	return uint64(store.stats.LeaderRegionCount) < lf.cfg.MaxLeaderCount
}

func (lf *leaderCountFilter) FilterTarget(store *storeInfo) bool {
	return false
}
