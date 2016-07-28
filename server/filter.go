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

// Filter is an interface to filter target store.
type Filter interface {
	// FilterFromStore checks whether `from stores` should be skipped.
	// If return value is true, we should not use this store as `from store` that is to be balanced.
	FilterFromStore(store *storeInfo, args ...interface{}) bool

	// FilterToStore checks whether to stores should be skipped.
	// If return value is true, we should not use this store as `to store` that is to be balanced to.
	FilterToStore(store *storeInfo, args ...interface{}) bool
}

func filterFromStore(store *storeInfo, filters []Filter, args ...interface{}) bool {
	for _, filter := range filters {
		if filter.FilterFromStore(store, args) {
			return true
		}
	}

	return false
}

func filterToStore(store *storeInfo, filters []Filter, args ...interface{}) bool {
	for _, filter := range filters {
		if filter.FilterToStore(store, args) {
			return true
		}
	}

	return false
}

type stateFilter struct {
	cfg *BalanceConfig
}

func newStateFilter(cfg *BalanceConfig) *stateFilter {
	return &stateFilter{cfg: cfg}
}

func (sf *stateFilter) filterBadStore(store *storeInfo) bool {
	if store.stats.Stats == nil {
		// The store is in unknown state.
		return true
	}
	if store.downDuration() >= sf.cfg.MaxStoreDownDuration {
		// The store is considered to be down.
		return true
	}
	return false
}

func (sf *stateFilter) FilterFromStore(store *storeInfo, args ...interface{}) bool {
	return sf.filterBadStore(store)
}

func (sf *stateFilter) FilterToStore(store *storeInfo, args ...interface{}) bool {
	return sf.filterBadStore(store)
}

type capacityFilter struct {
	cfg *BalanceConfig
}

func newCapacityFilter(cfg *BalanceConfig) *capacityFilter {
	return &capacityFilter{cfg: cfg}
}

func (cf *capacityFilter) FilterFromStore(store *storeInfo, args ...interface{}) bool {
	return store.usedRatio() <= cf.cfg.MinCapacityUsedRatio
}

func (cf *capacityFilter) FilterToStore(store *storeInfo, args ...interface{}) bool {
	return store.usedRatio() >= cf.cfg.MaxCapacityUsedRatio
}

type snapCountFilter struct {
	cfg *BalanceConfig
}

func newSnapCountFilter(cfg *BalanceConfig) *snapCountFilter {
	return &snapCountFilter{cfg: cfg}
}

func (sf *snapCountFilter) FilterFromStore(store *storeInfo, args ...interface{}) bool {
	return uint64(store.stats.Stats.GetSendingSnapCount()) > sf.cfg.MaxSendingSnapCount
}

func (sf *snapCountFilter) FilterToStore(store *storeInfo, args ...interface{}) bool {
	return uint64(store.stats.Stats.GetReceivingSnapCount()) > sf.cfg.MaxReceivingSnapCount
}

type leaderCountFilter struct {
	cfg *BalanceConfig
}

func newLeaderCountFilter(cfg *BalanceConfig) *leaderCountFilter {
	return &leaderCountFilter{cfg: cfg}
}

func (lf *leaderCountFilter) FilterFromStore(store *storeInfo, args ...interface{}) bool {
	return uint64(store.stats.LeaderRegionCount) < lf.cfg.MaxLeaderCount
}

func (lf *leaderCountFilter) FilterToStore(store *storeInfo, args ...interface{}) bool {
	return false
}
