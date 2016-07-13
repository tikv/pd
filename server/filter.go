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

type capacityFilter struct {
	minCapacityUsedRatio float64
	maxCapacityUsedRatio float64
}

func newCapacityFilter(minCapacityUsedRatio float64, maxCapacityUsedRatio float64) *capacityFilter {
	return &capacityFilter{
		minCapacityUsedRatio: minCapacityUsedRatio,
		maxCapacityUsedRatio: maxCapacityUsedRatio,
	}
}

func (cf *capacityFilter) FilterFromStore(store *storeInfo, args ...interface{}) bool {
	return store.usedRatio() <= cf.minCapacityUsedRatio
}

func (cf *capacityFilter) FilterToStore(store *storeInfo, args ...interface{}) bool {
	return store.usedRatio() >= cf.maxCapacityUsedRatio
}

type snapCountFilter struct {
	maxSendingSnapCount   uint64
	maxReceivingSnapCount uint64
}

func newSnapCountFilter(maxSendingSnapCount uint64, maxReceivingSnapCount uint64) *snapCountFilter {
	return &snapCountFilter{
		maxSendingSnapCount:   maxSendingSnapCount,
		maxReceivingSnapCount: maxReceivingSnapCount,
	}
}

func (sf *snapCountFilter) FilterFromStore(store *storeInfo, args ...interface{}) bool {
	return uint64(store.stats.Stats.GetSendingSnapCount()) > sf.maxSendingSnapCount
}

func (sf *snapCountFilter) FilterToStore(store *storeInfo, args ...interface{}) bool {
	return uint64(store.stats.Stats.GetReceivingSnapCount()) > sf.maxReceivingSnapCount
}

type leaderCountFilter struct {
	maxLeaderCount uint64
}

func newLeaderCountFilter(maxLeaderCount uint64) *leaderCountFilter {
	return &leaderCountFilter{
		maxLeaderCount: maxLeaderCount,
	}
}

func (lf *leaderCountFilter) FilterFromStore(store *storeInfo, args ...interface{}) bool {
	return uint64(store.stats.LeaderRegionCount) < lf.maxLeaderCount
}

func (lf *leaderCountFilter) FilterToStore(store *storeInfo, args ...interface{}) bool {
	return false
}
