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

package filter

import (
	"fmt"

	"github.com/pingcap/pd/pkg/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule/opt"
)

//revive:disable:unused-parameter

// Filter is an interface to filter source and target store.
type Filter interface {
	Type() string
	// Return true if the store should not be used as a source store.
	Source(opt opt.Options, store *core.StoreInfo) bool
	// Return true if the store should not be used as a target store.
	Target(opt opt.Options, store *core.StoreInfo) bool
}

// Source checks if store can pass all Filters as source store.
func Source(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.Source(opt, store) {
			filterCounter.WithLabelValues("filter-source", storeAddress, storeID, filter.Type()).Inc()
			return true
		}
	}
	return false
}

// Target checks if store can pass all Filters as target store.
func Target(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.Target(opt, store) {
			filterCounter.WithLabelValues("filter-target", storeAddress, storeID, filter.Type()).Inc()
			return true
		}
	}
	return false
}

type excludedFilter struct {
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified stores.
func NewExcludedFilter(sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) Type() string {
	return "exclude-filter"
}

func (f *excludedFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetID()]
	return ok
}

func (f *excludedFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetID()]
	return ok
}

type blockFilter struct{}

// NewBlockFilter creates a Filter that filters all stores that are blocked from balance.
func NewBlockFilter() Filter {
	return &blockFilter{}
}

func (f *blockFilter) Type() string {
	return "block-filter"
}

func (f *blockFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsBlocked()
}

func (f *blockFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsBlocked()
}

type overloadFilter struct{}

// NewOverloadFilter creates a Filter that filters all stores that are overloaded from balance.
func NewOverloadFilter() Filter {
	return &overloadFilter{}
}

func (f *overloadFilter) Type() string {
	return "overload-filter"
}

func (f *overloadFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

func (f *overloadFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

type stateFilter struct{}

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter() Filter {
	return &stateFilter{}
}

func (f *stateFilter) Type() string {
	return "state-filter"
}

func (f *stateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsTombstone()
}

func (f *stateFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsUp()
}

type healthFilter struct{}

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter() Filter {
	return &healthFilter{}
}

func (f *healthFilter) Type() string {
	return "health-filter"
}

func (f *healthFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}
	return store.DownTime() > opt.GetMaxStoreDownTime()
}

func (f *healthFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *healthFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type disconnectFilter struct{}

// NewDisconnectFilter creates a Filter that filters all stores that are disconnected.
func NewDisconnectFilter() Filter {
	return &disconnectFilter{}
}

func (f *disconnectFilter) Type() string {
	return "disconnect-filter"
}

func (f *disconnectFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsDisconnected()
}

func (f *disconnectFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsDisconnected()
}

type pendingPeerCountFilter struct{}

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pending peers.
func NewPendingPeerCountFilter() Filter {
	return &pendingPeerCountFilter{}
}

func (p *pendingPeerCountFilter) Type() string {
	return "pending-peer-filter"
}

func (p *pendingPeerCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if opt.GetMaxPendingPeerCount() == 0 {
		return false
	}
	return store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

func (p *pendingPeerCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

type snapshotCountFilter struct{}

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter() Filter {
	return &snapshotCountFilter{}
}

func (f *snapshotCountFilter) Type() string {
	return "snapshot-filter"
}

func (f *snapshotCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	return uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *snapshotCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type cacheFilter struct {
	cache *cache.TTLUint64
}

// NewCacheFilter creates a Filter that filters all stores that are in the cache.
func NewCacheFilter(cache *cache.TTLUint64) Filter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) Type() string {
	return "cache-filter"
}

func (f *cacheFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.cache.Exists(store.GetID())
}

func (f *cacheFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

type storageThresholdFilter struct{}

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter() Filter {
	return &storageThresholdFilter{}
}

func (f *storageThresholdFilter) Type() string {
	return "storage-threshold-filter"
}

func (f *storageThresholdFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsLowSpace(opt.GetLowSpaceRatio())
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	labels    []string
	stores    []*core.StoreInfo
	safeScore float64
}

// NewDistinctScoreFilter creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewDistinctScoreFilter(labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetID() == source.GetID() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		labels:    labels,
		stores:    newStores,
		safeScore: core.DistinctScore(labels, newStores, source),
	}
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return core.DistinctScore(f.labels, f.stores, store) < f.safeScore
}

type namespaceFilter struct {
	classifier namespace.Classifier
	namespace  string
}

// NewNamespaceFilter creates a Filter that filters all stores that are not
// belong to a namespace.
func NewNamespaceFilter(classifier namespace.Classifier, namespace string) Filter {
	return &namespaceFilter{
		classifier: classifier,
		namespace:  namespace,
	}
}

func (f *namespaceFilter) Type() string {
	return "namespace-filter"
}

func (f *namespaceFilter) filter(store *core.StoreInfo) bool {
	return f.classifier.GetStoreNamespace(store) != f.namespace
}

func (f *namespaceFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *namespaceFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

type rejectLeaderFilter struct{}

// NewRejectLeaderFilter creates a Filter that filters stores that marked as
// rejectLeader from being the target of leader transfer.
func NewRejectLeaderFilter() Filter {
	return rejectLeaderFilter{}
}

func (f rejectLeaderFilter) Type() string {
	return "reject-leader-filter"
}

func (f rejectLeaderFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

func (f rejectLeaderFilter) Target(opts opt.Options, store *core.StoreInfo) bool {
	return opts.CheckLabelProperty(opt.RejectLeader, store.GetLabels())
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
}

// Type returns the type of the Filter.
func (f StoreStateFilter) Type() string {
	return "store-state-filter"
}

// Source returns true when the store cannot be selected as the schedule
// source.
func (f StoreStateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader && (store.IsDisconnected() || store.IsBlocked()) {
		return true
	}

	if f.MoveRegion && f.filterMoveRegion(opt, store) {
		return true
	}
	return false
}

// Target returns true when the store cannot be selected as the schedule
// target.
func (f StoreStateFilter) Target(opts opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.IsOffline() ||
		store.DownTime() > opts.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader &&
		(store.IsDisconnected() ||
			store.IsBlocked() ||
			store.GetIsBusy() ||
			opts.CheckLabelProperty(opt.RejectLeader, store.GetLabels())) {
		return true
	}

	if f.MoveRegion {
		// only target consider the pending peers because pending more means the disk is slower.
		if opts.GetMaxPendingPeerCount() > 0 && store.GetPendingPeerCount() > int(opts.GetMaxPendingPeerCount()) {
			return true
		}

		if f.filterMoveRegion(opts, store) {
			return true
		}
	}
	return false
}

func (f StoreStateFilter) filterMoveRegion(opt opt.Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}

	if store.IsOverloaded() {
		return true
	}

	if uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount() {
		return true
	}
	return false
}

// BlacklistType the type of BlackListStore Filter.
type BlacklistType int

// some flags about blacklist type.
const (
	// blacklist associated with the source.
	BlacklistSource BlacklistType = 1 << iota
	// blacklist associated with the target.
	BlacklistTarget
)

// BlacklistStoreFilter filters the store according to the blacklist.
type BlacklistStoreFilter struct {
	blacklist map[uint64]struct{}
	flag      BlacklistType
}

// NewBlacklistStoreFilter creates a blacklist filter.
func NewBlacklistStoreFilter(typ BlacklistType) *BlacklistStoreFilter {
	return &BlacklistStoreFilter{
		blacklist: make(map[uint64]struct{}),
		flag:      typ,
	}
}

// Type implements the Filter.
func (f *BlacklistStoreFilter) Type() string {
	return "blacklist-store-filter"
}

// Source implements the Filter.
func (f *BlacklistStoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistSource != BlacklistSource {
		return false
	}
	return f.filter(store)
}

// Add adds the store to the blacklist.
func (f *BlacklistStoreFilter) Add(storeID uint64) {
	f.blacklist[storeID] = struct{}{}
}

// Target implements the Filter.
func (f *BlacklistStoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistTarget != BlacklistTarget {
		return false
	}
	return f.filter(store)
}

func (f *BlacklistStoreFilter) filter(store *core.StoreInfo) bool {
	_, ok := f.blacklist[store.GetID()]
	return ok
}
