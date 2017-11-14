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

package schedule

import (
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

// Filter is an interface to filter source and target store.
type Filter interface {
	// Return true if the store should not be used as a source store.
	FilterSource(store *core.StoreInfo, cluster Cluster) bool
	// Return true if the store should not be used as a target store.
	FilterTarget(store *core.StoreInfo, cluster Cluster) bool
}

// FilterSource checks if store can pass all Filters as source store.
func FilterSource(store *core.StoreInfo, cluster Cluster, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterSource(store, cluster) {
			return true
		}
	}
	return false
}

// FilterTarget checks if store can pass all Filters as target store.
func FilterTarget(store *core.StoreInfo, cluster Cluster, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterTarget(store, cluster) {
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

func (f *excludedFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	_, ok := f.sources[store.GetId()]
	return ok
}

func (f *excludedFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	_, ok := f.targets[store.GetId()]
	return ok
}

type blockFilter struct{}

// NewBlockFilter creates a Filter that filters all stores that are blocked from balance.
func NewBlockFilter() Filter {
	return &blockFilter{}
}

func (f *blockFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return store.IsBlocked()
}

func (f *blockFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return store.IsBlocked()
}

type stateFilter struct{}

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter() Filter {
	return &stateFilter{}
}

func (f *stateFilter) filter(store *core.StoreInfo) bool {
	return !store.IsUp()
}

func (f *stateFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store)
}

type healthFilter struct{}

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter() Filter {
	return &healthFilter{}
}

func (f *healthFilter) filter(store *core.StoreInfo, cluster Cluster) bool {
	if store.Stats.GetIsBusy() {
		return true
	}
	return store.DownTime() > cluster.GetMaxStoreDownTime()
}

func (f *healthFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store, cluster)
}

func (f *healthFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store, cluster)
}

type pendingPeerCountFilter struct{}

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pengding peers.
func NewPendingPeerCountFilter() Filter {
	return &pendingPeerCountFilter{}
}

func (p *pendingPeerCountFilter) filter(store *core.StoreInfo, cluster Cluster) bool {
	return store.PendingPeerCount > int(cluster.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return p.filter(store, cluster)
}

func (p *pendingPeerCountFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return p.filter(store, cluster)
}

type snapshotCountFilter struct{}

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter() Filter {
	return &snapshotCountFilter{}
}

func (f *snapshotCountFilter) filter(store *core.StoreInfo, cluster Cluster) bool {
	return uint64(store.Stats.GetSendingSnapCount()) > cluster.GetMaxSnapshotCount() ||
		uint64(store.Stats.GetReceivingSnapCount()) > cluster.GetMaxSnapshotCount() ||
		uint64(store.Stats.GetApplyingSnapCount()) > cluster.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store, cluster)
}

func (f *snapshotCountFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store, cluster)
}

type cacheFilter struct {
	cache *cache.TTLUint64
}

// NewCacheFilter creates a Filter that filters all stores that are in the cache.
func NewCacheFilter(cache *cache.TTLUint64) Filter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return f.cache.Exists(store.GetId())
}

func (f *cacheFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return false
}

type storageThresholdFilter struct{}

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter() Filter {
	return &storageThresholdFilter{}
}

func (f *storageThresholdFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return store.IsLowSpace()
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
		if s.GetId() == source.GetId() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		labels:    labels,
		stores:    newStores,
		safeScore: DistinctScore(labels, newStores, source),
	}
}

func (f *distinctScoreFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return DistinctScore(f.labels, f.stores, store) < f.safeScore
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

func (f *namespaceFilter) filter(store *core.StoreInfo) bool {
	return f.classifier.GetStoreNamespace(store) != f.namespace
}

func (f *namespaceFilter) FilterSource(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store)
}

func (f *namespaceFilter) FilterTarget(store *core.StoreInfo, cluster Cluster) bool {
	return f.filter(store)
}
