// Copyright 2021 TiKV Project Authors.
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

package placement

import (
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	minHitCountToCacheHit = 10 // RegionHit is cached only when the number of hits exceeds this
)

// RegionRuleFitCacheManager stores each region's RegionFit Result and involving variables
// only when the RegionFit result is satisfied with its rules
// RegionRuleFitCacheManager caches RegionFit result for each region only when:
// 1. region have no down peers
// 2. RegionFit is satisfied
// RegionRuleFitCacheManager will invalid the cache for the region only when:
// 1. region peer topology is changed
// 2. region have down peers
// 3. region leader is changed
// 4. any involved rule is changed
// 5. stores topology is changed
// 6. any store label is changed
// 7. any store state is changed
type RegionRuleFitCacheManager struct {
	mu           syncutil.RWMutex
	regionCaches map[uint64]*regionRuleFitCache
	storeCaches  map[uint64]*storeCache
}

// NewRegionRuleFitCacheManager returns RegionRuleFitCacheManager
func NewRegionRuleFitCacheManager() *RegionRuleFitCacheManager {
	return &RegionRuleFitCacheManager{
		regionCaches: make(map[uint64]*regionRuleFitCache),
		storeCaches:  make(map[uint64]*storeCache),
	}
}

// Invalid cache by regionID
func (manager *RegionRuleFitCacheManager) Invalid(regionID uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	delete(manager.regionCaches, regionID)
}

// CheckAndGetCache checks whether the region and rules are changed for the stored cache
// If the check pass, it will return the cache
func (manager *RegionRuleFitCacheManager) CheckAndGetCache(region *core.RegionInfo,
	rules []*Rule,
	stores []*core.StoreInfo) (bool, *RegionFit) {
	if !ValidateRegion(region) || !ValidateStores(stores) {
		return false, nil
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if cache, ok := manager.regionCaches[region.GetID()]; ok {
		if cache.IsUnchanged(region, rules, stores) {
			return true, cache.bestFit
		}
	}
	return false, nil
}

// SetCache stores RegionFit cache
func (manager *RegionRuleFitCacheManager) SetCache(region *core.RegionInfo, fit *RegionFit) {
	if !ValidateRegion(region) || !ValidateFit(fit) || !ValidateStores(fit.regionStores) {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if cache, ok := manager.regionCaches[region.GetID()]; ok {
		cache.hitCount++
		if cache.hitCount >= minHitCountToCacheHit {
			cache.bestFit = fit
		}
		return
	}
	manager.regionCaches[region.GetID()] = manager.toRegionRuleFitCache(region, fit)
}

// regionRuleFitCache stores regions RegionFit result and involving variables
type regionRuleFitCache struct {
	region       regionCache
	regionStores []*storeCache
	rules        []ruleCache
	bestFit      *RegionFit
	hitCount     uint32
}

// IsUnchanged checks whether the region and rules unchanged for the cache
func (cache *regionRuleFitCache) IsUnchanged(region *core.RegionInfo, rules []*Rule, stores []*core.StoreInfo) bool {
	if !ValidateRegion(region) || !ValidateStores(stores) {
		return false
	}
	return cache.isRegionUnchanged(region) && rulesEqual(cache.rules, rules) && storesEqual(cache.regionStores, stores)
}

func (cache *regionRuleFitCache) isRegionUnchanged(region *core.RegionInfo) bool {
	return region.GetLeader().StoreId == cache.region.leaderStoreID &&
		cache.region.epochEqual(region)
}

func rulesEqual(ruleCaches []ruleCache, rules []*Rule) bool {
	if len(ruleCaches) != len(rules) {
		return false
	}
	return slice.AllOf(ruleCaches, func(i int) bool {
		return slice.AnyOf(rules, func(j int) bool {
			return ruleCaches[i].ruleEqual(rules[j])
		})
	})
}

func storesEqual(a []*storeCache, b []*core.StoreInfo) bool {
	if len(a) != len(b) {
		return false
	}
	return slice.AllOf(a, func(i int) bool {
		return slice.AnyOf(b, func(j int) bool {
			return a[i].storeEqual(b[j])
		})
	})
}

func (manager *RegionRuleFitCacheManager) toRegionRuleFitCache(region *core.RegionInfo, fit *RegionFit) *regionRuleFitCache {
	return &regionRuleFitCache{
		region:       toRegionCache(region),
		regionStores: manager.toStoreCacheList(fit.regionStores),
		rules:        toRuleCacheList(fit.rules),
		bestFit:      nil,
		hitCount:     0,
	}
}

type ruleCache struct {
	id       string
	group    string
	version  uint64
	createTS uint64
}

func (r ruleCache) ruleEqual(rule *Rule) bool {
	if rule == nil {
		return false
	}
	return r.id == rule.ID && r.group == rule.GroupID && r.version == rule.Version && r.createTS == rule.CreateTimestamp
}

func toRuleCacheList(rules []*Rule) (c []ruleCache) {
	for _, rule := range rules {
		c = append(c, ruleCache{
			id:       rule.ID,
			group:    rule.GroupID,
			version:  rule.Version,
			createTS: rule.CreateTimestamp,
		})
	}
	return c
}

type storeCache struct {
	storeID uint64
	labels  map[string]string
	state   metapb.StoreState
}

func (s storeCache) storeEqual(store *core.StoreInfo) bool {
	if store == nil {
		return false
	}
	return s.storeID == store.GetID() &&
		s.state == store.GetState() &&
		labelEqual(s.labels, store.GetLabels())
}

func (manager *RegionRuleFitCacheManager) toStoreCacheList(stores []*core.StoreInfo) (c []*storeCache) {
	for _, s := range stores {
		sCache, ok := manager.storeCaches[s.GetID()]
		if !ok || !sCache.storeEqual(s) {
			m := make(map[string]string)
			for _, label := range s.GetLabels() {
				m[label.GetKey()] = label.GetValue()
			}
			sCache = &storeCache{
				storeID: s.GetID(),
				labels:  m,
				state:   s.GetState(),
			}
			manager.storeCaches[s.GetID()] = sCache
		}
		c = append(c, sCache)
	}
	return c
}

func labelEqual(label1 map[string]string, label2 []*metapb.StoreLabel) bool {
	if len(label1) != len(label2) {
		return false
	}
	return slice.AllOf(label2, func(i int) bool {
		k, v := label2[i].Key, label2[i].Value
		v1, ok := label1[k]
		return ok && v == v1
	})
}

type regionCache struct {
	regionID      uint64
	leaderStoreID uint64
	confVer       uint64
	version       uint64
}

func (r regionCache) epochEqual(region *core.RegionInfo) bool {
	v := region.GetRegionEpoch()
	if v == nil {
		return false
	}
	return r.confVer == v.ConfVer && r.version == v.Version
}

func toRegionCache(r *core.RegionInfo) regionCache {
	return regionCache{
		regionID:      r.GetID(),
		leaderStoreID: r.GetLeader().StoreId,
		confVer:       r.GetRegionEpoch().ConfVer,
		version:       r.GetRegionEpoch().Version,
	}
}

// ValidateStores checks whether store isn't offline, unhealthy and disconnected.
// Only Up store should be cached in RegionFitCache
func ValidateStores(stores []*core.StoreInfo) bool {
	return slice.NoneOf(stores, func(i int) bool {
		return stores[i].IsRemoving() || stores[i].IsDisconnected()
	})
}

// ValidateRegion checks whether region is healthy
func ValidateRegion(region *core.RegionInfo) bool {
	return region != nil && region.GetLeader() != nil && len(region.GetDownPeers()) == 0 && region.GetRegionEpoch() != nil
}

// ValidateFit checks whether regionFit is valid
func ValidateFit(fit *RegionFit) bool {
	return fit != nil && len(fit.rules) > 0 && len(fit.regionStores) > 0 && fit.IsSatisfied()
}
