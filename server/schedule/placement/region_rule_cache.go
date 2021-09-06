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
	"sync"

	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
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
	mu     sync.RWMutex
	caches map[uint64]*RegionRuleFitCache
}

// NewRegionRuleFitCacheManager returns RegionRuleFitCacheManager
func NewRegionRuleFitCacheManager() *RegionRuleFitCacheManager {
	return &RegionRuleFitCacheManager{
		caches: map[uint64]*RegionRuleFitCache{},
	}
}

// Invalid invalid cache by regionID
func (manager *RegionRuleFitCacheManager) Invalid(regionID uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	delete(manager.caches, regionID)
}

// InvalidAll invalids all cache
func (manager *RegionRuleFitCacheManager) InvalidAll() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.caches = make(map[uint64]*RegionRuleFitCache)
}

// CheckAndGetCache checks whether the region and rules are changed for the stored cache
// If the check pass, it will return the cache
func (manager *RegionRuleFitCacheManager) CheckAndGetCache(region *core.RegionInfo,
	rules []*Rule,
	stores []*core.StoreInfo) (bool, *RegionFit) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if cache, ok := manager.caches[region.GetID()]; ok && cache.bestFit != nil {
		if cache.IsUnchanged(region, rules, stores) {
			return true, cache.bestFit
		}
	}
	return false, nil
}

// SetCache stores RegionFit cache
func (manager *RegionRuleFitCacheManager) SetCache(region *core.RegionInfo, fit *RegionFit) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	fit.SetCached(true)
	manager.caches[region.GetID()] = &RegionRuleFitCache{
		region:  region,
		bestFit: fit,
	}
}

// RegionRuleFitCache stores regions RegionFit result and involving variables
type RegionRuleFitCache struct {
	bestFit *RegionFit
	region  *core.RegionInfo
}

// IsUnchanged checks whether the region and rules unchanged for the cache
func (cache *RegionRuleFitCache) IsUnchanged(region *core.RegionInfo, rules []*Rule, stores []*core.StoreInfo) bool {
	return cache.isRegionUnchanged(region) && rulesEqual(cache.bestFit.rules, rules) && storesEqual(cache.bestFit.regionStores, stores)
}

func (cache *RegionRuleFitCache) isRegionUnchanged(region *core.RegionInfo) bool {
	// we only cache region when it doesn't have down peers
	if len(region.GetDownPeers()) > 0 || region.GetLeader() == nil {
		return false
	}
	return region.GetLeader().StoreId == cache.region.GetLeader().StoreId &&
		regionEpochEqual(cache.region, region)
}

func regionEpochEqual(a, b *core.RegionInfo) bool {
	if a.GetRegionEpoch() == nil || b.GetRegionEpoch() == nil {
		return false
	}
	return a.GetRegionEpoch().Version == b.GetRegionEpoch().Version &&
		a.GetRegionEpoch().ConfVer == b.GetRegionEpoch().ConfVer
}

func rulesEqual(a, b []*Rule) bool {
	if len(a) != len(b) {
		return false
	}
	return slice.AllOf(a, func(i int) bool {
		return slice.AnyOf(b, func(j int) bool {
			return equalRules(a[i], b[j])
		})
	})
}

func storesEqual(a, b []*core.StoreInfo) bool {
	if len(a) != len(b) {
		return false
	}
	return slice.AllOf(a, func(i int) bool {
		return slice.AnyOf(b, func(j int) bool {
			return a[i].GetID() == b[j].GetID() &&
				a[i].IsEqualLabels(b[j].GetLabels()) &&
				a[i].GetState() == b[j].GetState()
		})
	})
}
