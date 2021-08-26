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

// GetCacheRegionFit get RegionFit result by regionID
func (manager *RegionRuleFitCacheManager) GetCacheRegionFit(regionID uint64) *RegionFit {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.caches[regionID].bestFit
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

// Check checks whether the region and rules are changed for the stored cache
func (manager *RegionRuleFitCacheManager) Check(region *core.RegionInfo, rules []*Rule) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if cache, ok := manager.caches[region.GetID()]; ok && cache.bestFit != nil {
		return cache.IsUnchanged(region, rules)
	}
	return false
}

// SetCache stores RegionFit cache
func (manager *RegionRuleFitCacheManager) SetCache(region *core.RegionInfo, rules []*Rule, fit *RegionFit) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.caches[region.GetID()] = &RegionRuleFitCache{
		region:  region,
		rules:   rules,
		bestFit: fit,
	}
}

// RegionRuleFitCache stores regions RegionFit result and involving variables
type RegionRuleFitCache struct {
	bestFit *RegionFit
	region  *core.RegionInfo
	rules   []*Rule
}

// IsUnchanged checks whether the region and rules unchanged for the cache
func (cache *RegionRuleFitCache) IsUnchanged(region *core.RegionInfo, rules []*Rule) bool {
	if region == nil {
		return false
	}
	return cache.isRegionUnchanged(region) && cache.isRulesUnchanged(rules)
}

func (cache *RegionRuleFitCache) isRulesUnchanged(rules []*Rule) bool {
	return isRulesEqual(cache.rules, rules)
}

func (cache *RegionRuleFitCache) isRegionUnchanged(region *core.RegionInfo) bool {
	// we only cache region when it doesn't have down peers
	if len(region.GetDownPeers()) > 0 || region.GetLeader() == nil {
		return false
	}
	if !(isPeersEqual(cache.region, region) &&
		region.GetLeader().StoreId == cache.region.GetLeader().StoreId) {
		return false
	}
	return true
}

func isPeersEqual(a, b *core.RegionInfo) bool {
	if len(a.GetPeers()) != len(b.GetPeers()) {
		return false
	}
	for _, apeer := range a.GetPeers() {
		find := false
		for _, bpeer := range b.GetPeers() {
			if apeer.GetId() == bpeer.GetId() && apeer.StoreId == bpeer.StoreId && apeer.Role == bpeer.Role {
				find = true
				break
			}
		}
		if !find {
			return false
		}
	}
	return true
}

func isRulesEqual(a, b []*Rule) bool {
	if len(a) != len(b) {
		return false
	}
	for _, arule := range a {
		find := false
		for _, brule := range b {
			if arule.ID == brule.ID &&
				arule.GroupID == brule.GroupID &&
				arule.Version == brule.Version &&
				arule.CreateTimestamp == brule.CreateTimestamp {
				find = true
				break
			}
		}
		if !find {
			return false
		}
	}
	return true
}
