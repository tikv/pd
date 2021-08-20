package placement

import (
	"sync"

	"github.com/tikv/pd/server/core"
)

type RegionRuleFitCacheManager struct {
	mu     sync.RWMutex
	caches map[uint64]*RegionRuleFitCache
}

func NewRegionRuleFitCacheManager() *RegionRuleFitCacheManager {
	return &RegionRuleFitCacheManager{
		caches: map[uint64]*RegionRuleFitCache{},
	}
}

func (manager *RegionRuleFitCacheManager) GetCacheRegionFit(regionID uint64) *RegionFit {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.caches[regionID].bestFit
}

func (manager *RegionRuleFitCacheManager) InValid(regionID uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	delete(manager.caches, regionID)
}

func (manager *RegionRuleFitCacheManager) Check(region *core.RegionInfo, rules []*Rule) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if cache, ok := manager.caches[region.GetID()]; ok {
		return cache.IsUnchanged(region, rules)
	}
	return false
}

func (manager *RegionRuleFitCacheManager) SetCache(region *core.RegionInfo, rules []*Rule, fit *RegionFit) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.caches[region.GetID()] = &RegionRuleFitCache{
		region:  region,
		rules:   rules,
		bestFit: fit,
	}
}

type RegionRuleFitCache struct {
	region  *core.RegionInfo
	rules   []*Rule
	bestFit *RegionFit
}

func (cache *RegionRuleFitCache) IsUnchanged(region *core.RegionInfo, rules []*Rule) bool {
	return cache.isRegionUnchanged(region) && cache.isRulesUnchanged(rules)
}

func (cache *RegionRuleFitCache) isRulesUnchanged(rules []*Rule) bool {
	return isEqualRules(cache.rules, rules)
}

func (cache *RegionRuleFitCache) isRegionUnchanged(region *core.RegionInfo) bool {
	if len(region.GetDownPeers()) > 0 {
		return false
	}
	if !(isPeerContains(cache.region, region) &&
		isPeerContains(region, cache.region) &&
		region.GetLeader().StoreId == cache.region.GetLeader().StoreId) {
		return false
	}
	return true
}

func isPeerContains(a, b *core.RegionInfo) bool {
	for _, apeer := range a.GetPeers() {
		find := false
		for _, bpeer := range b.GetPeers() {
			if apeer.StoreId == bpeer.StoreId && apeer.Role == bpeer.Role {
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

func isEqualRules(a, b []*Rule) bool {
	if len(a) != len(b) {
		return false
	}
	for _, arule := range a {
		find := false
		for _, brule := range b {
			if arule.ID == brule.ID && arule.GroupID == brule.GroupID && arule.version == brule.version {
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
