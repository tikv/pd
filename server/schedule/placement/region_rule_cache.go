package placement

import (
	"sync"

	"github.com/tikv/pd/server/core"
)

// RegionRuleFitCacheManager stores each region's RegionFit Result and involving variables
// only when the RegionFit result is satisfied with its rules
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
	region  *core.RegionInfo
	rules   []*Rule
	bestFit *RegionFit
}

// IsUnchanged checks whether the region and rules unchanged for the cache
func (cache *RegionRuleFitCache) IsUnchanged(region *core.RegionInfo, rules []*Rule) bool {
	return cache.isRegionUnchanged(region) && cache.isRulesUnchanged(rules)
}

func (cache *RegionRuleFitCache) isRulesUnchanged(rules []*Rule) bool {
	return isRulesEqual(cache.rules, rules)
}

func (cache *RegionRuleFitCache) isRegionUnchanged(region *core.RegionInfo) bool {
	// we only cache region when it doesn't have down peers
	if len(region.GetDownPeers()) > 0 {
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
