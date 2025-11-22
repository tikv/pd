// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// labelKey is the key for affinity group id in region label.
	labelKey = "affinity_group"
	// labelRuleIDPrefix is the prefix for affinity group label rules.
	labelRuleIDPrefix = "affinity_group/"
)

type regionCache struct {
	region      *core.RegionInfo
	groupInfo   *runtimeGroupInfo
	affinityVer uint64
	isAffinity  bool
}

// Manager is the manager of all affinity information.
type Manager struct {
	syncutil.RWMutex
	ctx              context.Context
	storage          endpoint.AffinityStorage
	storeSetInformer core.StoreSetInformer
	conf             config.SharedConfigProvider
	regionLabeler    *labeler.RegionLabeler // region labeler for syncing key ranges

	initialized         bool
	affinityRegionCount int
	groups              map[string]*runtimeGroupInfo // {group_id} -> runtimeGroupInfo
	regions             map[uint64]regionCache
	keyRanges           map[string][]keyRange // {group_id} -> key ranges, cached in memory to reduce labeler lock contention
	unavailableStores   map[uint64]storeState
}

// NewManager creates a new affinity Manager.
func NewManager(ctx context.Context, storage endpoint.AffinityStorage, storeSetInformer core.StoreSetInformer, conf config.SharedConfigProvider, regionLabeler *labeler.RegionLabeler) *Manager {
	return &Manager{
		ctx:              ctx,
		storage:          storage,
		storeSetInformer: storeSetInformer,
		conf:             conf,
		regionLabeler:    regionLabeler,
		groups:           make(map[string]*runtimeGroupInfo),
		regions:          make(map[uint64]regionCache),
		keyRanges:        make(map[string][]keyRange),
	}
}

// Initialize loads affinity groups from storage and rebuilds the group-label mapping.
func (m *Manager) Initialize() error {
	m.Lock()
	defer m.Unlock()
	if m.initialized {
		return nil
	}

	err := m.storage.LoadAllAffinityGroups(func(k string, v string) {
		group := &Group{}
		if err := json.Unmarshal([]byte(v), group); err != nil {
			log.Error("failed to unmarshal affinity group, skipping",
				zap.String("key", k),
				zap.Error(errs.ErrLoadRule.Wrap(err)))
		}
		m.updateGroupLabelRuleLocked(group.ID, nil)
	})
	if err != nil {
		return err
	}

	// load region labels
	if m.regionLabeler != nil {
		if err := m.loadRegionLabel(); err != nil {
			log.Error("failed to rebuild group-label mapping", zap.Error(err))
			return err
		}
	}

	m.initialized = true
	m.startAvailabilityCheckLoop()
	log.Info("affinity manager initialized", zap.Int("group-count", len(m.groups)))
	return nil
}

// IsInitialized returns whether the manager is initialized.
func (m *Manager) IsInitialized() bool {
	m.RLock()
	defer m.RUnlock()
	return m.initialized
}

// IsAvailable checks that the Manager has been initialized and contains at least one Group.
func (m *Manager) IsAvailable() bool {
	m.RLock()
	defer m.RUnlock()
	return m.initialized && len(m.groups) > 0
}

func (m *Manager) updateGroupEffectLocked(groupID string, affinityVer uint64, leaderStoreID uint64, voterStoreIDs []uint64) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		return
	}
	// Becoming effective requires the affinityVer to match.
	if leaderStoreID != 0 && groupInfo.AffinityVer != affinityVer {
		return
	}

	if leaderStoreID == 0 {
		// Set Effect = false
		groupInfo.Effect = false
	} else {
		// Set Effect = true. The affinityVer consistency has already been checked.
		groupInfo.Effect = true
		groupInfo.LeaderStoreID = leaderStoreID
		groupInfo.VoterStoreIDs = append([]uint64(nil), voterStoreIDs...)
	}
	// Reset Statistics
	m.affinityRegionCount -= groupInfo.AffinityRegionCount
	groupInfo.AffinityRegionCount = 0
	groupInfo.AffinityVer++
}

func (m *Manager) updateGroupLabelRuleLocked(groupID string, labelRule *labeler.LabelRule) {
	rangeCount := 0
	if labelRule != nil {
		if ranges, ok := labelRule.Data.([]*labeler.KeyRangeRule); ok {
			rangeCount = len(ranges)
		}
	}
	groupInfo, ok := m.groups[groupID]
	if !ok {
		groupInfo = &runtimeGroupInfo{
			Group: Group{
				ID:              groupID,
				CreateTimestamp: uint64(time.Now().Unix()),
				LeaderStoreID:   0,
				VoterStoreIDs:   nil,
			},
			Effect:              false,
			AffinityVer:         1,
			AffinityRegionCount: 0,
			Regions:             make(map[uint64]regionCache),
			LabelRule:           labelRule,
			RangeCount:          rangeCount,
		}
		m.groups[groupID] = groupInfo
	} else {
		// Reset Statistics
		m.affinityRegionCount -= groupInfo.AffinityRegionCount
		groupInfo.AffinityRegionCount = 0
		groupInfo.AffinityVer++
		// Set LabelRule
		groupInfo.LabelRule = labelRule
		groupInfo.RangeCount = rangeCount
	}
}

func (m *Manager) deleteGroupLocked(groupID string) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		return
	}

	delete(m.groups, groupID)
	m.affinityRegionCount -= groupInfo.AffinityRegionCount
	for regionID := range groupInfo.Regions {
		delete(m.regions, regionID)
	}
	delete(m.keyRanges, groupID)
}

func (m *Manager) deleteCacheLocked(regionID uint64) {
	cache, ok := m.regions[regionID]
	if !ok {
		return
	}
	if cache.isAffinity && cache.affinityVer == cache.groupInfo.AffinityVer {
		cache.groupInfo.AffinityRegionCount--
		m.affinityRegionCount--
	}
	delete(m.regions, regionID)
	delete(cache.groupInfo.Regions, regionID)
}

func (m *Manager) saveCache(region *core.RegionInfo, group *GroupState) *regionCache {
	regionID := region.GetID()
	cache := &regionCache{}
	cache.isAffinity = group.isRegionAffinity(region, cache)
	cache.region = region
	cache.groupInfo = group.groupInfoPtr
	cache.affinityVer = group.affinityVer
	// Save cache
	m.Lock()
	defer m.Unlock()
	// If the Group has changed, update it but do not save it afterward.
	groupInfo, ok := m.groups[group.ID]
	if ok && groupInfo == group.groupInfoPtr && groupInfo.AffinityVer == group.affinityVer {
		m.deleteCacheLocked(regionID)
		m.regions[regionID] = *cache
		groupInfo.Regions[regionID] = *cache
		if cache.isAffinity {
			m.affinityRegionCount++
			groupInfo.AffinityRegionCount++
		}
	}
	return cache
}

// InvalidCache invalidates the cache of the corresponding Region in the manager by its Region ID.
func (m *Manager) InvalidCache(regionID uint64) {
	m.RLock()
	initialized := m.initialized
	_, ok := m.regions[regionID]
	m.RUnlock()
	if !initialized || !ok {
		return
	}

	m.Lock()
	defer m.Unlock()
	cache, ok := m.regions[regionID]
	if !ok {
		return
	}
	if cache.isAffinity && cache.affinityVer == cache.groupInfo.AffinityVer {
		cache.groupInfo.AffinityRegionCount--
		m.affinityRegionCount--
	}
	delete(m.regions, regionID)
	delete(cache.groupInfo.Regions, regionID)
}

func (m *Manager) getCache(region *core.RegionInfo) (*regionCache, *GroupState) {
	m.RLock()
	defer m.RUnlock()
	cache, ok := m.regions[region.GetID()]
	if ok && cache.affinityVer == cache.groupInfo.AffinityVer {
		return &cache, newGroupState(cache.groupInfo)
	}
	return nil, nil
}

// GetRegionAffinityGroupState returns the affinity group state and isAffinity for a region.
func (m *Manager) GetRegionAffinityGroupState(region *core.RegionInfo) (*GroupState, bool) {
	if region == nil || !m.IsAvailable() {
		return nil, false
	}
	cache, group := m.getCache(region)
	if cache == nil || group == nil || region != cache.region {
		var groupID string
		if m.regionLabeler != nil {
			groupID = m.regionLabeler.GetRegionLabel(region, labelKey)
		}
		if groupID != "" {
			group = m.GetAffinityGroupState(groupID)
		}
		if group == nil {
			return nil, false
		}
		cache = m.saveCache(region, group)
	}

	return group, cache.isAffinity
}

// IsGroupExist checks if a group exists.
func (m *Manager) IsGroupExist(id string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.groups[id]
	return ok
}

// GetAffinityGroupState gets the runtime state of an affinity group.
func (m *Manager) GetAffinityGroupState(id string) *GroupState {
	m.RLock()
	defer m.RUnlock()
	groupInfo, ok := m.groups[id]
	if ok {
		return newGroupState(groupInfo)
	}
	return nil
}

// GetAllAffinityGroupStates returns all affinity groups.
func (m *Manager) GetAllAffinityGroupStates() []*GroupState {
	m.RLock()
	defer m.RUnlock()
	result := make([]*GroupState, 0, len(m.groups))
	for _, groupInfo := range m.groups {
		result = append(result, newGroupState(groupInfo))
	}
	return result
}

// GetGroups returns the internal groups map.
// Used for testing only.
// TODO: Move these tests.
func (m *Manager) GetGroups() map[string]*runtimeGroupInfo {
	m.RLock()
	defer m.RUnlock()
	return m.groups
}

// SetRegionGroup sets the affinity group for a region.
// Used for testing only.
func (m *Manager) SetRegionGroup(regionID uint64, groupID string) {
	m.Lock()
	defer m.Unlock()
	if groupID == "" {
		delete(m.regions, regionID)
		return
	}

	groupInfo, ok := m.groups[groupID]
	if !ok {
		return
	}

	cache := regionCache{
		region:      nil,
		affinityVer: groupInfo.AffinityVer,
		groupInfo:   groupInfo,
	}

	m.regions[regionID] = cache
	groupInfo.Regions[regionID] = cache
}
