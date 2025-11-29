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
	"slices"
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
	// defaultDegradedExpirationSeconds is the default expiration time for a degraded group.
	// After a group becomes degraded , it will automatically
	// expire after this duration if the stores don't recover.
	defaultDegradedExpirationSeconds = 600 // 10 minutes
)

type regionCache struct {
	region      *core.RegionInfo
	groupInfo   *runtimeGroupInfo
	affinityVer uint64
	isAffinity  bool
}

// Manager is the manager of all affinity information.
type Manager struct {
	// RWMutex protects only in-memory data. (Does not include auxiliary data needed for meta updates, such as keyRanges)
	// It can be acquired on its own or while already holding metaMutex,
	// but metaMutex must never be acquired while RWMutex is held to avoid deadlocks.
	syncutil.RWMutex
	// metaMutex protects both in-memory and storage metadata.
	// Metadata updates typically hold metaMutex for the whole operation,
	// and may acquire RWMutex only for the final in-memory update.
	// The only strict rule is that metaMutex must not be taken inside RWMutex.
	metaMutex syncutil.Mutex

	ctx              context.Context
	storage          endpoint.AffinityStorage
	storeSetInformer core.StoreSetInformer
	conf             config.SharedConfigProvider
	regionLabeler    *labeler.RegionLabeler // region labeler for syncing key ranges

	// The following members are protected by RWMutex.
	affinityRegionCount int
	groups              map[string]*runtimeGroupInfo // {group_id} -> runtimeGroupInfo
	regions             map[uint64]regionCache
	unavailableStores   map[uint64]condition

	// The following members are protected by metaMutex only, not protected by RWMutex.
	keyRanges map[string]GroupKeyRanges // {group_id} -> key ranges, cached in memory to reduce labeler lock contention
}

// NewManager creates a new affinity Manager.
func NewManager(ctx context.Context, storage endpoint.AffinityStorage, storeSetInformer core.StoreSetInformer, conf config.SharedConfigProvider, regionLabeler *labeler.RegionLabeler) (*Manager, error) {
	if regionLabeler == nil {
		return nil, errs.ErrAffinityDisabled
	}
	m := &Manager{
		ctx:                 ctx,
		storage:             storage,
		storeSetInformer:    storeSetInformer,
		conf:                conf,
		regionLabeler:       regionLabeler,
		affinityRegionCount: 0,
		groups:              make(map[string]*runtimeGroupInfo),
		regions:             make(map[uint64]regionCache),
		keyRanges:           make(map[string]GroupKeyRanges),
		unavailableStores:   make(map[uint64]condition),
	}
	if err := m.initialize(); err != nil {
		return nil, err
	}
	return m, nil
}

// initialize loads affinity groups from storage and rebuilds the group-label mapping.
func (m *Manager) initialize() error {
	m.Lock()
	defer m.Unlock()

	// load groups' info
	err := m.storage.LoadAllAffinityGroups(func(k, v string) {
		group := &Group{}
		if err := json.Unmarshal([]byte(v), group); err != nil {
			log.Error("failed to unmarshal affinity group, skipping",
				zap.String("key", k),
				zap.Error(errs.ErrLoadRule.Wrap(err)))
			return
		}
		m.initGroupLocked(group)
	})
	if err != nil {
		return err
	}

	// load region labels
	if err = m.loadRegionLabel(); err != nil {
		log.Error("failed to rebuild group-label mapping", zap.Error(err))
		return err
	}

	m.startAvailabilityCheckLoop()
	log.Info("affinity manager initialized", zap.Int("group-count", len(m.groups)))
	return nil
}

// IsAvailable checks that the Manager contains at least one Group.
func (m *Manager) IsAvailable() bool {
	m.RLock()
	defer m.RUnlock()
	return len(m.groups) > 0
}

func (*Manager) getExpiredAt() uint64 {
	return uint64(time.Now().Unix()) + defaultDegradedExpirationSeconds
}

func (m *Manager) initGroupLocked(group *Group) {
	if _, ok := m.groups[group.ID]; ok {
		log.Error("group already initialized", zap.String("group-id", group.ID))
		return
	}
	m.groups[group.ID] = &runtimeGroupInfo{
		Group: Group{
			ID:              group.ID,
			CreateTimestamp: group.CreateTimestamp,
			LeaderStoreID:   group.LeaderStoreID,
			VoterStoreIDs:   slices.Clone(group.VoterStoreIDs),
		},
		State:               groupDegraded,
		DegradedExpiredAt:   m.getExpiredAt(),
		AffinityVer:         1,
		AffinityRegionCount: 0,
		Regions:             make(map[uint64]regionCache),
		LabelRule:           nil,
		RangeCount:          0,
	}
}

func (m *Manager) noGroupsExist(groups []*Group) error {
	m.RLock()
	defer m.RUnlock()
	for _, group := range groups {
		if _, ok := m.groups[group.ID]; ok {
			return errs.ErrAffinityGroupExist.GenWithStackByArgs(group.ID)
		}
	}
	return nil
}

func (m *Manager) allGroupsExist(groupIDs []string) error {
	m.RLock()
	defer m.RUnlock()
	for _, groupID := range groupIDs {
		if _, ok := m.groups[groupID]; !ok {
			return errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
		}
	}
	return nil
}

func (m *Manager) createGroups(groups []*Group, labelRules []*labeler.LabelRule) {
	m.Lock()
	defer m.Unlock()
	for i, group := range groups {
		m.initGroupLocked(group)
		m.updateGroupLabelRuleLocked(group.ID, labelRules[i], false)
	}
}

func (m *Manager) resetCountLocked(groupInfo *runtimeGroupInfo) {
	m.affinityRegionCount -= groupInfo.AffinityRegionCount
	groupInfo.AffinityRegionCount = 0
	groupInfo.AffinityVer++
}

func (m *Manager) updateGroupPeers(groupID string, leaderStoreID uint64, voterStoreIDs []uint64) (*GroupState, error) {
	m.Lock()
	defer m.Unlock()

	groupInfo, ok := m.groups[groupID]
	if !ok {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}

	groupInfo.State = groupAvailable
	groupInfo.LeaderStoreID = leaderStoreID
	groupInfo.VoterStoreIDs = slices.Clone(voterStoreIDs)
	m.resetCountLocked(groupInfo)

	return newGroupState(groupInfo), nil
}

func (m *Manager) updateGroupStateLocked(groupID string, state condition) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		return
	}

	// If the expiration time has been reached, change groupDegraded to groupExpired.
	if groupInfo.State == groupDegraded && groupInfo.IsExpired() {
		groupInfo.State = groupExpired
	}

	// Update State
	state = state.toGroupState()
	if state == groupDegraded {
		// Only set the expiration time when transitioning from groupAvailable to groupDegraded.
		// Do nothing if the original state is already groupDegraded or groupExpired.
		if groupInfo.State == groupAvailable {
			groupInfo.State = groupDegraded
			groupInfo.DegradedExpiredAt = m.getExpiredAt()
		}
	} else {
		groupInfo.State = state
	}

	m.resetCountLocked(groupInfo)
}

// ExpireAffinityGroup changes the Group state to groupExpired.
func (m *Manager) ExpireAffinityGroup(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.updateGroupStateLocked(groupID, groupExpired)
}

func (m *Manager) updateGroupLabelRuleLocked(groupID string, labelRule *labeler.LabelRule, needClear bool) {
	rangeCount := 0
	if labelRule != nil {
		if ranges, ok := labelRule.Data.([]*labeler.KeyRangeRule); ok {
			rangeCount = len(ranges)
		}
	}
	groupInfo, ok := m.groups[groupID]
	if !ok {
		log.Error("group not initialized", zap.String("group-id", groupID))
	} else {
		if needClear {
			m.clearGroupCacheLocked(groupID)
		} else {
			m.resetCountLocked(groupInfo)
		}
		// Set LabelRule
		groupInfo.LabelRule = labelRule
		groupInfo.RangeCount = rangeCount
	}
}

func (m *Manager) updateGroupLabelRules(labels map[string]*labeler.LabelRule, needClear bool) {
	m.Lock()
	defer m.Unlock()
	for groupID, labelRule := range labels {
		m.updateGroupLabelRuleLocked(groupID, labelRule, needClear)
	}
}

func (m *Manager) clearGroupCacheLocked(groupID string) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		return
	}

	m.resetCountLocked(groupInfo)
	for regionID := range groupInfo.Regions {
		delete(m.regions, regionID)
	}
	groupInfo.Regions = make(map[uint64]regionCache)
}

func (m *Manager) deleteGroupLocked(groupID string) {
	m.clearGroupCacheLocked(groupID)
	delete(m.groups, groupID)
}

func (m *Manager) deleteGroups(groupIDs []string) {
	m.Lock()
	defer m.Unlock()
	for _, groupID := range groupIDs {
		m.deleteGroupLocked(groupID)
	}
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
	cache := &regionCache{
		region:      region,
		groupInfo:   group.groupInfoPtr,
		affinityVer: group.affinityVer,
		isAffinity:  group.isRegionAffinity(region),
	}
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
	_, ok := m.regions[regionID]
	m.RUnlock()
	if !ok {
		return
	}

	m.Lock()
	defer m.Unlock()
	m.deleteCacheLocked(regionID)
}

func (m *Manager) getCache(region *core.RegionInfo) (*regionCache, *GroupState) {
	m.RLock()
	defer m.RUnlock()
	cache, ok := m.regions[region.GetID()]
	if ok {
		return &cache, newGroupState(cache.groupInfo)
	}
	return nil, nil
}

// GetRegionAffinityGroupState returns the affinity group state and isAffinity for a region.
func (m *Manager) GetRegionAffinityGroupState(region *core.RegionInfo) (group *GroupState, isAffinity bool) {
	if region == nil || !m.IsAvailable() {
		return nil, false
	}
	var cache *regionCache
	cache, group = m.getCache(region)
	if cache == nil || group == nil || cache.affinityVer != group.affinityVer || region != cache.region {
		groupID := m.regionLabeler.GetRegionLabel(region, labelKey)
		group = m.GetAffinityGroupState(groupID)
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
	if id == "" {
		return nil
	}
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

// GetGroupsForTest returns the internal groups map.
// Used for testing only.
func (m *Manager) GetGroupsForTest() map[string]*runtimeGroupInfo {
	m.RLock()
	defer m.RUnlock()
	return m.groups
}

// SetRegionGroupForTest sets the affinity group for a region.
// Used for testing only.
func (m *Manager) SetRegionGroupForTest(regionID uint64, groupID string) {
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
