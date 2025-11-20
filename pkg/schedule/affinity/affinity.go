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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// labelKey is the key for affinity group id in region label.
	labelKey = "affinity_group"
	// labelRuleIDPrefix is the prefix for affinity group label rules.
	labelRuleIDPrefix = "affinity_group/"
)

// Group defines an affinity group. Regions belonging to it will tend to have the same distribution.
// NOTE: This type is exported by HTTP API and persisted in storage. Please pay more attention when modifying it.
type Group struct {
	// ID is a unique identifier for Group.
	ID string `json:"id"`
	// CreateTimestamp is the time when the Group was created.
	CreateTimestamp uint64 `json:"create_timestamp"`

	// The following parameters are all determined automatically.

	// LeaderStoreID indicates which store the leader should be on.
	LeaderStoreID uint64 `json:"leader_store_id"`
	// VoterStoreIDs indicates which stores Voters should be on.
	VoterStoreIDs []uint64 `json:"voter_store_ids"`
	// TODO: LearnerStoreIDs
}

// Clone returns a deep copy of the Group.
// This is used to return copies from the cache, preventing race conditions.
func (g *Group) Clone() *Group {
	clone := *g
	clone.VoterStoreIDs = append([]uint64(nil), g.VoterStoreIDs...)
	// TODO: Clone LearnerStoreIDs when added
	return &clone
}

func (g *Group) String() string {
	b, _ := json.Marshal(g)
	return string(b)
}

// GroupState defines the runtime state of an affinity group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type GroupState struct {
	Group
	// Effect parameter indicates whether the current constraint is in effect.
	Effect bool `json:"effect"`
	// RangeCount indicates how many key ranges are associated with this group.
	RangeCount int `json:"range_count"`
	// RegionCount indicates how many Regions are currently in the affinity state.
	RegionCount int `json:"region_count"`
	// AffinityRegionCount indicates how many Regions have all Voter and Leader peers in the correct stores.
	AffinityRegionCount int `json:"affinity_region_count"`

	// affinityVer is used to mark the version of the cache.
	affinityVer uint64
	// groupInfoPtr is a pointer to the original information.
	// It is used only for pointer comparison and should not access any internal data.
	groupInfoPtr *GroupInfo
}

type regionCache struct {
	region      *core.RegionInfo
	groupInfo   *GroupInfo
	affinityVer uint64
	isAffinity  bool
}

// IsRegionAffinity checks whether the Region is in an affinity state.
func (g *GroupState) isRegionAffinity(region *core.RegionInfo, cache *regionCache) bool {
	if region == nil {
		return false
	}

	// Use the result in the cache when both the Region pointer and the Group’s affinityVer remain unchanged.
	if region == cache.region && g.affinityVer == cache.affinityVer {
		return cache.isAffinity
	}

	// Compare the Leader
	if region.GetLeader().GetStoreId() != g.LeaderStoreID {
		return false
	}
	// Compare the Voters
	voters := region.GetVoters()
	if len(voters) != len(g.VoterStoreIDs) {
		return false
	}
	expected := make(map[uint64]struct{}, len(voters))
	for _, voter := range g.VoterStoreIDs {
		expected[voter] = struct{}{}
	}
	for _, voter := range voters {
		if _, ok := expected[voter.GetStoreId()]; !ok {
			return false
		}
	}
	// TODO: Compare the Learners.
	return true
}

// GroupInfo contains meta information and runtime statistics for the Group.
type GroupInfo struct {
	Group

	// Effect parameter indicates whether the current constraint is in effect.
	// Constraints are typically released when the store is in an abnormal state.
	Effect bool
	// AffinityVer initializes at 1 and increments by 1 each time the Group changes.
	AffinityVer uint64
	// AffinityRegionCount indicates how many Regions have all Voter and Leader peers in the correct stores. (AffinityVer equals).
	AffinityRegionCount int

	// nolint:unused
	Regions map[uint64]regionCache
	// nolint:unused
	// TODO: Consider separate modification support in the future (read-modify keyrange-write)
	// Currently using label's internal multiple keyrange mechanism
	labels *labeler.LabelRule
}

// newGroupState creates a GroupState from the given GroupInfo.
// GroupInfo may need to be accessed under a Lock.
func newGroupState(g *GroupInfo) *GroupState {
	return &GroupState{
		Group: Group{
			ID:              g.ID,
			CreateTimestamp: g.CreateTimestamp,
			LeaderStoreID:   g.LeaderStoreID,
			VoterStoreIDs:   append([]uint64(nil), g.VoterStoreIDs...),
		},
		Effect:              g.Effect,
		RangeCount:          0, // TODO: len(labels)
		RegionCount:         len(g.Regions),
		AffinityRegionCount: g.AffinityRegionCount,
		affinityVer:         g.AffinityVer,
		groupInfoPtr:        g,
	}
}

// Manager is the manager of all affinity information.
type Manager struct {
	syncutil.RWMutex
	ctx              context.Context
	storage          endpoint.AffinityStorage
	initialized      bool
	groups           map[string]*GroupInfo // {group_id} -> GroupInfo
	regions          map[uint64]regionCache
	keyRanges        map[string][]keyRange // {group_id} -> key ranges, cached in memory to reduce labeler lock contention
	storeSetInformer core.StoreSetInformer
	conf             config.SharedConfigProvider
	regionLabeler    *labeler.RegionLabeler // region labeler for syncing key ranges

	affinityRegionCount int
}

// NewManager creates a new affinity Manager.
func NewManager(ctx context.Context, storage endpoint.AffinityStorage, storeSetInformer core.StoreSetInformer, conf config.SharedConfigProvider, regionLabeler *labeler.RegionLabeler) *Manager {
	return &Manager{
		ctx:              ctx,
		storage:          storage,
		storeSetInformer: storeSetInformer,
		conf:             conf,
		regionLabeler:    regionLabeler,
		groups:           make(map[string]*GroupInfo),
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
		m.updateGroupLabelsLocked(group.ID, nil)
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
	m.startHealthCheckLoop()
	log.Info("affinity manager initialized", zap.Int("group-count", len(m.groups)))
	return nil
}

// IsInitialized returns whether the manager is initialized.
func (m *Manager) IsInitialized() bool {
	m.RLock()
	defer m.RUnlock()
	return m.initialized
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

func (m *Manager) updateGroupLabelsLocked(groupID string, labels *labeler.LabelRule) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		groupInfo = &GroupInfo{
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
			labels:              labels,
		}
		m.groups[groupID] = groupInfo
	} else {
		// Reset Statistics
		m.affinityRegionCount -= groupInfo.AffinityRegionCount
		groupInfo.AffinityRegionCount = 0
		groupInfo.AffinityVer++
		// Set labels
		groupInfo.labels = labels
	}
}

func (m *Manager) deleteGroupLocked(groupID string) {
	group, ok := m.groups[groupID]
	if !ok {
		return
	}

	delete(m.groups, groupID)
	m.affinityRegionCount -= group.AffinityRegionCount
	for regionID := range group.Regions {
		delete(m.regions, regionID)
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

// ObserveHealthyRegion observes healthy Regions and collects information to update the Peer distribution within the Group.
func (m *Manager) ObserveHealthyRegion(region *core.RegionInfo, group *GroupState) {
	// Use the peer distribution of the first observed healthy Region as the result.
	// TODO: Improve the strategy.
	if group == nil || group.Effect {
		return
	}
	leaderStoreID := region.GetLeader().GetStoreId()
	voterStoreIDs := make([]uint64, 0, len(region.GetVoters()))
	for _, voter := range region.GetVoters() {
		voterStoreIDs = append(voterStoreIDs, voter.GetStoreId())
	}
	m.Lock()
	defer m.Unlock()
	m.updateGroupEffectLocked(group.ID, group.affinityVer, leaderStoreID, voterStoreIDs)
}

// GetRegionAffinityGroupState returns the affinity group state and isAffinity for a region.
func (m *Manager) GetRegionAffinityGroupState(region *core.RegionInfo) (*GroupState, bool) {
	if region == nil {
		return nil, false
	}
	cache, group := m.getCache(region)
	if cache == nil || group == nil || region != cache.region {
		groupID := m.regionLabeler.GetRegionLabel(region, labelKey)
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

// AdjustGroup validates the group and sets default values.
func (m *Manager) AdjustGroup(g *Group) error {
	if g.ID == "" {
		return errs.ErrAffinityGroupContent.FastGenByArgs("group ID should not be empty")
	}
	// TODO: Add more validation logic here if needed.
	if len(g.VoterStoreIDs) == 0 {
		return errs.ErrAffinityGroupContent.FastGenByArgs("voter store IDs should not be empty")
	}

	if m.storeSetInformer.GetStore(g.LeaderStoreID) == nil {
		return errs.ErrAffinityGroupContent.FastGenByArgs("leader store does not exist")
	}

	leaderInVoters := false
	storeSet := make(map[uint64]struct{})
	for _, storeID := range g.VoterStoreIDs {
		if storeID == g.LeaderStoreID {
			leaderInVoters = true
		}
		if _, exists := storeSet[storeID]; exists {
			return errs.ErrAffinityGroupContent.FastGenByArgs("duplicate voter store ID")
		}
		storeSet[storeID] = struct{}{}

		if m.storeSetInformer.GetStore(storeID) == nil {
			return errs.ErrAffinityGroupContent.FastGenByArgs("voter store does not exist")
		}
	}
	if !leaderInVoters {
		return errs.ErrAffinityGroupContent.FastGenByArgs("leader must be in voter stores")
	}
	return nil
}

// IsGroupExist checks if a group exists.
func (m *Manager) IsGroupExist(id string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.groups[id]
	return ok
}

// KeyRangeInput represents a key range input for validation.
type KeyRangeInput struct {
	StartKey []byte
	EndKey   []byte
	GroupID  string
}

// ValidateKeyRanges validates that the given key ranges do not overlap with existing ones.
// This is a public method that can be called from handlers.
func (m *Manager) ValidateKeyRanges(ranges []KeyRangeInput) error {
	m.RLock()
	defer m.RUnlock()

	// Convert KeyRangeInput to internal keyRange type
	internalRanges := make([]keyRange, len(ranges))
	for i, r := range ranges {
		internalRanges[i] = keyRange{
			startKey: r.StartKey,
			endKey:   r.EndKey,
			groupID:  r.GroupID,
		}
	}

	return m.validateNoKeyRangeOverlap(internalRanges)
}

// GetLabelRuleID returns the label rule ID for an affinity group.
// This ensures consistent naming between label creation and deletion.
// Format: "affinity_group/{group_id}"
func GetLabelRuleID(groupID string) string {
	return labelRuleIDPrefix + groupID
}

// parseAffinityGroupIDFromLabelRule parses the affinity group ID from the label rule.
// It will return the group ID and a boolean indicating whether the label rule is an affinity label rule.
func parseAffinityGroupIDFromLabelRule(rule *labeler.LabelRule) (string, bool) {
	// Validate the ID matches the expected format "affinity_group/{group_id}".
	if rule == nil || !strings.HasPrefix(rule.ID, labelRuleIDPrefix) {
		return "", false
	}
	// Retrieve the group ID.
	groupID := strings.TrimPrefix(rule.ID, labelRuleIDPrefix)
	if groupID == "" {
		return "", false
	}
	// Double-check the group ID from the label rule.
	var groupIDFromLabel string
	for _, label := range rule.Labels {
		if label.Key == labelKey {
			groupIDFromLabel = label.Value
			break
		}
	}
	if groupID != groupIDFromLabel {
		return "", false
	}
	return groupID, true
}

// GetGroups returns the internal groups map.
// Used for testing only.
func (m *Manager) GetGroups() map[string]*GroupInfo {
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

// GetAffinityGroupState gets the runtime state of an affinity group.
func (m *Manager) GetAffinityGroupState(id string) *GroupState {
	m.RLock()
	defer m.RUnlock()
	group, ok := m.groups[id]
	if ok {
		return newGroupState(group)
	}
	return nil
}

// GetAllAffinityGroupStates returns all affinity groups.
// TODO: it is a mock function now, need to implement the real logic.
func (m *Manager) GetAllAffinityGroupStates() []*GroupState {
	m.RLock()
	defer m.RUnlock()
	return nil
}

// GroupWithRanges represents a group with its associated key ranges.
type GroupWithRanges struct {
	Group     *Group
	KeyRanges []any
}

// SaveAffinityGroups adds multiple affinity groups to storage and creates corresponding label rules.
func (m *Manager) SaveAffinityGroups(groupsWithRanges []GroupWithRanges) error {
	// Validate all groups first (without lock)
	for _, gwr := range groupsWithRanges {
		if err := m.AdjustGroup(gwr.Group); err != nil {
			return err
		}
	}

	m.Lock()
	defer m.Unlock()

	// Step 0: Re-validate key ranges no overlaps under write lock
	var allNewRanges []keyRange
	for _, gwr := range groupsWithRanges {
		if len(gwr.KeyRanges) > 0 {
			ranges, err := parseKeyRangesFromData(gwr.KeyRanges, gwr.Group.ID)
			if err != nil {
				return err
			}
			allNewRanges = append(allNewRanges, ranges...)
		}
	}
	if err := m.validateNoKeyRangeOverlap(allNewRanges); err != nil {
		return err
	}

	// Step 1: Build batch operations for storage
	batch := make([]func(txn kv.Txn) error, 0, len(groupsWithRanges))
	for _, gwr := range groupsWithRanges {
		localGroup := gwr.Group
		batch = append(batch, func(txn kv.Txn) error {
			return m.storage.SaveAffinityGroup(txn, localGroup.ID, localGroup)
		})
	}
	err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, batch)
	if err != nil {
		return err
	}

	// Step 2: Create label rules for each group
	labelRules := make(map[string]*labeler.LabelRule)
	if m.regionLabeler != nil {
		for _, gwr := range groupsWithRanges {
			if len(gwr.KeyRanges) > 0 {
				labelRule := &labeler.LabelRule{
					ID:       GetLabelRuleID(gwr.Group.ID),
					Labels:   []labeler.RegionLabel{{Key: labelKey, Value: gwr.Group.ID}},
					RuleType: labeler.KeyRange,
					Data:     gwr.KeyRanges,
				}
				if err := m.regionLabeler.SetLabelRule(labelRule); err != nil {
					log.Error("failed to create label rule",
						zap.String("failed-group-id", gwr.Group.ID),
						zap.Int("total-groups", len(groupsWithRanges)),
						zap.Error(err))
					// TODO: rollback newly created groups
					return err
				}
				labelRules[gwr.Group.ID] = labelRule
			}
		}
	}

	// Step 3: Update in-memory cache with label rule pointers and key ranges
	for _, gwr := range groupsWithRanges {
		labelRule := labelRules[gwr.Group.ID]
		m.updateGroupLabelsLocked(gwr.Group.ID, labelRule)
		// Update key ranges cache for this group
		if len(gwr.KeyRanges) > 0 {
			ranges, err := parseKeyRangesFromData(gwr.KeyRanges, gwr.Group.ID)
			if err == nil && len(ranges) > 0 {
				m.keyRanges[gwr.Group.ID] = ranges
			}
		} else {
			// No key ranges, remove from cache if exists
			delete(m.keyRanges, gwr.Group.ID)
		}

		log.Info("affinity group added/updated", zap.String("group", gwr.Group.String()))
	}

	return nil
}

// DeleteAffinityGroup deletes an affinity group by ID and removes its label rule.
func (m *Manager) DeleteAffinityGroup(id string) error {
	m.Lock()
	defer m.Unlock()

	// Step 1: Delete from storage
	err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.storage.DeleteAffinityGroup(txn, id)
	})
	if err != nil {
		return err
	}

	// Step 2: Delete the corresponding label rule
	if m.regionLabeler != nil {
		labelRuleID := GetLabelRuleID(id)
		if err := m.regionLabeler.DeleteLabelRule(labelRuleID); err != nil {
			log.Warn("failed to delete label rule for affinity group",
				zap.String("group-id", id),
				zap.String("label-rule-id", labelRuleID),
				zap.Error(err))
			// Don't return error here - the group is already deleted from storage
		}
	}

	// Step 3: Delete from in-memory key ranges cache
	delete(m.keyRanges, id)

	// Step 4: Clean up regions map
	// Remove all region entries that reference this group
	var regionsToDelete []uint64
	for regionID, cache := range m.regions {
		if cache.groupInfo.ID == id {
			regionsToDelete = append(regionsToDelete, regionID)
		}
	}
	for _, regionID := range regionsToDelete {
		delete(m.regions, regionID)
	}

	m.deleteGroupLocked(id)
	log.Info("affinity group deleted",
		zap.String("group-id", id),
		zap.Int("cleaned-regions", len(regionsToDelete)))
	return nil
}

const (
	// defaultHealthCheckInterval is the default interval for checking store health.
	defaultHealthCheckInterval = 10 * time.Second
)

var (
	// healthCheckIntervalForTest can be set in tests to speed up health checks.
	// Default is 0, which means use defaultHealthCheckInterval.
	healthCheckIntervalForTest time.Duration
)

// getHealthCheckInterval returns the health check interval, which can be overridden for testing.
func getHealthCheckInterval() time.Duration {
	if healthCheckIntervalForTest > 0 {
		return healthCheckIntervalForTest
	}
	return defaultHealthCheckInterval
}

// SetHealthCheckIntervalForTest sets the health check interval for testing. Only use this in tests.
func SetHealthCheckIntervalForTest(interval time.Duration) {
	healthCheckIntervalForTest = interval
}

// startHealthCheckLoop starts a goroutine to periodically check store health and invalidate groups with unhealthy stores.
func (m *Manager) startHealthCheckLoop() {
	interval := getHealthCheckInterval()
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				log.Info("affinity manager health check loop stopped")
				return
			case <-ticker.C:
				m.checkStoreHealth()
			}
		}
	}()
	log.Info("affinity manager health check loop started", zap.Duration("interval", interval))
}

// checkStoreHealth checks the health status of stores and invalidates groups with unhealthy stores.
func (m *Manager) checkStoreHealth() {
	m.Lock()
	defer m.Unlock()

	if !m.initialized {
		return
	}

	for groupID, groupInfo := range m.groups {
		// Check if any store in the group is unhealthy
		unhealthyStores := m.getUnhealthyStores(groupInfo)

		if len(unhealthyStores) > 0 && groupInfo.Effect {
			// If the group was previously in effect and now has unhealthy stores, invalidate it
			m.updateGroupEffectLocked(groupID, 0, 0, nil)
			log.Warn("affinity group invalidated due to unhealthy stores",
				zap.String("group-id", groupID),
				zap.Uint64s("unhealthy-stores", unhealthyStores))
		}
	}
}

// getUnhealthyStores returns the list of unhealthy store IDs in the group.
func (m *Manager) getUnhealthyStores(groupInfo *GroupInfo) []uint64 {
	var unhealthyStores []uint64
	unhealthyStoreSet := make(map[uint64]struct{})

	isStoreUnhealthy := func(storeID uint64) bool {
		store := m.storeSetInformer.GetStore(storeID)
		if store == nil {
			return true
		}
		// Check if store is removed or physically destroyed
		if store.IsRemoved() || store.IsPhysicallyDestroyed() {
			return true
		}
		// Check if store is removing
		if store.IsRemoving() {
			return true
		}
		// Use IsUnhealthy (10min) to avoid frequent state flapping
		// IsUnhealthy: DownTime > 10min (storeUnhealthyDuration)
		// IsDisconnected: DownTime > 20s (storeDisconnectDuration) - too sensitive
		if store.IsUnhealthy() {
			return true
		}
		// Note: We intentionally do NOT check:
		// - IsDisconnected(): Too sensitive (20s), would cause frequent flapping
		// - IsSlow(): Performance issue, not availability issue
		// - IsLowSpace(): Not mentioned in design doc, needs separate consideration
		// TODO: maybe IsLowSpace() should also be considered in the future
		return false
	}

	// Check leader store
	if isStoreUnhealthy(groupInfo.LeaderStoreID) {
		unhealthyStoreSet[groupInfo.LeaderStoreID] = struct{}{}
	}

	// Check voter stores
	for _, voterStoreID := range groupInfo.VoterStoreIDs {
		if isStoreUnhealthy(voterStoreID) {
			unhealthyStoreSet[voterStoreID] = struct{}{}
		}
	}

	// Convert set to slice
	for storeID := range unhealthyStoreSet {
		unhealthyStores = append(unhealthyStores, storeID)
	}

	return unhealthyStores
}

// keyRange represents a key range extracted from label rules.
type keyRange struct {
	startKey []byte
	endKey   []byte
	groupID  string
}

// parseKeyRangesFromData parses key ranges from []any format (from API or label rule).
// This is a common helper to avoid code duplication.
func parseKeyRangesFromData(data []any, groupID string) ([]keyRange, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var ranges []keyRange
	for _, item := range data {
		rangeMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		startKeyStr, ok1 := rangeMap["start_key"].(string)
		endKeyStr, ok2 := rangeMap["end_key"].(string)
		if !ok1 || !ok2 {
			continue
		}

		startKey, err := hex.DecodeString(startKeyStr)
		if err != nil {
			log.Warn("failed to decode start key", zap.String("group-id", groupID), zap.Error(err))
			continue
		}

		endKey, err := hex.DecodeString(endKeyStr)
		if err != nil {
			log.Warn("failed to decode end key", zap.String("group-id", groupID), zap.Error(err))
			continue
		}

		ranges = append(ranges, keyRange{
			startKey: startKey,
			endKey:   endKey,
			groupID:  groupID,
		})
	}

	return ranges, nil
}

// extractKeyRangesFromLabelRule extracts key ranges from a label rule data.
func extractKeyRangesFromLabelRule(rule *labeler.LabelRule) ([]keyRange, error) {
	if rule == nil || rule.Data == nil {
		return nil, nil
	}

	groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
	if !ok {
		return nil, nil
	}

	dataSlice, ok := rule.Data.([]any)
	if !ok {
		return nil, errs.ErrAffinityGroupContent.FastGenByArgs("invalid label rule data format")
	}

	return parseKeyRangesFromData(dataSlice, groupID)
}

// checkKeyRangesOverlap checks if two key ranges overlap.
// Returns true if [start1, end1) and [start2, end2) have any overlap.
func checkKeyRangesOverlap(start1, end1, start2, end2 []byte) bool {
	// Handle empty keys (representing infinity)
	if len(start1) == 0 && len(end1) == 0 {
		return true // Range covers everything
	}
	if len(start2) == 0 && len(end2) == 0 {
		return true // Range covers everything
	}

	// Check if ranges are disjoint
	// Range 1 ends before Range 2 starts: end1 <= start2
	if len(end1) > 0 && len(start2) > 0 && bytes.Compare(end1, start2) <= 0 {
		return false
	}

	// Range 2 ends before Range 1 starts: end2 <= start1
	if len(end2) > 0 && len(start1) > 0 && bytes.Compare(end2, start1) <= 0 {
		return false
	}

	return true
}

// validateNoKeyRangeOverlap validates that the given key ranges do not overlap with existing ones.
// It should be called with the manager lock held.
// Uses in-memory keyRanges cache to avoid repeated labeler access and reduce lock contention.
func (m *Manager) validateNoKeyRangeOverlap(newRanges []keyRange) error {
	// First, check for overlaps within the new ranges themselves
	for i := range newRanges {
		for j := i + 1; j < len(newRanges); j++ {
			if checkKeyRangesOverlap(
				newRanges[i].startKey, newRanges[i].endKey,
				newRanges[j].startKey, newRanges[j].endKey,
			) {
				return errs.ErrAffinityGroupContent.FastGenByArgs(
					"key ranges overlap within the same request: group " +
						newRanges[i].groupID + " and " + newRanges[j].groupID)
			}
		}
	}

	// Then, check for overlaps with existing key ranges from in-memory cache
	// This avoids repeated labeler lock acquisition for better performance
	for _, newRange := range newRanges {
		for groupID, existingRanges := range m.keyRanges {
			// Skip if it's the same group (updating existing group)
			if newRange.groupID == groupID {
				continue
			}

			for _, existingRange := range existingRanges {
				if checkKeyRangesOverlap(
					newRange.startKey, newRange.endKey,
					existingRange.startKey, existingRange.endKey,
				) {
					return errs.ErrAffinityGroupContent.FastGenByArgs(
						"key range overlaps with existing group: new group " +
							newRange.groupID + " overlaps with group " + existingRange.groupID)
				}
			}
		}
	}

	return nil
}

// loadRegionLabel rebuilds the mapping between groups and label rules after restart.
// It should be called with the manager lock held.
func (m *Manager) loadRegionLabel() error {
	if m.regionLabeler == nil {
		return nil
	}

	// Collect all key ranges from label rules and populate in-memory cache
	var allRanges []keyRange

	m.regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
		groupID, ok := parseAffinityGroupIDFromLabelRule(rule)
		if !ok {
			// Not an affinity label rule, skip
			return true
		}

		ranges, err := extractKeyRangesFromLabelRule(rule)
		if err != nil {
			log.Warn("failed to extract key ranges from label rule during rebuild",
				zap.String("rule-id", rule.ID),
				zap.String("group-id", groupID),
				zap.Error(err))
			return true
		}

		if len(ranges) > 0 {
			allRanges = append(allRanges, ranges...)
			// Populate in-memory key ranges cache
			m.keyRanges[groupID] = ranges
		}

		// Associate the label rule with the group
		if _, ok = m.keyRanges[groupID]; !ok {
			log.Warn("found label rule for unknown affinity group",
				zap.String("group-id", groupID),
				zap.String("rule-id", rule.ID))
		} else {
			m.updateGroupLabelsLocked(groupID, rule)
		}

		return true
	})

	// Validate that all key ranges are non-overlapping
	for i := range allRanges {
		for j := i + 1; j < len(allRanges); j++ {
			if checkKeyRangesOverlap(
				allRanges[i].startKey, allRanges[i].endKey,
				allRanges[j].startKey, allRanges[j].endKey,
			) {
				return errs.ErrAffinityGroupContent.FastGenByArgs(
					"found overlapping key ranges during rebuild: group " +
						allRanges[i].groupID + " overlaps with group " + allRanges[j].groupID)
			}
		}
	}

	log.Info("rebuilt group-label mapping",
		zap.Int("total-groups", len(m.keyRanges)),
		zap.Int("total-ranges", len(allRanges)))

	return nil
}

// IsRegionAffinity checks if a region conforms to its affinity group distribution requirements.
// Returns true if the region:
// Belongs to an affinity group and satisfies all distribution constraints:
//   - Leader is on the expected store
//   - All voters are on the expected stores
func (m *Manager) IsRegionAffinity(region *core.RegionInfo) bool {
	_, isAffinity := m.GetRegionAffinityGroupState(region)
	return isAffinity
}
