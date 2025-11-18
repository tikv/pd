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
	"errors"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
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
	// LeaderStoreID indicates which store the leader should be on.
	LeaderStoreID uint64 `json:"leader_store_id"`
	// VoterStoreIDs indicates which stores Voters should be on.
	VoterStoreIDs []uint64 `json:"voter_store_ids"`
	// LabelCount indicates how many key ranges are associated with this group.
	LabelCount int `json:"label_count"`
	// RegionCount indicates how many Regions are currently in the affinity state.
	RegionCount int `json:"region_count"`
	// RegionVotersReadyCount indicates how many Regions have all Voter peers in the correct stores.
	RegionVotersReadyCount int `json:"region_voters_ready_count"`
	// RegionLeadersReadyCount indicates how many Regions have their leader in the correct store.
	RegionLeadersReadyCount int `json:"region_leaders_ready_count"`
}

// GroupInfo contains meta information and runtime statistics for the Group.
type GroupInfo struct {
	Group

	// Effect parameter indicates whether the current constraint is in effect.
	// Constraints are typically released when the store is in an abnormal state.
	Effect bool
	// AffinityRegionCount indicates how many Regions are currently in the affinity state.
	AffinityRegionCount uint64

	// nolint:unused
	regions map[uint64]struct{}
	// nolint:unused
	labels map[string]*labeler.LabelRule
}

// Manager is the manager of all affinity information.
type Manager struct {
	syncutil.RWMutex
	ctx              context.Context
	storage          endpoint.AffinityStorage
	initialized      bool
	groups           map[string]*GroupInfo // {group_id} -> GroupInfo
	regions          map[uint64]*GroupInfo // {region_id} -> GroupInfo
	storeSetInformer core.StoreSetInformer
	conf             config.SharedConfigProvider
}

// NewManager creates a new affinity Manager.
func NewManager(ctx context.Context, storage endpoint.AffinityStorage, storeSetInformer core.StoreSetInformer, conf config.SharedConfigProvider) *Manager {
	return &Manager{
		ctx:              ctx,
		storage:          storage,
		storeSetInformer: storeSetInformer,
		conf:             conf,
		groups:           make(map[string]*GroupInfo),
		regions:          make(map[uint64]*GroupInfo),
	}
}

// Initialize loads affinity groups from storage.
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
		m.groups[group.ID] = &GroupInfo{Group: *group, Effect: true}
	})
	if err != nil {
		return err
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

// GetAffinityGroup gets a specific affinity group by ID.
// It returns a clone of the persisted Group object.
func (m *Manager) GetAffinityGroup(id string) *Group {
	m.RLock()
	defer m.RUnlock()
	if info, ok := m.groups[id]; ok {
		return info.Clone() // Return a clone of Group to prevent concurrent modification
	}
	return nil
}

// GetAllAffinityGroups returns all affinity groups.
func (m *Manager) GetAllAffinityGroups() []*Group {
	m.RLock()
	defer m.RUnlock()
	groups := make([]*Group, 0, len(m.groups))
	for _, info := range m.groups {
		groups = append(groups, info.Clone()) // Clone Group, not GroupInfo
	}
	// Sort by ID for deterministic output
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].ID < groups[j].ID
	})
	return groups
}

// IsGroupExist checks if a group exists.
func (m *Manager) IsGroupExist(id string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.groups[id]
	return ok
}

// GetLabelRuleID returns the label rule ID for an affinity group.
// This ensures consistent naming between label creation and deletion.
// Format: "affinity_group/{group_id}"
func GetLabelRuleID(groupID string) string {
	return "affinity_group/" + groupID
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

	m.regions[regionID] = groupInfo
	if groupInfo.regions != nil {
		groupInfo.regions[regionID] = struct{}{}
	}
}

// GetRegionAffinityGroup returns the affinity group info for a region.
// Returns nil if the region doesn't belong to any affinity group.
// NOTE: Returns a lightweight copy with only essential fields to avoid concurrent issues
// and minimize overhead. The regions and labels maps are not copied.
func (m *Manager) GetRegionAffinityGroup(regionID uint64) *GroupInfo {
	m.RLock()
	defer m.RUnlock()

	groupInfo := m.regions[regionID]
	if groupInfo == nil {
		return nil
	}

	if _, exists := m.groups[groupInfo.ID]; !exists {
		return nil
	}

	// Return a lightweight copy with only essential fields
	// This is more efficient than full Clone() and sufficient for checker usage
	return &GroupInfo{
		Group: Group{
			ID:              groupInfo.ID,
			CreateTimestamp: groupInfo.CreateTimestamp,
			LeaderStoreID:   groupInfo.LeaderStoreID,
			VoterStoreIDs:   append([]uint64(nil), groupInfo.VoterStoreIDs...), // Copy slice
		},
		Effect:              groupInfo.Effect,
		AffinityRegionCount: groupInfo.AffinityRegionCount,
		// regions and labels maps are intentionally not copied for performance
	}
}

// AllocAffinityGroup alloc store IDs for a new affinity group based on the current cluster state.
// TODO: it is a mock function now, need to implement the real logic.
func (m *Manager) AllocAffinityGroup(name string, ranges []keyutil.KeyRange, dataLayout string, tableGroup string) (group *Group, err error) {
	m.Lock()
	defer m.Unlock()
	log.Info("allocating affinity group",
		zap.String("group-name", name),
		zap.String("data-layout", dataLayout),
		zap.String("table-group", tableGroup),
		zap.Int("key-range-count", len(ranges)),
	)
	return nil, errors.New("not implement")
}

// GetAffinityGroupState gets the runtime state of an affinity group.
// TODO: it is a mock function now, need to implement the real logic.
func (m *Manager) GetAffinityGroupState(id string) *GroupState {
	m.RLock()
	defer m.RUnlock()
	log.Info("getting affinity group state", zap.String("group-id", id))
	groupState := &GroupState{}
	return groupState
}

// SaveAffinityGroup saves an affinity group to storage.
func (m *Manager) SaveAffinityGroup(group *Group) error {
	if err := m.AdjustGroup(group); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.storage.SaveAffinityGroup(txn, group.ID, group)
	})
	if err != nil {
		return err
	}

	// Update in-memory cache
	info, ok := m.groups[group.ID]
	if ok {
		// TODO: Do we need to overwrite runtime info?
		info.Group = *group
	} else {
		m.groups[group.ID] = &GroupInfo{Group: *group, Effect: true}
	}

	log.Info("affinity group added/updated", zap.String("group", group.String()))
	return nil
}

// SaveAffinityGroups adds multiple affinity groups to storage.
func (m *Manager) SaveAffinityGroups(groups []*Group) error {
	for _, group := range groups {
		if err := m.AdjustGroup(group); err != nil {
			return err
		}
	}
	m.Lock()
	defer m.Unlock()

	// Build batch operations
	batch := make([]func(txn kv.Txn) error, 0, len(groups))
	for _, group := range groups {
		localGroup := group
		batch = append(batch, func(txn kv.Txn) error {
			return m.storage.SaveAffinityGroup(txn, localGroup.ID, localGroup)
		})
	}

	// Execute batch operations
	err := endpoint.RunBatchOpInTxn(m.ctx, m.storage, batch)
	if err != nil {
		return err
	}

	// Update in-memory cache
	for _, group := range groups {
		info, ok := m.groups[group.ID]
		if ok {
			info.Group = *group
		} else {
			m.groups[group.ID] = &GroupInfo{
				Group:   *group,
				Effect:  true,
				regions: make(map[uint64]struct{}),
				labels:  make(map[string]*labeler.LabelRule),
			}
		}

		log.Info("affinity group added/updated", zap.String("group", group.String()))
	}

	// TODO: Update regions map by scanning regions with affinity_group label
	return nil
}

// DeleteAffinityGroup deletes an affinity group by ID.
// This is analogous to RuleManager.DeleteRule[cite: 2810].
func (m *Manager) DeleteAffinityGroup(id string) error {
	m.Lock()
	defer m.Unlock()

	// Check existence first and save reference for cleanup
	info, ok := m.groups[id]
	if !ok {
		return errs.ErrAffinityGroupNotFound.GenWithStackByArgs(id)
	}

	err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.storage.DeleteAffinityGroup(txn, id)
	})
	if err != nil {
		return err
	}

	// Clean up regions map
	// Remove all region entries that reference this group
	var regionsToDelete []uint64
	for regionID, groupInfo := range m.regions {
		if groupInfo.ID == id {
			regionsToDelete = append(regionsToDelete, regionID)
		}
	}
	for _, regionID := range regionsToDelete {
		delete(m.regions, regionID)
	}

	// Clean up labels (if label rules are stored in GroupInfo)
	if info.labels != nil {
		for labelID := range info.labels {
			log.Debug("label rule reference found in group info",
				zap.String("group-id", id),
				zap.String("label-id", labelID))
			// Labels are managed by regionLabeler, not directly deleted here
		}
	}

	// Delete from in-memory cache
	delete(m.groups, id)

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

		if len(unhealthyStores) > 0 {
			// If the group was previously in effect and now has unhealthy stores, invalidate it
			if groupInfo.Effect {
				groupInfo.Effect = false
				log.Warn("affinity group invalidated due to unhealthy stores",
					zap.String("group-id", groupID),
					zap.Uint64s("unhealthy-stores", unhealthyStores))
			}
		} else {
			// If all stores are healthy and the group was previously invalidated, restore it
			if !groupInfo.Effect {
				groupInfo.Effect = true
				log.Info("affinity group restored to effect state",
					zap.String("group-id", groupID))
			}
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
