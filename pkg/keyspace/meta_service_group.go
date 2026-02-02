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

package keyspace

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

const (
	flushInterval  = 5 * time.Minute
	flushThreshold = 1000
)

// MetaServiceGroupManager manages external meta-service groups.
type MetaServiceGroupManager struct {
	ctx   context.Context
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	autoAssign        bool
	metaServiceGroups map[string]string
	fallbackRatio     float64
	cachedStatus      map[string]*endpoint.MetaServiceGroupStatus
	dirtyCount        int
	flushCh           chan struct{}
	isLeader          func() bool
}

// NewMetaServiceGroupManager creates a new MetaServiceGroupManager.
func NewMetaServiceGroupManager(
	ctx context.Context,
	store endpoint.MetaServiceGroupStorage,
	config Config,
) (*MetaServiceGroupManager, error) {
	m := &MetaServiceGroupManager{
		ctx:               ctx,
		store:             store,
		autoAssign:        config.GetAutoAssignMetaServiceGroups(),
		metaServiceGroups: config.GetMetaServiceGroups(),
		fallbackRatio:     config.GetMetaServiceGroupsFallbackRatio(),
		flushCh:           make(chan struct{}, 1),
	}
	if err := m.RefreshCache(); err != nil {
		return nil, err
	}
	go m.flushLoop()
	return m, nil
}

// SetLeaderChecker sets a function to determine leader ownership.
func (m *MetaServiceGroupManager) SetLeaderChecker(isLeader func() bool) {
	m.Lock()
	defer m.Unlock()
	m.isLeader = isLeader
}

// RefreshCache loads the current status from storage into memory.
func (m *MetaServiceGroupManager) RefreshCache() error {
	m.Lock()
	defer m.Unlock()
	var (
		err       error
		statusMap map[string]*endpoint.MetaServiceGroupStatus
	)

	if err = m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		statusMap, err = m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Error("[keyspace] failed to load meta-service groups statuses from storage", zap.Error(err))
		return err
	}
	log.Info("[keyspace] meta-service groups statuses loaded from storage", zap.Any("meta-service groups statuses", statusMap))
	m.cachedStatus = statusMap
	return nil
}

// flushLoop handles timing and signals to persist data.
func (m *MetaServiceGroupManager) flushLoop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			log.Info("[keyspace] periodic flush of meta-service groups statuses to storage")
			if err := m.flushToStorage(); err != nil {
				log.Error("[keyspace] failed to flush meta-service groups statuses to storage", zap.Error(err))
			}
		case <-m.flushCh:
			log.Info("[keyspace] triggered flush of meta-service groups statuses to storage")
			if err := m.flushToStorage(); err != nil {
				log.Error("[keyspace] failed to flush meta-service groups statuses to storage", zap.Error(err))
			}
		}
	}
}

// flushToStorage persists all in-memory status to storage in a single transaction.
func (m *MetaServiceGroupManager) flushToStorage() error {
	m.Lock()
	defer m.Unlock()
	// Safeguard: only leader can flush data.
	if m.isLeader != nil && !m.isLeader() {
		return nil
	}
	if m.dirtyCount == 0 {
		return nil
	}
	err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for id, status := range m.cachedStatus {
			if err := m.store.SaveMetaServiceGroupStatus(txn, id, status); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		m.dirtyCount = 0
	}
	return err
}

// GetStatus returns the status of each meta-service group.
func (m *MetaServiceGroupManager) GetStatus() (map[string]*endpoint.MetaServiceGroupStatus, error) {
	m.RLock()
	defer m.RUnlock()
	statuses := make(map[string]*endpoint.MetaServiceGroupStatus, len(m.cachedStatus))
	for groupID, status := range m.cachedStatus {
		copiedStatus := *status
		statuses[groupID] = &copiedStatus
	}
	return statuses, nil
}

// MetaServiceGroupStatusPatch represents a patch operation for a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatusPatch struct {
	AssignedCount *int  `json:"assigned_count,omitempty"` // nil means no change, 0 means reset to 0
	Enabled       *bool `json:"enabled,omitempty"`        // nil means no change, true means enable, false means disable
}

// PatchStatus applies a patch to the status of a meta-service group.
func (m *MetaServiceGroupManager) PatchStatus(groupID string, patch *MetaServiceGroupStatusPatch) error {
	m.Lock()
	defer m.Unlock()
	currentStatus, exists := m.cachedStatus[groupID]
	if !exists {
		return errUnknownMetaServiceGroup
	}
	newStatus := *currentStatus
	if patch.AssignedCount != nil {
		newStatus.AssignmentCount = *patch.AssignedCount
	}
	if patch.Enabled != nil {
		newStatus.Enabled = *patch.Enabled
	}
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.store.SaveMetaServiceGroupStatus(txn, groupID, &newStatus)
	}); err != nil {
		return err
	}
	m.cachedStatus[groupID] = &newStatus
	return nil
}

// AssignToGroup increments count of the enabled meta-service group with least assigned keyspaces.
// It returns the assigned meta-service group ID, or an empty string if assignment falls back to PD or no group is available.
func (m *MetaServiceGroupManager) AssignToGroup(count int) string {
	// Use failpoint to test with RLock in test scenarios
	failpoint.Inject("useRLockInAssignToGroup", func() {
		m.RLock()
		defer m.RUnlock()
		failpoint.Return(m.assignToGroupImpl(count))
	})
	m.Lock()
	defer m.Unlock()
	return m.assignToGroupImpl(count)
}

// assignToGroupImpl contains the actual implementation of AssignToGroup.
func (m *MetaServiceGroupManager) assignToGroupImpl(count int) string {
	if roll := rand.Float64(); roll < m.fallbackRatio {
		log.Info("[keyspace] fallback meta-service group assignment to PD due to fallback ratio",
			zap.Float64("roll", roll),
			zap.Float64("fallback ratio", m.fallbackRatio),
		)
		return ""
	}

	// Search meta-service group with min assigned count.
	var assignedGroup string
	minCount := math.MaxInt
	for groupID, status := range m.cachedStatus {
		if status.Enabled && status.AssignmentCount < minCount {
			minCount = status.AssignmentCount
			assignedGroup = groupID
		}
	}
	if assignedGroup == "" {
		log.Warn("[keyspace] fallback meta-service group assignment to PD due to no available meta-service group",
			zap.Any("meta-service groups status", m.cachedStatus),
		)
		return ""
	}
	// Update assigned group status, trigger flush if necessary
	m.cachedStatus[assignedGroup].AssignmentCount += count
	m.dirtyCount += count
	if m.dirtyCount >= flushThreshold {
		select {
		case m.flushCh <- struct{}{}:
		default:
		}
	}
	return assignedGroup
}

// UpdateAssignment moves a keyspace from one meta-service group to another.
// It returns an error if any.
func (m *MetaServiceGroupManager) UpdateAssignment(oldGroupID, newGroupID string) error {
	m.Lock()
	defer m.Unlock()
	// Newly assigned meta-service group must be available.
	if newGroupID != "" && m.metaServiceGroups[newGroupID] == "" {
		return errUnknownMetaServiceGroup
	}
	if m.cachedStatus[oldGroupID] != nil {
		m.cachedStatus[oldGroupID].AssignmentCount--
	}
	if m.cachedStatus[newGroupID] != nil {
		m.cachedStatus[newGroupID].AssignmentCount++
	}
	m.dirtyCount += 2
	if m.dirtyCount >= flushThreshold {
		select {
		case m.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// AttachEndpoints append potential meta-service group endpoint to the given keyspace config map.
func (m *MetaServiceGroupManager) AttachEndpoints(keyspaceConfig map[string]string) {
	groupID := keyspaceConfig[MetaServiceGroupIDKey]
	if groupID == "" {
		return
	}
	m.RLock()
	defer m.RUnlock()
	if endpoints := m.metaServiceGroups[groupID]; endpoints != "" {
		keyspaceConfig[MetaServiceGroupAddressesKey] = endpoints
	}
}

// GetGroups returns currently available meta-service groups.
func (m *MetaServiceGroupManager) GetGroups() map[string]string {
	m.RLock()
	defer m.RUnlock()
	return m.metaServiceGroups
}

// HasGroup returns whether the given meta-service group exists.
func (m *MetaServiceGroupManager) HasGroup(groupID string) bool {
	if groupID == "" {
		return false
	}
	m.RLock()
	defer m.RUnlock()
	_, ok := m.metaServiceGroups[groupID]
	return ok
}

// GetAutoAssign returns whether auto-assigning keyspaces to meta-service groups is enabled.
func (m *MetaServiceGroupManager) GetAutoAssign() bool {
	m.RLock()
	defer m.RUnlock()
	return m.autoAssign
}

// updateConfig updates currently available meta-service groups.
func (m *MetaServiceGroupManager) updateConfig(autoAssign bool, metaServiceGroups map[string]string, fallbackRatio float64) {
	m.Lock()
	defer m.Unlock()
	m.autoAssign = autoAssign
	m.fallbackRatio = fallbackRatio
	m.metaServiceGroups = metaServiceGroups
	// Handle newly added meta-service groups.
	for groupID := range metaServiceGroups {
		if _, ok := m.cachedStatus[groupID]; !ok {
			m.cachedStatus[groupID] = &endpoint.MetaServiceGroupStatus{}
		}
	}
	// Handle newly removed meta-service groups.
	for groupID := range m.cachedStatus {
		if _, ok := m.metaServiceGroups[groupID]; !ok {
			delete(m.cachedStatus, groupID)
		}
	}
}
