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

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

// MetaServiceGroupManager manages external meta-service groups.
type MetaServiceGroupManager struct {
	ctx   context.Context
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	autoAssign        bool
	metaServiceGroups map[string]string
	fallbackRatio     float64
}

// NewMetaServiceGroupManager creates a new MetaServiceGroupManager.
func NewMetaServiceGroupManager(
	ctx context.Context,
	store endpoint.MetaServiceGroupStorage,
	config Config,
) *MetaServiceGroupManager {
	return &MetaServiceGroupManager{
		ctx:               ctx,
		store:             store,
		autoAssign:        config.GetAutoAssignMetaServiceGroups(),
		metaServiceGroups: config.GetMetaServiceGroups(),
		fallbackRatio:     config.GetMetaServiceGroupsFallbackRatio(),
	}
}

// GetAssignmentCounts returns the count of each meta-service group.
func (m *MetaServiceGroupManager) GetStatus() (map[string]*endpoint.MetaServiceGroupStatus, error) {
	m.RLock()
	defer m.RUnlock()
	var (
		err       error
		statusMap map[string]*endpoint.MetaServiceGroupStatus
	)
	err = m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		statusMap, err = m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		if err != nil {
			return err
		}
		return nil
	})
	return statusMap, err
}

// MetaServiceGroupStatusPatch represents a patch operation for a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatusPatch struct {
	AssignedCount *int  `json:"assigned_count,omitempty"` // nil means no change, 0 means reset to 0
	Enabled       *bool `json:"enabled,omitempty"`        // nil means no change, true means enable, false means disable
}

// PatchStatus applies a patch to the status of a meta-service group.
func (m *MetaServiceGroupManager) PatchStatus(groupID string, patch *MetaServiceGroupStatusPatch) error {
	m.RLock()
	defer m.RUnlock()
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		if err != nil {
			return err
		}
		status, exists := statusMap[groupID]
		if !exists {
			return errUnknownMetaServiceGroup
		}
		if patch.AssignedCount != nil {
			status.AssignmentCount = *patch.AssignedCount
		}
		if patch.Enabled != nil {
			status.Enabled = *patch.Enabled
		}
		return m.store.SaveMetaServiceGroupStatus(txn, groupID, status)
	})
}

// AssignToGroup increments count of the enabled meta-service group with least assigned keyspaces.
// It returns the assigned meta-service group and an error if any.
func (m *MetaServiceGroupManager) AssignToGroup(count int) (string, error) {
	m.RLock()
	defer m.RUnlock()
	if roll := rand.Float64(); roll < m.fallbackRatio {
		log.Info("[keyspace] fallback meta-service group assignment to PD due to fallback ratio",
			zap.Float64("roll", roll),
			zap.Float64("fallback ratio", m.fallbackRatio),
		)
		return "", nil
	}
	var (
		assignedGroup       string
		assignedGroupStatus *endpoint.MetaServiceGroupStatus
	)
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		if err != nil {
			return err
		}
		minCount := math.MaxInt
		for currentGroup, currentGroupStatus := range statusMap {
			// only consider enabled groups
			if currentGroupStatus.Enabled && currentGroupStatus.AssignmentCount < minCount {
				minCount = currentGroupStatus.AssignmentCount
				assignedGroup = currentGroup
				assignedGroupStatus = currentGroupStatus
			}
		}
		if assignedGroup == "" {
			log.Warn("[keyspace] fallback meta-service group assignment to PD due to no available meta-service group",
				zap.Any("meta-service groups status", statusMap),
			)
			return nil
		}
		assignedGroupStatus.AssignmentCount += count
		return m.store.SaveMetaServiceGroupStatus(txn, assignedGroup, assignedGroupStatus)
	}); err != nil {
		return "", err
	}
	return assignedGroup, nil
}

// UpdateAssignment moves a keyspace from one meta-service group to another.
// It returns an error if any.
func (m *MetaServiceGroupManager) UpdateAssignment(oldGroupID, newGroupID string) error {
	m.RLock()
	defer m.RUnlock()
	// Newly assigned meta-service group must be available.
	if newGroupID != "" && m.metaServiceGroups[newGroupID] == "" {
		return errUnknownMetaServiceGroup
	}
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.UpdateAssignmentWithTxn(txn, oldGroupID, newGroupID)
	})
}

// UpdateAssignmentWithTxn updates the assignment of a keyspace from one meta-service group to another within a transaction.
func (m *MetaServiceGroupManager) UpdateAssignmentWithTxn(txn kv.Txn, oldGroupID string, newGroupID string) error {
	statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
	if err != nil {
		return err
	}
	if status, exists := statusMap[oldGroupID]; exists {
		log.Info("[keyspace] update meta-service group assignment", zap.Any("status", status))
		status.AssignmentCount--
		if err := m.store.SaveMetaServiceGroupStatus(txn, oldGroupID, status); err != nil {
			return err
		}
	}
	if status, exists := statusMap[newGroupID]; exists {
		status.AssignmentCount++
		if err := m.store.SaveMetaServiceGroupStatus(txn, newGroupID, status); err != nil {
			return err
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
	m.metaServiceGroups = metaServiceGroups
	m.fallbackRatio = fallbackRatio
}
