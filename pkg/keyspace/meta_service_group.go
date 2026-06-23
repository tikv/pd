// Copyright 2026 TiKV Project Authors.
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
	"fmt"
	"math"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
)

// MetaServiceGroupManager manages external meta-service groups.
type MetaServiceGroupManager struct {
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	// metaServiceGroups is the available external meta-service groups.
	// The key is the meta-service group name, and the value is the corresponding endpoint.
	metaServiceGroups map[string]string
}

// NewMetaServiceGroupManager creates a new MetaServiceGroupManager.
func NewMetaServiceGroupManager(
	store endpoint.MetaServiceGroupStorage,
	metaServiceGroups map[string]string,
) *MetaServiceGroupManager {
	return &MetaServiceGroupManager{
		store:             store,
		metaServiceGroups: metaServiceGroups,
	}
}

// GetAssignmentCounts returns the count of each meta-service group.
// todo: optimize by caching the counts and watching the changes of meta-service groups.
func (m *MetaServiceGroupManager) GetAssignmentCounts(ctx context.Context) (map[string]int, error) {
	m.RLock()
	defer m.RUnlock()
	var (
		err   error
		count map[string]int
	)
	err = m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		count, err = m.store.GetAssignmentCount(txn, m.metaServiceGroups)
		return err
	})
	return count, err
}

func (m *MetaServiceGroupManager) findMinMetaGroup(txn kv.Txn) (string, error) {
	countMap, err := m.store.GetAssignmentCount(txn, m.metaServiceGroups)
	if err != nil {
		return "", err
	}
	minCount := math.MaxInt
	var assignedGroup string
	for currentGroup, currentCount := range countMap {
		if currentCount < minCount {
			minCount = currentCount
			assignedGroup = currentGroup
		}
	}
	if assignedGroup == "" {
		return "", errNoAvailableMetaServiceGroups
	}
	return assignedGroup, nil
}

// PickGroup returns the meta-service group with the least assigned keyspaces
// without updating the persisted assignment count.
func (m *MetaServiceGroupManager) PickGroup(ctx context.Context) (string, error) {
	m.RLock()
	defer m.RUnlock()
	var assignedGroup string
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		if assignedGroup, err = m.findMinMetaGroup(txn); err != nil {
			return err
		}
		return m.updateAssignmentTxn(txn, "", assignedGroup)
	}); err != nil {
		return "", err
	}
	return assignedGroup, nil
}

// AssignToGroup increments count of the meta-service group with least assigned keyspaces.
// It returns the assigned meta-service group and an error if any.
// only used for testing now, as it doesn't guarantee the atomicity of select and update. UpdateAssignment should be used in production code instead.
func (m *MetaServiceGroupManager) AssignToGroup(ctx context.Context, count int) (string, error) {
	m.RLock()
	defer m.RUnlock()
	var assignedGroup string
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		assignedGroup, err = m.findMinMetaGroup(txn)
		if err != nil {
			return err
		}
		return m.store.IncrementAssignmentCount(txn, assignedGroup, count)
	}); err != nil {
		return "", err
	}
	return assignedGroup, nil
}

// reassignKeyspaceLocked validates that newGroupID (if any) still exists and
// moves a single keyspace assignment from oldGroupID to newGroupID within txn.
// The caller must hold the read lock for the whole enclosing transaction so a
// concurrent UpdateGroupsSafely cannot delete a group between this validation
// and the persisted assignment count update.
func (m *MetaServiceGroupManager) reassignKeyspaceLocked(txn kv.Txn, oldGroupID, newGroupID string) error {
	if newGroupID != "" {
		if _, ok := m.metaServiceGroups[newGroupID]; !ok {
			return errUnknownMetaServiceGroup
		}
	}
	return m.updateAssignmentTxn(txn, oldGroupID, newGroupID)
}

func (m *MetaServiceGroupManager) updateAssignmentTxn(txn kv.Txn, oldGroupID, newGroupID string) error {
	if oldGroupID != "" {
		if err := m.store.IncrementAssignmentCount(txn, oldGroupID, -1); err != nil {
			return err
		}
	}
	if newGroupID != "" {
		if err := m.store.IncrementAssignmentCount(txn, newGroupID, 1); err != nil {
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
	groups := make(map[string]string, len(m.metaServiceGroups))
	for id, endpoints := range m.metaServiceGroups {
		groups[id] = endpoints
	}
	return groups
}

// UpdateGroupsSafely persists and applies meta-service group changes while
// blocking concurrent keyspace assignments.
func (m *MetaServiceGroupManager) UpdateGroupsSafely(
	ctx context.Context,
	metaServiceGroups map[string]string,
	deletedGroups []string,
	persist func() error,
	afterPersist func(),
) error {
	if err := config.AdjustMetaServiceGroups(metaServiceGroups); err != nil {
		return err
	}
	m.Lock()

	var assignmentCounts map[string]int
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		assignmentCounts, err = m.store.GetAssignmentCount(txn, m.metaServiceGroups)
		return err
	}); err != nil {
		m.Unlock()
		return err
	}
	for _, id := range deletedGroups {
		if assignmentCounts[id] > 0 {
			m.Unlock()
			return fmt.Errorf("cannot delete meta-service group with assigned keyspaces: %s", id)
		}
	}
	if err := persist(); err != nil {
		m.Unlock()
		return err
	}
	m.metaServiceGroups = metaServiceGroups
	m.Unlock()

	if afterPersist != nil {
		afterPersist()
	}
	return nil
}

// updateGroups updates currently available meta-service groups.
func (m *MetaServiceGroupManager) updateGroups(metaServiceGroups map[string]string) {
	m.Lock()
	defer m.Unlock()
	m.metaServiceGroups = metaServiceGroups
}
