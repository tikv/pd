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
	"math"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// MetaServiceGroupManager manages external meta-service groups.
type MetaServiceGroupManager struct {
	ctx   context.Context
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	metaServiceGroups map[string]string
}

// NewMetaServiceGroupManager creates a new MetaServiceGroupManager.
func NewMetaServiceGroupManager(
	ctx context.Context,
	store endpoint.MetaServiceGroupStorage,
	metaServiceGroups map[string]string,
) *MetaServiceGroupManager {
	return &MetaServiceGroupManager{
		ctx:               ctx,
		store:             store,
		metaServiceGroups: metaServiceGroups,
	}
}

// GetAssignmentCounts returns the count of each meta-service group.
func (m *MetaServiceGroupManager) GetAssignmentCounts() (map[string]int, error) {
	m.RLock()
	defer m.RUnlock()
	var (
		err   error
		count map[string]int
	)
	err = m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		count, err = m.store.GetAssignmentCount(txn, m.metaServiceGroups)
		return err
	})
	return count, err
}

func (m *MetaServiceGroupManager) selectGroupTxn(txn kv.Txn) (string, error) {
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

// SelectGroup returns the meta-service group with the least assigned keyspaces
// without updating the persisted assignment count.
func (m *MetaServiceGroupManager) SelectGroup() (string, error) {
	m.RLock()
	defer m.RUnlock()
	var assignedGroup string
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		assignedGroup, err = m.selectGroupTxn(txn)
		return err
	}); err != nil {
		return "", err
	}
	return assignedGroup, nil
}

// AssignToGroup increments count of the meta-service group with least assigned keyspaces.
// It returns the assigned meta-service group and an error if any.
func (m *MetaServiceGroupManager) AssignToGroup(count int) (string, error) {
	m.RLock()
	defer m.RUnlock()
	var assignedGroup string
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		assignedGroup, err = m.selectGroupTxn(txn)
		if err != nil {
			return err
		}
		return m.store.IncrementAssignmentCount(txn, assignedGroup, count)
	}); err != nil {
		return "", err
	}
	return assignedGroup, nil
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
		return m.updateAssignmentTxn(txn, oldGroupID, newGroupID)
	})
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

// updateGroups updates currently available meta-service groups.
func (m *MetaServiceGroupManager) updateGroups(metaServiceGroups map[string]string) {
	m.Lock()
	defer m.Unlock()
	m.metaServiceGroups = metaServiceGroups
}
