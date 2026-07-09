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

	"go.uber.org/zap"

	"github.com/pingcap/log"

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
	// keyspaceAssignmentCounter, when set, returns the actual number of keyspaces
	// assigned to each of the given groups by scanning keyspace metadata. It is
	// the authoritative source for the delete guard so a stale persisted counter
	// cannot permanently block removing an actually-empty group.
	keyspaceAssignmentCounter func(groupIDs []string) (map[string]int, error)
}

// SetKeyspaceAssignmentCounter sets the authoritative keyspace assignment
// counter used by the delete guard. It must be called during initialization,
// before any concurrent group update.
func (m *MetaServiceGroupManager) SetKeyspaceAssignmentCounter(counter func(groupIDs []string) (map[string]int, error)) {
	m.keyspaceAssignmentCounter = counter
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

// GetStatus returns the status of each meta-service group.
func (m *MetaServiceGroupManager) GetStatus(ctx context.Context) (map[string]*endpoint.MetaServiceGroupStatus, error) {
	m.RLock()
	defer m.RUnlock()
	var (
		err       error
		statusMap map[string]*endpoint.MetaServiceGroupStatus
	)
	err = m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		statusMap, err = m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		return err
	})
	return statusMap, err
}

// MetaServiceGroupStatusPatch represents a patch operation for a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatusPatch struct {
	AssignmentCount *int  `json:"assignment_count,omitempty"` // nil means no change, 0 means reset to 0
	Enabled         *bool `json:"enabled,omitempty"`          // nil means no change, true means enable, false means disable
}

// PatchStatus applies a patch to the status of a meta-service group.
func (m *MetaServiceGroupManager) PatchStatus(ctx context.Context, groupID string, patch *MetaServiceGroupStatusPatch) error {
	if patch.AssignmentCount != nil && *patch.AssignmentCount < 0 {
		return ErrInvalidAssignmentCount
	}
	m.RLock()
	defer m.RUnlock()
	// Validate existence against the in-memory group set under the lock, then
	// touch only the target group's status in the txn. Loading every group would
	// widen the etcd compare set so an unrelated concurrent assignment could make
	// this patch fail with a spurious txn conflict.
	if _, ok := m.metaServiceGroups[groupID]; !ok {
		return ErrUnknownMetaServiceGroup
	}
	return m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		status, err := m.loadGroupStatus(txn, groupID)
		if err != nil {
			return err
		}
		if patch.AssignmentCount != nil {
			status.AssignmentCount = *patch.AssignmentCount
		}
		if patch.Enabled != nil {
			status.Enabled = *patch.Enabled
		}
		return m.store.SaveMetaServiceGroupStatus(txn, groupID, status)
	})
}

func (m *MetaServiceGroupManager) findMinMetaGroup(txn kv.Txn) (string, error) {
	statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
	if err != nil {
		return "", err
	}
	minCount := math.MaxInt
	var assignedGroup string
	for currentGroup, status := range statusMap {
		if status.Enabled && status.AssignmentCount < minCount {
			minCount = status.AssignmentCount
			assignedGroup = currentGroup
		}
	}
	if assignedGroup == "" {
		return "", errNoAvailableMetaServiceGroups
	}
	return assignedGroup, nil
}

// hasGroupsLocked reports whether any meta-service group is currently available.
// The caller must hold the read lock.
func (m *MetaServiceGroupManager) hasGroupsLocked() bool {
	if m == nil {
		return false
	}
	return len(m.metaServiceGroups) > 0
}

func (m *MetaServiceGroupManager) hasGroups() bool {
	if m == nil {
		return false
	}
	m.RLock()
	defer m.RUnlock()
	return m.hasGroupsLocked()
}

// reAssignKeyspaceLocked validates that newGroupID (if any) still exists and
// moves a single keyspace assignment from oldGroupID to newGroupID within txn.
// The caller must hold the read lock for the whole enclosing transaction so a
// concurrent UpdateGroupsSafely cannot delete a group between this validation
// and the persisted assignment count update.
func (m *MetaServiceGroupManager) reAssignKeyspaceLocked(txn kv.Txn, oldGroupID, newGroupID string) error {
	if newGroupID != "" {
		if _, ok := m.metaServiceGroups[newGroupID]; !ok {
			return ErrUnknownMetaServiceGroup
		}
		// Disabled groups are skipped by automatic assignment, so reject moving a
		// keyspace into one to keep manual reassignment consistent with it.
		statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, map[string]string{newGroupID: ""})
		if err != nil {
			return err
		}
		if status := statusMap[newGroupID]; status == nil || !status.Enabled {
			return ErrMetaServiceGroupDisabled
		}
	}
	return m.updateAssignmentTxn(txn, oldGroupID, newGroupID)
}

func (m *MetaServiceGroupManager) updateAssignmentTxn(txn kv.Txn, oldGroupID, newGroupID string) error {
	// Load only the affected groups instead of the whole m.metaServiceGroups map:
	// some callers (e.g. RemoveKeyspace) reach this without holding the
	// meta-service group lock, so reading the shared map here would race with
	// UpdateGroupsSafely.
	if oldGroupID != "" {
		status, err := m.loadGroupStatus(txn, oldGroupID)
		if err != nil {
			return err
		}
		// Only persist a decrement when the group still has assignments. A deleted
		// group's status key is already removed, so skipping avoids recreating a
		// stale zero-value status; a count of 0 needs no change anyway. This also
		// guards against underflow after a manual reset via PatchStatus, which
		// would otherwise make findMinMetaGroup prefer the group.
		if status.AssignmentCount > 0 {
			status.AssignmentCount--
			if err := m.store.SaveMetaServiceGroupStatus(txn, oldGroupID, status); err != nil {
				return err
			}
		}
	}
	if newGroupID != "" {
		status, err := m.loadGroupStatus(txn, newGroupID)
		if err != nil {
			return err
		}
		status.AssignmentCount++
		if err := m.store.SaveMetaServiceGroupStatus(txn, newGroupID, status); err != nil {
			return err
		}
	}
	return nil
}

// loadGroupStatus loads the persisted status of a single meta-service group
// within txn, without touching the shared m.metaServiceGroups map.
func (m *MetaServiceGroupManager) loadGroupStatus(txn kv.Txn, groupID string) (*endpoint.MetaServiceGroupStatus, error) {
	statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, map[string]string{groupID: ""})
	if err != nil {
		return nil, err
	}
	return statusMap[groupID], nil
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
	if err := m.persistGroupsLocked(ctx, metaServiceGroups, deletedGroups, persist); err != nil {
		return err
	}
	if afterPersist != nil {
		afterPersist()
	}
	return nil
}

// persistGroupsLocked performs the delete-guard check and persists the new
// groups while holding the write lock, which blocks concurrent keyspace
// assignment (create/reassign all take the read lock).
func (m *MetaServiceGroupManager) persistGroupsLocked(
	ctx context.Context,
	metaServiceGroups map[string]string,
	deletedGroups []string,
	persist func() error,
) error {
	m.Lock()
	defer m.Unlock()
	if len(deletedGroups) > 0 {
		counts, err := m.countKeyspacesForGroupDeleteGuard(ctx, deletedGroups)
		if err != nil {
			return err
		}
		for _, id := range deletedGroups {
			if counts[id] > 0 {
				return fmt.Errorf("%w: %s", ErrGroupHasAssignedKeyspaces, id)
			}
		}
	}
	if err := persist(); err != nil {
		return err
	}
	m.metaServiceGroups = metaServiceGroups
	// Clear the persisted status for deleted groups so re-adding a group with
	// the same ID does not inherit a stale assignment count or enabled state,
	// which would skew list output and assignment balancing. Best-effort: the
	// config deletion is already persisted and the delete guard relies on
	// actual keyspace scans, not this counter.
	if len(deletedGroups) > 0 {
		if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
			for _, id := range deletedGroups {
				if err := m.store.RemoveMetaServiceGroupStatus(txn, id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Warn("[keyspace] failed to clear status for deleted meta-service groups",
				zap.Strings("deleted-groups", deletedGroups), zap.Error(err))
		}
	}
	return nil
}

// countKeyspacesForGroupDeleteGuard returns assignment counts for groups being
// deleted. It is intentionally scoped to the delete guard because the
// authoritative path may scan all keyspace metadata from etcd. It prefers the
// authoritative keyspace scan (immune to counter drift) and falls back to the
// persisted counter when no scanner is configured, e.g. in unit tests without a
// keyspace manager.
func (m *MetaServiceGroupManager) countKeyspacesForGroupDeleteGuard(ctx context.Context, groupIDs []string) (map[string]int, error) {
	if m.keyspaceAssignmentCounter != nil {
		return m.keyspaceAssignmentCounter(groupIDs)
	}
	// Fallback path: derive counts from the persisted status. The caller holds
	// the write lock, so m.metaServiceGroups is accessed without an extra read
	// lock (which would deadlock against the held write lock).
	var counts map[string]int
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		if err != nil {
			return err
		}
		counts = make(map[string]int, len(statusMap))
		for id, status := range statusMap {
			counts[id] = status.AssignmentCount
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return counts, nil
}

// updateGroups updates currently available meta-service groups.
func (m *MetaServiceGroupManager) updateGroups(metaServiceGroups map[string]string) {
	m.Lock()
	defer m.Unlock()
	m.metaServiceGroups = metaServiceGroups
}
