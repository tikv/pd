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
	"crypto/tls"
	"fmt"
	"maps"
	"math"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
)

// MetaServiceGroupManager manages external meta-service groups.
type MetaServiceGroupManager struct {
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	updateMu        sync.Mutex
	tlsConfigLoader func() (*tls.Config, error)
	// metaServiceGroups is the available external meta-service groups.
	// The key is the meta-service group name, and the value is the corresponding endpoint.
	metaServiceGroups map[string]string
	etcdClients       map[string]*clientv3.Client
	closed            bool
	// Initial file config is usable only before persisted config has been observed.
	initialMetaServiceGroups map[string]string
	hasPersistedConfig       bool
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
	tlsConfigLoader func() (*tls.Config, error),
) *MetaServiceGroupManager {
	return &MetaServiceGroupManager{
		store:                    store,
		tlsConfigLoader:          tlsConfigLoader,
		metaServiceGroups:        cloneMetaServiceGroups(metaServiceGroups),
		initialMetaServiceGroups: cloneMetaServiceGroups(metaServiceGroups),
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
		return errs.ErrInvalidAssignmentCount
	}
	m.RLock()
	defer m.RUnlock()
	// Validate existence against the in-memory group set under the lock, then
	// touch only the target group's status in the txn. Loading every group would
	// widen the etcd compare set so an unrelated concurrent assignment could make
	// this patch fail with a spurious txn conflict.
	if _, ok := m.metaServiceGroups[groupID]; !ok {
		return errs.ErrUnknownMetaServiceGroup
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
			return errs.ErrUnknownMetaServiceGroup
		}
		// Disabled groups are skipped by automatic assignment, so reject moving a
		// keyspace into one to keep manual reassignment consistent with it.
		statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, map[string]string{newGroupID: ""})
		if err != nil {
			return err
		}
		if status := statusMap[newGroupID]; status == nil || !status.Enabled {
			return errs.ErrMetaServiceGroupDisabled
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
		if err := m.decrementAssignmentTxn(txn, oldGroupID, 1); err != nil {
			return err
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

// decrementAssignmentTxn decreases the persisted assignment count of one
// meta-service group. Callers that remove multiple keyspaces should aggregate
// their counts per group and invoke this method once per group. Besides using
// fewer etcd transaction operations, this avoids scheduling duplicate writes
// to the same key in one transaction.
func (m *MetaServiceGroupManager) decrementAssignmentTxn(txn kv.Txn, groupID string, count int) error {
	if groupID == "" || count <= 0 {
		return nil
	}
	status, err := m.loadGroupStatus(txn, groupID)
	if err != nil {
		return err
	}
	// Only persist a decrement when the group still has assignments. A deleted
	// group's status key is already removed, so skipping avoids recreating a
	// stale zero-value status. Clamp the result at zero to guard against a stale
	// or manually reset assignment count.
	if status.AssignmentCount == 0 {
		return nil
	}
	status.AssignmentCount = max(0, status.AssignmentCount-count)
	return m.store.SaveMetaServiceGroupStatus(txn, groupID, status)
}

// loadGroupStatus loads the persisted status of a single meta-service group
// within txn, without touching the shared m.metaServiceGroups map.
func (m *MetaServiceGroupManager) loadGroupStatus(txn kv.Txn, groupID string) (*endpoint.MetaServiceGroupStatus, error) {
	statusMap, err := m.store.LoadMetaServiceGroupStatus(txn, map[string]string{groupID: ""})
	if err != nil {
		return nil, err
	}
	status := statusMap[groupID]
	if status == nil {
		return &endpoint.MetaServiceGroupStatus{}, nil
	}
	return status, nil
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
	maps.Copy(groups, m.metaServiceGroups)
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
	m.updateMu.Lock()
	defer m.updateMu.Unlock()

	if err := config.AdjustMetaServiceGroups(metaServiceGroups); err != nil {
		return err
	}
	err := m.checkNewGroupsHealth(ctx, metaServiceGroups)
	if err != nil {
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

// checkNewGroupsHealth verifies every configured endpoint before a group is
// added or its endpoint is changed.
func (m *MetaServiceGroupManager) checkNewGroupsHealth(ctx context.Context, metaServiceGroups map[string]string) error {
	groups := make(map[string]string)
	m.RLock()
	for groupID, addresses := range metaServiceGroups {
		if currentAddresses, exists := m.metaServiceGroups[groupID]; !exists || currentAddresses != addresses {
			groups[groupID] = addresses
		}
	}
	m.RUnlock()
	for groupID, addresses := range groups {
		endpoints, err := parseMetaServiceGroupEndpoints(addresses)
		if err != nil {
			return fmt.Errorf("%w: group %s: %v", errs.ErrMetaServiceGroupUnhealthy, groupID, err)
		}
		for _, endpoint := range endpoints {
			client, err := m.newEtcdClient([]string{endpoint})
			if err != nil {
				return fmt.Errorf("%w: group %s endpoint %s: %v", errs.ErrMetaServiceGroupUnhealthy, groupID, endpoint, err)
			}
			healthy := etcdutil.IsHealthy(ctx, client)
			_ = client.Close()
			if !healthy {
				return fmt.Errorf("%w: group %s endpoint %s: etcd health check failed",
					errs.ErrMetaServiceGroupUnhealthy, groupID, endpoint)
			}
		}
	}
	return nil
}

func splitMetaServiceGroupEndpoints(addresses string) []string {
	rawEndpoints := strings.Split(addresses, ",")
	endpoints := make([]string, 0, len(rawEndpoints))
	for _, endpoint := range rawEndpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
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
				return fmt.Errorf("%w: %s", errs.ErrGroupHasAssignedKeyspaces, id)
			}
		}
	}
	if err := persist(); err != nil {
		return err
	}
	m.reconcileEtcdClientsLocked(metaServiceGroups)
	m.metaServiceGroups = cloneMetaServiceGroups(metaServiceGroups)
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
	m.reconcileEtcdClientsLocked(metaServiceGroups)
	m.metaServiceGroups = cloneMetaServiceGroups(metaServiceGroups)
}

func cloneMetaServiceGroups(metaServiceGroups map[string]string) map[string]string {
	if metaServiceGroups == nil {
		return nil
	}
	cloned := make(map[string]string, len(metaServiceGroups))
	maps.Copy(cloned, metaServiceGroups)
	return cloned
}
