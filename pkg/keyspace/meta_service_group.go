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
//
// Persistence model: only the administrative Enabled flag of each group is
// persisted. AssignmentCount is a derived, best-effort load-balancing hint, not a
// source of truth: it is rebuilt from the authoritative keyspace metadata
// (keyspaceAssignmentCounter) on every leader term via RefreshCache, and
// maintained purely in memory during the term as keyspaces are created, removed
// or reassigned. Nothing writes the count to storage, so there is no async flush
// to fence against leadership changes and no stale count to reconcile; a lost
// in-memory delta self-heals on the next term's rebuild.
//
// Locking: the embedded RWMutex guards metaServiceGroups; statusMu is a leaf lock
// guarding cachedStatus only. statusMu must be the innermost lock: never acquire
// the RWMutex or the keyspace metaLock, and never perform storage I/O, while
// holding it. This lets the count-update paths that already hold the keyspace
// metaLock (RemoveKeyspace, tombstone unassignment) mutate the cache without
// taking the RWMutex, which would otherwise invert the RWMutex->metaLock order
// used by the create/config paths and deadlock. Allowed orders are
// RWMutex->statusMu and metaLock->statusMu; both are safe because statusMu wraps
// nothing.
type MetaServiceGroupManager struct {
	ctx   context.Context
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	// metaServiceGroups is the available external meta-service groups.
	// The key is the meta-service group name, and the value is the corresponding endpoint.
	metaServiceGroups map[string]string
	// keyspaceAssignmentCounter returns the actual number of keyspaces assigned to
	// each of the given groups by scanning keyspace metadata. It is the
	// authoritative source for both the delete guard and the derived assignment
	// counts rebuilt by RefreshCache.
	keyspaceAssignmentCounter func(groupIDs map[string]struct{}) (map[string]int, error)
	// statusMu guards cachedStatus. See the type comment for the leaf-lock
	// discipline it must follow.
	statusMu     syncutil.Mutex
	cachedStatus map[string]*endpoint.MetaServiceGroupStatus
}

// SetKeyspaceAssignmentCounter sets the authoritative keyspace assignment
// counter. It must be called during initialization, before any concurrent group
// update.
func (m *MetaServiceGroupManager) SetKeyspaceAssignmentCounter(counter func(groupIDs map[string]struct{}) (map[string]int, error)) {
	m.keyspaceAssignmentCounter = counter
}

// NewMetaServiceGroupManager creates a new MetaServiceGroupManager.
func NewMetaServiceGroupManager(
	ctx context.Context,
	store endpoint.MetaServiceGroupStorage,
	metaServiceGroups map[string]string,
) (*MetaServiceGroupManager, error) {
	m := &MetaServiceGroupManager{
		ctx:               ctx,
		store:             store,
		metaServiceGroups: metaServiceGroups,
	}
	if err := m.RefreshCache(); err != nil {
		return nil, err
	}
	return m, nil
}

// RefreshCache rebuilds the in-memory status of every current group: the Enabled
// flag from storage and the AssignmentCount from the authoritative keyspace scan.
// It is called at construction and on each leadership acquisition, so every leader
// term starts from counts derived from actual keyspace metadata rather than a
// persisted value that could have drifted or been lost.
func (m *MetaServiceGroupManager) RefreshCache() error {
	m.Lock()
	defer m.Unlock()
	var stored map[string]*endpoint.MetaServiceGroupStatus
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		stored, err = m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		return err
	}); err != nil {
		log.Error("[keyspace] failed to load meta-service group status from storage", zap.Error(err))
		return err
	}
	// Rebuild counts from keyspace metadata. The counter is nil before the keyspace
	// manager wires it (e.g. at construction) and in unit tests without one; counts
	// then start at zero and are corrected on the next RefreshCache.
	counts := map[string]int{}
	if m.keyspaceAssignmentCounter != nil {
		set := make(map[string]struct{}, len(m.metaServiceGroups))
		for id := range m.metaServiceGroups {
			set[id] = struct{}{}
		}
		var err error
		if counts, err = m.keyspaceAssignmentCounter(set); err != nil {
			return err
		}
	}
	cache := make(map[string]*endpoint.MetaServiceGroupStatus, len(m.metaServiceGroups))
	for id := range m.metaServiceGroups {
		enabled := false
		if s := stored[id]; s != nil {
			enabled = s.Enabled
		}
		cache[id] = &endpoint.MetaServiceGroupStatus{AssignmentCount: counts[id], Enabled: enabled}
	}
	m.statusMu.Lock()
	m.cachedStatus = cache
	m.statusMu.Unlock()
	log.Info("[keyspace] meta-service group status rebuilt", zap.Any("meta-service-group-status", cache))
	return nil
}

// GetStatus returns the status of each meta-service group.
func (m *MetaServiceGroupManager) GetStatus(_ context.Context) (map[string]*endpoint.MetaServiceGroupStatus, error) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	return copyStatusMap(m.cachedStatus), nil
}

// GetAssignmentCounts returns the count of each meta-service group.
func (m *MetaServiceGroupManager) GetAssignmentCounts(ctx context.Context) (map[string]int, error) {
	statusMap, err := m.GetStatus(ctx)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int, len(statusMap))
	for id, status := range statusMap {
		counts[id] = status.AssignmentCount
	}
	return counts, nil
}

func copyStatusMap(statusMap map[string]*endpoint.MetaServiceGroupStatus) map[string]*endpoint.MetaServiceGroupStatus {
	statuses := make(map[string]*endpoint.MetaServiceGroupStatus, len(statusMap))
	for groupID, status := range statusMap {
		if status == nil {
			statuses[groupID] = &endpoint.MetaServiceGroupStatus{}
			continue
		}
		copiedStatus := *status
		statuses[groupID] = &copiedStatus
	}
	return statuses
}

// MetaServiceGroupStatusPatch represents a patch operation for a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatusPatch struct {
	AssignmentCount *int  `json:"assignment_count,omitempty"` // nil means no change, 0 means reset to 0
	Enabled         *bool `json:"enabled,omitempty"`          // nil means no change, true means enable, false means disable
}

// PatchStatus applies a patch to the status of a meta-service group. Only the
// Enabled flag is persisted; a patched AssignmentCount is applied to the derived
// in-memory hint and is superseded by the next RefreshCache, kept for API
// compatibility.
func (m *MetaServiceGroupManager) PatchStatus(ctx context.Context, groupID string, patch *MetaServiceGroupStatusPatch) error {
	if patch.AssignmentCount != nil && *patch.AssignmentCount < 0 {
		return ErrInvalidAssignmentCount
	}
	m.Lock()
	defer m.Unlock()
	if _, ok := m.metaServiceGroups[groupID]; !ok {
		return ErrUnknownMetaServiceGroup
	}
	// Persist the Enabled flag (the only persisted field) synchronously when it
	// changes. The mgm write lock serializes this with persistGroupsLocked, so the
	// group cannot be deleted between the existence check and the write.
	if patch.Enabled != nil {
		if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
			return m.store.SaveMetaServiceGroupStatus(txn, groupID, &endpoint.MetaServiceGroupStatus{Enabled: *patch.Enabled})
		}); err != nil {
			return err
		}
	}
	m.statusMu.Lock()
	status := m.cachedStatus[groupID]
	if status == nil {
		status = &endpoint.MetaServiceGroupStatus{}
		m.cachedStatus[groupID] = status
	}
	if patch.AssignmentCount != nil {
		status.AssignmentCount = *patch.AssignmentCount
	}
	if patch.Enabled != nil {
		status.Enabled = *patch.Enabled
	}
	m.statusMu.Unlock()
	return nil
}

// findMinMetaGroupStatusLocked returns the enabled group with the least assigned
// keyspaces. The caller must hold statusMu.
func (m *MetaServiceGroupManager) findMinMetaGroupStatusLocked() (string, error) {
	minCount := math.MaxInt
	var assignedGroup string
	for currentGroup, status := range m.cachedStatus {
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

// PickGroup returns the meta-service group with the least assigned keyspaces and
// increments its in-memory assignment count.
func (m *MetaServiceGroupManager) PickGroup(_ context.Context) (string, error) {
	m.Lock()
	defer m.Unlock()
	return m.pickGroupLocked()
}

// hasGroupsLocked reports whether any meta-service group is currently available.
// The caller must hold the lock.
func (m *MetaServiceGroupManager) hasGroupsLocked() bool {
	return len(m.metaServiceGroups) > 0
}

// pickGroupLocked is PickGroup with the mgm lock already held by the caller.
// Callers that need group selection and the subsequent keyspace metadata save to
// be atomic with respect to group deletion (which takes the write lock) must hold
// the mgm lock across both, e.g. via Manager.assignGroupAndSaveKeyspace. The
// selection and the reservation are done together under statusMu so a concurrent
// assignment cannot pick the same minimum twice.
func (m *MetaServiceGroupManager) pickGroupLocked() (string, error) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	assignedGroup, err := m.findMinMetaGroupStatusLocked()
	if err != nil {
		return "", err
	}
	if err := m.applyAssignmentDeltaStatusLocked("", assignedGroup); err != nil {
		return "", err
	}
	return assignedGroup, nil
}

// AssignToGroup increments count of the meta-service group with least assigned keyspaces.
// It returns the assigned meta-service group and an error if any.
// only used for testing now, as it doesn't guarantee the atomicity of select and update. UpdateAssignment should be used in production code instead.
func (m *MetaServiceGroupManager) AssignToGroup(_ context.Context, count int) (string, error) {
	if count < 0 {
		return "", ErrInvalidAssignmentCount
	}
	m.Lock()
	defer m.Unlock()
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	assignedGroup, err := m.findMinMetaGroupStatusLocked()
	if err != nil {
		return "", err
	}
	m.cachedStatus[assignedGroup].AssignmentCount += count
	return assignedGroup, nil
}

// reassignKeyspaceLocked validates that newGroupID (if any) still exists and
// moves a single keyspace assignment from oldGroupID to newGroupID. The caller
// must hold the mgm lock for the whole enclosing transaction so a concurrent
// UpdateGroupsSafely cannot delete a group between this validation and the cached
// assignment count update; metaServiceGroups is therefore read without taking
// the mgm lock again. statusMu guards the cache reads and the count update.
func (m *MetaServiceGroupManager) reassignKeyspaceLocked(_ kv.Txn, oldGroupID, newGroupID string) error {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	if newGroupID != "" {
		if _, ok := m.metaServiceGroups[newGroupID]; !ok {
			return ErrUnknownMetaServiceGroup
		}
		// Disabled groups are skipped by automatic assignment, so reject moving a
		// keyspace into one to keep manual reassignment consistent with it.
		if status := m.cachedStatus[newGroupID]; status == nil || !status.Enabled {
			return ErrMetaServiceGroupDisabled
		}
	}
	return m.applyAssignmentDeltaStatusLocked(oldGroupID, newGroupID)
}

// updateAssignmentTxn moves an assignment count delta in the in-memory cache. It
// takes only statusMu (the leaf lock), deliberately NOT the mgm lock: callers
// such as RemoveKeyspace and the tombstone unassignment already hold the keyspace
// metaLock, and the create/config paths take the mgm lock before metaLock, so
// acquiring the mgm lock here would invert that order and deadlock. The txn
// parameter is unused: the count is a derived hint kept only in memory, not
// written to storage, so it is not tied to the caller's storage transaction. Any
// drift from a caller txn that fails to commit self-heals on the next RefreshCache,
// and the delete guard relies on the authoritative keyspace scan, not this count.
func (m *MetaServiceGroupManager) updateAssignmentTxn(_ kv.Txn, oldGroupID, newGroupID string) error {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	return m.applyAssignmentDeltaStatusLocked(oldGroupID, newGroupID)
}

// applyAssignmentDeltaStatusLocked moves one assignment from oldGroupID to
// newGroupID in the cached status. The caller must hold statusMu.
func (m *MetaServiceGroupManager) applyAssignmentDeltaStatusLocked(oldGroupID, newGroupID string) error {
	if oldGroupID != "" {
		if status := m.cachedStatus[oldGroupID]; status != nil && status.AssignmentCount > 0 {
			status.AssignmentCount--
		}
	}
	if newGroupID != "" {
		status := m.cachedStatus[newGroupID]
		if status == nil {
			return ErrUnknownMetaServiceGroup
		}
		status.AssignmentCount++
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

// HasGroups reports whether any meta-service group is currently available. It
// avoids the map copy of GetGroups for callers that only need the existence
// check, e.g. the keyspace creation path.
func (m *MetaServiceGroupManager) HasGroups() bool {
	m.RLock()
	defer m.RUnlock()
	return m.hasGroupsLocked()
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
// assignment (AssignToGroup/PickGroup/reassign all take the lock).
func (m *MetaServiceGroupManager) persistGroupsLocked(
	ctx context.Context,
	metaServiceGroups map[string]string,
	deletedGroups []string,
	persist func() error,
) error {
	m.Lock()
	defer m.Unlock()
	if len(deletedGroups) > 0 {
		counts, err := m.assignedKeyspaceCounts(deletedGroups)
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
	// Clear the persisted Enabled flag for groups leaving the set and for groups
	// entering it, so a re-added group starts disabled instead of inheriting a
	// stale Enabled left in storage. This is done before mutating memory and fails
	// hard on error (the operation is idempotent on retry), so the reset invariant
	// always holds rather than being best-effort.
	m.statusMu.Lock()
	var addedGroups []string
	for id := range metaServiceGroups {
		if m.cachedStatus[id] == nil {
			addedGroups = append(addedGroups, id)
		}
	}
	m.statusMu.Unlock()
	clearGroups := make([]string, 0, len(deletedGroups)+len(addedGroups))
	clearGroups = append(clearGroups, deletedGroups...)
	clearGroups = append(clearGroups, addedGroups...)
	if len(clearGroups) > 0 {
		if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
			for _, id := range clearGroups {
				if err := m.store.RemoveMetaServiceGroupStatus(txn, id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	// Apply the change to the in-memory view only after the storage writes above.
	// Added groups start at zero: a new group has no keyspaces, and the delete guard
	// guarantees a removed (hence re-addable) group had none either.
	m.metaServiceGroups = metaServiceGroups
	m.statusMu.Lock()
	for id := range metaServiceGroups {
		if m.cachedStatus[id] == nil {
			m.cachedStatus[id] = &endpoint.MetaServiceGroupStatus{}
		}
	}
	for _, id := range deletedGroups {
		delete(m.cachedStatus, id)
	}
	m.statusMu.Unlock()
	return nil
}

// assignedKeyspaceCounts returns the number of keyspaces assigned to each of the
// given groups. It prefers the authoritative keyspace scan (immune to counter
// drift) and falls back to the cached counter when no scanner is configured,
// e.g. in unit tests without a keyspace manager.
func (m *MetaServiceGroupManager) assignedKeyspaceCounts(groupIDs []string) (map[string]int, error) {
	if m.keyspaceAssignmentCounter != nil {
		set := make(map[string]struct{}, len(groupIDs))
		for _, id := range groupIDs {
			set[id] = struct{}{}
		}
		return m.keyspaceAssignmentCounter(set)
	}
	// Fallback path: derive counts from the cached status under statusMu.
	counts := make(map[string]int, len(groupIDs))
	m.statusMu.Lock()
	for _, id := range groupIDs {
		if status := m.cachedStatus[id]; status != nil {
			counts[id] = status.AssignmentCount
		}
	}
	m.statusMu.Unlock()
	return counts, nil
}

// updateGroups updates currently available meta-service groups.
func (m *MetaServiceGroupManager) updateGroups(metaServiceGroups map[string]string) {
	m.Lock()
	defer m.Unlock()
	m.metaServiceGroups = metaServiceGroups
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	for groupID := range metaServiceGroups {
		if m.cachedStatus[groupID] == nil {
			m.cachedStatus[groupID] = &endpoint.MetaServiceGroupStatus{}
		}
	}
	for groupID := range m.cachedStatus {
		if _, ok := metaServiceGroups[groupID]; !ok {
			delete(m.cachedStatus, groupID)
		}
	}
}
