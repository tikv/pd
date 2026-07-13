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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
)

const (
	flushInterval  = 5 * time.Minute
	flushThreshold = 1000
)

// MetaServiceGroupManager manages external meta-service groups.
//
// Locking: the embedded RWMutex guards metaServiceGroups and isLeader, while
// statusMu is a dedicated leaf lock guarding cachedStatus and dirtyCount only.
// statusMu must always be the innermost lock: never acquire the RWMutex or the
// keyspace metaLock, and never perform storage I/O, while holding it. This lets
// the assignment-count update paths that already hold the keyspace metaLock
// (RemoveKeyspace, tombstone unassignment) mutate the cache without taking the
// RWMutex, which would otherwise invert the RWMutex->metaLock order used by the
// create/config paths and deadlock. Allowed orders are RWMutex->statusMu and
// metaLock->statusMu; both are safe because statusMu wraps nothing.
type MetaServiceGroupManager struct {
	ctx   context.Context
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	// metaServiceGroups is the available external meta-service groups.
	// The key is the meta-service group name, and the value is the corresponding endpoint.
	metaServiceGroups map[string]string
	// keyspaceAssignmentCounter, when set, returns the actual number of keyspaces
	// assigned to each of the given groups by scanning keyspace metadata. It is
	// the authoritative source for the delete guard so a stale persisted counter
	// cannot permanently block removing an actually-empty group.
	keyspaceAssignmentCounter func(groupIDs map[string]struct{}) (map[string]int, error)
	isLeader                  func() bool
	// statusMu guards cachedStatus and dirtyCount. See the type comment for the
	// leaf-lock discipline it must follow.
	statusMu     syncutil.Mutex
	cachedStatus map[string]*endpoint.MetaServiceGroupStatus
	dirtyCount   int
	flushCh      chan struct{}
}

// SetKeyspaceAssignmentCounter sets the authoritative keyspace assignment
// counter used by the delete guard. It must be called during initialization,
// before any concurrent group update.
func (m *MetaServiceGroupManager) SetKeyspaceAssignmentCounter(counter func(groupIDs map[string]struct{}) (map[string]int, error)) {
	m.keyspaceAssignmentCounter = counter
}

// SetLeaderChecker sets a function to determine leader ownership.
func (m *MetaServiceGroupManager) SetLeaderChecker(isLeader func() bool) {
	m.Lock()
	defer m.Unlock()
	m.isLeader = isLeader
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
		flushCh:           make(chan struct{}, 1),
	}
	if err := m.RefreshCache(); err != nil {
		return nil, err
	}
	go m.flushLoop()
	return m, nil
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
		return err
	}); err != nil {
		log.Error("[keyspace] failed to load meta-service group status from storage", zap.Error(err))
		return err
	}
	m.statusMu.Lock()
	m.cachedStatus = statusMap
	m.dirtyCount = 0
	m.statusMu.Unlock()
	log.Info("[keyspace] meta-service group status loaded from storage", zap.Any("meta-service-group-status", statusMap))
	return nil
}

func (m *MetaServiceGroupManager) flushLoop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if err := m.flushToStorage(); err != nil {
				log.Error("[keyspace] failed to flush meta-service group status to storage", zap.Error(err))
			}
		case <-m.flushCh:
			if err := m.flushToStorage(); err != nil {
				log.Error("[keyspace] failed to flush meta-service group status to storage", zap.Error(err))
			}
		}
	}
}

func (m *MetaServiceGroupManager) flushToStorage() error {
	// Hold the mgm write lock for the whole flush, including the storage write, so
	// it serializes with PatchStatus and persistGroupsLocked (both take the mgm
	// lock). Without that, the snapshot below could overwrite a status those paths
	// persisted synchronously in the meantime, or recreate a status key that
	// persistGroupsLocked just deleted. This cannot deadlock: the metaLock-holding
	// paths (RemoveKeyspace, tombstone unassignment) only take statusMu, never the
	// mgm lock, so nothing waits on the mgm lock while holding metaLock, and flush
	// never takes metaLock.
	m.Lock()
	defer m.Unlock()
	// Only the serving leader persists; a follower's next leader term reloads the
	// authoritative status via RefreshCache.
	if m.isLeader != nil && !m.isLeader() {
		return nil
	}
	// Snapshot the dirty state under statusMu (a deep copy) and reset the dirty
	// counter, without holding statusMu across the storage write. The snapshot
	// keeps the write from reading cachedStatus while a concurrent statusMu holder
	// (an assignment-count update under metaLock) mutates it; such updates re-mark
	// the counter dirty and are flushed on the next tick. On failure the dirty
	// count is restored. Only flushLoop calls this, so no concurrent flush races
	// the reset/restore.
	m.statusMu.Lock()
	if m.dirtyCount == 0 {
		m.statusMu.Unlock()
		return nil
	}
	snapshot := copyStatusMap(m.cachedStatus)
	flushed := m.dirtyCount
	m.dirtyCount = 0
	m.statusMu.Unlock()

	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for id, status := range snapshot {
			if err := m.store.SaveMetaServiceGroupStatus(txn, id, status); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		m.statusMu.Lock()
		m.dirtyCount += flushed
		m.statusMu.Unlock()
		return err
	}
	return nil
}

// GetStatus returns the status of each meta-service group.
func (m *MetaServiceGroupManager) GetStatus(_ context.Context) (map[string]*endpoint.MetaServiceGroupStatus, error) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	return copyStatusMap(m.cachedStatus), nil
}

// GetAssignmentCounts returns the count of each meta-service group.
// todo: optimize by caching the counts and watching the changes of meta-service groups.
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

// PatchStatus applies a patch to the status of a meta-service group.
func (m *MetaServiceGroupManager) PatchStatus(ctx context.Context, groupID string, patch *MetaServiceGroupStatusPatch) error {
	if patch.AssignmentCount != nil && *patch.AssignmentCount < 0 {
		return ErrInvalidAssignmentCount
	}
	m.Lock()
	defer m.Unlock()
	if _, ok := m.metaServiceGroups[groupID]; !ok {
		return ErrUnknownMetaServiceGroup
	}
	m.statusMu.Lock()
	newStatus := endpoint.MetaServiceGroupStatus{}
	if currentStatus := m.cachedStatus[groupID]; currentStatus != nil {
		newStatus = *currentStatus
	}
	m.statusMu.Unlock()
	if patch.AssignmentCount != nil {
		newStatus.AssignmentCount = *patch.AssignmentCount
	}
	if patch.Enabled != nil {
		newStatus.Enabled = *patch.Enabled
	}
	// Persist synchronously so API updates are immediately durable, then reflect
	// them in the cache. The store I/O runs without statusMu (it is never held
	// across I/O); the enclosing mgm write lock serializes concurrent patches.
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		return m.store.SaveMetaServiceGroupStatus(txn, groupID, &newStatus)
	}); err != nil {
		return err
	}
	m.statusMu.Lock()
	m.cachedStatus[groupID] = &newStatus
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

// PickGroup returns the meta-service group with the least assigned keyspaces
// and updates the cached assignment count.
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
// be atomic with respect to group deletion (which takes the write lock) must
// hold the mgm lock across both, e.g. via Manager.assignGroupAndSaveKeyspace. The
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
	m.markDirtyStatusLocked(count)
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

// updateAssignmentTxn applies an assignment count delta to the cache. It takes
// only statusMu (the leaf lock), deliberately NOT the mgm lock: callers such as
// RemoveKeyspace and the tombstone unassignment already hold the keyspace
// metaLock, and the create/config paths take the mgm lock before metaLock, so
// acquiring the mgm lock here would invert that order and deadlock. The txn
// parameter is intentionally unused: unlike the pre-cache implementation the
// count is no longer written inside the caller's storage transaction, so callers
// must not assume the count update is atomic with their txn. Counts are
// best-effort load-balancing hints; the delete guard uses the authoritative
// keyspace scan, and RefreshCache reconciles any drift on the next leader term.
func (m *MetaServiceGroupManager) updateAssignmentTxn(_ kv.Txn, oldGroupID, newGroupID string) error {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	return m.applyAssignmentDeltaStatusLocked(oldGroupID, newGroupID)
}

// applyAssignmentDeltaStatusLocked moves one assignment from oldGroupID to
// newGroupID in the cached status and marks it dirty for the flush loop to
// persist later. The caller must hold statusMu.
func (m *MetaServiceGroupManager) applyAssignmentDeltaStatusLocked(oldGroupID, newGroupID string) error {
	dirtyCount := 0
	if oldGroupID != "" {
		if status := m.cachedStatus[oldGroupID]; status != nil && status.AssignmentCount > 0 {
			status.AssignmentCount--
			dirtyCount++
		}
	}
	if newGroupID != "" {
		status := m.cachedStatus[newGroupID]
		if status == nil {
			return ErrUnknownMetaServiceGroup
		}
		status.AssignmentCount++
		dirtyCount++
	}
	if dirtyCount > 0 {
		m.markDirtyStatusLocked(dirtyCount)
	}
	return nil
}

// markDirtyStatusLocked accumulates dirty count and signals the flush loop once
// the threshold is reached. The caller must hold statusMu.
func (m *MetaServiceGroupManager) markDirtyStatusLocked(count int) {
	m.dirtyCount += count
	if m.dirtyCount < flushThreshold {
		return
	}
	select {
	case m.flushCh <- struct{}{}:
	default:
	}
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
		counts, err := m.assignedKeyspaceCounts(ctx, deletedGroups)
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
	// Clear the persisted status for deleted groups before touching memory, so all
	// storage writes complete before the in-memory view changes. This keeps
	// re-adding a group with the same ID from inheriting a stale assignment count
	// or enabled state, which would skew list output and PickGroup balancing.
	// Best-effort: the config deletion is already persisted and the delete guard
	// relies on actual keyspace scans, not this counter.
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
	// Apply the change to the in-memory view only after the storage writes above.
	m.metaServiceGroups = metaServiceGroups
	m.statusMu.Lock()
	for groupID := range metaServiceGroups {
		if m.cachedStatus[groupID] == nil {
			m.cachedStatus[groupID] = &endpoint.MetaServiceGroupStatus{}
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
func (m *MetaServiceGroupManager) assignedKeyspaceCounts(_ context.Context, groupIDs []string) (map[string]int, error) {
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
