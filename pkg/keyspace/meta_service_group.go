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
	cachedStatus              map[string]*endpoint.MetaServiceGroupStatus
	dirtyCount                int
	flushCh                   chan struct{}
	isLeader                  func() bool
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
	m.cachedStatus = statusMap
	m.dirtyCount = 0
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
	m.Lock()
	defer m.Unlock()
	if m.isLeader != nil && !m.isLeader() {
		return nil
	}
	if m.dirtyCount == 0 {
		return nil
	}
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for id, status := range m.cachedStatus {
			if err := m.store.SaveMetaServiceGroupStatus(txn, id, status); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	m.dirtyCount = 0
	return nil
}

// GetStatus returns the status of each meta-service group.
func (m *MetaServiceGroupManager) GetStatus(ctx context.Context) (map[string]*endpoint.MetaServiceGroupStatus, error) {
	_ = ctx
	m.RLock()
	defer m.RUnlock()
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
	currentStatus := m.cachedStatus[groupID]
	if currentStatus == nil {
		currentStatus = &endpoint.MetaServiceGroupStatus{}
	}
	newStatus := *currentStatus
	if patch.AssignmentCount != nil {
		newStatus.AssignmentCount = *patch.AssignmentCount
	}
	if patch.Enabled != nil {
		newStatus.Enabled = *patch.Enabled
	}
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		return m.store.SaveMetaServiceGroupStatus(txn, groupID, &newStatus)
	}); err != nil {
		return err
	}
	m.cachedStatus[groupID] = &newStatus
	return nil
}

func (m *MetaServiceGroupManager) findMinMetaGroupLocked() (string, error) {
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
func (m *MetaServiceGroupManager) PickGroup(ctx context.Context) (string, error) {
	_ = ctx
	m.Lock()
	defer m.Unlock()
	return m.pickGroupLocked(ctx)
}

// hasGroupsLocked reports whether any meta-service group is currently available.
// The caller must hold the lock.
func (m *MetaServiceGroupManager) hasGroupsLocked() bool {
	return len(m.metaServiceGroups) > 0
}

// pickGroupLocked is PickGroup with the lock already held by the caller.
// Callers that need group selection and the subsequent keyspace metadata save to
// be atomic with respect to group deletion (which takes the write lock) must
// hold the lock across both, e.g. via Manager.assignGroupAndSaveKeyspace.
func (m *MetaServiceGroupManager) pickGroupLocked(ctx context.Context) (string, error) {
	_ = ctx
	assignedGroup, err := m.findMinMetaGroupLocked()
	if err != nil {
		return "", err
	}
	if err := m.updateAssignmentLockedTxn(nil, "", assignedGroup); err != nil {
		return "", err
	}
	return assignedGroup, nil
}

// AssignToGroup increments count of the meta-service group with least assigned keyspaces.
// It returns the assigned meta-service group and an error if any.
// only used for testing now, as it doesn't guarantee the atomicity of select and update. UpdateAssignment should be used in production code instead.
func (m *MetaServiceGroupManager) AssignToGroup(ctx context.Context, count int) (string, error) {
	_ = ctx
	if count < 0 {
		return "", ErrInvalidAssignmentCount
	}
	m.Lock()
	defer m.Unlock()
	assignedGroup, err := m.findMinMetaGroupLocked()
	if err != nil {
		return "", err
	}
	m.cachedStatus[assignedGroup].AssignmentCount += count
	m.markDirtyLocked(count)
	return assignedGroup, nil
}

// reassignKeyspaceLocked validates that newGroupID (if any) still exists and
// moves a single keyspace assignment from oldGroupID to newGroupID within txn.
// The caller must hold the lock for the whole enclosing transaction so a
// concurrent UpdateGroupsSafely cannot delete a group between this validation
// and the cached assignment count update.
func (m *MetaServiceGroupManager) reassignKeyspaceLocked(txn kv.Txn, oldGroupID, newGroupID string) error {
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
	return m.updateAssignmentLockedTxn(txn, oldGroupID, newGroupID)
}

func (m *MetaServiceGroupManager) updateAssignmentTxn(txn kv.Txn, oldGroupID, newGroupID string) error {
	m.Lock()
	defer m.Unlock()
	return m.updateAssignmentLockedTxn(txn, oldGroupID, newGroupID)
}

func (m *MetaServiceGroupManager) updateAssignmentLockedTxn(txn kv.Txn, oldGroupID, newGroupID string) error {
	_ = txn
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
		m.markDirtyLocked(dirtyCount)
	}
	return nil
}

func (m *MetaServiceGroupManager) markDirtyLocked(count int) {
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
	m.metaServiceGroups = metaServiceGroups
	for groupID := range metaServiceGroups {
		if m.cachedStatus[groupID] == nil {
			m.cachedStatus[groupID] = &endpoint.MetaServiceGroupStatus{}
		}
	}
	// Clear the persisted status for deleted groups so re-adding a group with
	// the same ID does not inherit a stale assignment count or enabled state,
	// which would skew list output and PickGroup balancing. Best-effort: the
	// config deletion is already persisted and the delete guard relies on
	// actual keyspace scans, not this counter.
	if len(deletedGroups) > 0 {
		for _, id := range deletedGroups {
			delete(m.cachedStatus, id)
		}
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
	// Fallback path: derive counts from the cached status. The caller holds
	// the write lock, so m.metaServiceGroups is accessed without an extra read
	// lock (which would deadlock against the held write lock).
	counts := make(map[string]int, len(groupIDs))
	for _, id := range groupIDs {
		if status := m.cachedStatus[id]; status != nil {
			counts[id] = status.AssignmentCount
		}
	}
	return counts, nil
}

// updateGroups updates currently available meta-service groups.
func (m *MetaServiceGroupManager) updateGroups(metaServiceGroups map[string]string) {
	m.Lock()
	defer m.Unlock()
	m.metaServiceGroups = metaServiceGroups
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
