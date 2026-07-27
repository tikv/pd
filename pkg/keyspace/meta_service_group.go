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

const assignmentCountRebuildRetryInterval = time.Second

// MetaServiceGroupManager manages external meta-service groups.
//
// Persistence model: only the administrative Enabled flag of each group is
// persisted. AssignmentCount is a derived, best-effort load-balancing hint, not a
// source of truth: it is rebuilt from the authoritative keyspace metadata
// (keyspaceAssignmentCounter) asynchronously on every leader term, and maintained
// purely in memory during the term as keyspaces are created, removed or
// reassigned. Nothing writes the count to storage, so there is no async flush to
// fence against leadership changes and no stale count to reconcile; a lost
// in-memory delta self-heals on the next successful rebuild.
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
	store endpoint.MetaServiceGroupStorage
	syncutil.RWMutex
	// metaServiceGroups is the available external meta-service groups.
	// The key is the meta-service group name, and the value is the corresponding endpoint.
	metaServiceGroups map[string]string
	// keyspaceAssignmentCounter returns the actual number of keyspaces assigned to
	// each of the given groups by scanning keyspace metadata. It is the
	// authoritative source for both the delete guard and the derived assignment
	// counts rebuilt by RefreshCache.
	keyspaceAssignmentCounter func(ctx context.Context, groupIDs map[string]struct{}) (map[string]int, error)
	// statusMu guards cachedStatus. See the type comment for the leaf-lock
	// discipline it must follow.
	statusMu     syncutil.Mutex
	cachedStatus map[string]*endpoint.MetaServiceGroupStatus
	// termGen is the leader-term generation. It is bumped only at term boundaries
	// (RefreshCache at construction, RefreshPersistedStatus on leadership change),
	// never by assignment deltas. A rebuild worker snapshots it when scheduled and,
	// on completion, only commits its scan while termGen is unchanged. A worker left
	// over from a previous term finds termGen advanced and exits without touching
	// cachedStatus, countsReady or rebuilding, which the current term's worker owns.
	termGen uint64
	// countsReady reports whether an authoritative scan has completed in the current
	// term. It is exposed via the status API (assignment_count_ready).
	countsReady bool
	// rebuilding ensures at most one rebuild scan is in flight, so serving traffic
	// cannot pile up concurrent keyspace scans against storage.
	rebuilding bool
	// rebuildDeltas records assignment deltas applied to cachedStatus while the
	// current rebuild scan is in flight. When the scan completes, the committed
	// count is scan result plus these deltas so writes during loading are not lost.
	rebuildDeltas map[string]int
}

// SetKeyspaceAssignmentCounter sets the authoritative keyspace assignment
// counter. It must be called during initialization, before any concurrent group
// update.
func (m *MetaServiceGroupManager) SetKeyspaceAssignmentCounter(counter func(ctx context.Context, groupIDs map[string]struct{}) (map[string]int, error)) {
	m.keyspaceAssignmentCounter = counter
}

// NewMetaServiceGroupManager creates a new MetaServiceGroupManager.
func NewMetaServiceGroupManager(
	ctx context.Context,
	store endpoint.MetaServiceGroupStorage,
	metaServiceGroups map[string]string,
) (*MetaServiceGroupManager, error) {
	m := &MetaServiceGroupManager{
		store:             store,
		metaServiceGroups: metaServiceGroups,
	}
	if err := m.RefreshCache(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// RefreshCache rebuilds the in-memory status of every current group: the Enabled
// flag from storage and the AssignmentCount from the authoritative keyspace scan.
// It is called at construction and on each leadership acquisition, so every leader
// term starts from counts derived from actual keyspace metadata rather than a
// persisted value that could have drifted or been lost.
func (m *MetaServiceGroupManager) RefreshCache(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()
	var stored map[string]*endpoint.MetaServiceGroupStatus
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		stored, err = m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		return err
	}); err != nil {
		log.Error("[keyspace] failed to load meta-service group status from storage", zap.Error(err))
		return err
	}
	// Rebuild counts from keyspace metadata. The counter is nil before the keyspace
	// manager wires it (e.g. at construction) and in unit tests without one; counts
	// then start at zero and countsReady stays false until the counter is wired and
	// a real rebuild (StartAssignmentCountRebuild) completes.
	counts := map[string]int{}
	scanned := false
	if m.keyspaceAssignmentCounter != nil {
		set := make(map[string]struct{}, len(m.metaServiceGroups))
		for id := range m.metaServiceGroups {
			set[id] = struct{}{}
		}
		var err error
		if counts, err = m.keyspaceAssignmentCounter(ctx, set); err != nil {
			return err
		}
		scanned = true
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
	m.termGen++
	m.countsReady = scanned
	m.rebuilding = false
	m.rebuildDeltas = nil
	m.statusMu.Unlock()
	log.Info("[keyspace] meta-service group status rebuilt", zap.Any("meta-service-group-status", cache))
	return nil
}

// RefreshPersistedStatus reloads the persisted administrative status only. It is
// intentionally lightweight for the leader-ready path; assignment counts are
// rebuilt asynchronously by StartAssignmentCountRebuild.
func (m *MetaServiceGroupManager) RefreshPersistedStatus(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()
	var stored map[string]*endpoint.MetaServiceGroupStatus
	if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		stored, err = m.store.LoadMetaServiceGroupStatus(txn, m.metaServiceGroups)
		return err
	}); err != nil {
		log.Error("[keyspace] failed to load meta-service group status from storage", zap.Error(err))
		return err
	}

	m.statusMu.Lock()
	cache := make(map[string]*endpoint.MetaServiceGroupStatus, len(m.metaServiceGroups))
	for id := range m.metaServiceGroups {
		count := 0
		if current := m.cachedStatus[id]; current != nil {
			count = current.AssignmentCount
		}
		enabled := false
		if s := stored[id]; s != nil {
			enabled = s.Enabled
		}
		cache[id] = &endpoint.MetaServiceGroupStatus{AssignmentCount: count, Enabled: enabled}
	}
	m.cachedStatus = cache
	m.termGen++
	m.countsReady = false
	m.rebuilding = false
	m.rebuildDeltas = nil
	m.statusMu.Unlock()
	log.Info("[keyspace] meta-service group persisted status loaded", zap.Any("meta-service-group-status", cache))
	return nil
}

// IsAssignmentCountReady reports whether the current cached assignment counts
// have completed an authoritative rebuild in this leader term.
func (m *MetaServiceGroupManager) IsAssignmentCountReady() bool {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	return m.countsReady
}

// StartAssignmentCountRebuild asynchronously rebuilds assignment counts from
// authoritative keyspace metadata. Assignment deltas applied during the scan
// window are recorded separately and merged with the scan result, so writes made
// while loading counts are preserved. The scan is discarded only when the leader
// term changes (which bumps termGen), so under steady traffic a rebuild converges
// in a single scan. At most one scan runs at a time.
func (m *MetaServiceGroupManager) StartAssignmentCountRebuild(ctx context.Context) {
	m.RLock()
	groupIDs := make(map[string]struct{}, len(m.metaServiceGroups))
	for id := range m.metaServiceGroups {
		groupIDs[id] = struct{}{}
	}
	m.statusMu.Lock()
	if m.rebuilding {
		m.statusMu.Unlock()
		m.RUnlock()
		return
	}
	m.rebuilding = true
	m.countsReady = false
	m.rebuildDeltas = make(map[string]int, len(groupIDs))
	startTerm := m.termGen
	m.statusMu.Unlock()
	m.RUnlock()

	go m.rebuildAssignmentCounts(ctx, groupIDs, startTerm)
}

func (m *MetaServiceGroupManager) rebuildAssignmentCounts(ctx context.Context, groupIDs map[string]struct{}, startTerm uint64) {
	// Without a counter (before the keyspace manager wires it, or in unit tests)
	// there is no authoritative source, so mark ready without overwriting counts.
	if m.keyspaceAssignmentCounter == nil {
		m.statusMu.Lock()
		if m.termGen == startTerm {
			m.countsReady = true
			m.rebuilding = false
			m.rebuildDeltas = nil
		}
		m.statusMu.Unlock()
		return
	}
	counts, err := m.keyspaceAssignmentCounter(ctx, groupIDs)
	if !m.finishRebuild(startTerm, counts, err) {
		return
	}
	// The scan failed within the current term; back off and retry so a transient
	// storage error does not leave counts underived for the rest of the term.
	select {
	case <-ctx.Done():
	case <-time.After(assignmentCountRebuildRetryInterval):
		m.StartAssignmentCountRebuild(ctx)
	}
}

// finishRebuild applies a rebuild scan result under statusMu and reports whether
// the caller should schedule a retry. A worker whose startTerm no longer matches
// termGen is a leftover from a previous leader term: the current term owns the
// rebuilding/countsReady/cachedStatus state, so the stale worker exits without
// touching any of it and without retrying.
func (m *MetaServiceGroupManager) finishRebuild(startTerm uint64, counts map[string]int, scanErr error) (retry bool) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	if m.termGen != startTerm {
		return false
	}
	if scanErr != nil {
		log.Warn("[keyspace] failed to rebuild meta-service group assignment counts", zap.Error(scanErr))
		m.rebuilding = false
		m.rebuildDeltas = nil
		return true
	}
	for id, status := range m.cachedStatus {
		status.AssignmentCount = max(0, counts[id]+m.rebuildDeltas[id])
	}
	m.countsReady = true
	m.rebuilding = false
	m.rebuildDeltas = nil
	return false
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
	AssignmentCount *int  `json:"assignment_count,omitempty"` // unsupported; assignment count is derived
	Enabled         *bool `json:"enabled,omitempty"`          // nil means no change, true means enable, false means disable
}

// PatchStatus applies a patch to the status of a meta-service group. Only the
// Enabled flag can be patched and persisted; AssignmentCount is derived from
// authoritative keyspace metadata and cannot be patched.
func (m *MetaServiceGroupManager) PatchStatus(ctx context.Context, groupID string, patch *MetaServiceGroupStatusPatch) error {
	if patch.AssignmentCount != nil {
		return ErrAssignmentCountPatchUnsupported
	}
	m.RLock()
	defer m.RUnlock()
	if _, ok := m.metaServiceGroups[groupID]; !ok {
		return ErrUnknownMetaServiceGroup
	}
	// Persist the Enabled flag (the only persisted field) synchronously when it
	// changes. persistGroupsLocked (group deletion) takes the write lock, so
	// holding the read lock here is enough to keep the group from being deleted
	// between the existence check and the write.
	if patch.Enabled != nil {
		if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
			// Load before saving so the txn carries an implicit compare on the
			// status key's current value. Without it this is a blind put: a
			// write from a leader that has since lost its term can still commit
			// after a newer leader's write, silently reverting it. The loaded
			// value itself is unused; only the CAS side effect matters here.
			if _, err := m.store.LoadMetaServiceGroupStatus(txn, map[string]string{groupID: ""}); err != nil {
				return err
			}
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
	m.recordRebuildDeltaLocked(assignedGroup, count)
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
// newGroupID in the cached status. The caller must hold statusMu. newGroupID's
// existence is checked before either side is mutated, so a missing target
// leaves the decrement side untouched instead of losing a count with no
// matching increment.
func (m *MetaServiceGroupManager) applyAssignmentDeltaStatusLocked(oldGroupID, newGroupID string) error {
	if newGroupID != "" {
		if _, ok := m.cachedStatus[newGroupID]; !ok {
			return ErrUnknownMetaServiceGroup
		}
	}
	if oldGroupID != "" {
		if status := m.cachedStatus[oldGroupID]; status != nil {
			if status.AssignmentCount > 0 {
				status.AssignmentCount--
			}
			m.recordRebuildDeltaLocked(oldGroupID, -1)
		}
	}
	if newGroupID != "" {
		m.cachedStatus[newGroupID].AssignmentCount++
		m.recordRebuildDeltaLocked(newGroupID, 1)
	}
	return nil
}

func (m *MetaServiceGroupManager) recordRebuildDeltaLocked(groupID string, delta int) {
	if !m.rebuilding || groupID == "" {
		return
	}
	if m.rebuildDeltas == nil {
		m.rebuildDeltas = make(map[string]int)
	}
	m.rebuildDeltas[groupID] += delta
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
	// Reset the persisted Enabled flag of groups entering the set BEFORE persisting
	// the config, so a re-added group starts disabled instead of inheriting a stale
	// flag left in storage. Failing hard here keeps the reset invariant without
	// leaving the config and the in-memory view diverged: nothing below has changed
	// yet, so the operation is a clean, retryable no-op on error.
	m.statusMu.Lock()
	var addedGroups []string
	for id := range metaServiceGroups {
		if m.cachedStatus[id] == nil {
			addedGroups = append(addedGroups, id)
		}
	}
	m.statusMu.Unlock()
	if len(addedGroups) > 0 {
		if err := m.store.RunInTxn(ctx, func(txn kv.Txn) error {
			for _, id := range addedGroups {
				if err := m.store.RemoveMetaServiceGroupStatus(txn, id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if err := persist(); err != nil {
		return err
	}
	// Apply the change to the in-memory view only after the config is persisted, so
	// the two never diverge. Added groups start at zero: a new group has no
	// keyspaces, and the delete guard guarantees a removed (hence re-addable) group
	// had none either.
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
	// Best-effort cleanup of the deleted groups' now-orphan persisted status. It is
	// safe if this fails: RefreshCache only loads status for current groups, so an
	// orphan key is never read, and a future re-add clears it via the reset above.
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

// assignedKeyspaceCounts returns the number of keyspaces assigned to each of the
// given groups. It prefers the authoritative keyspace scan (immune to counter
// drift) and falls back to the cached counter when no scanner is configured,
// e.g. in unit tests without a keyspace manager.
func (m *MetaServiceGroupManager) assignedKeyspaceCounts(ctx context.Context, groupIDs []string) (map[string]int, error) {
	if m.keyspaceAssignmentCounter != nil {
		set := make(map[string]struct{}, len(groupIDs))
		for _, id := range groupIDs {
			set[id] = struct{}{}
		}
		return m.keyspaceAssignmentCounter(ctx, set)
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
