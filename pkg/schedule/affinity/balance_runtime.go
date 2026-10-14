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

package affinity

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
)

// BalanceActionMode selects which action class the checker builds. Leader
// transfers are exhausted across a table before voter replacements are built.
type BalanceActionMode uint8

const (
	// BalanceBuildLeaders builds leader transfer actions.
	BalanceBuildLeaders BalanceActionMode = iota
	// BalanceBuildVoters builds voter replacement actions.
	BalanceBuildVoters
	// BalanceCheckConvergence scans complete range coverage and peer alignment
	// without constructing placement actions.
	BalanceCheckConvergence
	// BalanceValidateTarget rechecks one selected target in bounded pages.
	BalanceValidateTarget
)

// BalanceActionBuildResult reports one bounded page of placement work.
type BalanceActionBuildResult struct {
	Actions    BalanceActionSet
	Cursor     any
	Complete   bool
	Ready      bool
	BlockTable bool
	// InvalidTarget reports a selected target rejected by placement or store rules.
	InvalidTarget bool
	// TemporaryBlocked means some candidates must be retried before stability
	// can be concluded. Other confirmed actions remain usable.
	TemporaryBlocked bool
	Checks           int
	// RegionBytes contains only sizes observed on this page.
	RegionBytes map[uint64]uint64
}

// BalanceActionProgressBuilder builds a page of legal actions using the same
// store snapshot as the table plan. A ready but incomplete result is resumed
// by the affinity worker.
type BalanceActionProgressBuilder func(context.Context, *GroupState, GroupKeyRanges, *Group, []uint64, BalanceActionMode, any, int) BalanceActionBuildResult

type balancePhase uint8

const (
	balanceIdle balancePhase = iota
	balanceConverging
	balanceStable
)

type balanceFingerprint struct {
	table, stores, rules uint64
}

type balanceTableLock struct {
	mutex sync.Mutex
	users int
}

func (m *Manager) acquireBalanceTableLock(tableID string) *sync.Mutex {
	m.Lock()
	defer m.Unlock()
	lock := m.balanceLocks[tableID]
	if lock == nil {
		lock = &balanceTableLock{}
		m.balanceLocks[tableID] = lock
	}
	lock.users++
	return &lock.mutex
}

func (m *Manager) releaseBalanceTableLock(tableID string, mutex *sync.Mutex) {
	m.Lock()
	defer m.Unlock()
	lock := m.balanceLocks[tableID]
	if lock != nil && &lock.mutex == mutex {
		lock.users--
		if lock.users == 0 {
			delete(m.balanceLocks, tableID)
		}
	}
}

// ObserveBalanceRegion invalidates only on changes that affect affinity
// actions. Ordinary heartbeat statistics do not invalidate the action cache.
func (m *Manager) ObserveBalanceRegion(region *core.RegionInfo) {
	scheduleDenied := m.regionLabeler.ScheduleDisabled(region)
	m.RLock()
	_, observed := m.regions[region.GetID()]
	hasGroups := len(m.groups) > 0
	m.RUnlock()
	if !hasGroups {
		return
	}
	var groupID string
	if !observed {
		groupID = m.regionLabeler.GetRegionLabel(region, labelKey)
		if groupID == "" {
			return
		}
	}
	m.Lock()
	defer m.Unlock()
	if cached, ok := m.regions[region.GetID()]; ok {
		previous := cached.balanceInput()
		regionChanged := balanceRegionChanged(previous, region)
		scheduleChanged := cached.balanceScheduleDenied != scheduleDenied
		if regionChanged || scheduleChanged {
			m.invalidateBalanceLocked(cached.groupInfo.ID)
			cached.balanceRegion = region
			cached.balanceScheduleDenied = scheduleDenied
			m.regions[region.GetID()] = cached
			cached.groupInfo.Regions[region.GetID()] = cached
		} else if previous.GetApproximateSize() != region.GetApproximateSize() {
			cached.balanceRegion = region
			m.regions[region.GetID()] = cached
			cached.groupInfo.Regions[region.GetID()] = cached
			m.updateBalanceRegionBytesLocked(cached.groupInfo.ID, region.GetID(), approximateRegionBytes(region))
		}
	} else if _, ok := m.groups[groupID]; ok {
		m.invalidateBalanceLocked(groupID)
	}
}

func (cache regionCache) balanceInput() *core.RegionInfo {
	if cache.balanceRegion != nil {
		return cache.balanceRegion
	}
	return cache.region
}

func balanceRegionChanged(a, b *core.RegionInfo) bool {
	leaderStoreID := func(region *core.RegionInfo) uint64 {
		if region.GetLeader() == nil {
			return 0
		}
		return region.GetLeader().GetStoreId()
	}
	return a.GetRegionEpoch().GetVersion() != b.GetRegionEpoch().GetVersion() ||
		a.GetRegionEpoch().GetConfVer() != b.GetRegionEpoch().GetConfVer() ||
		leaderStoreID(a) != leaderStoreID(b) ||
		!bytes.Equal(a.GetStartKey(), b.GetStartKey()) || !bytes.Equal(a.GetEndKey(), b.GetEndKey()) ||
		!core.SortedPeersEqual(a.GetPeers(), b.GetPeers()) ||
		!core.SortedPeersEqual(a.GetPendingPeers(), b.GetPendingPeers()) ||
		!core.SortedPeersStatsEqual(a.GetDownPeers(), b.GetDownPeers())
}

type balanceTableState struct {
	phase        balancePhase
	version      uint64
	rangeVersion uint64
	ruleVersion  uint64
	ruleRanges   uint64
	fingerprint  balanceFingerprint
	groups       map[string]struct{}
	actions      map[string]*balanceGroupActions
	pending      *balanceSnapshot
	queued       bool
	retryAfter   time.Time
	retryDelay   time.Duration
}

// Completed action records are immutable except for regionBytes, which is
// accessed only under the Manager lock. Replacements use copy-on-write so a
// worker can retain a record while heartbeats refresh its migration estimate.
type balanceGroupActions struct {
	set                           BalanceActionSet
	leadersReady, votersReady     bool
	leadersBlocked, votersBlocked bool
	regionBytes                   map[uint64]uint64
}

// SetBalanceActionBuilder installs the checker's sole, resumable action
// interface. The worker owns unfinished scans; Region checks only enqueue work.
func (m *Manager) SetBalanceActionBuilder(builder BalanceActionProgressBuilder, ruleRevision func() uint64) {
	m.Lock()
	start := m.balanceBuilder == nil
	m.balanceBuilder, m.balanceRuleRevision = builder, ruleRevision
	m.balanceMetadataSource++
	m.balanceMetadataVerified = nil
	m.Unlock()
	if start && builder != nil {
		go m.runBalanceWorker()
	}
}

// SetBalanceRuleFingerprint supplies a fingerprint of placement rules that
// actually intersect the table. It runs on the worker, outside Manager locks.
func (m *Manager) SetBalanceRuleFingerprint(fingerprint func([]GroupKeyRanges) uint64) {
	m.Lock()
	m.balanceRuleFingerprint = fingerprint
	for _, state := range m.balanceTables {
		state.ruleRanges = 0
		state.phase = balanceIdle
	}
	m.Unlock()
}

func (m *Manager) runBalanceWorker() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case groupID := <-m.balanceQueue:
			_, err, incomplete := m.balanceTableWithProgress(m.ctx, groupID, true, 256)
			if err != nil {
				log.Warn("failed to balance affinity table", zap.String("group-id", groupID), zap.Error(err))
			}
			m.Lock()
			if state := m.balanceTables[TableGroupPrefix(groupID)]; state != nil {
				state.queued = false
				if incomplete && err == nil {
					select {
					case m.balanceQueue <- groupID:
						state.queued = true
					default:
						state.retryAfter = time.Now().Add(time.Second)
					}
				} else {
					state.retryDelay = min(max(time.Second, state.retryDelay*2), 30*time.Second)
					state.retryAfter = time.Now().Add(state.retryDelay)
				}
			}
			m.Unlock()
		}
	}
}

// AutoBalanceGroup only checks cheap generations and coalesces worker requests.
// It never takes the metadata mutex or scans table ranges on a Region check.
func (m *Manager) AutoBalanceGroup(groupID string) (*BalancePlan, error) {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[TableGroupPrefix(groupID)]
	if state == nil || m.balanceBuilder == nil || state.queued {
		return nil, nil
	}
	rules := m.currentBalanceRuleRevisionLocked()
	if state.phase == balanceStable && state.fingerprint == m.balanceFingerprintLocked(state) &&
		state.ruleVersion == rules && state.ruleRanges == state.rangeVersion {
		return nil, nil
	}
	if time.Now().Before(state.retryAfter) && state.ruleVersion == rules {
		return nil, nil
	}
	select {
	case m.balanceQueue <- groupID:
		state.queued = true
	default:
	}
	return nil, nil
}

func (m *Manager) currentBalanceRuleRevisionLocked() uint64 {
	if m.balanceRuleRevision != nil {
		return m.balanceRuleRevision()
	}
	return 0
}

func (m *Manager) balanceFingerprintLocked(state *balanceTableState) balanceFingerprint {
	return balanceFingerprint{table: state.version, stores: m.balanceStoreRevision, rules: state.fingerprint.rules}
}

// refreshBalanceRuleFingerprint samples ranges and rule generations together.
// An unrelated rule update advances ruleVersion without deleting legal actions.
func (m *Manager) refreshBalanceRuleFingerprint(tableID string) bool {
	m.metaMutex.Lock()
	m.RLock()
	state := m.balanceTables[tableID]
	if state == nil {
		m.RUnlock()
		m.metaMutex.Unlock()
		return false
	}
	revision, rangesVersion := m.currentBalanceRuleRevisionLocked(), state.rangeVersion
	if state.ruleVersion == revision && state.ruleRanges == rangesVersion {
		m.RUnlock()
		m.metaMutex.Unlock()
		return true
	}
	callback := m.balanceRuleFingerprint
	ranges := make([]GroupKeyRanges, 0, len(state.groups))
	for id := range state.groups {
		ranges = append(ranges, cloneBalanceRanges(m.keyRanges[id]))
	}
	m.RUnlock()
	m.metaMutex.Unlock()
	fingerprint := revision
	if callback != nil {
		fingerprint = callback(ranges)
	}
	m.Lock()
	defer m.Unlock()
	state = m.balanceTables[tableID]
	if state == nil || state.rangeVersion != rangesVersion || m.currentBalanceRuleRevisionLocked() != revision {
		return false
	}
	if state.fingerprint.rules != fingerprint {
		clear(state.actions)
		state.pending = nil
		state.phase = balanceIdle
	}
	state.fingerprint.rules = fingerprint
	state.ruleVersion, state.ruleRanges = revision, rangesVersion
	return true
}

func (m *Manager) invalidateBalanceLocked(groupID string) {
	tableID := TableGroupPrefix(groupID)
	state := m.balanceTables[tableID]
	if state == nil {
		state = &balanceTableState{groups: make(map[string]struct{}), actions: make(map[string]*balanceGroupActions)}
		m.balanceTables[tableID] = state
	}
	if _, exists := state.groups[groupID]; !exists {
		state.rangeVersion++
	}
	state.groups[groupID] = struct{}{}
	state.version++
	state.phase = balanceIdle
	delete(state.actions, groupID)
	// A pending scan is owned by the worker. Never inspect its mutable cursor,
	// weights or maps here, even when this pointer is also held by the worker.
	state.pending = nil
	state.retryAfter, state.retryDelay = time.Time{}, 0
}

// InvalidateBalance discards one group's actions and the table's stable result.
func (m *Manager) InvalidateBalance(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.invalidateBalanceLocked(groupID)
}

// A snapshot owns all unfinished work and submission dependencies. The table
// only retains its pointer between pages; invalidators never access its fields.
type balanceSnapshot struct {
	tableID         string
	fingerprint     balanceFingerprint
	rangeVersion    uint64
	ruleRevision    uint64
	groups          []*GroupState
	autoGroups      []*GroupState
	ranges          map[string]GroupKeyRanges
	actions         map[string]*balanceGroupActions
	stores          []uint64
	globalLeaders   map[uint64]int
	globalVoters    map[uint64]int
	builder         BalanceActionProgressBuilder
	admission       func() bool
	dependencies    map[string]string
	conditions      []clientv3.Cmp
	applied         bool
	mode            BalanceActionMode
	next            int
	cursor          any
	regionBytes     map[uint64]uint64
	blocked         bool
	groupBlocked    bool
	validationPlans []BalancePlan
}

func (m *Manager) balanceSnapshot(groupID string) *balanceSnapshot {
	return m.newBalanceSnapshot(groupID, false)
}

func (m *Manager) newBalanceSnapshot(groupID string, manual bool) *balanceSnapshot {
	tableID := TableGroupPrefix(groupID)
	if !m.refreshBalanceRuleFingerprint(tableID) {
		return nil
	}
	stores, storeFingerprint := m.loadBalanceStoreSnapshot()
	unavailable := m.collectUnavailableStoresFor(stores)
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[tableID]
	if state == nil || m.balanceBuilder == nil {
		return nil
	}
	m.updateBalanceStoreFingerprintLocked(storeFingerprint)
	if state.fingerprint.stores != m.balanceStoreRevision {
		clear(state.actions)
		state.pending = nil
		state.phase = balanceIdle
	}
	fingerprint := m.balanceFingerprintLocked(state)
	if !manual && state.phase == balanceStable && state.fingerprint == fingerprint {
		return nil
	}
	if !manual && state.pending != nil && state.pending.fingerprint == fingerprint &&
		state.pending.rangeVersion == state.rangeVersion &&
		state.pending.ruleRevision == m.currentBalanceRuleRevisionLocked() {
		return state.pending
	}
	state.fingerprint = fingerprint
	result := &balanceSnapshot{
		tableID: tableID, fingerprint: fingerprint, rangeVersion: state.rangeVersion, ruleRevision: m.currentBalanceRuleRevisionLocked(),
		builder: m.balanceBuilder, admission: m.balanceAdmission, mode: BalanceCheckConvergence,
		ranges:        make(map[string]GroupKeyRanges, len(state.groups)),
		actions:       make(map[string]*balanceGroupActions, len(state.groups)),
		stores:        availableStoreIDsFromStores(stores, unavailable),
		globalLeaders: maps.Clone(m.balanceLeaderCounts), globalVoters: maps.Clone(m.balanceVoterCounts),
	}
	for id := range state.groups {
		if group := m.groups[id]; group != nil {
			copy := newGroupState(group)
			result.groups = append(result.groups, copy)
			result.ranges[id] = cloneBalanceRanges(m.keyRanges[id])
			if copy.BalancePolicy != BalancePolicyFixed {
				result.autoGroups = append(result.autoGroups, copy)
			}
		}
		if record := state.actions[id]; record != nil {
			if record.leadersBlocked || record.votersBlocked {
				// Temporary omissions are usable in the current pass, but must
				// be reconstructed on every new pass before declaring stability.
				delete(state.actions, id)
			} else {
				result.actions[id] = record
			}
		}
	}
	slices.SortFunc(result.groups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
	slices.SortFunc(result.autoGroups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
	return result
}

func (m *Manager) balanceTable(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	return m.balanceTableWithBudget(ctx, groupID, apply, 0)
}

// RebalanceAffinityGroup previews or applies one legal balance step.
func (m *Manager) RebalanceAffinityGroup(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	if m.GetAffinityGroupState(groupID) == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}
	return m.balanceTable(ctx, groupID, apply)
}

func (m *Manager) balanceTableWithBudget(ctx context.Context, groupID string, apply bool, budget int) ([]BalancePlan, error) {
	plans, err, _ := m.balanceTableWithProgress(ctx, groupID, apply, budget)
	return plans, err
}

func cloneBalanceRanges(ranges GroupKeyRanges) GroupKeyRanges {
	ranges.KeyRanges = slices.Clone(ranges.KeyRanges)
	for i := range ranges.KeyRanges {
		ranges.KeyRanges[i].StartKey = slices.Clone(ranges.KeyRanges[i].StartKey)
		ranges.KeyRanges[i].EndKey = slices.Clone(ranges.KeyRanges[i].EndKey)
	}
	return ranges
}

func balanceRangesEqual(a, b GroupKeyRanges) bool {
	if a.GroupID != b.GroupID || len(a.KeyRanges) != len(b.KeyRanges) {
		return false
	}
	for i := range a.KeyRanges {
		if !bytes.Equal(a.KeyRanges[i].StartKey, b.KeyRanges[i].StartKey) || !bytes.Equal(a.KeyRanges[i].EndKey, b.KeyRanges[i].EndKey) {
			return false
		}
	}
	return true
}

//nolint:revive // the last boolean requests another bounded worker page.
func (m *Manager) balanceTableWithProgress(ctx context.Context, groupID string, apply bool, budget int) ([]BalancePlan, error, bool) {
	tableID := TableGroupPrefix(groupID)
	lock := m.acquireBalanceTableLock(tableID)
	defer m.releaseBalanceTableLock(tableID, lock)
	if !lock.TryLock() {
		return nil, nil, false
	}
	defer lock.Unlock()
	snapshot := m.balanceSnapshot(groupID)
	if snapshot == nil {
		return nil, nil, false
	}
	retain := false
	defer func() {
		if !retain {
			m.releaseBalanceDependencies(snapshot)
		}
	}()
	for _, group := range snapshot.autoGroups {
		if group.Phase != PhaseStable || !group.AffinitySchedulingAllowed {
			return nil, nil, false
		}
	}
	groups := make([]*Group, 0, len(snapshot.groups))
	for _, group := range snapshot.groups {
		groups = append(groups, &group.Group)
	}
	remaining := budget
	yield := func() ([]BalancePlan, error, bool) {
		retain = m.retainBalanceSnapshot(snapshot)
		return nil, ctx.Err(), retain
	}
	for {
		if snapshot.mode == BalanceValidateTarget {
			return m.finishBalancePlan(ctx, snapshot, apply, remaining, budget > 0, &retain)
		}
		if len(snapshot.autoGroups) == 0 || len(snapshot.groups) == 1 {
			break // One group cannot strictly lower either sum of squared counts.
		}
		if budget > 0 && remaining == 0 {
			return yield()
		}
		complete, left, resume := m.buildBalanceActionStage(ctx, snapshot, snapshot.autoGroups, remaining)
		remaining = left
		if ctx.Err() != nil || !m.balanceSnapshotCurrent(snapshot) {
			return nil, ctx.Err(), false
		}
		if !complete {
			if resume {
				return yield()
			}
			return nil, nil, false
		}
		if snapshot.mode == BalanceCheckConvergence {
			snapshot.mode, snapshot.next = BalanceBuildLeaders, 0
			continue
		}
		sets := m.balanceActionSets(snapshot)
		plans, err := PlanBalance(groups, BalanceOptions{
			Stores: snapshot.stores, ActionSets: sets,
			GlobalLeaderCounts: snapshot.globalLeaders, GlobalVoterCounts: snapshot.globalVoters,
		})
		if err != nil {
			return nil, err, false
		}
		if findChangedPlan(plans) != nil {
			if err := m.captureBalanceDependencies(ctx, snapshot); err != nil {
				if errs.ErrEtcdTxnConflict.Equal(err) {
					return nil, nil, false
				}
				return nil, err, false
			}
			snapshot.mode, snapshot.cursor, snapshot.validationPlans = BalanceValidateTarget, nil, plans
			continue
		}
		if snapshot.blocked {
			return plans, nil, false
		}
		if snapshot.mode == BalanceBuildLeaders {
			snapshot.mode, snapshot.next = BalanceBuildVoters, 0
			continue
		}
		break
	}
	// A no-op still verifies watcher inputs before caching stability.
	if err := m.captureBalanceDependencies(ctx, snapshot); err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return nil, nil, false
		}
		return nil, err, false
	}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: snapshot.stores, ActionSets: m.balanceActionSets(snapshot)})
	if err == nil {
		m.Lock()
		if state := m.balanceTables[tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint && state.rangeVersion == snapshot.rangeVersion {
			state.phase = balanceStable
		}
		m.Unlock()
	}
	return plans, err, false
}

// buildBalanceActionStage retains at most one unfinished group cursor. Each
// page charges all work, including unready scans, against the shared budget.
func (m *Manager) buildBalanceActionStage(ctx context.Context, snapshot *balanceSnapshot, groups []*GroupState, budget int) (bool, int, bool) {
	remaining := budget
	for snapshot.next < len(groups) {
		group := groups[snapshot.next]
		record := snapshot.actions[group.ID]
		if snapshot.mode == BalanceBuildLeaders && record != nil && record.leadersReady ||
			snapshot.mode == BalanceBuildVoters && record != nil && record.votersReady {
			snapshot.blocked = snapshot.blocked || record.leadersBlocked || record.votersBlocked
			snapshot.next++
			continue
		}
		if budget > 0 && remaining == 0 {
			return false, remaining, true
		}
		page := snapshot.builder(ctx, group, snapshot.ranges[group.ID], &group.Group, snapshot.stores, snapshot.mode, snapshot.cursor, remaining)
		if page.Checks < 0 || budget > 0 && page.Checks > remaining {
			return false, remaining, false
		}
		if budget > 0 {
			remaining -= page.Checks
		}
		if ctx.Err() != nil || page.BlockTable || !page.Ready {
			snapshot.cursor, snapshot.regionBytes = nil, nil
			return false, remaining, false
		}
		snapshot.blocked = snapshot.blocked || page.TemporaryBlocked
		snapshot.groupBlocked = snapshot.groupBlocked || page.TemporaryBlocked
		if snapshot.mode != BalanceCheckConvergence {
			if !m.cacheBalanceBuildPage(snapshot, group.ID, page) {
				return false, remaining, false
			}
		}
		if !page.Complete {
			snapshot.cursor = page.Cursor
			return false, remaining, page.Checks > 0
		}
		snapshot.next++
		snapshot.cursor, snapshot.regionBytes = nil, nil
		snapshot.groupBlocked = false
	}
	return true, remaining, false
}

func (m *Manager) cacheBalanceBuildPage(snapshot *balanceSnapshot, groupID string, page BalanceActionBuildResult) bool {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || m.balanceFingerprintLocked(state) != snapshot.fingerprint || state.rangeVersion != snapshot.rangeVersion ||
		m.currentBalanceRuleRevisionLocked() != snapshot.ruleRevision {
		return false
	}
	if snapshot.regionBytes == nil {
		snapshot.regionBytes = make(map[uint64]uint64)
	}
	for id, size := range page.RegionBytes {
		if cached, ok := m.regions[id]; ok && cached.groupInfo.ID == groupID {
			size = approximateRegionBytes(cached.balanceInput())
		}
		snapshot.regionBytes[id] = size
	}
	if !page.Complete {
		return true
	}
	record := &balanceGroupActions{regionBytes: snapshot.regionBytes}
	if previous := snapshot.actions[groupID]; previous != nil {
		*record = *previous
	}
	if snapshot.mode == BalanceBuildLeaders {
		record.set = cloneBalanceActionSet(page.Actions)
		record.leadersReady, record.leadersBlocked = true, snapshot.groupBlocked
	} else {
		voters := cloneBalanceActionSet(page.Actions)
		voters.LeaderTargets = record.set.LeaderTargets
		record.set = voters
		record.votersReady, record.votersBlocked = true, snapshot.groupBlocked
	}
	// Sizes are accumulated incrementally; only a completed scan visits its
	// observations once to publish the final total.
	if len(snapshot.regionBytes) > 0 {
		record.regionBytes = snapshot.regionBytes
		record.set.MoveBytes = 0
		for id, size := range record.regionBytes {
			if cached, ok := m.regions[id]; ok && cached.groupInfo.ID == groupID {
				size = approximateRegionBytes(cached.balanceInput())
				record.regionBytes[id] = size
			}
			record.set.MoveBytes += size
		}
	}
	state.actions[groupID], snapshot.actions[groupID] = record, record
	return true
}

func (m *Manager) balanceActionSets(snapshot *balanceSnapshot) map[string]BalanceActionSet {
	m.RLock()
	defer m.RUnlock()
	snapshot.globalLeaders, snapshot.globalVoters = maps.Clone(m.balanceLeaderCounts), maps.Clone(m.balanceVoterCounts)
	sets := make(map[string]BalanceActionSet, len(snapshot.actions))
	state := m.balanceTables[snapshot.tableID]
	for id, record := range snapshot.actions {
		actions := record.set
		if state != nil && state.actions[id] != nil {
			actions.MoveBytes = state.actions[id].set.MoveBytes
		}
		sets[id] = actions
	}
	return sets
}

func (m *Manager) retainBalanceSnapshot(snapshot *balanceSnapshot) bool {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || m.balanceFingerprintLocked(state) != snapshot.fingerprint || state.rangeVersion != snapshot.rangeVersion ||
		m.currentBalanceRuleRevisionLocked() != snapshot.ruleRevision {
		return false
	}
	state.pending = snapshot
	return true
}

func (m *Manager) releaseBalanceDependencies(snapshot *balanceSnapshot) {
	m.Lock()
	defer m.Unlock()
	if state := m.balanceTables[snapshot.tableID]; state != nil && state.pending == snapshot {
		state.pending = nil
	}
}

//nolint:revive // the last boolean requests another bounded validation page.
func (m *Manager) finishBalancePlan(ctx context.Context, snapshot *balanceSnapshot, apply bool, budget int, bounded bool, retain *bool) ([]BalancePlan, error, bool) {
	plan := findChangedPlan(snapshot.validationPlans)
	groupIndex := slices.IndexFunc(snapshot.groups, func(group *GroupState) bool { return group.ID == plan.GroupID })
	group := snapshot.groups[groupIndex]
	if bounded && budget == 0 {
		*retain = m.retainBalanceSnapshot(snapshot)
		return nil, nil, *retain
	}
	target := group.Group
	target.LeaderStoreID, target.VoterStoreIDs = plan.LeaderStoreID, plan.VoterStoreIDs
	page := snapshot.builder(ctx, group, snapshot.ranges[group.ID], &target, snapshot.stores, BalanceValidateTarget, snapshot.cursor, budget)
	if page.Checks < 0 || bounded && page.Checks > budget || ctx.Err() != nil || page.BlockTable || !page.Ready {
		return nil, ctx.Err(), false
	}
	if page.InvalidTarget {
		m.removeIllegalBalanceAction(snapshot, group, *plan)
		return nil, nil, false
	}
	if !page.Complete {
		snapshot.cursor = page.Cursor
		*retain = page.Checks > 0 && m.retainBalanceSnapshot(snapshot)
		return nil, nil, *retain
	}
	if !m.balanceSnapshotCurrent(snapshot) {
		return nil, nil, false
	}
	if apply {
		committed, err := m.commitBalancePlans(ctx, snapshot, []BalancePlan{*plan})
		if !committed || err != nil {
			return nil, err, false
		}
	}
	return snapshot.validationPlans, nil, false
}

func (m *Manager) removeIllegalBalanceAction(snapshot *balanceSnapshot, group *GroupState, plan BalancePlan) {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || m.balanceFingerprintLocked(state) != snapshot.fingerprint || state.rangeVersion != snapshot.rangeVersion {
		return
	}
	if previous := state.actions[group.ID]; previous != nil {
		record := *previous
		record.set = discardBalanceAction(record.set, group, plan)
		state.actions[group.ID] = &record
	}
	state.pending = nil
}

func discardBalanceAction(actions BalanceActionSet, group *GroupState, plan BalancePlan) BalanceActionSet {
	actions = cloneBalanceActionSet(actions)
	if slices.Equal(normalizeVoters(plan.VoterStoreIDs), normalizeVoters(group.VoterStoreIDs)) {
		actions.LeaderTargets = slices.DeleteFunc(actions.LeaderTargets, func(id uint64) bool { return id == plan.LeaderStoreID })
	} else {
		for from, targets := range actions.VoterReplacements {
			if !slices.Contains(plan.VoterStoreIDs, from) {
				for to, leaders := range targets {
					if !slices.Contains(group.VoterStoreIDs, to) && slices.Contains(plan.VoterStoreIDs, to) {
						targets[to] = slices.DeleteFunc(leaders, func(id uint64) bool { return id == plan.LeaderStoreID })
						if len(targets[to]) == 0 {
							delete(targets, to)
						}
					}
				}
			}
		}
	}
	return actions
}

func findChangedPlan(plans []BalancePlan) *BalancePlan {
	for i := range plans {
		if plans[i].Changed {
			return &plans[i]
		}
	}
	return nil
}

// refreshBalanceStores runs in affinity's existing availability loop and
// before a planning pass, not on each Region heartbeat.
func (m *Manager) refreshBalanceStores() {
	_, fingerprint := m.loadBalanceStoreSnapshot()
	m.Lock()
	defer m.Unlock()
	m.updateBalanceStoreFingerprintLocked(fingerprint)
}

func (m *Manager) loadBalanceStoreSnapshot() ([]*core.StoreInfo, uint64) {
	if m.storeSetInformer == nil {
		return nil, 0
	}
	stores := slices.Clone(m.storeSetInformer.GetStores())
	slices.SortFunc(stores, func(a, b *core.StoreInfo) int {
		if a == nil {
			if b == nil {
				return 0
			}
			return -1
		}
		if b == nil {
			return 1
		}
		if a.GetID() < b.GetID() {
			return -1
		}
		if a.GetID() > b.GetID() {
			return 1
		}
		return 0
	})
	h := fnv.New64a()
	for _, store := range stores {
		if store == nil {
			continue
		}
		input := struct {
			ID         uint64
			State      int32
			NodeState  int32
			Labels     []*metapb.StoreLabel
			Flags      [10]bool
			LimitFlags [12]bool
		}{}
		input.ID = store.GetID()
		input.State = int32(store.GetMeta().GetState())
		input.NodeState = int32(store.GetMeta().GetNodeState())
		input.Labels = store.GetLabels()
		input.Flags = [10]bool{store.IsUnhealthy(), store.IsDisconnected(), store.IsBusy(), store.IsLowSpace(m.conf.GetLowSpaceRatio()),
			store.AllowLeaderTransferIn(), store.AllowLeaderTransferOut(), store.EvictedAsSlowStore(),
			store.EvictedAsStoppingStore(), store.IsEvictedAsSlowTrend(), m.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels())}
		limitIndex := 0
		for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
			for level := constant.Low; level < constant.PriorityLevelLen; level++ {
				if limit := store.GetStoreLimit(); limit != nil {
					input.LimitFlags[limitIndex] = limit.Available(storelimit.RegionInfluence[typ], typ, level)
				}
				limitIndex++
			}
		}
		data, _ := json.Marshal(input)
		_, _ = h.Write(data)
	}
	return stores, h.Sum64()
}

func (m *Manager) updateBalanceStoreFingerprintLocked(fingerprint uint64) {
	if fingerprint == m.balanceStoreFingerprint {
		return
	}
	m.balanceStoreFingerprint = fingerprint
	m.balanceStoreRevision++
}

func (m *Manager) updateBalanceRegionBytesLocked(groupID string, regionID, bytes uint64) {
	state := m.balanceTables[TableGroupPrefix(groupID)]
	if state == nil {
		return
	}
	previous := state.actions[groupID]
	if previous == nil {
		return
	}
	size, ok := previous.regionBytes[regionID]
	if !ok {
		return
	}
	previous.regionBytes[regionID] = bytes
	record := *previous
	record.set.MoveBytes = record.set.MoveBytes - size + bytes
	state.actions[groupID] = &record
}

func approximateRegionBytes(region *core.RegionInfo) uint64 {
	if region == nil || region.GetApproximateSize() <= 0 {
		return 0
	}
	return uint64(region.GetApproximateSize()) * 1024 * 1024
}

func cloneBalanceActionSet(actions BalanceActionSet) BalanceActionSet {
	cloned := BalanceActionSet{
		LeaderTargets:     slices.Clone(actions.LeaderTargets),
		VoterReplacements: make(map[uint64]map[uint64][]uint64, len(actions.VoterReplacements)),
		MoveBytes:         actions.MoveBytes,
	}
	for from, targets := range actions.VoterReplacements {
		clonedTargets := make(map[uint64][]uint64, len(targets))
		for to, leaders := range targets {
			clonedTargets[to] = slices.Clone(leaders)
		}
		cloned.VoterReplacements[from] = clonedTargets
	}
	return cloned
}

// TableGroupPrefix identifies the table to which an affinity group belongs.
// Non-partitioned and custom groups each form their own balance domain.
func TableGroupPrefix(groupID string) string {
	if strings.HasPrefix(groupID, "_tidb_pt_") {
		if index := strings.LastIndex(groupID, "_p"); index >= len("_tidb_pt_") {
			return groupID[:index]
		}
	}
	return groupID
}

func availableStoreIDsFromStores(stores []*core.StoreInfo, unavailable map[uint64]storeCondition) []uint64 {
	ids := make([]uint64, 0, len(stores))
	for _, store := range stores {
		if store == nil || !store.IsUp() || !store.IsTiKV() {
			continue
		}
		if condition, unavailable := unavailable[store.GetID()]; unavailable && !condition.affectsLeaderOnly() {
			continue
		}
		ids = append(ids, store.GetID())
	}
	slices.Sort(ids)
	return ids
}
