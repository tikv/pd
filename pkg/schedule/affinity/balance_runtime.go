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
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
)

// BalanceActionBuilder builds the intersection of legal single-step actions
// for a fully covered and converged affinity group. False means that checker
// repair or an operator must finish before the group can be balanced.
type BalanceActionBuilder func(context.Context, *GroupState, GroupKeyRanges) (BalanceActionSet, bool)

// BalanceActionMode selects which action class the checker builds. Leader
// transfers are exhausted across a table before voter replacements are built.
type BalanceActionMode uint8

const (
	// BalanceBuildAll builds both leader and voter actions.
	BalanceBuildAll BalanceActionMode = iota
	// BalanceBuildLeaders builds leader transfer actions.
	BalanceBuildLeaders
	// BalanceBuildVoters builds voter replacement actions.
	BalanceBuildVoters
)

// BalanceActionBuildResult reports one bounded page of placement work.
type BalanceActionBuildResult struct {
	Actions     BalanceActionSet
	Cursor      any
	Complete    bool
	Ready       bool
	BlockTable  bool
	Checks      int
	RegionBytes map[uint64]uint64
}

// BalanceActionProgressBuilder builds a page of legal actions using the same
// store snapshot as the table plan. A ready but incomplete result is resumed
// by the affinity worker.
type BalanceActionProgressBuilder func(context.Context, *GroupState, GroupKeyRanges, []uint64, BalanceActionMode, any, int) BalanceActionBuildResult

// VirtualBalanceActionProgressBuilder builds actions for a virtual target
// while validating every scanned region against the actual group state.
type VirtualBalanceActionProgressBuilder func(context.Context, *GroupState, GroupKeyRanges, *Group, []uint64, BalanceActionMode, any, int) BalanceActionBuildResult

// BalanceActionValidator rechecks the selected action before submission.
// Unready groups wait for convergence; illegal actions are removed from the cache.
type BalanceActionValidator func(context.Context, *GroupState, GroupKeyRanges, BalancePlan) (ready, legal bool)

type balancePhase uint8

const (
	balanceIdle balancePhase = iota
	balanceConverging
	balanceStable
)

type balanceFingerprint struct {
	table, stores, rules uint64
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
	phase                 balancePhase
	version               uint64
	fingerprint           balanceFingerprint
	groups                map[string]struct{}
	actions               map[string]BalanceActionSet
	leaderTargets         map[string][]uint64
	voterActions          map[string]BalanceActionSet
	mode                  BalanceActionMode
	cursors               map[string]any
	regionBytes           map[string]map[uint64]uint64
	nextGroup             string
	pending               *balanceSnapshot
	dependencies          map[string]string
	dependencyConditions  []clientv3.Cmp
	dependencyFingerprint balanceFingerprint
	queued                bool
	retryAfter            time.Time
}

// SetBalanceActionBuilder connects the checker to the pure affinity planner.
// One background worker builds action indexes so Region checks never perform
// placement searches. Repeated requests for a table are coalesced.
func (m *Manager) SetBalanceActionBuilder(builder BalanceActionBuilder, ruleRevision func() uint64, validator BalanceActionValidator) {
	m.SetBalanceActionBuilders(builder, nil, nil, ruleRevision, validator)
}

// SetBalanceActionBuilders connects the legacy and resumable checkers to the
// affinity planner. The legacy builder remains for package tests and callers
// that do not need bounded placement scans.
func (m *Manager) SetBalanceActionBuilders(builder BalanceActionBuilder, progressBuilder BalanceActionProgressBuilder,
	virtualBuilder VirtualBalanceActionProgressBuilder, ruleRevision func() uint64, validator BalanceActionValidator) {
	m.Lock()
	start := m.balanceBuilder == nil && m.balanceProgressBuilder == nil
	m.balanceBuilder, m.balanceProgressBuilder, m.balanceVirtualBuilder = builder, progressBuilder, virtualBuilder
	m.balanceRuleRevision = ruleRevision
	m.balanceValidator = validator
	m.Unlock()
	if start {
		go m.runBalanceWorker()
	}
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
					state.retryAfter = time.Time{}
					select {
					case m.balanceQueue <- groupID:
						state.queued = true
					default:
						state.retryAfter = time.Now().Add(time.Second)
					}
				} else {
					// Unready groups and failed writes retry without rebuilding
					// placement on every heartbeat.
					state.retryAfter = time.Now().Add(time.Second)
				}
			}
			m.Unlock()
		}
	}
}

// AutoBalanceGroup requests one balance pass for the table containing groupID.
// Stable tables return immediately. Planning runs on the affinity worker;
// targets converge through the existing AffinityChecker operator path.
func (m *Manager) AutoBalanceGroup(groupID string) (*BalancePlan, error) {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[TableGroupPrefix(groupID)]
	if state == nil || (m.balanceBuilder == nil && m.balanceProgressBuilder == nil) || state.queued {
		return nil, nil
	}
	fingerprint := m.balanceFingerprintLocked(state)
	if state.phase == balanceStable && state.fingerprint == fingerprint {
		return nil, nil
	}
	if time.Now().Before(state.retryAfter) {
		return nil, nil
	}
	select {
	case m.balanceQueue <- groupID:
		state.queued = true
	default:
		// A full worker queue is retried by the next affinity check.
	}
	return nil, nil
}

func (m *Manager) balanceFingerprintLocked(state *balanceTableState) balanceFingerprint {
	var rules uint64
	if m.balanceRuleRevision != nil {
		rules = m.balanceRuleRevision()
	}
	return balanceFingerprint{state.version, m.balanceStoreRevision, rules}
}

func (m *Manager) invalidateBalanceLocked(groupID string) {
	tableID := TableGroupPrefix(groupID)
	state := m.balanceTables[tableID]
	if state == nil {
		state = &balanceTableState{
			groups:        make(map[string]struct{}),
			actions:       make(map[string]BalanceActionSet),
			leaderTargets: make(map[string][]uint64),
			voterActions:  make(map[string]BalanceActionSet),
			cursors:       make(map[string]any),
			regionBytes:   make(map[string]map[uint64]uint64),
			mode:          BalanceBuildLeaders,
		}
		m.balanceTables[tableID] = state
	}
	state.groups[groupID] = struct{}{}
	state.version++
	state.phase = balanceIdle
	state.mode = BalanceBuildLeaders
	delete(state.actions, groupID)
	delete(state.leaderTargets, groupID)
	delete(state.voterActions, groupID)
	delete(state.cursors, groupID)
	delete(state.regionBytes, groupID)
	state.pending = nil
	clearBalanceDependencies(state)
	state.nextGroup = ""
	state.retryAfter = time.Time{}
}

// InvalidateBalance invalidates the changed group's legal actions and its
// table's final no-improvement decision.
func (m *Manager) InvalidateBalance(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.invalidateBalanceLocked(groupID)
}

type balanceSnapshot struct {
	tableID        string
	fingerprint    balanceFingerprint
	groups         []*GroupState
	ranges         map[string]GroupKeyRanges
	actions        map[string]BalanceActionSet
	leaderTargets  map[string][]uint64
	stores         []uint64
	mode           BalanceActionMode
	cursors        map[string]any
	regionBytes    map[string]map[uint64]uint64
	nextGroup      string
	globalLeaders  map[uint64]int
	globalVoters   map[uint64]int
	unreadyGroups  map[string]struct{}
	planningGroups []*Group
	autoGroups     []*GroupState
	progress       BalanceActionProgressBuilder
	virtual        VirtualBalanceActionProgressBuilder
	validator      BalanceActionValidator
	dependencies   map[string]string
	conditions     []clientv3.Cmp
	applied        bool
	metadata       map[string]string
	admission      func() bool
	prepared       bool
}

func (m *Manager) balanceSnapshot(groupID string) *balanceSnapshot {
	// Range metadata and target updates share metaMutex. Snapshotting both
	// here avoids using ranges from a different group target generation.
	liveStores, storeFingerprint := m.loadBalanceStoreSnapshot()
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	m.Lock()
	defer m.Unlock()
	tableID := TableGroupPrefix(groupID)
	state := m.balanceTables[tableID]
	if state == nil || (m.balanceBuilder == nil && m.balanceProgressBuilder == nil) {
		return nil
	}
	m.updateBalanceStoreFingerprintLocked(storeFingerprint)
	fingerprint := m.balanceFingerprintLocked(state)
	if state.phase == balanceStable && state.fingerprint == fingerprint {
		return nil
	}
	if fingerprint.stores != state.fingerprint.stores || fingerprint.rules != state.fingerprint.rules {
		clear(state.actions)
		clear(state.leaderTargets)
		clear(state.voterActions)
		clear(state.cursors)
		clear(state.regionBytes)
		state.pending = nil
		clearBalanceDependencies(state)
		state.phase = balanceIdle
		state.mode = BalanceBuildLeaders
		state.nextGroup = ""
	}
	if state.pending != nil {
		if state.pending.fingerprint == fingerprint && state.dependencyFingerprint == fingerprint {
			return state.pending
		}
		state.pending = nil
		clearBalanceDependencies(state)
	}
	liveUnavailable := m.collectUnavailableStoresFor(liveStores)
	globalLeaders, globalVoters := make(map[uint64]int, len(m.groups)), make(map[uint64]int, len(m.groups))
	for _, group := range m.groups {
		if group.LeaderStoreID != 0 {
			globalLeaders[group.LeaderStoreID]++
		}
		for _, storeID := range group.VoterStoreIDs {
			globalVoters[storeID]++
		}
	}
	progressBuilder := m.balanceProgressBuilder
	mode := state.mode
	if progressBuilder == nil && m.balanceBuilder != nil {
		legacyBuilder := m.balanceBuilder
		progressBuilder = func(ctx context.Context, group *GroupState, ranges GroupKeyRanges, _ []uint64, _ BalanceActionMode, _ any, _ int) BalanceActionBuildResult {
			actions, ready := legacyBuilder(ctx, group, ranges)
			return BalanceActionBuildResult{Actions: actions, Complete: ready, Ready: ready, BlockTable: !ready, Checks: 1}
		}
		mode = BalanceBuildAll
	}
	result := &balanceSnapshot{
		tableID:       tableID,
		fingerprint:   fingerprint,
		progress:      progressBuilder,
		virtual:       m.balanceVirtualBuilder,
		validator:     m.balanceValidator,
		admission:     m.balanceAdmission,
		ranges:        make(map[string]GroupKeyRanges, len(state.groups)),
		actions:       make(map[string]BalanceActionSet, len(state.groups)),
		leaderTargets: make(map[string][]uint64, len(state.leaderTargets)),
		cursors:       make(map[string]any, len(state.cursors)),
		regionBytes:   make(map[string]map[uint64]uint64, len(state.regionBytes)),
		stores:        availableStoreIDsFromStores(liveStores, liveUnavailable),
		mode:          mode,
		nextGroup:     state.nextGroup,
		globalLeaders: globalLeaders,
		globalVoters:  globalVoters,
		unreadyGroups: make(map[string]struct{}),
	}
	if state.dependencyFingerprint == fingerprint {
		result.dependencies = maps.Clone(state.dependencies)
		result.conditions = slices.Clone(state.dependencyConditions)
	}
	for id := range state.groups {
		group := m.groups[id]
		if group == nil {
			continue
		}
		result.groups = append(result.groups, newGroupState(group))
		result.ranges[id] = m.keyRanges[id]
		if leaders, ok := state.leaderTargets[id]; ok {
			result.leaderTargets[id] = slices.Clone(leaders)
		}
		if actions, ok := state.actions[id]; ok && mode == BalanceBuildAll {
			result.actions[id] = cloneBalanceActionSet(actions)
		}
		switch mode {
		case BalanceBuildLeaders:
			if leaders, ok := state.leaderTargets[id]; ok {
				result.actions[id] = BalanceActionSet{LeaderTargets: slices.Clone(leaders)}
			}
		case BalanceBuildVoters:
			if actions, ok := state.voterActions[id]; ok {
				result.actions[id] = cloneBalanceActionSet(actions)
			}
		}
		if cursor, ok := state.cursors[id]; ok {
			result.cursors[id] = cursor
		}
		if weights, ok := state.regionBytes[id]; ok {
			result.regionBytes[id] = maps.Clone(weights)
		}
	}
	return result
}

func (m *Manager) balanceTable(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	return m.balanceTableWithBudget(ctx, groupID, apply, 0)
}

// RebalanceAffinityGroup previews or applies the complete final rebalance plan
// for the containing table. Region movement converges asynchronously after an
// applied plan.
func (m *Manager) RebalanceAffinityGroup(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	if m.GetAffinityGroupState(groupID) == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}
	return m.rebalanceTable(ctx, groupID, apply)
}

func (m *Manager) balanceTableWithBudget(ctx context.Context, groupID string, apply bool, buildBudget int) ([]BalancePlan, error) {
	plans, err, _ := m.balanceTableWithProgress(ctx, groupID, apply, buildBudget)
	return plans, err
}

//nolint:revive // the boolean reports whether a bounded build should resume.
func (m *Manager) balanceTableWithProgress(ctx context.Context, groupID string, apply bool, buildBudget int) ([]BalancePlan, error, bool) {
	tableID := TableGroupPrefix(groupID)
	m.Lock()
	tableLock := m.balanceLocks[tableID]
	if tableLock == nil {
		tableLock = &sync.Mutex{}
		m.balanceLocks[tableID] = tableLock
	}
	m.Unlock()
	// The lock map is kept for the Manager lifetime so deleting and recreating
	// a table cannot leave two planners holding different locks for one table.
	if !tableLock.TryLock() {
		return nil, nil, false
	}
	defer tableLock.Unlock()

	var snapshot *balanceSnapshot
	retainDependencies := false
	defer func() {
		if snapshot != nil && !retainDependencies {
			m.releaseBalanceDependencies(snapshot)
		}
	}()

	snapshot = m.balanceSnapshot(groupID)
	if snapshot == nil {
		return nil, nil, false
	}
	if err := m.captureBalanceDependencies(ctx, snapshot); err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return nil, nil, false
		}
		return nil, err, false
	}

	if !snapshot.prepared {
		slices.SortFunc(snapshot.groups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
		snapshot.planningGroups = make([]*Group, 0, len(snapshot.groups))
		snapshot.autoGroups = make([]*GroupState, 0, len(snapshot.groups))
		for _, group := range snapshot.groups {
			snapshot.planningGroups = append(snapshot.planningGroups, &group.Group)
			if group.BalancePolicy != BalancePolicyFixed {
				snapshot.autoGroups = append(snapshot.autoGroups, group)
			}
		}
		snapshot.prepared = true
	}
	groups, autoGroups := snapshot.planningGroups, snapshot.autoGroups
	for _, group := range autoGroups {
		if group.Phase != PhaseStable || !group.AffinitySchedulingAllowed {
			return nil, nil, false
		}
	}
	if len(autoGroups) == 0 || len(snapshot.stores) == 0 {
		return nil, nil, false
	}
	remaining := buildBudget
	for {
		complete, nextRemaining, yield := m.buildBalanceActionStage(ctx, snapshot, autoGroups, remaining)
		remaining = nextRemaining
		if ctx.Err() != nil {
			return nil, ctx.Err(), false
		}
		if !complete {
			if yield && m.retainBalanceSnapshot(snapshot) {
				retainDependencies = true
			} else {
				yield = false
			}
			return nil, nil, yield
		}
		if !m.refreshBalanceSnapshotScores(snapshot) {
			return nil, nil, false
		}
		if snapshot.mode == BalanceBuildVoters {
			for id, actions := range snapshot.actions {
				actions.LeaderTargets = slices.Clone(snapshot.leaderTargets[id])
				snapshot.actions[id] = actions
			}
		}

		plans, err := PlanBalance(groups, BalanceOptions{
			Stores: snapshot.stores, ActionSets: snapshot.actions,
			GlobalLeaderCounts: snapshot.globalLeaders, GlobalVoterCounts: snapshot.globalVoters,
		})
		if err != nil {
			return plans, err, false
		}
		changed := findChangedPlan(plans)
		if snapshot.mode == BalanceBuildLeaders && changed == nil {
			// Every group must have a complete leader action set before the
			// table can advance to voter replacement planning. A temporarily
			// unready group must stay in leader mode and be retried.
			if len(snapshot.unreadyGroups) > 0 {
				return plans, nil, false
			}
			for id, actions := range snapshot.actions {
				snapshot.leaderTargets[id] = slices.Clone(actions.LeaderTargets)
			}
			if !m.balanceSnapshotCurrent(snapshot) {
				return nil, nil, false
			}
			m.Lock()
			state := m.balanceTables[snapshot.tableID]
			switched := false
			var voterActions map[string]BalanceActionSet
			if state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
				switched = true
				state.mode = BalanceBuildVoters
				state.phase = balanceIdle
				for id, leaders := range snapshot.leaderTargets {
					state.leaderTargets[id] = slices.Clone(leaders)
				}
				clear(state.actions)
				clear(state.cursors)
				state.nextGroup = ""
				voterActions = make(map[string]BalanceActionSet, len(state.voterActions))
				for id, actions := range state.voterActions {
					cloned := cloneBalanceActionSet(actions)
					state.actions[id] = cloned
					voterActions[id] = cloned
				}
			}
			m.Unlock()
			if !switched {
				return nil, nil, false
			}
			snapshot.mode = BalanceBuildVoters
			snapshot.actions = voterActions
			for id := range snapshot.unreadyGroups {
				delete(snapshot.actions, id)
			}
			snapshot.cursors = make(map[string]any, len(autoGroups))
			snapshot.nextGroup = ""
			if buildBudget > 0 && remaining <= 0 {
				if m.retainBalanceSnapshot(snapshot) {
					retainDependencies = true
				} else {
					return nil, nil, false
				}
				return nil, nil, true
			}
			continue
		}
		if changed != nil {
			return m.finishBalancePlan(ctx, snapshot, plans, *changed, apply)
		}
		if len(snapshot.unreadyGroups) > 0 {
			return plans, nil, false
		}
		m.Lock()
		if state := m.balanceTables[snapshot.tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
			state.phase = balanceStable
			state.fingerprint = snapshot.fingerprint
		}
		m.Unlock()
		return plans, nil, false
	}
}

func (m *Manager) buildBalanceActionStage(ctx context.Context, snapshot *balanceSnapshot, groups []*GroupState, budget int) (bool, int, bool) {
	ordered := rotateBalanceGroups(groups, snapshot.nextGroup)
	complete := true
	remaining := budget
	for _, group := range ordered {
		if _, unready := snapshot.unreadyGroups[group.ID]; unready {
			continue
		}
		if _, built := snapshot.actions[group.ID]; built {
			if _, hasCursor := snapshot.cursors[group.ID]; !hasCursor {
				continue
			}
		}
		if budget > 0 && remaining <= 0 {
			return false, remaining, true
		}
		cursor := snapshot.cursors[group.ID]
		page := snapshot.progress(ctx, group, snapshot.ranges[group.ID], snapshot.stores, snapshot.mode, cursor, remaining)
		if ctx.Err() != nil {
			return false, remaining, false
		}
		if page.BlockTable {
			return false, remaining, false
		}
		if !page.Ready {
			snapshot.unreadyGroups[group.ID] = struct{}{}
			snapshot.actions[group.ID] = BalanceActionSet{VoterReplacements: make(map[uint64]map[uint64][]uint64)}
			delete(snapshot.cursors, group.ID)
			continue
		}
		if page.Checks < 0 || (budget > 0 && page.Checks > remaining) {
			return false, remaining, false
		}
		if budget > 0 {
			remaining -= page.Checks
		}
		actions, ok := m.cacheBalanceBuildPage(snapshot, group.ID, page)
		if !ok {
			return false, remaining, false
		}
		snapshot.actions[group.ID] = actions
		if page.Complete {
			delete(snapshot.cursors, group.ID)
		} else {
			if page.Checks == 0 {
				return false, remaining, false
			}
			snapshot.cursors[group.ID] = page.Cursor
			complete = false
		}
		weights := snapshot.regionBytes[group.ID]
		if weights == nil {
			weights = make(map[uint64]uint64, len(page.RegionBytes))
			snapshot.regionBytes[group.ID] = weights
		}
		maps.Copy(weights, page.RegionBytes)
		snapshot.nextGroup = group.ID
	}
	return complete, remaining, !complete
}

func (m *Manager) cacheBalanceBuildPage(snapshot *balanceSnapshot, groupID string, page BalanceActionBuildResult) (BalanceActionSet, bool) {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || m.balanceFingerprintLocked(state) != snapshot.fingerprint {
		return BalanceActionSet{}, false
	}
	weights := state.regionBytes[groupID]
	if weights == nil {
		weights = make(map[uint64]uint64, len(page.RegionBytes))
		state.regionBytes[groupID] = weights
	}
	for regionID, bytes := range page.RegionBytes {
		if cached, ok := m.regions[regionID]; ok && cached.groupInfo.ID == groupID {
			bytes = approximateRegionBytes(cached.balanceInput())
		}
		weights[regionID] = bytes
	}
	actions := cloneBalanceActionSet(page.Actions)
	actions.MoveBytes = sumRegionBytes(weights)
	if state.voterActions == nil {
		state.voterActions = make(map[string]BalanceActionSet)
	}
	if snapshot.mode == BalanceBuildLeaders {
		state.leaderTargets[groupID] = slices.Clone(actions.LeaderTargets)
	} else if snapshot.mode == BalanceBuildVoters && page.Complete {
		state.voterActions[groupID] = cloneBalanceActionSet(actions)
	}
	state.actions[groupID] = actions
	if page.Complete {
		delete(state.cursors, groupID)
	} else {
		state.cursors[groupID] = page.Cursor
	}
	state.nextGroup = groupID
	state.fingerprint = snapshot.fingerprint
	return cloneBalanceActionSet(actions), true
}

func clearBalanceDependencies(state *balanceTableState) {
	state.dependencies = nil
	state.dependencyConditions = nil
	state.dependencyFingerprint = balanceFingerprint{}
}

func (m *Manager) releaseBalanceDependencies(snapshot *balanceSnapshot) {
	m.Lock()
	defer m.Unlock()
	if state := m.balanceTables[snapshot.tableID]; state != nil {
		if state.pending == snapshot {
			state.pending = nil
		}
		if state.dependencyFingerprint == snapshot.fingerprint {
			clearBalanceDependencies(state)
		}
	}
}

func (m *Manager) retainBalanceSnapshot(snapshot *balanceSnapshot) bool {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || m.balanceFingerprintLocked(state) != snapshot.fingerprint ||
		state.dependencyFingerprint != snapshot.fingerprint || state.dependencies == nil {
		return false
	}
	state.pending = snapshot
	return true
}

func (m *Manager) refreshBalanceSnapshotScores(snapshot *balanceSnapshot) bool {
	m.RLock()
	defer m.RUnlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || m.balanceFingerprintLocked(state) != snapshot.fingerprint {
		return false
	}
	clear(snapshot.globalLeaders)
	clear(snapshot.globalVoters)
	for _, group := range m.groups {
		if group.LeaderStoreID != 0 {
			snapshot.globalLeaders[group.LeaderStoreID]++
		}
		for _, storeID := range group.VoterStoreIDs {
			snapshot.globalVoters[storeID]++
		}
	}
	for id, actions := range snapshot.actions {
		if current, ok := state.actions[id]; ok {
			actions.MoveBytes = current.MoveBytes
			snapshot.actions[id] = actions
		}
	}
	return true
}

//nolint:revive // the boolean reports whether a bounded build should resume.
func (m *Manager) finishBalancePlan(ctx context.Context, snapshot *balanceSnapshot, plans []BalancePlan, plan BalancePlan, apply bool) ([]BalancePlan, error, bool) {
	for _, group := range snapshot.groups {
		if group.ID != plan.GroupID {
			continue
		}
		if snapshot.validator != nil {
			ready, legal := snapshot.validator(ctx, group, snapshot.ranges[group.ID], plan)
			if !ready {
				return nil, ctx.Err(), false
			}
			if !legal {
				m.removeIllegalBalanceAction(snapshot, group, plan)
				return nil, nil, false
			}
		}
		if !apply {
			return plans, nil, false
		}
		m.refreshBalanceStores()
		if !m.balanceSnapshotCurrent(snapshot) {
			return nil, nil, false
		}
		committed, err := m.commitBalancePlans(ctx, snapshot, []BalancePlan{plan})
		if !committed {
			return nil, err, false
		}
		return plans, err, false
	}
	return nil, nil, false
}

func (m *Manager) removeIllegalBalanceAction(snapshot *balanceSnapshot, group *GroupState, plan BalancePlan) {
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || m.balanceFingerprintLocked(state) != snapshot.fingerprint {
		return
	}
	actions := state.actions[group.ID]
	if slices.Equal(plan.VoterStoreIDs, group.VoterStoreIDs) {
		actions.LeaderTargets = slices.DeleteFunc(actions.LeaderTargets, func(id uint64) bool { return id == plan.LeaderStoreID })
	} else {
		for from, targets := range actions.VoterReplacements {
			if !slices.Contains(plan.VoterStoreIDs, from) {
				for to, leaders := range targets {
					if !slices.Contains(group.VoterStoreIDs, to) && slices.Contains(plan.VoterStoreIDs, to) {
						targets[to] = slices.DeleteFunc(leaders, func(id uint64) bool { return id == plan.LeaderStoreID })
					}
				}
			}
		}
	}
	state.actions[group.ID] = actions
}

func findChangedPlan(plans []BalancePlan) *BalancePlan {
	for i := range plans {
		if plans[i].Changed {
			return &plans[i]
		}
	}
	return nil
}

func rotateBalanceGroups(groups []*GroupState, after string) []*GroupState {
	if len(groups) == 0 || after == "" {
		return groups
	}
	index := slices.IndexFunc(groups, func(group *GroupState) bool { return group.ID == after })
	if index < 0 || index+1 == len(groups) {
		return groups
	}
	result := make([]*GroupState, 0, len(groups))
	result = append(result, groups[index+1:]...)
	result = append(result, groups[:index+1]...)
	return result
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
			ID        uint64
			State     int32
			NodeState int32
			Labels    []*metapb.StoreLabel
			Flags     [10]bool
		}{
			store.GetID(), int32(store.GetMeta().GetState()), int32(store.GetMeta().GetNodeState()), store.GetLabels(),
			[10]bool{store.IsUnhealthy(), store.IsDisconnected(), store.IsBusy(), store.IsLowSpace(m.conf.GetLowSpaceRatio()),
				store.AllowLeaderTransferIn(), store.AllowLeaderTransferOut(), store.EvictedAsSlowStore(),
				store.EvictedAsStoppingStore(), store.IsEvictedAsSlowTrend(), m.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels())},
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
	weights := state.regionBytes[groupID]
	if weights == nil {
		return
	}
	previous, ok := weights[regionID]
	if !ok {
		return
	}
	weights[regionID] = bytes
	if actions, ok := state.actions[groupID]; ok {
		actions.MoveBytes = actions.MoveBytes - previous + bytes
		state.actions[groupID] = actions
	}
	if actions, ok := state.voterActions[groupID]; ok {
		actions.MoveBytes = actions.MoveBytes - previous + bytes
		state.voterActions[groupID] = actions
	}
}

func approximateRegionBytes(region *core.RegionInfo) uint64 {
	if region == nil || region.GetApproximateSize() <= 0 {
		return 0
	}
	return uint64(region.GetApproximateSize()) * 1024 * 1024
}

func sumRegionBytes(weights map[uint64]uint64) uint64 {
	var total uint64
	for _, bytes := range weights {
		total += bytes
	}
	return total
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
