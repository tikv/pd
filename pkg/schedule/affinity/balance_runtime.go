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
	"slices"
	"strings"
	"sync"
	"time"

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
		if balanceRegionChanged(cached.balanceInput(), region) {
			m.invalidateBalanceLocked(cached.groupInfo.ID)
			cached.balanceRegion = region
			m.regions[region.GetID()] = cached
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
	return a.GetRegionEpoch().GetVersion() != b.GetRegionEpoch().GetVersion() ||
		a.GetRegionEpoch().GetConfVer() != b.GetRegionEpoch().GetConfVer() ||
		a.GetLeader().GetStoreId() != b.GetLeader().GetStoreId() ||
		!bytes.Equal(a.GetStartKey(), b.GetStartKey()) || !bytes.Equal(a.GetEndKey(), b.GetEndKey()) ||
		!core.SortedPeersEqual(a.GetPeers(), b.GetPeers()) ||
		!core.SortedPeersEqual(a.GetPendingPeers(), b.GetPendingPeers()) ||
		!core.SortedPeersStatsEqual(a.GetDownPeers(), b.GetDownPeers())
}

type balanceTableState struct {
	phase       balancePhase
	version     uint64
	fingerprint balanceFingerprint
	groups      map[string]struct{}
	actions     map[string]BalanceActionSet
	queued      bool
	retryAfter  time.Time
}

// SetBalanceActionBuilder connects the checker to the pure affinity planner.
// One background worker builds action indexes so Region checks never perform
// placement searches. Repeated requests for a table are coalesced.
func (m *Manager) SetBalanceActionBuilder(builder BalanceActionBuilder, ruleRevision func() uint64, validator BalanceActionValidator, ruleLock sync.Locker) {
	m.Lock()
	start := m.balanceBuilder == nil
	m.balanceBuilder, m.balanceRuleRevision = builder, ruleRevision
	m.balanceValidator, m.balanceRuleLock = validator, ruleLock
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
			_, err := m.balanceTableWithBudget(m.ctx, groupID, true, 8)
			if err != nil {
				log.Warn("failed to balance affinity table", zap.String("group-id", groupID), zap.Error(err))
			}
			m.Lock()
			if state := m.balanceTables[TableGroupPrefix(groupID)]; state != nil {
				state.queued = false
				// Unready groups and failed writes retry without rebuilding
				// placement on every heartbeat.
				state.retryAfter = time.Now().Add(time.Second)
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
	if state == nil || m.balanceBuilder == nil || state.queued {
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
		state = &balanceTableState{groups: make(map[string]struct{}), actions: make(map[string]BalanceActionSet)}
		m.balanceTables[tableID] = state
	}
	state.groups[groupID] = struct{}{}
	state.version++
	state.phase = balanceIdle
	state.retryAfter = time.Time{}
	delete(state.actions, groupID)
}

// InvalidateBalance invalidates the changed group's legal actions and its
// table's final no-improvement decision.
func (m *Manager) InvalidateBalance(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.invalidateBalanceLocked(groupID)
}

type balanceSnapshot struct {
	tableID     string
	fingerprint balanceFingerprint
	groups      []*GroupState
	ranges      map[string]GroupKeyRanges
	actions     map[string]BalanceActionSet
	stores      []uint64
	builder     BalanceActionBuilder
	validator   BalanceActionValidator
	ruleLock    sync.Locker
}

func (m *Manager) balanceSnapshot(groupID string) *balanceSnapshot {
	// Range metadata and target updates share metaMutex. Snapshotting both
	// here avoids using ranges from a different group target generation.
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	m.Lock()
	defer m.Unlock()
	tableID := TableGroupPrefix(groupID)
	state := m.balanceTables[tableID]
	if state == nil || m.balanceBuilder == nil {
		return nil
	}
	fingerprint := m.balanceFingerprintLocked(state)
	if state.phase == balanceStable && state.fingerprint == fingerprint {
		return nil
	}
	if fingerprint.stores != state.fingerprint.stores || fingerprint.rules != state.fingerprint.rules {
		clear(state.actions)
		state.phase = balanceIdle
	}
	result := &balanceSnapshot{
		tableID: tableID, fingerprint: fingerprint,
		builder: m.balanceBuilder, validator: m.balanceValidator, ruleLock: m.balanceRuleLock,
		ranges:  make(map[string]GroupKeyRanges, len(state.groups)),
		actions: make(map[string]BalanceActionSet, len(state.groups)),
		stores:  availableStoreIDs(m.storeSetInformer, m.unavailableStores),
	}
	for id := range state.groups {
		group := m.groups[id]
		if group == nil {
			continue
		}
		result.groups = append(result.groups, newGroupState(group))
		result.ranges[id] = m.keyRanges[id]
		if actions, ok := state.actions[id]; ok {
			result.actions[id] = actions
		}
	}
	return result
}

func (m *Manager) balanceTable(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	return m.balanceTableWithBudget(ctx, groupID, apply, 0)
}

// RebalanceAffinityGroup previews or applies one legal step for the containing
// table. An unready or already stable table keeps its current targets.
func (m *Manager) RebalanceAffinityGroup(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	if m.GetAffinityGroupState(groupID) == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}
	return m.balanceTable(ctx, groupID, apply)
}

func (m *Manager) balanceTableWithBudget(ctx context.Context, groupID string, apply bool, buildBudget int) ([]BalancePlan, error) {
	// Serialize automatic and manual passes without holding Manager locks
	// during placement checks or storage calls.
	m.balanceMutex.Lock()
	defer m.balanceMutex.Unlock()
	m.refreshBalanceStores()
	snapshot := m.balanceSnapshot(groupID)
	if snapshot == nil {
		return nil, nil
	}
	groups := make([]*Group, 0, len(snapshot.groups))
	hasAuto := false
	built := 0
	for _, group := range snapshot.groups {
		groups = append(groups, &group.Group)
		if group.BalancePolicy == BalancePolicyFixed {
			continue
		}
		if group.Phase != PhaseStable || !group.AffinitySchedulingAllowed {
			return nil, nil
		}
		hasAuto = true
		if _, ok := snapshot.actions[group.ID]; !ok {
			if buildBudget > 0 && built >= buildBudget {
				return nil, nil
			}
			actions, ready := snapshot.builder(ctx, group, snapshot.ranges[group.ID])
			built++
			if !ready || ctx.Err() != nil {
				return nil, ctx.Err()
			}
			snapshot.actions[group.ID] = actions
			m.Lock()
			state := m.balanceTables[snapshot.tableID]
			if state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
				state.actions[group.ID] = actions
				state.fingerprint = snapshot.fingerprint
			}
			m.Unlock()
		}
	}
	if !hasAuto || len(snapshot.stores) == 0 {
		return nil, nil
	}
	plans, err := PlanBalance(groups, BalanceOptions{Stores: snapshot.stores, ActionSets: snapshot.actions})
	if err != nil {
		return plans, err
	}
	for _, plan := range plans {
		if !plan.Changed {
			continue
		}
		for _, group := range snapshot.groups {
			if group.ID == plan.GroupID {
				if snapshot.validator != nil {
					ready, legal := snapshot.validator(ctx, group, snapshot.ranges[group.ID], plan)
					if !ready {
						return nil, ctx.Err()
					}
					if !legal {
						m.Lock()
						if state := m.balanceTables[snapshot.tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
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
						m.Unlock()
						return nil, nil
					}
				}
				if !apply {
					return plans, nil
				}
				m.refreshBalanceStores()
				var updated *GroupState
				updated, err = m.updateAffinityGroupPeersWithBalanceSnapshot(group.ID, group.affinityVer, plan.LeaderStoreID, plan.VoterStoreIDs, snapshot)
				if err == nil && (updated == nil || updated.LeaderStoreID != plan.LeaderStoreID || !slices.Equal(updated.VoterStoreIDs, plan.VoterStoreIDs)) {
					return nil, nil
				}
				return plans, err
			}
		}
	}
	m.Lock()
	if state := m.balanceTables[snapshot.tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
		state.phase = balanceStable
		state.fingerprint = snapshot.fingerprint
	}
	m.Unlock()
	return plans, nil
}

// refreshBalanceStores runs in affinity's existing availability loop and
// before a planning pass, not on each Region heartbeat.
func (m *Manager) refreshBalanceStores() {
	if m.storeSetInformer == nil {
		return
	}
	stores := m.storeSetInformer.GetStores()
	slices.SortFunc(stores, func(a, b *core.StoreInfo) int {
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
	m.Lock()
	defer m.Unlock()
	if fingerprint := h.Sum64(); fingerprint != m.balanceStoreFingerprint {
		m.balanceStoreFingerprint = fingerprint
		m.balanceStoreRevision++
	}
}

// TableGroupPrefix identifies the table to which an affinity group belongs.
// Non-partitioned and custom groups each form their own balance domain.
func TableGroupPrefix(groupID string) string {
	if strings.HasPrefix(groupID, "_tidb_pt_") {
		if index := strings.LastIndex(groupID, "_p"); index > 0 {
			return groupID[:index]
		}
	}
	return groupID
}

func availableStoreIDs(informer core.StoreSetInformer, unavailable map[uint64]storeCondition) []uint64 {
	if informer == nil {
		return nil
	}
	ids := make([]uint64, 0)
	for _, store := range informer.GetStores() {
		if store == nil || !store.IsUp() || !store.IsTiKV() {
			continue
		}
		if _, unavailable := unavailable[store.GetID()]; unavailable {
			continue
		}
		ids = append(ids, store.GetID())
	}
	slices.Sort(ids)
	return ids
}
