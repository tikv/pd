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
	"encoding/json"
	"hash/fnv"
	"maps"
	"slices"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// BalanceVersion identifies the local inputs of a table search. Rules and
// Labels are cheap global generations; the checker decides whether their
// changes affect this table before discarding its search progress.
type BalanceVersion struct {
	Table, Ranges, Stores, Rules, Labels uint64
}

type balanceTableState struct {
	version, rangeVersion uint64
	groups                map[string]struct{}
}

// BalanceSnapshot owns a copy of the table metadata and commit dependencies.
// The checker owns the search; Manager never retains its mutable progress.
type BalanceSnapshot struct {
	TableID            string
	Version            BalanceVersion
	Groups             []*GroupState
	Ranges             map[string]GroupKeyRanges
	Stores             []uint64
	GlobalLeaderCounts map[uint64]int
	GlobalVoterCounts  map[uint64]int
	dependencies       map[string]string
	conditions         []clientv3.Cmp
}

// SetBalancePlacementGuard supplies the local placement generation and the
// metadata used to reconcile a scheduling service's independent watchers.
func (m *Manager) SetBalancePlacementGuard(revision func() uint64, metadata func() map[string]string) {
	m.Lock()
	defer m.Unlock()
	m.balanceRuleRevision, m.balanceMetadata = revision, metadata
	m.balanceMetadataSource++
	m.balanceMetadataVerified = nil
}

func (m *Manager) currentBalanceRuleRevisionLocked() uint64 {
	if m.balanceRuleRevision != nil {
		return m.balanceRuleRevision()
	}
	return 0
}

func (m *Manager) balanceVersionLocked(state *balanceTableState) BalanceVersion {
	return BalanceVersion{Table: state.version, Ranges: state.rangeVersion, Stores: m.balanceStoreRevision,
		Rules: m.currentBalanceRuleRevisionLocked(), Labels: m.regionLabeler.GetRevision()}
}

// GetBalanceVersion reads generations without taking the metadata mutex or
// scanning ranges. It is suitable for the Region check's enqueue fast path.
func (m *Manager) GetBalanceVersion(groupID string) (BalanceVersion, bool) {
	m.RLock()
	defer m.RUnlock()
	state := m.balanceTables[TableGroupPrefix(groupID)]
	if state == nil {
		return BalanceVersion{}, false
	}
	return m.balanceVersionLocked(state), true
}

// GetBalanceSnapshot copies only this table and the incremental global counts.
// It performs no placement checks and starts no background work.
func (m *Manager) GetBalanceSnapshot(groupID string) *BalanceSnapshot {
	stores, fingerprint := m.loadBalanceStoreSnapshot()
	m.metaMutex.Lock()
	m.Lock()
	m.updateBalanceStoreFingerprintLocked(fingerprint)
	state := m.balanceTables[TableGroupPrefix(groupID)]
	if state == nil {
		m.Unlock()
		m.metaMutex.Unlock()
		return nil
	}
	result := &BalanceSnapshot{
		TableID: TableGroupPrefix(groupID), Version: m.balanceVersionLocked(state),
		Groups: make([]*GroupState, 0, len(state.groups)), Ranges: make(map[string]GroupKeyRanges, len(state.groups)),
		Stores:             balanceCandidateStores(stores),
		GlobalLeaderCounts: maps.Clone(m.balanceLeaderCounts), GlobalVoterCounts: maps.Clone(m.balanceVoterCounts),
	}
	for id := range state.groups {
		if group := m.groups[id]; group != nil {
			result.Groups = append(result.Groups, newGroupState(group))
			result.Ranges[id] = cloneBalanceRanges(m.keyRanges[id])
		}
	}
	m.Unlock()
	m.metaMutex.Unlock()
	slices.SortFunc(result.Groups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
	return result
}

// ObserveBalanceRegion invalidates searches on topology, health and scheduling
// changes. Approximate size is read directly while validating a candidate.
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
		if balanceRegionChanged(cached.balanceInput(), region) || cached.balanceScheduleDenied != scheduleDenied {
			m.invalidateBalanceLocked(cached.groupInfo.ID)
			cached.balanceRegion, cached.balanceScheduleDenied = region, scheduleDenied
			m.regions[region.GetID()] = cached
			cached.groupInfo.Regions[region.GetID()] = cached
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

func (m *Manager) invalidateBalanceLocked(groupID string) {
	tableID := TableGroupPrefix(groupID)
	state := m.balanceTables[tableID]
	if state == nil {
		state = &balanceTableState{groups: make(map[string]struct{})}
		m.balanceTables[tableID] = state
	}
	if _, exists := state.groups[groupID]; !exists {
		state.rangeVersion++
	}
	state.groups[groupID] = struct{}{}
	// Generations cannot be reused when a deleted table is recreated.
	m.balanceGeneration++
	state.version = m.balanceGeneration
}

// InvalidateBalance advances the table's generation after an observed change.
func (m *Manager) InvalidateBalance(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.invalidateBalanceLocked(groupID)
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
	return a.GroupID == b.GroupID && slices.EqualFunc(a.KeyRanges, b.KeyRanges, func(a, b keyutil.KeyRange) bool {
		return bytes.Equal(a.StartKey, b.StartKey) && bytes.Equal(a.EndKey, b.EndKey)
	})
}

// refreshBalanceStores runs in the existing availability loop, rather than
// hashing all stores on every Region heartbeat.
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
	stores := slices.DeleteFunc(slices.Clone(m.storeSetInformer.GetStores()), func(store *core.StoreInfo) bool { return store == nil })
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
			Flags     [8]bool
		}{ID: store.GetID(), State: int32(store.GetMeta().GetState()), NodeState: int32(store.GetMeta().GetNodeState()), Labels: store.GetLabels(),
			Flags: [8]bool{store.IsUnhealthy(), store.IsLowSpace(m.conf.GetLowSpaceRatio()),
				store.AllowLeaderTransferIn(), store.AllowLeaderTransferOut(), store.EvictedAsSlowStore(),
				store.EvictedAsStoppingStore(), store.IsEvictedAsSlowTrend(), m.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels())}}
		data, _ := json.Marshal(input)
		_, _ = h.Write(data)
	}
	return stores, h.Sum64()
}

func (m *Manager) updateBalanceStoreFingerprintLocked(fingerprint uint64) {
	if fingerprint != m.balanceStoreFingerprint {
		m.balanceStoreFingerprint = fingerprint
		m.balanceStoreRevision++
	}
}

// TableGroupPrefix identifies the table to which an affinity group belongs.
func TableGroupPrefix(groupID string) string {
	if strings.HasPrefix(groupID, "_tidb_pt_") {
		if index := strings.LastIndex(groupID, "_p"); index >= len("_tidb_pt_") {
			return groupID[:index]
		}
	}
	return groupID
}

// Temporary restrictions stay in the candidate search so an unavailable store
// cannot turn an incomplete search into a stable result.
func balanceCandidateStores(stores []*core.StoreInfo) []uint64 {
	ids := make([]uint64, 0, len(stores))
	for _, store := range stores {
		if !store.IsUp() || !store.IsTiKV() {
			continue
		}
		ids = append(ids, store.GetID())
	}
	return ids
}
