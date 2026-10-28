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
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// BalancePolicy controls whether an affinity group may be balanced.
type BalancePolicy string

const (
	// BalancePolicyAuto allows the group's placement to be improved.
	BalancePolicyAuto BalancePolicy = "auto"
	// BalancePolicyFixed keeps the group's placement unchanged.
	BalancePolicyFixed BalancePolicy = "fixed"
)

// IsFixed reports whether a group with policy p keeps its target. An empty
// policy means auto. Any other value, such as one that a newer version writes,
// keeps the target like fixed.
func (p BalancePolicy) IsFixed() bool {
	return p != "" && p != BalancePolicyAuto
}

// TableID identifies the table of TiDB partition groups.
type TableID string

// partitionGroupIDRegexp matches the IDs of TiDB partition groups:
// _tidb_pt_{table}_p{partition}, or _tidb_ks{keyspace}_pt_{table}_p{partition} in NextGen.
var partitionGroupIDRegexp = regexp.MustCompile(`^_tidb_(ks[0-9]+_)?pt_[0-9]+_p[0-9]+$`)

// TableIDForGroup returns the table of a TiDB partition group. Other groups
// belong to no table and are not balanced.
func TableIDForGroup(groupID string) (TableID, bool) {
	if !partitionGroupIDRegexp.MatchString(groupID) {
		return "", false
	}
	return TableID(groupID[:strings.LastIndex(groupID, "_p")]), true
}

// balanceTable records the groups of a table.
type balanceTable struct {
	// version changes when a group is added to or removed from the table, or
	// when the target, policy, availability or ranges of a group change.
	version uint64
	groups  map[string]struct{}
}

// touchBalanceTableLocked adds the group to its table and gives the table a
// new version. The write lock of RWMutex must be held.
func (m *Manager) touchBalanceTableLocked(groupID string) {
	tableID, ok := TableIDForGroup(groupID)
	if !ok {
		return
	}
	table := m.balanceTables[tableID]
	if table == nil {
		table = &balanceTable{groups: make(map[string]struct{})}
		m.balanceTables[tableID] = table
	}
	table.groups[groupID] = struct{}{}
	// Versions come from one counter, so a table that is created again does
	// not reuse a version of the deleted one.
	m.lastBalanceVersion++
	table.version = m.lastBalanceVersion
}

// removeBalanceGroupLocked removes a deleted group from its table. The write
// lock of RWMutex must be held.
func (m *Manager) removeBalanceGroupLocked(groupID string) {
	tableID, ok := TableIDForGroup(groupID)
	if !ok {
		return
	}
	table := m.balanceTables[tableID]
	if table == nil {
		return
	}
	delete(table.groups, groupID)
	if len(table.groups) == 0 {
		delete(m.balanceTables, tableID)
		return
	}
	m.lastBalanceVersion++
	table.version = m.lastBalanceVersion
}

// GetBalanceVersion returns the version of a table. It returns false if the
// table has no groups.
func (m *Manager) GetBalanceVersion(tableID TableID) (uint64, bool) {
	m.RLock()
	defer m.RUnlock()
	table := m.balanceTables[tableID]
	if table == nil {
		return 0, false
	}
	return table.version, true
}

// IsBalanceTableEligible reports whether a table has more than one group and
// at least one of them is not fixed.
func (m *Manager) IsBalanceTableEligible(tableID TableID) bool {
	m.RLock()
	defer m.RUnlock()
	table := m.balanceTables[tableID]
	if table == nil || len(table.groups) <= 1 {
		return false
	}
	for id := range table.groups {
		if group := m.groups[id]; group != nil && group.BalancePolicy != BalancePolicyFixed {
			return true
		}
	}
	return false
}

// BalanceCounts counts, for each store, the groups whose target leader or
// voters are on the store.
type BalanceCounts struct {
	Leader map[uint64]int
	Voter  map[uint64]int
}

// GetBalanceCounts counts the targets of all groups.
func (m *Manager) GetBalanceCounts() BalanceCounts {
	m.RLock()
	defer m.RUnlock()
	counts := BalanceCounts{Leader: make(map[uint64]int), Voter: make(map[uint64]int)}
	for _, group := range m.groups {
		if group.LeaderStoreID != 0 {
			counts.Leader[group.LeaderStoreID]++
		}
		for _, storeID := range group.VoterStoreIDs {
			counts.Voter[storeID]++
		}
	}
	return counts
}

// BalanceSnapshot is a copy of the state of a table to plan its targets from.
type BalanceSnapshot struct {
	TableID TableID
	Version uint64
	// Groups is sorted by ID.
	Groups []*GroupState
	Ranges map[string]GroupKeyRanges
	// Stores lists the IDs of the up TiKV stores in ascending order.
	Stores []uint64
	// Counts counts the targets of all groups, including those of this table.
	Counts BalanceCounts
}

// GetBalanceSnapshot copies the state of a table. counts comes from
// GetBalanceCounts and may be shared by several tables. It returns nil if the
// table has no groups.
func (m *Manager) GetBalanceSnapshot(tableID TableID, counts BalanceCounts) *BalanceSnapshot {
	stores := m.balanceStores()
	// keyRanges is protected by metaMutex.
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	m.RLock()
	defer m.RUnlock()
	table := m.balanceTables[tableID]
	if table == nil {
		return nil
	}
	snapshot := &BalanceSnapshot{
		TableID: tableID,
		Version: table.version,
		Groups:  make([]*GroupState, 0, len(table.groups)),
		Ranges:  make(map[string]GroupKeyRanges, len(table.groups)),
		Stores:  stores,
		Counts:  counts,
	}
	for id := range table.groups {
		group := m.groups[id]
		if group == nil {
			continue
		}
		snapshot.Groups = append(snapshot.Groups, newGroupState(group))
		ranges := m.keyRanges[id]
		keyRanges := make([]keyutil.KeyRange, 0, len(ranges.KeyRanges))
		for _, r := range ranges.KeyRanges {
			keyRanges = append(keyRanges, keyutil.KeyRange{StartKey: slices.Clone(r.StartKey), EndKey: slices.Clone(r.EndKey)})
		}
		ranges.KeyRanges = keyRanges
		snapshot.Ranges[id] = ranges
	}
	slices.SortFunc(snapshot.Groups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
	return snapshot
}

// balanceStores returns the IDs of the up TiKV stores in ascending order.
func (m *Manager) balanceStores() []uint64 {
	var ids []uint64
	for _, store := range m.storeSetInformer.GetStores() {
		if store.IsUp() && store.IsTiKV() {
			ids = append(ids, store.GetID())
		}
	}
	slices.Sort(ids)
	return ids
}

// CommitBalanceTargets writes targets planned from snapshot and returns the
// IDs of the written groups. A target is written only if its stored group
// still equals the group in snapshot.
func (m *Manager) CommitBalanceTargets(ctx context.Context, snapshot *BalanceSnapshot, targets []*Group) ([]string, error) {
	groupTargets := make([]GroupTarget, 0, len(targets))
	for _, target := range targets {
		i := slices.IndexFunc(snapshot.Groups, func(group *GroupState) bool { return group.ID == target.ID })
		if i < 0 {
			return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(target.ID)
		}
		groupTargets = append(groupTargets, GroupTarget{
			Expected:      snapshot.Groups[i].Group,
			LeaderStoreID: target.LeaderStoreID,
			VoterStoreIDs: target.VoterStoreIDs,
		})
	}
	if m.targetWriter == nil {
		return m.UpdateAffinityGroupTargets(groupTargets)
	}
	return m.sendGroupTargets(ctx, groupTargets)
}
