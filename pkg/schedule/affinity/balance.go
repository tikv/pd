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
	"errors"
	"regexp"
	"slices"
	"strings"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

// BalancePolicy controls whether an affinity group may be balanced.
type BalancePolicy string

const (
	// BalancePolicyAuto allows the group's placement to be improved.
	BalancePolicyAuto BalancePolicy = "auto"
	// BalancePolicyFixed keeps the group's target until the group expires: the
	// balancer does not change it, and while the group is degraded a Region can
	// only confirm it. An expired group takes a new target by first-win.
	BalancePolicyFixed BalancePolicy = "fixed"
)

// IsFixed reports whether a group with policy p keeps its target. An empty
// policy means auto. Any other value, such as one that a newer version writes,
// keeps the target like fixed.
func (p BalancePolicy) IsFixed() bool {
	return p != "" && p != BalancePolicyAuto
}

// TableID identifies the table of TiDB partition affinity groups.
type TableID string

// partitionAffinityGroupIDRegexp matches the IDs of TiDB partition affinity groups:
// _tidb_pt_{table}_p{partition}, or _tidb_ks{keyspace}_pt_{table}_p{partition} in NextGen.
var partitionAffinityGroupIDRegexp = regexp.MustCompile(`^_tidb_(ks[0-9]+_)?pt_[0-9]+_p[0-9]+$`)

// TableIDForGroup returns the table of a TiDB partition affinity group. Other groups
// belong to no table and are not balanced.
// Group IDs are validated against the cluster's kernel mode at creation and clusters never switch
// between Classic and NextGen, so matching the partition ID pattern suffices without rerunning ValidateGroupID.
func TableIDForGroup(groupID string) (TableID, bool) {
	if !partitionAffinityGroupIDRegexp.MatchString(groupID) {
		return "", false
	}
	return TableID(groupID[:strings.LastIndex(groupID, "_p")]), true
}

// balanceTable records the groups of a table.
type balanceTable struct {
	// version changes when a group is added to or removed from the table, or
	// when its target, policy, persisted revision, observed availability, or
	// ranges change. Lazy expiration checks do not advance the version.
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
		if group := m.groups[id]; group != nil && !group.BalancePolicy.IsFixed() {
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
		Version: table.version,
		Groups:  make([]*GroupState, 0, len(table.groups)),
		Ranges:  make(map[string]GroupKeyRanges, len(table.groups)),
		Stores:  stores,
		Counts:  counts,
	}
	for id := range table.groups {
		snapshot.Groups = append(snapshot.Groups, newGroupState(m.groups[id]))
		ranges := m.keyRanges[id]
		ranges.KeyRanges = slices.Clone(ranges.KeyRanges)
		snapshot.Ranges[id] = ranges
	}
	slices.SortFunc(snapshot.Groups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
	return snapshot
}

// Group returns the group with the given ID, or nil if the snapshot has none.
func (s *BalanceSnapshot) Group(id string) *GroupState {
	i, found := slices.BinarySearchFunc(s.Groups, id, func(g *GroupState, id string) int { return strings.Compare(g.ID, id) })
	if !found {
		return nil
	}
	return s.Groups[i]
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
// still equals the group in snapshot. Targets whose stored groups changed are
// skipped, as are targets absent from snapshot or already fixed in snapshot.
// Another group of the table can still change after the final checks, for
// example when an admin updates the peers of a fixed group. With a
// partially written plan or such a change, this round may not improve the
// table; the next round replans from the latest state.
func (m *Manager) CommitBalanceTargets(ctx context.Context, snapshot *BalanceSnapshot, targets []*Group) ([]string, error) {
	groupTargets := make([]GroupTarget, 0, len(targets))
	for _, target := range targets {
		group := snapshot.Group(target.ID)
		if group == nil || group.BalancePolicy.IsFixed() {
			continue
		}
		groupTargets = append(groupTargets, GroupTarget{
			Expected:            group.Group,
			ExpectedModRevision: group.modRevision,
			LeaderStoreID:       target.LeaderStoreID,
			VoterStoreIDs:       target.VoterStoreIDs,
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(groupTargets) == 0 {
		return nil, nil
	}
	if m.targetWriter == nil {
		result, err := m.UpdateAffinityGroupTargets(ctx, groupTargets)
		return result.WrittenGroupIDs, err
	}
	var written []string
	for chunk := range slices.Chunk(groupTargets, etcdutil.MaxEtcdTxnOps) {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		result, err := m.writeBalanceTargets(ctx, chunk)
		written = append(written, result.WrittenGroupIDs...)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, ErrTargetWriterNotReady) {
				if unconfirmed := len(chunk) - len(result.WrittenGroupIDs); unconfirmed > 0 {
					targetWriteFailureCounter.Add(float64(unconfirmed))
				}
				log.Warn("failed to write affinity group targets",
					zap.Int("target-count", len(chunk)), zap.Int("written-count", len(result.WrittenGroupIDs)), errs.ZapError(err))
			}
			return written, err
		}
	}
	return written, nil
}

// Track deletions during the HTTP request so a response cannot install a
// confirmation for a group the watcher has already seen deleted.
func (m *Manager) writeBalanceTargets(ctx context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
	deletedAt := make([]int64, len(targets))
	m.targetWriteMutex.Lock()
	if m.balancingTargets == nil {
		m.balancingTargets = make(map[string][]*int64)
	}
	for i, target := range targets {
		id := target.Expected.ID
		m.balancingTargets[id] = append(m.balancingTargets[id], &deletedAt[i])
	}
	m.targetWriteMutex.Unlock()

	result, err := m.targetWriter.WriteGroupTargets(ctx, targets)
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	m.targetWriteMutex.Lock()
	defer m.targetWriteMutex.Unlock()
	m.Lock()
	defer m.Unlock()
	for i, target := range targets {
		id := target.Expected.ID
		inFlight := m.balancingTargets[id]
		index := slices.Index(inFlight, &deletedAt[i])
		inFlight = slices.Delete(inFlight, index, index+1)
		if len(inFlight) == 0 {
			delete(m.balancingTargets, id)
		} else {
			m.balancingTargets[id] = inFlight
		}
		if revision := result.WrittenRevisions[id]; revision > 0 {
			m.protectConfirmedTarget(&target, revision, deletedAt[i])
		}
	}
	return result, err
}
