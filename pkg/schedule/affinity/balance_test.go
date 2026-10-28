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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestTableIDForGroup(t *testing.T) {
	re := require.New(t)
	for _, tc := range []struct{ id, table string }{
		{"_tidb_pt_100_p0", "_tidb_pt_100"},
		{"_tidb_pt_100_p12", "_tidb_pt_100"},
		{"_tidb_ks1_pt_100_p0", "_tidb_ks1_pt_100"},
		{"_tidb_ks1_pt_100_p12", "_tidb_ks1_pt_100"},
		{"_tidb_pt_100", ""}, {"_tidb_pt_100_px", ""}, {"_tidb_pt_1_100_p0", ""},
		{"_tidb_t_100", ""}, {"_tidb_ks1_t_100", ""}, {"custom", ""},
	} {
		id, ok := TableIDForGroup(tc.id)
		re.Equal(TableID(tc.table), id, tc.id)
		re.Equal(tc.table != "", ok, tc.id)
	}
}

func TestBalanceTableLifecycle(t *testing.T) {
	re := require.New(t)
	m, _ := newTargetTestManager(t)
	const table TableID = "_tidb_pt_100"
	ids := []string{"_tidb_pt_100_p12", "_tidb_pt_100_p0", "_tidb_pt_101_p0", "_tidb_pt_100"}
	var version uint64
	changed := func() {
		t.Helper()
		next, exists := m.GetBalanceVersion(table)
		re.True(exists)
		re.Greater(next, version)
		version = next
	}
	re.False(m.IsBalanceTableEligible(table))
	ranges := GroupKeyRanges{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	changed()
	re.False(m.IsBalanceTableEligible(table))
	_, err := m.UpdateAffinityGroupPeers(ids[0], 1, []uint64{1, 2, 3})
	re.NoError(err)
	changed()
	// Region cache updates keep the version.
	m.GetAndCacheRegionAffinityGroupState(generateRegionForTest(1, []uint64{1, 2, 3}, ranges.KeyRanges[0]))
	re.Equal(version, m.GetBalanceSnapshot(table, m.GetBalanceCounts()).Version)
	_, err = m.UpdateAffinityGroupPolicy(ids[0], BalancePolicyFixed)
	re.NoError(err)
	changed()
	re.Equal(1, m.GetAffinityGroupState(ids[0]).AffinityRegionCount)
	synced := m.GetAffinityGroupState(ids[0]).Group
	synced.BalancePolicy = BalancePolicyAuto
	m.SyncGroupFromEtcd(&synced)
	changed()
	re.Equal(PhaseStable, m.GetAffinityGroupState(ids[0]).Phase)
	added := GroupKeyRanges{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: []byte("c"), EndKey: []byte("d")}}}
	re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{added}, nil))
	changed()
	m.ExpireAffinityGroup(ids[0])
	changed()
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[1]}, {GroupID: ids[2]}, {GroupID: ids[3]}}))
	changed()
	for _, id := range ids[1:] {
		_, err = m.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
		re.NoError(err)
	}
	changed()
	re.True(m.IsBalanceTableEligible(table))
	// The snapshot has the groups of this table sorted by ID and the up TiKV stores.
	stores := m.storeSetInformer.(*core.StoresInfo)
	for _, store := range []*metapb.Store{
		{Id: 8, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
		{Id: 7, State: metapb.StoreState_Tombstone, NodeState: metapb.NodeState_Removed},
		{Id: 6, State: metapb.StoreState_Offline, NodeState: metapb.NodeState_Removing},
		{Id: 5, NodeState: metapb.NodeState_Preparing},
	} {
		stores.PutStore(core.NewStoreInfo(store))
	}
	counts := m.GetBalanceCounts()
	re.Equal(BalanceCounts{Leader: map[uint64]int{1: 4}, Voter: map[uint64]int{1: 4, 2: 4, 3: 4}}, counts)
	snapshot := m.GetBalanceSnapshot(table, counts)
	re.Len(snapshot.Groups, 2)
	re.Equal([]string{ids[1], ids[0]}, []string{snapshot.Groups[0].ID, snapshot.Groups[1].ID})
	re.Len(snapshot.Ranges, 2)
	re.Equal([]uint64{1, 2, 3, 4, 5}, snapshot.Stores)
	counts.Leader[4] = 7
	re.Equal(counts, snapshot.Counts)
	// Mutate both a key buffer and its range entry to check both copy layers.
	snapshot.Groups[0].VoterStoreIDs[0] = 4
	snapshot.Ranges[ids[0]].KeyRanges[0].StartKey[0] = 'x'
	snapshot.Ranges[ids[0]].KeyRanges[0].EndKey[0] = 'y'
	snapshot.Ranges[ids[0]].KeyRanges[0] = keyutil.KeyRange{StartKey: []byte("x"), EndKey: []byte("y")}
	re.Equal([]uint64{1, 2, 3}, m.GetAffinityGroupState(ids[1]).VoterStoreIDs)
	re.Equal([]keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}, {StartKey: []byte("c"), EndKey: []byte("d")}},
		m.GetBalanceSnapshot(table, counts).Ranges[ids[0]].KeyRanges)
	// A table whose groups are all fixed is not eligible. Deleting its last group
	// removes it, and creating the group again gives a larger version.
	for _, id := range ids[:2] {
		_, err = m.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
		re.NoError(err)
		changed()
	}
	re.False(m.IsBalanceTableEligible(table))
	re.NoError(m.DeleteAffinityGroups(ids[1:2], true))
	changed()
	re.NoError(m.DeleteAffinityGroups(ids[:1], true))
	_, exists := m.GetBalanceVersion(table)
	re.False(exists)
	re.Nil(m.GetBalanceSnapshot(table, m.GetBalanceCounts()))
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[0]}}))
	changed()
}

func TestCommitBalanceTargets(t *testing.T) {
	writeErr := errors.New("writer unavailable")
	for _, scenario := range []string{"stored", "admin", "fixed", "recreated", "writer", "watched", "superseded", "error"} {
		t.Run(scenario, func(t *testing.T) {
			re := require.New(t)
			m, store := newTargetTestManager(t)
			const id = "_tidb_pt_100_p0"
			expected := Group{ID: id, CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, id, &expected) }))
			m.SyncGroupFromEtcd(&expected)
			m.ExpireAffinityGroup(id)
			snapshot := m.GetBalanceSnapshot("_tidb_pt_100", m.GetBalanceCounts())
			target := expected
			target.LeaderStoreID, target.VoterStoreIDs = 2, []uint64{1, 2, 4}
			stored, want, allowed := target, target, true
			wantWritten := []string{id}
			switch scenario {
			case "admin":
				_, err := m.UpdateAffinityGroupPeers(id, 3, []uint64{1, 3, 4})
				re.NoError(err)
			case "fixed":
				_, err := m.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
				re.NoError(err)
			case "recreated":
				re.NoError(m.DeleteAffinityGroups([]string{id}, true))
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
				_, err := m.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
				re.NoError(err)
			case "writer", "watched", "superseded", "error":
				stored = expected
				m.SetTargetWriter(&fakeTargetWriter{reply: func(GroupTarget) ([]string, error) {
					if scenario == "error" {
						return nil, writeErr
					}
					return []string{id}, nil
				}, write: func(g GroupTarget) {
					re.Equal(GroupTarget{Expected: expected, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 4}}, g)
					if scenario == "watched" || scenario == "superseded" {
						m.SyncGroupFromEtcd(&want)
					}
				}})
			}
			stale := scenario == "admin" || scenario == "fixed" || scenario == "recreated"
			if stale {
				stored = m.GetAffinityGroupState(id).Group
				want, allowed, wantWritten = stored, m.GetAffinityGroupState(id).AffinitySchedulingAllowed, nil
			} else if scenario == "superseded" {
				// The watcher delivers another target, which is used instead.
				want.LeaderStoreID, want.VoterStoreIDs = 4, []uint64{1, 3, 4}
			} else if scenario == "error" {
				want, allowed, wantWritten = expected, false, nil
			}
			written, err := m.CommitBalanceTargets(t.Context(), snapshot, []*Group{&target})
			if scenario == "error" {
				re.ErrorIs(err, writeErr)
			} else {
				re.NoError(err)
			}
			re.ElementsMatch(wantWritten, written)
			state := m.GetAffinityGroupState(id)
			re.Equal(want, state.Group)
			re.Equal(allowed, state.AffinitySchedulingAllowed)
			re.Equal(stored, *loadStoredGroup(re, store, id))
		})
	}
}
