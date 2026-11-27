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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
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

func balanceKey(key string) []byte {
	if !kerneltype.IsNextGen() {
		return []byte(key)
	}
	prefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 1)
	return codec.EncodeBytes(append(prefix, []byte(key)...))
}

const balanceTestTableID TableID = "_tidb_ks1_pt_100"

func balanceTestSnapshot(m *Manager) *BalanceSnapshot {
	return m.GetBalanceSnapshot(balanceTestTableID, m.GetBalanceCounts())
}

func balanceTestTargets(test *targetTest, n int) ([]*Group, *BalanceSnapshot) {
	changes := make([]GroupKeyRanges, n)
	for i := range changes {
		changes[i].GroupID = fmt.Sprintf("%s_p%d", balanceTestTableID, i+1)
	}
	test.NoError(test.CreateAffinityGroups(changes))
	snapshot := balanceTestSnapshot(test.Manager)
	targets := make([]*Group, n)
	for i, change := range changes {
		targets[i] = test.target(change.GroupID, 1, 1, 2).result()
	}
	return targets, snapshot
}

func TestBalanceTableLifecycle(t *testing.T) {
	re := require.New(t)
	test := newTargetTest(t)
	m := test.Manager
	const table TableID = "_tidb_ks1_pt_100"
	ids := []string{"_tidb_ks1_pt_100_p12", "_tidb_ks1_pt_100_p1", "_tidb_ks1_pt_101_p1", "custom"}
	var version uint64
	changed := func() {
		t.Helper()
		next, exists := m.GetBalanceVersion(table)
		re.True(exists)
		re.Greater(next, version)
		version = next
	}
	re.False(m.IsBalanceTableEligible(table))
	ranges := GroupKeyRanges{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: balanceKey("a"), EndKey: balanceKey("b")}}}
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	m.checkGroupsAvailability()
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
	added := GroupKeyRanges{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: balanceKey("c"), EndKey: balanceKey("d")}}}
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
	// Key buffers are immutable; the snapshot owns its range entries and voters.
	snapshot.Groups[0].VoterStoreIDs[0] = 4
	snapshot.Ranges[ids[0]].KeyRanges[0] = keyutil.KeyRange{StartKey: []byte("x"), EndKey: []byte("y")}
	re.Equal([]uint64{1, 2, 3}, m.GetAffinityGroupState(ids[1]).VoterStoreIDs)
	re.Equal([]keyutil.KeyRange{{StartKey: balanceKey("a"), EndKey: balanceKey("b")}, {StartKey: balanceKey("c"), EndKey: balanceKey("d")}},
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
	for _, scenario := range []string{"stored", "admin", "fixed", "recreated", "writer", "watched", "superseded", "error", "partial"} {
		t.Run(scenario, func(t *testing.T) {
			re := require.New(t)
			test := newTargetTest(t)
			m, store := test.Manager, test.store
			const id = "_tidb_ks1_pt_100_p1"
			expected := Group{ID: id, CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, id, &expected) }))
			m.SyncGroupFromEtcd(&expected)
			m.checkGroupsAvailability()
			m.ExpireAffinityGroup(id)
			snapshot := balanceTestSnapshot(m)
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
			case "writer", "watched", "superseded", "error", "partial":
				stored = expected
				test.useWriter(func(_ context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
					re.Equal([]GroupTarget{{Expected: expected, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 4}}}, targets)
					if scenario == "watched" || scenario == "superseded" {
						m.SyncGroupFromEtcd(&want)
					}
					if scenario == "error" {
						return GroupTargetsResponse{}, writeErr
					}
					if scenario == "partial" {
						return GroupTargetsResponse{WrittenGroupIDs: []string{id}}, writeErr
					}
					return GroupTargetsResponse{WrittenGroupIDs: []string{id}}, nil
				})
			}
			stale := scenario == "admin" || scenario == "fixed" || scenario == "recreated"
			if stale {
				stored = m.GetAffinityGroupState(id).Group
				want, allowed, wantWritten = stored, m.GetAffinityGroupState(id).AffinitySchedulingAllowed, nil
			} else if scenario == "superseded" {
				// The watcher delivers another target, which is used instead.
				want.LeaderStoreID, want.VoterStoreIDs = 4, []uint64{1, 3, 4}
			} else if scenario == "writer" || scenario == "partial" {
				want, allowed = expected, false
			} else if scenario == "error" {
				want, allowed, wantWritten = expected, false, nil
			}
			written, err := m.CommitBalanceTargets(t.Context(), snapshot, []*Group{&target})
			if scenario == "error" || scenario == "partial" {
				re.ErrorIs(err, writeErr)
			} else {
				re.NoError(err)
			}
			re.ElementsMatch(wantWritten, written)
			state := m.GetAffinityGroupState(id)
			re.Equal(want, state.Group)
			re.Equal(allowed, state.AffinitySchedulingAllowed)
			re.Equal(stored, *test.stored(id))
			if scenario == "writer" || scenario == "partial" {
				// The reply only reports persisted IDs; the watcher publishes them.
				m.SyncGroupFromEtcd(&target)
				state = m.GetAffinityGroupState(id)
				re.Equal(target, state.Group)
				re.True(state.AffinitySchedulingAllowed)
			}
		})
	}
}

func TestCommitBalanceTargetsRejectsStaleRevision(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	const id = "_tidb_ks1_pt_100_p1"
	store := storage.NewStorageWithEtcdBackend(client)
	test := newTargetTestWithStorage(t, t.Context(), store, id)
	_, err := test.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
	test.NoError(err)
	key := keypath.AffinityGroupPath(id)
	before, err := client.Get(t.Context(), key)
	test.NoError(err)
	test.Len(before.Kvs, 1)
	test.SyncGroupFromEtcdWithRevision(test.stored(id), before.Kvs[0].ModRevision)
	snapshot := balanceTestSnapshot(test.Manager)
	target := snapshot.Group(id).Group
	target.LeaderStoreID = 2
	_, err = client.Put(t.Context(), key, string(before.Kvs[0].Value))
	test.NoError(err)
	written, err := test.CommitBalanceTargets(t.Context(), snapshot, []*Group{&target})
	test.NoError(err)
	test.Empty(written)
	after, err := client.Get(t.Context(), key)
	test.NoError(err)
	test.Equal(before.Kvs[0].Value, after.Kvs[0].Value)
	test.Greater(after.Kvs[0].ModRevision, before.Kvs[0].ModRevision)
}

// TestCommitBalanceTargetsAfterLostReply checks a commit that succeeded but
// returned an error: the next plan, decided from the stale memory, is not
// written and refreshes the memory, so that the plan after it is written.
func TestCommitBalanceTargetsAfterLostReply(t *testing.T) {
	re := require.New(t)
	test := newTargetTest(t)
	m := test.Manager
	const id = "_tidb_ks1_pt_100_p1"
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
	lost := &lostReplyStorage{AffinityStorage: m.storage, lose: true}
	m.storage = lost
	commit := func(leader uint64) ([]string, error) {
		snapshot := balanceTestSnapshot(m)
		target := test.target(id, leader, 1, 2, 3)
		return m.CommitBalanceTargets(t.Context(), snapshot, []*Group{target.result()})
	}
	_, err := commit(1)
	re.Error(err)
	re.Equal(uint64(1), test.stored(id).LeaderStoreID)
	re.Zero(m.GetAffinityGroupState(id).LeaderStoreID)
	written, err := commit(2)
	re.NoError(err)
	re.Empty(written)
	re.Equal(uint64(1), m.GetAffinityGroupState(id).LeaderStoreID)
	written, err = commit(2)
	re.NoError(err)
	re.Equal([]string{id}, written)
	re.Equal(uint64(2), test.stored(id).LeaderStoreID)
}

type cancelTargetStorage struct {
	endpoint.AffinityStorage
	cancel       context.CancelFunc
	beforeCommit bool
	transactions int
}

func (s *cancelTargetStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	s.transactions++
	if s.beforeCommit {
		s.cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.AffinityStorage.RunInTxn(ctx, f)
	s.cancel()
	return err
}

func TestCommitBalanceTargetsCancellation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		beforeCommit bool
	}{{"between transactions", false}, {"in transaction", true}} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			test := newTargetTest(t)
			m, store := test.Manager, test.store
			targets, snapshot := balanceTestTargets(test, etcdutil.MaxEtcdTxnOps+1)
			first, last := targets[0].ID, targets[len(targets)-1].ID
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wrapped := &cancelTargetStorage{AffinityStorage: store, cancel: cancel, beforeCommit: tc.beforeCommit}
			m.storage = wrapped
			written, err := m.CommitBalanceTargets(ctx, snapshot, targets)
			re.ErrorIs(err, context.Canceled)
			re.NoError(m.ctx.Err())
			re.Equal(1, wrapped.transactions, "cancelled callers must not start another transaction")
			if tc.beforeCommit {
				re.Empty(written)
				re.Zero(test.stored(first).LeaderStoreID)
			} else {
				re.Equal([]string{first}, written)
				re.Equal(uint64(1), test.stored(first).LeaderStoreID)
				re.Equal(uint64(1), m.GetAffinityGroupState(first).LeaderStoreID)
			}
			re.Zero(test.stored(last).LeaderStoreID)
		})
	}
}

func TestCommitBalanceTargetsWriterChunks(t *testing.T) {
	for _, outcome := range []string{"success", "error", "partial", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			re := require.New(t)
			test := newTargetTest(t)
			m := test.Manager
			targets, snapshot := balanceTestTargets(test, etcdutil.MaxEtcdTxnOps+1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writeErr := errors.New("later chunk failed")
			calls := 0
			test.useWriter(func(gotCtx context.Context, batch []GroupTarget) (GroupTargetsResponse, error) {
				re.Equal(ctx, gotCtx)
				calls++
				re.Len(batch, []int{etcdutil.MaxEtcdTxnOps, 1}[calls-1])
				ids := make([]string, len(batch))
				for i, target := range batch {
					ids[i] = target.Expected.ID
				}
				if calls == 1 && outcome == "cancel" {
					cancel()
				}
				if calls == 2 {
					if outcome == "error" {
						return GroupTargetsResponse{}, writeErr
					}
					if outcome == "partial" {
						return GroupTargetsResponse{WrittenGroupIDs: ids}, writeErr
					}
				}
				return GroupTargetsResponse{WrittenGroupIDs: ids}, nil
			})
			written, err := m.CommitBalanceTargets(ctx, snapshot, targets)
			want := len(targets)
			switch outcome {
			case "success":
				re.NoError(err)
			case "cancel":
				re.ErrorIs(err, context.Canceled)
				re.Equal(1, calls)
				want--
			default:
				re.ErrorIs(err, writeErr)
				if outcome == "error" {
					want--
				}
			}
			re.Len(written, want)
			for i, id := range written {
				re.Equal(targets[i].ID, id)
			}
		})
	}
}

func TestBalanceTargetWriterFailureMetrics(t *testing.T) {
	for _, outcome := range []string{"error", "not-ready", "unsupported", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			re := require.New(t)
			test := newTargetTest(t)
			m := test.Manager
			id := "_tidb_ks1_pt_100_p1"
			re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
				switch outcome {
				case "not-ready":
					return GroupTargetsResponse{}, errors.Annotate(ErrTargetWriterNotReady, "no PD leader")
				case "unsupported":
					return GroupTargetsResponse{}, errors.Annotate(ErrTargetWriterUnsupported, "404 Not Found")
				case "cancel":
					cancel()
					return GroupTargetsResponse{}, context.Canceled
				default:
					return GroupTargetsResponse{}, errors.New("failed request")
				}
			})
			before := testutil.ToFloat64(targetWriteFailureCounter)
			target := test.target(id, 1, 1, 2)
			_, err := m.CommitBalanceTargets(ctx, balanceTestSnapshot(m), []*Group{target.result()})
			re.Error(err)
			want := before
			if outcome == "error" {
				want++
			}
			re.Equal(want, testutil.ToFloat64(targetWriteFailureCounter))
		})
	}
}

func TestUnpersistedTargetPolicyChangeAdvancesBalanceVersion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		test := newTargetTest(t)
		m := test.Manager
		const id = "_tidb_ks1_pt_100_p1"
		const table TableID = "_tidb_ks1_pt_100"
		r := keyutil.KeyRange{StartKey: balanceKey("a"), EndKey: balanceKey("b")}
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{r}}}))
		stored := test.stored(id)
		stored.BalancePolicy = BalancePolicyFixed
		m.SyncGroupFromEtcd(stored)
		m.applyStoreConditions(nil)
		test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
			return GroupTargetsResponse{}, ErrTargetWriterUnsupported
		})
		region := generateRegionForTest(1, []uint64{1, 2, 3}, r)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id))
		synctest.Wait()
		_, aligned := m.GetAndCacheRegionAffinityGroupState(region)
		re.True(aligned)
		before := m.GetAffinityGroupState(id)
		re.True(before.HasUnpersistedTarget())
		re.Equal(PhaseStable, before.Phase)
		re.Equal(1, before.RegionCount)
		re.Equal(1, before.AffinityRegionCount)
		version, exists := m.GetBalanceVersion(table)
		re.True(exists)

		stored.BalancePolicy = BalancePolicyAuto
		m.SyncGroupFromEtcd(stored)
		after := m.GetAffinityGroupState(id)
		want := *before
		want.BalancePolicy = BalancePolicyAuto
		re.Equal(want, *after)
		next, exists := m.GetBalanceVersion(table)
		re.True(exists)
		re.Greater(next, version, "the balancer must wake an idle table after the policy change")

		m.SyncGroupFromEtcd(stored)
		same, exists := m.GetBalanceVersion(table)
		re.True(exists)
		re.Equal(next, same)
		m.SyncGroupFromEtcd(&after.Group)
		re.False(m.GetAffinityGroupState(id).HasUnpersistedTarget())
		confirmed, exists := m.GetBalanceVersion(table)
		re.True(exists)
		re.Greater(confirmed, same, "confirming the local target changes the expected stored group")

		m.SyncGroupFromEtcdWithRevision(&after.Group, 10)
		revised, exists := m.GetBalanceVersion(table)
		re.True(exists)
		re.Greater(revised, confirmed, "the stored revision changed with identical group data")
		m.SyncGroupFromEtcdWithRevision(&after.Group, 10)
		unchanged, exists := m.GetBalanceVersion(table)
		re.True(exists)
		re.Equal(revised, unchanged)
	})
}

func TestSkippedPersistOnlyWritePreservesBalanceConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const id = "_tidb_ks1_pt_100_p1"
		test := newTargetTest(t, id)
		test.SyncGroupFromEtcdWithRevision(test.stored(id), 10)
		test.checkGroupsAvailability()
		state := func() *GroupState { return test.GetAffinityGroupState(id) }
		target := test.target(id, 1, 1, 2)
		releaseSkipped := make(chan struct{})
		defer close(releaseSkipped)
		test.useWriter(func(_ context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			test.Len(targets, 1)
			switch len(test.calls) {
			case 1:
				return GroupTargetsResponse{}, ErrTargetWriterUnsupported
			case 2:
				test.Equal(target.result(), targets[0].result())
				test.Equal(int64(10), targets[0].ExpectedModRevision)
				<-releaseSkipped
				return GroupTargetsResponse{}, nil
			default:
				test.Equal(target.result(), targets[0].result())
				return GroupTargetsResponse{
					WrittenGroupIDs: []string{id}, WrittenRevisions: map[string]int64{id: 11},
				}, nil
			}
		})
		test.observe(id, 1, 2)
		local := state()
		test.True(local.HasUnpersistedTarget())
		test.Equal(*target.result(), local.Group)

		time.Sleep(time.Second)
		test.observe(id, 1, 2)
		test.Len(test.calls, 2)
		written, err := test.CommitBalanceTargets(t.Context(), balanceTestSnapshot(test.Manager), []*Group{target.result()})
		test.NoError(err)
		test.Equal([]string{id}, written)
		test.Equal(int64(11), test.groups[id].confirmedRevision)
		test.Equal(local.affinityVer, state().affinityVer)
		test.Equal(int64(10), state().modRevision)

		releaseSkipped <- struct{}{}
		synctest.Wait()
		test.Equal(local, state(), "the skipped reply must preserve the confirmed local target")
	})
}

func TestCommitBalanceTargetsSkipsEmptyAndIneligibleTargets(t *testing.T) {
	for _, mode := range []string{"classic", "mcs"} {
		t.Run(mode, func(t *testing.T) {
			const id = "_tidb_ks1_pt_100_p1"
			test := newTargetTest(t, id)
			_, err := test.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
			test.NoError(err)
			if mode == "mcs" {
				test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
					t.Error("unexpected remote write")
					return GroupTargetsResponse{}, nil
				})
			}

			snapshot := balanceTestSnapshot(test.Manager)
			test.True(snapshot.Group(id).BalancePolicy.IsFixed())
			written, err := test.CommitBalanceTargets(t.Context(), snapshot, nil)
			test.NoError(err)
			test.Nil(written)
			written, err = test.CommitBalanceTargets(t.Context(), snapshot, []*Group{
				test.target(id, 1, 1, 2).result(),
				{ID: "_tidb_ks1_pt_100_p2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
			})
			test.NoError(err)
			test.Nil(written)
			test.Zero(test.GetAffinityGroupState(id).LeaderStoreID)
		})
	}
}
