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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const balanceTestTableID TableID = "_tidb_ks1_pt_100"
const balanceTestGroupID = "_tidb_ks1_pt_100_p1"

func (test *targetTest) balanceSnapshot() *BalanceSnapshot {
	return test.GetBalanceSnapshot(balanceTestTableID, test.GetBalanceCounts())
}

func (test *targetTest) groupTarget(id string, leader uint64, voters ...uint64) *Group {
	return test.target(id, leader, voters...).result()
}

func (test *targetTest) balanceTargets(n int) ([]*Group, []string, *BalanceSnapshot) {
	changes := make([]GroupKeyRanges, n)
	ids := make([]string, n)
	for i := range changes {
		ids[i] = fmt.Sprintf("%s_p%d", balanceTestTableID, i+1)
		changes[i].GroupID = ids[i]
	}
	test.NoError(test.CreateAffinityGroups(changes))
	snapshot := test.balanceSnapshot()
	targets := make([]*Group, n)
	for i, id := range ids {
		targets[i] = test.groupTarget(id, 1, 1, 2)
	}
	return targets, ids, snapshot
}

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
	test := newTargetTest(t)
	ids := []string{"_tidb_ks1_pt_100_p12", balanceTestGroupID, "_tidb_ks1_pt_101_p1", "custom"}
	var version uint64
	changed := func() {
		t.Helper()
		next, exists := test.GetBalanceVersion(balanceTestTableID)
		test.True(exists)
		test.Greater(next, version)
		version = next
	}
	test.False(test.IsBalanceTableEligible(balanceTestTableID))
	ranges := GroupKeyRanges{GroupID: ids[0], KeyRanges: testKeyRanges("a", "b")}
	test.NoError(test.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	test.checkGroupsAvailability()
	changed()
	test.False(test.IsBalanceTableEligible(balanceTestTableID))
	_, err := test.UpdateAffinityGroupPeers(ids[0], 1, []uint64{1, 2, 3})
	test.NoError(err)
	changed()
	// Region cache updates keep the version.
	test.GetAndCacheRegionAffinityGroupState(generateRegionForTest(1, []uint64{1, 2, 3}, ranges.KeyRanges[0]))
	test.Equal(version, test.balanceSnapshot().Version)
	_, err = test.UpdateAffinityGroupPolicy(ids[0], BalancePolicyFixed)
	test.NoError(err)
	changed()
	test.Equal(1, test.GetAffinityGroupState(ids[0]).AffinityRegionCount)
	synced := test.GetAffinityGroupState(ids[0]).Group
	synced.BalancePolicy = BalancePolicyAuto
	test.SyncGroupFromEtcd(&synced)
	changed()
	test.Equal(PhaseStable, test.GetAffinityGroupState(ids[0]).Phase)
	test.NoError(test.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: ids[0], KeyRanges: testKeyRanges("c", "d")}}, nil))
	changed()
	test.ExpireAffinityGroup(ids[0])
	changed()
	test.NoError(test.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[1]}, {GroupID: ids[2]}, {GroupID: ids[3]}}))
	changed()
	for _, id := range ids[1:] {
		_, err = test.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
		test.NoError(err)
	}
	changed()
	test.True(test.IsBalanceTableEligible(balanceTestTableID))

	// The snapshot has the groups of this table sorted by ID and the up TiKV stores.
	stores := test.storeSetInformer.(*core.StoresInfo)
	for _, store := range []*metapb.Store{
		{Id: 8, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
		{Id: 7, State: metapb.StoreState_Tombstone, NodeState: metapb.NodeState_Removed},
		{Id: 6, State: metapb.StoreState_Offline, NodeState: metapb.NodeState_Removing},
		{Id: 5, NodeState: metapb.NodeState_Preparing},
	} {
		stores.PutStore(core.NewStoreInfo(store))
	}
	counts := test.GetBalanceCounts()
	test.Equal(BalanceCounts{Leader: map[uint64]int{1: 4}, Voter: map[uint64]int{1: 4, 2: 4, 3: 4}}, counts)
	snapshot := test.GetBalanceSnapshot(balanceTestTableID, counts)
	test.Len(snapshot.Groups, 2)
	test.Equal([]string{ids[1], ids[0]}, []string{snapshot.Groups[0].ID, snapshot.Groups[1].ID})
	test.Len(snapshot.Ranges, 2)
	test.Equal([]uint64{1, 2, 3, 4, 5}, snapshot.Stores)
	counts.Leader[4] = 7
	test.Equal(counts, snapshot.Counts)

	// Key buffers are immutable; the snapshot owns its range entries and voters.
	snapshot.Groups[0].VoterStoreIDs[0] = 4
	snapshot.Ranges[ids[0]].KeyRanges[0] = keyutil.KeyRange{StartKey: []byte("x"), EndKey: []byte("y")}
	test.Equal([]uint64{1, 2, 3}, test.GetAffinityGroupState(ids[1]).VoterStoreIDs)
	test.Equal(testKeyRanges("a", "b", "c", "d"), test.GetBalanceSnapshot(balanceTestTableID, counts).Ranges[ids[0]].KeyRanges)

	// A table whose groups are all fixed is not eligible. Deleting its last group
	// removes it, and creating the group again gives a larger version.
	for _, id := range ids[:2] {
		_, err = test.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
		test.NoError(err)
		changed()
	}
	test.False(test.IsBalanceTableEligible(balanceTestTableID))
	test.NoError(test.DeleteAffinityGroups(ids[1:2], true))
	changed()
	test.NoError(test.DeleteAffinityGroups(ids[:1], true))
	_, exists := test.GetBalanceVersion(balanceTestTableID)
	test.False(exists)
	test.Nil(test.balanceSnapshot())
	test.NoError(test.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[0]}}))
	changed()
}

func TestCommitBalanceTargets(t *testing.T) {
	writeErr := errors.New("writer unavailable")
	for _, scenario := range []string{"stored", "admin", "fixed", "recreated", "writer", "watched", "superseded", "error", "partial"} {
		t.Run(scenario, func(t *testing.T) {
			test := newTargetTest(t)
			const id = balanceTestGroupID
			expected := Group{ID: id, CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			test.NoError(test.store.SaveAffinityGroup(test.store, id, &expected))
			test.SyncGroupFromEtcd(&expected)
			test.checkGroupsAvailability()
			test.ExpireAffinityGroup(id)
			snapshot := test.balanceSnapshot()
			target := expected
			target.LeaderStoreID, target.VoterStoreIDs = 2, []uint64{1, 2, 4}
			stored, want, allowed := target, target, true
			wantWritten := []string{id}
			var wantErr error
			useWriter := false
			switch scenario {
			case "admin":
				_, err := test.UpdateAffinityGroupPeers(id, 3, []uint64{1, 3, 4})
				test.NoError(err)
			case "fixed":
				_, err := test.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
				test.NoError(err)
			case "recreated":
				test.NoError(test.DeleteAffinityGroups([]string{id}, true))
				test.NoError(test.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
				_, err := test.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
				test.NoError(err)
			case "watched":
				useWriter = true
			case "superseded":
				// The watcher delivers another target, which is used instead.
				useWriter = true
				want.LeaderStoreID, want.VoterStoreIDs = 4, []uint64{1, 3, 4}
			case "writer":
				useWriter, want, allowed = true, expected, false
			case "partial":
				useWriter, want, allowed, wantErr = true, expected, false, writeErr
			case "error":
				useWriter, want, allowed, wantWritten, wantErr = true, expected, false, nil, writeErr
			}
			if useWriter {
				stored = expected
				test.useWriter(func(_ context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
					test.Equal([]GroupTarget{{Expected: expected, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 4}}}, targets)
					if scenario == "watched" || scenario == "superseded" {
						test.SyncGroupFromEtcd(&want)
					}
					return GroupTargetsResponse{WrittenGroupIDs: wantWritten}, wantErr
				})
			} else if scenario != "stored" {
				current := test.GetAffinityGroupState(id)
				stored, want, allowed, wantWritten = current.Group, current.Group, current.AffinitySchedulingAllowed, nil
			}
			written, err := test.CommitBalanceTargets(t.Context(), snapshot, []*Group{&target})
			if wantErr != nil {
				test.ErrorIs(err, wantErr)
			} else {
				test.NoError(err)
			}
			test.ElementsMatch(wantWritten, written)
			state := test.GetAffinityGroupState(id)
			test.Equal(want, state.Group)
			test.Equal(allowed, state.AffinitySchedulingAllowed)
			test.Equal(stored, *test.stored(id))
			if scenario == "writer" || scenario == "partial" {
				// The reply only reports persisted IDs; the watcher publishes them.
				test.SyncGroupFromEtcd(&target)
				state = test.GetAffinityGroupState(id)
				test.Equal(target, state.Group)
				test.True(state.AffinitySchedulingAllowed)
			}
		})
	}
}

func TestCommitBalanceTargetsRejectsStaleRevision(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	const id = balanceTestGroupID
	test := newTargetTestWithStorage(t, t.Context(), storage.NewStorageWithEtcdBackend(client), id)
	_, err := test.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
	test.NoError(err)
	key := keypath.AffinityGroupPath(id)
	before, err := client.Get(t.Context(), key)
	test.NoError(err)
	test.Len(before.Kvs, 1)
	test.SyncGroupFromEtcdWithRevision(test.stored(id), before.Kvs[0].ModRevision)
	snapshot := test.balanceSnapshot()
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
	const id = balanceTestGroupID
	test := newTargetTest(t, id)
	test.storage = &lostReplyStorage{AffinityStorage: test.storage, lose: true}
	commit := func(leader uint64) ([]string, error) {
		return test.CommitBalanceTargets(t.Context(), test.balanceSnapshot(), []*Group{test.groupTarget(id, leader, 1, 2, 3)})
	}
	_, err := commit(1)
	test.Error(err)
	test.Equal(uint64(1), test.stored(id).LeaderStoreID)
	test.Zero(test.GetAffinityGroupState(id).LeaderStoreID)
	written, err := commit(2)
	test.NoError(err)
	test.Empty(written)
	test.Equal(uint64(1), test.GetAffinityGroupState(id).LeaderStoreID)
	written, err = commit(2)
	test.NoError(err)
	test.Equal([]string{id}, written)
	test.Equal(uint64(2), test.stored(id).LeaderStoreID)
}

func TestCommitBalanceTargetsCancellation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		beforeCommit bool
	}{{"between transactions", false}, {"in transaction", true}} {
		t.Run(tc.name, func(t *testing.T) {
			test := newTargetTest(t)
			targets, ids, snapshot := test.balanceTargets(etcdutil.MaxEtcdTxnOps + 1)
			first, last := ids[0], ids[len(ids)-1]
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			transactions := 0
			test.interceptTxns(func(txnCtx context.Context, apply func(kv.Txn) error) error {
				transactions++
				if tc.beforeCommit {
					cancel()
				}
				if err := txnCtx.Err(); err != nil {
					return err
				}
				err := test.store.RunInTxn(txnCtx, apply)
				cancel()
				return err
			})
			written, err := test.CommitBalanceTargets(ctx, snapshot, targets)
			test.ErrorIs(err, context.Canceled)
			test.NoError(test.ctx.Err())
			test.Equal(1, transactions, "cancelled callers must not start another transaction")
			if tc.beforeCommit {
				test.Empty(written)
				test.Zero(test.stored(first).LeaderStoreID)
			} else {
				test.Equal([]string{first}, written)
				test.Equal(uint64(1), test.stored(first).LeaderStoreID)
				test.Equal(uint64(1), test.GetAffinityGroupState(first).LeaderStoreID)
			}
			test.Zero(test.stored(last).LeaderStoreID)
		})
	}
}

func TestCommitBalanceTargetsWriterChunks(t *testing.T) {
	for _, outcome := range []string{"success", "error", "partial", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			test := newTargetTest(t)
			targets, allIDs, snapshot := test.balanceTargets(etcdutil.MaxEtcdTxnOps + 1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writeErr := errors.New("later chunk failed")
			calls := 0
			test.useWriter(func(gotCtx context.Context, batch []GroupTarget) (GroupTargetsResponse, error) {
				test.Equal(ctx, gotCtx)
				calls++
				chunkIDs := [][]string{allIDs[:etcdutil.MaxEtcdTxnOps], allIDs[etcdutil.MaxEtcdTxnOps:]}[calls-1]
				test.Len(batch, len(chunkIDs))
				for i, target := range batch {
					test.Equal(chunkIDs[i], target.Expected.ID)
				}
				if calls == 1 && outcome == "cancel" {
					cancel()
				}
				if calls == 2 {
					switch outcome {
					case "error":
						return GroupTargetsResponse{}, writeErr
					case "partial":
						return GroupTargetsResponse{WrittenGroupIDs: chunkIDs}, writeErr
					}
				}
				return GroupTargetsResponse{WrittenGroupIDs: chunkIDs}, nil
			})
			written, err := test.CommitBalanceTargets(ctx, snapshot, targets)
			want := len(targets)
			switch outcome {
			case "success":
				test.NoError(err)
			case "cancel":
				test.ErrorIs(err, context.Canceled)
				test.Equal(1, calls)
				want--
			default:
				test.ErrorIs(err, writeErr)
				if outcome == "error" {
					want--
				}
			}
			test.Equal(allIDs[:want], written)
		})
	}
}

func TestBalanceTargetWriterFailureMetrics(t *testing.T) {
	for outcome, writeErr := range map[string]error{
		"error":     errors.New("failed request"),
		"not-ready": errors.Annotate(ErrTargetWriterNotReady, "no PD leader"),
		"cancel":    context.Canceled,
	} {
		t.Run(outcome, func(t *testing.T) {
			test := newTargetTest(t, balanceTestGroupID)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
				if outcome == "cancel" {
					cancel()
				}
				return GroupTargetsResponse{}, writeErr
			})
			before := testutil.ToFloat64(targetWriteFailureCounter)
			_, err := test.CommitBalanceTargets(ctx, test.balanceSnapshot(), []*Group{test.groupTarget(balanceTestGroupID, 1, 1, 2)})
			test.Error(err)
			want := before
			if outcome == "error" {
				want++
			}
			test.Equal(want, testutil.ToFloat64(targetWriteFailureCounter))
		})
	}
}

func TestSkippedFirstWinWritePreservesBalanceConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const id = balanceTestGroupID
		test := newTargetTest(t, id)
		test.SyncGroupFromEtcdWithRevision(test.stored(id), 10)
		test.checkGroupsAvailability()
		target := test.groupTarget(id, 1, 1, 2)
		firstWin := test.target(id, 3, 3, 4)
		releaseSkipped := make(chan struct{})
		defer close(releaseSkipped)
		test.useWriter(func(_ context.Context, targets []GroupTarget) (GroupTargetsResponse, error) {
			test.Len(targets, 1)
			test.Equal(int64(10), targets[0].ExpectedModRevision)
			if len(test.calls) == 1 {
				test.Equal(firstWin.result(), targets[0].result())
				<-releaseSkipped
				return GroupTargetsResponse{}, nil
			}
			test.Equal(target, targets[0].result())
			return GroupTargetsResponse{
				WrittenGroupIDs: []string{id}, WrittenRevisions: map[string]int64{id: 11},
			}, nil
		})
		test.observe(id, 3, 4)
		test.Len(test.calls, 1)
		written, err := test.CommitBalanceTargets(t.Context(), test.balanceSnapshot(), []*Group{target})
		test.NoError(err)
		test.Equal([]string{id}, written)
		test.Equal(int64(11), test.groups[id].confirmedRevision)
		confirmed := test.GetAffinityGroupState(id)
		test.Equal(int64(10), confirmed.modRevision)
		test.False(confirmed.AffinitySchedulingAllowed)

		releaseSkipped <- struct{}{}
		synctest.Wait()
		test.Equal(confirmed, test.GetAffinityGroupState(id))
		test.Equal(int64(11), test.groups[id].confirmedRevision)
		test.True(test.groups[id].awaitingTarget)
		test.True(test.groups[id].lastSentWritten)
		test.SyncGroupFromEtcdWithRevision(target, 11)
		test.Equal(*target, test.GetAffinityGroupState(id).Group)
		test.True(test.GetAffinityGroupState(id).AffinitySchedulingAllowed)
	})
}

func TestCommitBalanceTargetsSkipsEmptyAndIneligibleTargets(t *testing.T) {
	const id = balanceTestGroupID
	test := newTargetTest(t, id)
	_, err := test.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
	test.NoError(err)
	snapshot := test.balanceSnapshot()
	test.True(snapshot.Group(id).BalancePolicy.IsFixed())
	ineligible := []*Group{
		test.groupTarget(id, 1, 1, 2),
		{ID: "_tidb_ks1_pt_100_p2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
	}
	for _, mcs := range []bool{false, true} {
		if mcs {
			test.useWriter(func(context.Context, []GroupTarget) (GroupTargetsResponse, error) {
				t.Error("unexpected remote write")
				return GroupTargetsResponse{}, nil
			})
		}
		for _, targets := range [][]*Group{nil, ineligible} {
			written, err := test.CommitBalanceTargets(t.Context(), snapshot, targets)
			test.NoError(err)
			test.Nil(written)
		}
		test.Zero(test.GetAffinityGroupState(id).LeaderStoreID)
	}
}
