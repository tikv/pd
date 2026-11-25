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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
	pdtestutil "github.com/tikv/pd/pkg/utils/testutil"
)

var targetTestRange = pdtestutil.TxnKeyspaceRange(nonOverlappingRange.StartKey, nonOverlappingRange.EndKey)

func newTargetTestManager(t *testing.T, ctx context.Context, store storage.Storage) *Manager {
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving},
			core.SetLastHeartbeatTS(time.Now())))
	}
	labels, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	require.NoError(t, err)
	manager, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), labels)
	require.NoError(t, err)
	return manager
}

type targetTest struct {
	*require.Assertions
	*Manager
	store storage.Storage
	calls []GroupTarget
	reply func(context.Context, []GroupTarget) ([]string, error)
}

func newTargetTest(t *testing.T, ids ...string) *targetTest {
	return newTargetTestWithStorage(t, t.Context(), storage.NewStorageWithMemoryBackend(), ids...)
}

func newTargetTestWithStorage(t *testing.T, ctx context.Context, store storage.Storage, ids ...string) *targetTest {
	test := &targetTest{Assertions: require.New(t), Manager: newTargetTestManager(t, ctx, store), store: store}
	ranges := make([]GroupKeyRanges, len(ids))
	for i, id := range ids {
		ranges[i].GroupID = id
	}
	if len(ranges) > 0 {
		test.NoError(test.CreateAffinityGroups(ranges))
	}
	return test
}

func (test *targetTest) target(id string, leader uint64, voters ...uint64) GroupTarget {
	return GroupTarget{Expected: test.GetAffinityGroupState(id).Group, LeaderStoreID: leader, VoterStoreIDs: voters}
}

func (test *targetTest) stored(id string) *Group {
	value, err := test.store.LoadAffinityGroup(id)
	test.NoError(err)
	group := &Group{}
	test.NoError(json.Unmarshal([]byte(value), group))
	return group
}

func (test *targetTest) write(want []string, targets ...GroupTarget) {
	written, err := test.UpdateAffinityGroupTargets(targets)
	test.NoError(err)
	test.Equal(want, written)
}

func (test *targetTest) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	test.calls = append(test.calls, targets...)
	return test.reply(ctx, targets)
}

func (test *targetTest) observe(id string, voters ...uint64) {
	test.ObserveAvailableRegion(targetRegion(voters...), test.GetAffinityGroupState(id))
	synctest.Wait()
}

func targetRegion(voters ...uint64) *core.RegionInfo {
	return generateRegionForTest(10, voters, targetTestRange)
}

func TestUpdateAffinityGroupTargetsValidation(t *testing.T) {
	test := newTargetTest(t, "g", "h")
	valid := test.target("g", 1, 1, 2)
	negativeRevision := valid
	negativeRevision.ExpectedModRevision = -1
	for name, targets := range map[string][]GroupTarget{
		"empty":             nil,
		"duplicate":         {valid, test.target("g", 2, 1, 2)},
		"missing peers":     {test.target("g", 0)},
		"negative revision": {negativeRevision},
		"invalid second group": {test.target("h", 1, 1, 2),
			{Expected: Group{ID: "bad id"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := test.UpdateAffinityGroupTargets(targets)
			require.Error(t, err)
		})
	}
	for _, id := range []string{"g", "h"} {
		test.Zero(test.stored(id).LeaderStoreID, "validation must precede all writes")
	}

	test.applyStoreConditions(map[uint64]storeCondition{4: storeDown})
	stale := valid
	stale.Expected.CreateTimestamp++
	for _, skipped := range []GroupTarget{
		{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}},
		stale,
		test.target("g", 1, 1, 4),
	} {
		test.write([]string{"h"}, skipped, test.target("h", 2, 3, 2, 1))
	}
	test.Zero(test.stored("g").LeaderStoreID)
	test.Equal([]uint64{1, 2, 3}, test.GetAffinityGroupState("h").VoterStoreIDs)
}

func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, source := range []string{"watcher", "conflict", "written"} {
		t.Run(source, func(t *testing.T) {
			test := newTargetTest(t)
			old := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			test.SyncGroupFromEtcdWithRevision(&old, 1)
			test.NoError(test.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{targetTestRange}}}, nil))
			test.RestoreAffinityGroup("g")
			_, cached := test.GetAndCacheRegionAffinityGroupState(targetRegion(1, 2, 3))
			test.True(cached)

			recreated := old
			recreated.CreateTimestamp++
			test.NoError(test.store.RunInTxn(t.Context(), func(txn kv.Txn) error {
				return test.store.SaveAffinityGroup(txn, "g", &recreated)
			}))
			target := test.target("g", 2, 1, 2, 3)
			switch source {
			case "watcher":
				test.SyncGroupFromEtcdWithRevision(&recreated, 2)
				test.SyncGroupFromEtcdWithRevision(&old, 1)
			case "conflict":
				test.write([]string{}, target)
			case "written":
				target.Expected = recreated
				test.write([]string{"g"}, target)
				recreated.LeaderStoreID = 2
			}
			state := test.GetAffinityGroupState("g")
			test.Equal(recreated, state.Group)
			test.Zero(state.RegionCount)
			test.Equal(1, state.RangeCount)
		})
	}
}

func TestFirstWinStoreConditions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		test := newTargetTest(t, "loaded", "empty")
		loaded := test.target("loaded", 1, 1, 2, 3)
		test.write([]string{"loaded"}, loaded)
		test.Manager = newTargetTestManager(t, t.Context(), test.store)
		state := test.GetAffinityGroupState
		test.SetTargetWriter(test)
		test.reply = func(_ context.Context, targets []GroupTarget) ([]string, error) {
			return test.UpdateAffinityGroupTargets(targets)
		}
		test.False(test.storeConditionsKnown)
		test.False(state("loaded").AffinitySchedulingAllowed)
		test.observe("loaded", 2, 1, 3)
		test.Empty(test.calls, "a loaded target waits for the first store check")
		test.Equal(loaded.result(), test.stored("loaded"))

		empty := test.target("empty", 2, 1, 2, 3)
		test.observe("empty", 2, 1, 3)
		test.Equal([]GroupTarget{empty}, test.calls)
		test.Equal(empty.result(), test.stored("empty"))
		test.Equal(groupDegraded, test.groups["empty"].GetAvailability())
		test.checkGroupsAvailability()
		test.True(state("loaded").AffinitySchedulingAllowed)
		test.True(state("empty").AffinitySchedulingAllowed)

		test.ExpireAffinityGroup("loaded")
		test.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
		loaded.LeaderStoreID = 2
		stored := loaded.result()
		test.SyncGroupFromEtcdWithRevision(stored, 2)
		degraded := state("loaded")
		test.Equal(*stored, degraded.Group)
		test.False(degraded.RegularSchedulingAllowed)
		test.False(degraded.AffinitySchedulingAllowed)
		deadline := newDegradedExpiredAtFromNow()
		test.Equal(deadline, test.groups["loaded"].degradedExpiredAt)
		time.Sleep(time.Second)
		test.SyncGroupFromEtcdWithRevision(stored, 3)
		test.Equal(degraded, state("loaded"))
		test.Equal(deadline, test.groups["loaded"].degradedExpiredAt)
	})
}

func TestFirstWinSingleFlightAndGlobalBackoff(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "failed", err: errors.New("PD unavailable")},
		{name: "not ready", err: fmt.Errorf("PD leader is not found: %w", ErrTargetWriterNotReady)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t, "g", "h", "i", "j")
				failures := testutil.ToFloat64(targetWriteFailureCounter)
				release := make(chan struct{})
				defer close(release)
				test.SetTargetWriter(test)
				test.reply = func(_ context.Context, targets []GroupTarget) ([]string, error) {
					if len(test.calls) == 1 {
						<-release
					}
					if len(targets) == 2 {
						<-release
						return nil, tc.err
					}
					return []string{targets[0].Expected.ID}, nil
				}
				g, h := test.target("g", 1, 1, 2), test.target("h", 1, 1, 2)
				i, j := test.target("i", 1, 1, 2), test.target("j", 1, 1, 2)
				test.observe("g", 1, 2)
				test.observe("h", 1, 2)
				test.observe("i", 1, 2)
				test.Equal([]GroupTarget{g}, test.calls, "only one request can be in flight")
				release <- struct{}{}
				synctest.Wait()
				test.Equal([]GroupTarget{g, h, i}, test.calls, "queued groups are sent together")
				test.observe("j", 1, 2)
				release <- struct{}{}
				synctest.Wait()
				test.Len(test.calls, 3, "failure drops pending observations")
				test.observe("j", 1, 2)
				test.Len(test.calls, 3, "failure delays all groups")
				if !errors.Is(tc.err, ErrTargetWriterNotReady) {
					failures += 2
				}
				test.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))

				time.Sleep(time.Second)
				test.observe("h", 1, 2)
				test.Zero(test.GetAffinityGroupState("h").LeaderStoreID, "HTTP success waits for the watcher")
				test.observe("j", 1, 2)
				test.Equal([]GroupTarget{g, h, i, h, j}, test.calls, "success does not delay other groups")
			})
		})
	}
}

func TestFirstWinSkippedGroupDoesNotBlockPatrol(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		skippedIDs := []string{"a"}
		for index := range 1023 {
			skippedIDs = append(skippedIDs, fmt.Sprintf("a%04d", index))
		}
		ids := append(skippedIDs, "b", "c")
		pd, test := newTargetTest(t, ids...), newTargetTest(t)
		for _, id := range ids {
			test.SyncGroupFromEtcd(pd.stored(id))
		}
		failures := testutil.ToFloat64(targetWriteFailureCounter)
		release := make(chan struct{})
		defer close(release)
		var skippedAt []time.Time
		var written []string
		test.SetTargetWriter(test)
		test.reply = func(_ context.Context, targets []GroupTarget) ([]string, error) {
			test.LessOrEqual(len(targets), 128)
			if targets[0].Expected.ID == "a" {
				skippedAt = append(skippedAt, time.Now())
				<-release
			}
			for len(targets) > 0 && targets[0].Expected.ID < "b" {
				targets = targets[1:]
			}
			if len(targets) == 0 {
				return nil, nil
			}
			ids, err := pd.UpdateAffinityGroupTargets(targets)
			written = append(written, ids...)
			return ids, err
		}

		b := test.target("b", 2, 2, 3)
		c := test.target("c", 3, 3, 4)
		want := make([]GroupTarget, 0, len(ids))
		for _, id := range skippedIDs {
			want = append(want, test.target(id, 1, 1, 2))
		}
		want = append(want, b, c)
		region := targetRegion(1, 2)
		for patrol := range 3 {
			start := time.Now()
			before := len(test.calls)
			test.observe("a", 1, 2)
			for _, id := range skippedIDs[1:] {
				test.ObserveAvailableRegion(region, test.GetAffinityGroupState(id))
			}
			test.observe("b", 1, 2)
			test.observe("b", 2, 3)
			test.observe("c", 3, 4)
			test.observe("a", 1, 2) // A repeat queued behind its in-flight request.
			test.Equal(want[:1], test.calls[before:])
			release <- struct{}{}
			synctest.Wait()

			test.Equal(start, time.Now(), "the queued groups drain without waiting for a cooldown")
			test.Equal([]string{"b", "c"}, written[patrol*2:])
			test.Len(test.calls[before:], len(want))
			test.Equal(want, test.calls[before:])
			test.Zero(pd.stored("a").LeaderStoreID)
			test.Equal(b.result(), pd.stored("b"))
			test.Equal(c.result(), pd.stored("c"))

			time.Sleep(time.Second - time.Millisecond)
			test.observe("a", 1, 2)
			test.observe("b", 2, 3)
			test.observe("c", 3, 4)
			test.Len(test.calls, before+len(want), "reobserved groups are still in cooldown")
			time.Sleep(time.Millisecond)
		}
		test.Len(skippedAt, 3)
		for i := 1; i < len(skippedAt); i++ {
			test.Equal(time.Second, skippedAt[i].Sub(skippedAt[i-1]))
		}
		test.Equal(failures+float64(3*len(skippedIDs)), testutil.ToFloat64(targetWriteFailureCounter))
	})
}

func TestFirstWinCancelsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		test := newTargetTestWithStorage(t, ctx, storage.NewStorageWithMemoryBackend(), "g")
		test.SetTargetWriter(test)
		stopped := false
		test.reply = func(ctx context.Context, _ []GroupTarget) ([]string, error) {
			<-ctx.Done()
			stopped = true
			return nil, ctx.Err()
		}
		test.observe("g", 1, 2)
		cancel()
		time.Sleep(time.Second)
		test.observe("g", 1, 2)
		test.True(stopped)
		test.Len(test.calls, 1)
	})
}

func TestFirstWinLateReply(t *testing.T) {
	for _, name := range []string{"new revision", "expired", "recreated in the same second", "store down"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				test := newTargetTest(t)
				group := Group{ID: "g", CreateTimestamp: 1}
				if name == "new revision" {
					group.LeaderStoreID, group.VoterStoreIDs = 2, []uint64{1, 2}
				}
				test.SyncGroupFromEtcdWithRevision(&group, 1)
				switch name {
				case "new revision":
					test.applyStoreConditions(map[uint64]storeCondition{2: storeDown})
				case "store down":
					test.checkGroupsAvailability()
				}
				before := test.GetAffinityGroupState("g")
				release := make(chan struct{})
				defer close(release)
				test.SetTargetWriter(test)
				test.reply = func(context.Context, []GroupTarget) ([]string, error) {
					<-release
					return nil, ErrTargetWriterUnsupported
				}
				test.observe("g", 1, 3)
				switch name {
				case "new revision":
					test.SyncGroupFromEtcdWithRevision(&group, 2)
					test.Equal(before, test.GetAffinityGroupState("g"), "a same-peers PUT changes only the revision")
				case "expired":
					test.ExpireAffinityGroup("g")
				case "recreated in the same second":
					test.SyncGroupDeleteFromEtcd("g")
					test.SyncGroupFromEtcdWithRevision(&group, 1)
					recreated := test.GetAffinityGroupState("g")
					test.NotSame(before.groupInfoPtr, recreated.groupInfoPtr)
					test.Equal(before.Group, recreated.Group)
					test.Equal(before.affinityVer, recreated.affinityVer)
				case "store down":
					test.applyStoreConditions(map[uint64]storeCondition{1: storeDown})
					test.Equal(before, test.GetAffinityGroupState("g"), "an unset group has no voters to invalidate")
				}
				want := test.GetAffinityGroupState("g")
				release <- struct{}{}
				synctest.Wait()
				test.Equal(want, test.GetAffinityGroupState("g"), "a late 404 must preserve the newer state")
				test.observe("g", 3, 4)
				test.True(test.GetAffinityGroupState("g").HasUnpersistedTarget())
				test.Len(test.calls, 1, "a fresh observation applies during the 404 cooldown")
			})
		})
	}
}

func TestFirstWinReconcilesLocalTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pd, test := newTargetTest(t, "g", "h"), newTargetTest(t)
		test.SyncGroupFromEtcd(pd.stored("g"))
		test.SyncGroupFromEtcd(pd.stored("h"))
		test.checkGroupsAvailability()
		test.SetTargetWriter(test)
		failures := testutil.ToFloat64(targetWriteFailureCounter)
		release := make(chan struct{})
		defer close(release)
		test.reply = func(_ context.Context, targets []GroupTarget) ([]string, error) {
			if len(test.calls) == 1 {
				<-release
			}
			if len(test.calls) <= 2 {
				return nil, fmt.Errorf("404 Not Found: %w", ErrTargetWriterUnsupported)
			}
			return pd.UpdateAffinityGroupTargets(targets)
		}
		target := test.target("g", 1, 1, 2)
		test.observe("g", 1, 2)
		test.observe("h", 2, 3)
		release <- struct{}{}
		synctest.Wait()
		local := test.GetAffinityGroupState("g")
		test.Equal(*target.result(), local.Group)
		test.True(local.HasUnpersistedTarget())
		test.True(local.AffinitySchedulingAllowed)
		test.Zero(pd.stored("g").LeaderStoreID)
		test.True(test.GetAffinityGroupState("h").HasUnpersistedTarget(), "the queued observation also falls back to a local target")
		test.observe("g", 4, 1)
		test.Len(test.calls, 1, "the cooldown allows local targets without sending HTTP requests")
		test.Equal(local, test.GetAffinityGroupState("g"))
		time.Sleep(time.Second)
		test.observe("g", 1, 2)
		test.Equal([]GroupTarget{target, target}, test.calls)
		test.Equal(local, test.GetAffinityGroupState("g"), "another 404 keeps the active target")
		test.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))

		time.Sleep(time.Second)
		test.observe("g", 1, 2)
		persisted := pd.stored("g")
		test.Equal(target.result(), persisted)
		test.Equal(local, test.GetAffinityGroupState("g"), "success waits for confirmation")
		test.SyncGroupFromEtcdWithRevision(persisted, 2)
		confirmed := test.GetAffinityGroupState("g")
		test.False(confirmed.HasUnpersistedTarget())
		test.Equal(local.affinityVer, confirmed.affinityVer)
		test.DegradeAffinityGroup("g")
		test.observe("g", 1, 2)
		test.True(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
		test.Len(test.calls, 3, "a matching observation restores the persisted target without writing")
		test.observe("h", 2, 3)
		test.Equal(uint64(2), pd.stored("h").LeaderStoreID, "a successful probe clears the cooldown")

		test.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
		test.True(test.GetAffinityGroupState("h").HasUnpersistedTarget())
		test.Equal(groupDegraded, test.groups["h"].GetAvailability())
		time.Sleep(time.Second)
		test.SyncGroupFromEtcdWithRevision(pd.stored("h"), 3)
		test.False(test.GetAffinityGroupState("h").HasUnpersistedTarget())
		test.Equal(groupDegraded, test.groups["h"].GetAvailability())
		test.Equal(newDegradedExpiredAtFromNow(), test.groups["h"].degradedExpiredAt)
	})
}

type targetReplyStorage struct {
	endpoint.AffinityStorage
	beforeCommit func()
	loseReply    bool
}

func (s *targetReplyStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	err := s.AffinityStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			s.beforeCommit()
		}
		return nil
	})
	if err == nil && s.loseReply {
		return errors.New("committed reply lost")
	}
	return err
}

func TestTargetCASAndLostReplyWithEtcd(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	test := newTargetTestWithStorage(t, t.Context(), store, "g", "h")
	other := newTargetTestManager(t, t.Context(), store)
	adminWrite := func(leader uint64) {
		_, err := other.UpdateAffinityGroupPeers("g", leader, []uint64{1, 2, 3})
		test.NoError(err)
	}
	read := func() *mvccpb.KeyValue {
		resp, err := client.Get(t.Context(), keypath.AffinityGroupPath("g"))
		test.NoError(err)
		test.Len(resp.Kvs, 1)
		return resp.Kvs[0]
	}
	stale := test.target("g", 1, 1, 2, 3)
	test.checkGroupsAvailability()
	before := test.GetAffinityGroupState("h").affinityVer
	commits := 0
	wrapped := &targetReplyStorage{AffinityStorage: store, beforeCommit: func() {
		commits++
		if commits == 2 {
			adminWrite(2)
		}
	}}
	test.storage = wrapped
	written, err := test.UpdateAffinityGroupTargets([]GroupTarget{test.target("h", 1, 1, 2, 3), stale})
	test.Error(err, "another Manager wrote after the read, before commit")
	test.Equal([]string{"h"}, written)
	test.Equal(uint64(1), test.stored("h").LeaderStoreID, "earlier commits survive a later error")
	test.Equal(before+1, test.GetAffinityGroupState("h").affinityVer)
	test.Equal(uint64(2), test.stored("g").LeaderStoreID)
	test.Zero(test.GetAffinityGroupState("g").LeaderStoreID)

	wrapped.beforeCommit = nil
	test.ExpireAffinityGroup("g")
	test.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
	test.write([]string{}, stale)
	test.Equal(uint64(2), test.GetAffinityGroupState("g").LeaderStoreID)
	test.False(test.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	test.False(test.GetAffinityGroupState("g").RegularSchedulingAllowed, "the reconciled target gets a degraded window")
	test.applyStoreConditions(nil)

	target := test.target("g", 3, 1, 2, 3)
	original := read()
	target.ExpectedModRevision = original.ModRevision
	adminWrite(1)
	adminWrite(2)
	restored := read()
	test.Equal(original.Value, restored.Value)
	test.Greater(restored.ModRevision, original.ModRevision)
	test.write([]string{}, target)
	test.Equal(restored.ModRevision, read().ModRevision, "a stale revision must reject ABA even when the group matches")

	target.ExpectedModRevision = restored.ModRevision
	wrapped.loseReply = true
	_, err = test.UpdateAffinityGroupTargets([]GroupTarget{target})
	test.ErrorContains(err, "committed reply lost")
	test.Equal(uint64(2), test.GetAffinityGroupState("g").LeaderStoreID)
	test.Equal(uint64(3), test.stored("g").LeaderStoreID)
	wrapped.loseReply = false
	beforeRetry := read()
	target.Expected.VoterStoreIDs = []uint64{3, 2, 1}
	test.write([]string{"g"}, target)
	test.Equal(*test.stored("g"), test.GetAffinityGroupState("g").Group)
	test.Equal(beforeRetry.ModRevision, read().ModRevision, "idempotent retries do not put again")

	test.NoError(test.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{targetTestRange}}}, nil))
	test.GetAndCacheRegionAffinityGroupState(targetRegion(3, 1, 2))
	stable := test.GetAffinityGroupState("g")
	test.Equal(1, stable.AffinityRegionCount)
	test.Equal(PhaseStable, stable.Phase)
	test.write([]string{"g"}, target)
	test.Equal(stable, test.GetAffinityGroupState("g"))
}
