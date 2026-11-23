// Copyright 2022 TiKV Project Authors.
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

package schedulers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockhbstream"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestEvictLeader(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add regions 1, 2, 3 with leaders in stores 1, 2, 3
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 2, 1)
	tc.AddLeaderRegion(3, 3, 1)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	re.True(sl.IsScheduleAllowed(tc))
	ops, _ := sl.Schedule(tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2, 3})
	re.False(ops[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 1, []uint64{2, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})))
	re.True(ops[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 2, []uint64{1, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})))
	exhaustTransferLeaderInLimit(t, tc, 2, 3)
	ops, _ = sl.Schedule(tc, false)
	re.Empty(ops)
	tc.SetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited)
	tc.ResetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited/time.Minute.Seconds())
	ops, _ = sl.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
}

func bindEvictStream(re *require.Assertions, oc *operator.Controller, r *core.RegionInfo) func() *pdpb.TransferLeader {
	stream := mockhbstream.NewHeartbeatStream()
	hbs := oc.GetHBStreams()
	hbs.BindStream(r.GetLeader().GetStoreId(), stream)
	// Binding and sending use independent queues. Wait for a round trip.
	testutil.Eventually(re, func() bool { hbs.SendMsg(r, &hbstream.Operation{}); return stream.Recv() != nil })
	return func() *pdpb.TransferLeader {
		var transfer *pdpb.TransferLeader
		testutil.Eventually(re, func() bool {
			if msg := stream.Recv(); msg != nil {
				transfer = msg.(*pdpb.RegionHeartbeatResponse).GetTransferLeader()
			}
			return transfer != nil
		})
		return transfer
	}
}

func TestEvictReadHotLeader(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest(true)
	defer cancel()
	for _, id := range []uint64{1, 2, 3} {
		tc.AddLeaderStore(id, 0)
	}
	tc.AddRegionWithReadInfo(10, 1, 100<<20, 10000, 10000, 10, []uint64{2, 3}, 32)
	region := tc.GetRegion(10)
	setRead := func(id, rate uint64) {
		end := uint64(time.Now().Unix())
		tc.PutStore(tc.GetStore(id), core.SetStoreStats(&pdpb.StoreStats{
			BytesRead: rate * 10, Interval: &pdpb.TimeInterval{StartTimestamp: end - 10, EndTimestamp: end},
		}), core.SetLastHeartbeatTS(time.Now()))
	}
	setRead(2, 40<<20)
	setRead(3, 1<<20)
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), nil)
	re.NoError(err)
	ids := []uint64{2, 3}
	pick := func(a, b uint64) uint64 {
		id, _ := pickEvictLeaderTarget(tc, oc, sl.GetName(), region, ids, a, b)
		return id
	}
	testutil.Eventually(re, func() bool { return pick(2, 3) == 3 })
	re.Equal(uint64(2), pick(2, 2))
	// A single 10 MiB/s arrival exceeds the measured 3 MiB/s gap. All
	// possible draw pairs retain their ordinary choice, even across passes.
	setRead(2, 4<<20)
	for _, a := range ids {
		for _, b := range ids {
			re.Equal(a, pick(a, b))
		}
	}
	// A fresh heartbeat may carry an old sample or none at all.
	for _, interval := range []*pdpb.TimeInterval{nil, {StartTimestamp: 10, EndTimestamp: 20}} {
		tc.PutStore(tc.GetStore(3), core.SetStoreStats(&pdpb.StoreStats{Interval: interval}))
		re.Equal(uint64(2), pick(2, 3))
	}
	setRead(3, 1<<20)
	// A busy statistics worker cannot hold up the eviction.
	setRead(2, 40<<20)
	entered, resume := make(chan struct{}), make(chan struct{})
	re.True(tc.CheckReadAsync(func(*statistics.HotPeerCache) { close(entered); <-resume }))
	<-entered
	func() {
		defer close(resume)
		done := make(chan uint64, 1)
		go func() { done <- pick(2, 3) }()
		select {
		case result := <-done:
			re.Equal(uint64(2), result)
		case <-time.After(time.Second):
			t.Fatal("eviction waited for the hot cache")
		}
	}()
	// Exercise real operator admission, heartbeat redispatch and timeout history.
	recv := bindEvictStream(re, oc, region)
	for _, lateReply := range []bool{true, false} {
		ops, _ := sl.Schedule(tc, false)
		re.Len(ops, 1)
		op := ops[0]
		re.True(oc.AddOperator(op))
		first := recv().GetPeer()
		duplicates, _ := sl.Schedule(tc, false)
		re.Empty(duplicates)
		for _, reason := range []string{operator.DispatchFromHeartBeat, operator.DispatchFromNotifierQueue} {
			oc.Dispatch(region, reason, nil)
			retry := recv()
			re.Equal(first, retry.GetPeer())
			re.Len(retry.GetPeers(), 2)
		}
		leader := first
		if !lateReply {
			// Both followers still look eligible to PD. Only the primary is silent.
			op.SetStatusReachTime(operator.STARTED, time.Now().Add(-time.Hour))
			oc.Dispatch(region, operator.DispatchFromHeartBeat, nil)
			re.Equal(operator.TIMEOUT, op.Status())
			ops, _ = sl.Schedule(tc, false)
			re.Len(ops, 1)
			op = ops[0]
			re.True(oc.AddOperator(op))
			alternate := recv()
			leader = alternate.GetPeer()
			re.NotEqual(first, leader)
			re.Empty(alternate.GetPeers())
		}
		moved := region.Clone(core.WithLeader(leader))
		tc.PutRegion(moved)
		oc.Dispatch(moved, operator.DispatchFromHeartBeat, nil)
		re.Equal(operator.SUCCESS, op.Status())
		tc.PutRegion(region)
	}
}

func TestEvictLeaderRespectsOverlappingPlacement(t *testing.T) {
	for _, role := range []placement.PeerRoleType{placement.Leader, placement.Follower} {
		t.Run(string(role), func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, oc := prepareSchedulersTest(true)
			defer cancel()
			tc.SetEnablePlacementRules(true)
			for id, zone := range map[uint64]string{1: "east", 2: "west", 3: "east"} {
				tc.AddLeaderStore(id, 0)
				tc.SetStoreLabel(id, map[string]string{"zone": zone})
			}
			tc.AddLeaderRegion(10, 1, 2, 3)
			sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), nil)
			re.NoError(err)
			// The running operator was created before the new rule.
			next := bindEvictStream(re, oc, tc.GetRegion(10))
			ops, _ := sl.Schedule(tc, false)
			re.Len(ops, 1)
			re.True(oc.AddOperator(ops[0]))
			re.Len(next().GetPeers(), 2)
			rm := tc.GetRuleManager()
			re.NoError(rm.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "others", Index: 1, Role: placement.Voter, Count: 2}))
			zone := "east"
			if role == placement.Follower {
				zone = "west"
			}
			re.NoError(rm.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: role, Count: 1,
				LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{zone}}}}))
			r := tc.GetRegion(10)
			oc.Dispatch(r, operator.DispatchFromHeartBeat, nil)
			cmd := next()
			re.Equal(uint64(3), cmd.GetPeer().GetStoreId())
			re.Equal([]*metapb.Peer{r.GetStorePeer(3)}, cmd.GetPeers())
			re.True(rm.FitRegionWithoutCache(tc, r.Clone(core.WithLeader(cmd.GetPeer()))).IsSatisfied())
			re.True(oc.RemoveOperator(ops[0]))
			ops, _ = sl.Schedule(tc, false)
			re.Len(ops, 1)
			operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{3})
		})
	}
}

func TestEvictLeaderWithUnhealthyPeer(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add region 1, which has 3 peers. 1 is leader. 2 is healthy or pending, 3 is healthy or down.
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.MockRegionInfo(1, 1, []uint64{2, 3}, nil, nil)
	withDownPeer := core.WithDownPeers([]*pdpb.PeerStats{{
		Peer:        region.GetPeers()[2],
		DownSeconds: 1000,
	}})
	withPendingPeer := core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[1]})

	// A witness must not be offered as the primary or as a fallback.
	tc.PutRegion(region.Clone(core.WithWitness(region.GetStorePeer(2).GetId())))
	ops, _ := sl.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{3})

	// only pending
	tc.PutRegion(region.Clone(withPendingPeer))
	ops, _ = sl.Schedule(tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{3})
	// only down
	tc.PutRegion(region.Clone(withDownPeer))
	ops, _ = sl.Schedule(tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	// pending + down
	tc.PutRegion(region.Clone(withPendingPeer, withDownPeer))
	ops, _ = sl.Schedule(tc, false)
	re.Empty(ops)
}

func TestConfigClone(t *testing.T) {
	re := require.New(t)

	emptyConf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]keyutil.KeyRange)}
	con2 := emptyConf.clone()
	re.Empty(con2.getKeyRangesByID(1))

	var err error
	con2.StoreIDWithRanges[1], err = getKeyRanges([]string{"a", "b", "c", "d"})
	re.NoError(err)
	con3 := con2.clone()
	re.Len(con3.getRanges(1), len(con2.getRanges(1)))

	con3.StoreIDWithRanges[1][0].StartKey = []byte("aaa")
	con4 := con3.clone()
	re.True(bytes.Equal(con4.StoreIDWithRanges[1][0].StartKey, con3.StoreIDWithRanges[1][0].StartKey))

	con4.Batch = 10
	con5 := con4.clone()
	re.Equal(con5.getBatch(), con4.getBatch())
}

func TestBatchEvict(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// the random might be the same, so we add 1000 regions to make sure the batch is full
	for i := 1; i <= 10000; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 2, 3)
	}
	tc.AddLeaderRegion(6, 2, 1, 3)
	tc.AddLeaderRegion(7, 3, 1, 2)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	re.True(sl.IsScheduleAllowed(tc))
	testutil.Eventually(re, func() bool {
		ops, _ := sl.Schedule(tc, false)
		return len(ops) == 3
	})
	sl.(*evictLeaderScheduler).conf.Batch = 5
	testutil.Eventually(re, func() bool {
		ops, _ := sl.Schedule(tc, false)
		return len(ops) == 5
	})
	sl.(*evictLeaderScheduler).conf.Batch = maxEvictLeaderBatchSize
	testutil.Eventually(re, func() bool {
		ops, _ := sl.Schedule(tc, false)
		return len(ops) == maxEvictLeaderBatchSize
	})
}

func TestEvictLeaderBatchLimit(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)

	body, err := json.Marshal(map[string]any{"batch": maxEvictLeaderBatchSize})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusOK, resp.Code)
	re.Equal(maxEvictLeaderBatchSize, sl.(*evictLeaderScheduler).conf.getBatch())

	body, err = json.Marshal(map[string]any{"batch": maxEvictLeaderBatchSize + 1})
	re.NoError(err)
	req = httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp = httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Equal(maxEvictLeaderBatchSize, sl.(*evictLeaderScheduler).conf.getBatch())

	body, err = json.Marshal(map[string]any{"batch": 1.5})
	re.NoError(err)
	req = httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp = httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Equal(maxEvictLeaderBatchSize, sl.(*evictLeaderScheduler).conf.getBatch())

	body, err = json.Marshal(map[string]any{"batch": "abc"})
	re.NoError(err)
	req = httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp = httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Equal("\"invalid argument for 'batch': expected a number, got string\"\n", resp.Body.String())
	re.Equal(maxEvictLeaderBatchSize, sl.(*evictLeaderScheduler).conf.getBatch())
}

func TestEvictLeaderInvalidBatchKeepsLeaderTransferState(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	re.NoError(sl.PrepareConfig(tc))
	re.False(tc.GetStore(1).AllowLeaderTransferIn())

	body, err := json.Marshal(map[string]any{"store_id": 1, "batch": "abc"})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Equal("\"invalid argument for 'batch': expected a number, got string\"\n", resp.Body.String())
	re.False(tc.GetStore(1).AllowLeaderTransferIn())

	body, err = json.Marshal(map[string]any{"store_id": 2, "batch": "abc"})
	re.NoError(err)
	req = httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp = httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Equal("\"invalid argument for 'batch': expected a number, got string\"\n", resp.Body.String())
	re.True(tc.GetStore(2).AllowLeaderTransferIn())
}

func TestEvictLeaderUpdateConfigStoreIDsValidation(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)

	post := func(input map[string]any) *httptest.ResponseRecorder {
		body, err := json.Marshal(input)
		re.NoError(err)
		req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
		resp := httptest.NewRecorder()
		sl.ServeHTTP(resp, req)
		return resp
	}

	// store_id and store_ids are mutually exclusive.
	resp := post(map[string]any{"store_id": 1, "store_ids": []int{2}})
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Contains(resp.Body.String(), "only one of store_id and store_ids can be set")

	// fractional store ids must be rejected instead of silently truncated,
	// both as a single value and inside a store_ids array.
	resp = post(map[string]any{"store_id": 1.5})
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Contains(resp.Body.String(), "please input a right store id")

	resp = post(map[string]any{"store_ids": []float64{1, 2.5}})
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Contains(resp.Body.String(), "please input a right store id")

	// a JSON array of strings for "ranges" decodes as []any, not []string;
	// it must still be accepted and applied.
	resp = post(map[string]any{"store_ids": []int{2}, "ranges": []string{"a", "b"}})
	re.Equal(http.StatusOK, resp.Code)
	conf := sl.(*evictLeaderScheduler).conf
	re.Equal([]keyutil.KeyRange{keyutil.NewKeyRange("a", "b")}, conf.StoreIDWithRanges[2])
}

func TestEvictLeaderUpdateConfigBatchPreservesExistingRanges(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)

	// Store 1 is created with a custom (non-default) range.
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1", "a", "b"}), func(string) error { return nil })
	re.NoError(err)
	conf := sl.(*evictLeaderScheduler).conf
	customRanges := conf.StoreIDWithRanges[1]
	re.Equal([]keyutil.KeyRange{keyutil.NewKeyRange("a", "b")}, customRanges)

	// Batch-adding store 2 alongside the already-existing store 1, without
	// specifying ranges, must leave store 1's custom ranges untouched and
	// only default store 2 to the whole key space.
	body, err := json.Marshal(map[string]any{"store_ids": []int{1, 2}})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusOK, resp.Code)

	re.Equal(customRanges, conf.StoreIDWithRanges[1])
	re.Equal([]keyutil.KeyRange{keyutil.NewKeyRange("", "")}, conf.StoreIDWithRanges[2])
}

func TestEvictLeaderUpdateConfigBatchCopiesExplicitRangesPerStore(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	conf := sl.(*evictLeaderScheduler).conf

	body, err := json.Marshal(map[string]any{"store_ids": []int{1, 2}, "ranges": []string{"c", "d"}})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusOK, resp.Code)

	expected := []keyutil.KeyRange{keyutil.NewKeyRange("c", "d")}
	re.Equal(expected, conf.StoreIDWithRanges[1])
	re.Equal(expected, conf.StoreIDWithRanges[2])

	// Each store must own an independent backing array: mutating one
	// store's range must not affect the other.
	conf.StoreIDWithRanges[1][0].StartKey = []byte("zzz")
	re.Equal([]byte("c"), conf.StoreIDWithRanges[2][0].StartKey)
}

func TestEvictLeaderUpdateConfigEmptyRangesPreservesExisting(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1", "a", "b"}), func(string) error { return nil })
	re.NoError(err)
	conf := sl.(*evictLeaderScheduler).conf
	customRanges := conf.StoreIDWithRanges[1]
	re.Equal([]keyutil.KeyRange{keyutil.NewKeyRange("a", "b")}, customRanges)

	// An empty "ranges" array carries no range pairs; it must be treated
	// like an omitted field rather than resetting existing stores to the
	// whole key space.
	body, err := json.Marshal(map[string]any{"store_ids": []int{1, 2}, "ranges": []string{}})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusOK, resp.Code)

	re.Equal(customRanges, conf.StoreIDWithRanges[1])
	re.Equal([]keyutil.KeyRange{keyutil.NewKeyRange("", "")}, conf.StoreIDWithRanges[2])
}

func TestEvictLeaderUpdateConfigBatchRollbackPreservesExisting(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1", "a", "b"}), func(string) error { return nil })
	re.NoError(err)
	re.NoError(sl.PrepareConfig(tc))
	conf := sl.(*evictLeaderScheduler).conf
	prevRanges := conf.StoreIDWithRanges[1]
	prevBatch := conf.getBatch()
	re.False(tc.GetStore(1).AllowLeaderTransferIn())
	re.True(tc.GetStore(2).AllowLeaderTransferIn())

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail"))
	}()

	body, err := json.Marshal(map[string]any{"store_ids": []int{1, 2}, "batch": prevBatch + 1})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)

	// Store 1 (already existed) keeps its old ranges; store 2 (newly added
	// by this failed call) is removed entirely and its leader transfer is
	// resumed; Batch is restored to its pre-call value.
	re.Equal(prevRanges, conf.StoreIDWithRanges[1])
	re.NotContains(conf.StoreIDWithRanges, uint64(2))
	re.Equal(prevBatch, conf.getBatch())
	re.False(tc.GetStore(1).AllowLeaderTransferIn())
	re.True(tc.GetStore(2).AllowLeaderTransferIn())
}

// TestEvictLeaderUpdateConfigBatchRollbackOnMidLoopPauseFailure covers a
// batch that fails partway through the id loop, rather than at the final
// save: store 999 doesn't exist, so pausing it fails after store 1 (already
// existing) and store 2 (new) have already been applied to StoreIDWithRanges
// in memory. Both must still be rolled back exactly as if the failure had
// happened at save time.
func TestEvictLeaderUpdateConfigBatchRollbackOnMidLoopPauseFailure(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	// Store 999 is intentionally never registered, so pausing it fails.

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1", "a", "b"}), func(string) error { return nil })
	re.NoError(err)
	re.NoError(sl.PrepareConfig(tc))
	conf := sl.(*evictLeaderScheduler).conf
	prevRanges := conf.StoreIDWithRanges[1]

	body, err := json.Marshal(map[string]any{"store_ids": []int{1, 2, 999}, "ranges": []string{"c", "d"}})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)

	// Store 1's ranges must not be left overwritten by the explicit ranges
	// from this failed request, store 2 must not be left as a phantom
	// unpersisted entry, and store 2's leader transfer must be resumed.
	re.Equal(prevRanges, conf.StoreIDWithRanges[1])
	re.NotContains(conf.StoreIDWithRanges, uint64(2))
	re.NotContains(conf.StoreIDWithRanges, uint64(999))
	re.True(tc.GetStore(2).AllowLeaderTransferIn())
}

func TestEvictLeaderAddWaitingOperatorLimit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxWaiting int
		expected   int
	}{
		{name: "default-waiting-limit", maxWaiting: 0, expected: 5},
		{name: "batch-sized-waiting-limit", maxWaiting: maxEvictLeaderBatchSize, expected: maxEvictLeaderBatchSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cancel, opt, cluster, oc := prepareSchedulersTest(false)
			t.Cleanup(cancel)
			if tc.maxWaiting > 0 {
				cfg := opt.GetScheduleConfig().Clone()
				cfg.SchedulerMaxWaitingOperator = uint64(tc.maxWaiting)
				opt.SetScheduleConfig(cfg)
			}

			cluster.AddLeaderStore(1, 0)
			cluster.AddLeaderStore(2, 0)
			cluster.AddLeaderStore(3, 0)
			ops := newEvictLeaderOperators(t, cluster, maxEvictLeaderBatchSize)
			re.Equal(tc.expected, oc.AddWaitingOperator(ops...))
		})
	}
}

func newEvictLeaderOperators(tb testing.TB, cluster *mockcluster.Cluster, count int) []*operator.Operator {
	tb.Helper()
	ops := make([]*operator.Operator, 0, count)
	for i := range count {
		region := cluster.AddLeaderRegion(uint64(i+1), 1, 2, 3)
		op, err := operator.CreateTransferLeaderOperator(types.EvictLeaderScheduler.String(), cluster, region, 2, []uint64{2, 3}, operator.OpLeader)
		if err != nil {
			tb.Fatal(err)
		}
		ops = append(ops, op)
	}
	return ops
}

func BenchmarkEvictLeaderScheduleBatch(b *testing.B) {
	tc, _, sl := prepareEvictLeaderBatchScenario(b, 0)

	totalOps := 0

	for b.Loop() {
		ops, _ := sl.Schedule(tc, false)
		if len(ops) == 0 {
			b.Fatal("expected evict leader operators")
		}
		totalOps += len(ops)
	}
	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(totalOps)/elapsed, "ops/s")
	b.ReportMetric(float64(totalOps)*60/elapsed, "ops/min")
}

func prepareEvictLeaderBatchScenario(tb testing.TB, maxWaiting int) (*mockcluster.Cluster, *operator.Controller, Scheduler) {
	restoreLogger := log.ReplaceGlobals(zap.NewNop(), nil)
	tb.Cleanup(restoreLogger)

	cancel, opt, tc, oc := prepareSchedulersTest(false)
	tb.Cleanup(cancel)
	if maxWaiting > 0 {
		cfg := opt.GetScheduleConfig().Clone()
		cfg.SchedulerMaxWaitingOperator = uint64(maxWaiting)
		opt.SetScheduleConfig(cfg)
	}

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	regionCount := maxEvictLeaderBatchSize * 100
	for i := range regionCount {
		tc.AddLeaderRegion(uint64(i+1), 1, 2, 3)
	}

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	if err != nil {
		tb.Fatal(err)
	}
	sl.(*evictLeaderScheduler).conf.Batch = maxEvictLeaderBatchSize
	return tc, oc, sl
}

func TestEvictLeaderSchedulerCompatibility(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	saveConf := &evictLeaderSchedulerConfig{
		StoreIDWithRanges: map[uint64][]keyutil.KeyRange{
			1: {keyutil.KeyRange{StartKey: []byte(""), EndKey: []byte("")}},
		},
	}

	configJSON, err := json.Marshal(saveConf)
	re.NoError(err)

	// Save the serialized config to storage
	err = tc.GetStorage().SaveSchedulerConfig(string(types.EvictLeaderScheduler), configJSON)
	re.NoError(err)

	scheduleNames, configs, err := tc.GetStorage().LoadAllSchedulerConfigs()
	re.NoError(err)
	re.Len(scheduleNames, 1)
	data := configs[0]
	es, err := CreateScheduler(types.EvictLeaderScheduler, oc, tc.GetStorage(), ConfigJSONDecoder([]byte(data)), func(string) error { return nil })
	re.NoError(err)
	re.Equal(3, es.(*evictLeaderScheduler).conf.Batch)
	re.NotEmpty(es.(*evictLeaderScheduler).conf.StoreIDWithRanges[1])
}

func TestEvictLeaderMultiStoreCreationArgs(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	// EvictLeaderMultiStoreArgs' output decodes to every store, each
	// defaulted to the whole key space, and the plain single-store-id
	// decode path is left untouched.
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{EvictLeaderMultiStoreArgs([]uint64{1, 2, 3})}),
		func(string) error { return nil })
	re.NoError(err)
	conf := sl.(*evictLeaderScheduler).conf
	re.Len(conf.StoreIDWithRanges, 3)
	for _, id := range []uint64{1, 2, 3} {
		re.Equal([]keyutil.KeyRange{keyutil.NewKeyRange("", "")}, conf.StoreIDWithRanges[id])
	}

	// A single store ID still decodes on the old, single-store path (no
	// comma), not the multi-store one.
	sl, err = CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	conf = sl.(*evictLeaderScheduler).conf
	re.Equal(map[uint64][]keyutil.KeyRange{1: {keyutil.NewKeyRange("", "")}}, conf.StoreIDWithRanges)

	// An invalid ID inside the list is rejected, same as a single invalid ID.
	_, err = CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1,x,3"}), func(string) error { return nil })
	re.Error(err)

	// Ranges aren't supported alongside a multi-store arg list.
	_, err = CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1,2", "a", "b"}), func(string) error { return nil })
	re.Error(err)
}

func TestEvictLeaderDeleteWithFailedCallback(t *testing.T) {
	re := require.New(t)

	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	// When deleting the last store, if removeSchedulerCb fails,
	// the scheduler should be able to rollback correctly.
	callbackIsCalled := false
	removeSchedulerCb := func(string) error {
		callbackIsCalled = true
		// Simulate removeSchedulerCb failure
		return errors.New("failed to remove scheduler")
	}

	// Create scheduler with one store
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), removeSchedulerCb)
	re.NoError(err)
	conf := sl.(*evictLeaderScheduler).conf

	// Verify the store is in the config
	re.Contains(conf.StoreIDWithRanges, uint64(1))
	keyRanges := conf.StoreIDWithRanges[1]
	re.NotNil(keyRanges)

	// Try to delete the last store, which should trigger removeSchedulerCb
	resp, err := conf.delete(1)
	re.Error(err)
	re.Contains(err.Error(), "failed to remove scheduler")
	re.True(callbackIsCalled)

	re.Contains(conf.StoreIDWithRanges, uint64(1), "store should be restored after failed delete")
	re.Equal(keyRanges, conf.StoreIDWithRanges[1], "key ranges should be restored")
	re.Empty(resp)
}

func TestEvictLeaderDeleteWithSaveFailure(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Enable failpoint to simulate save failure
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail"))
	}()

	removeSchedulerCb := func(string) error {
		return nil
	}

	// Create scheduler with two stores
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, tc.GetStorage(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), removeSchedulerCb)
	re.NoError(err)
	conf := sl.(*evictLeaderScheduler).conf

	// Add another store
	ranges, _ := getKeyRanges([]string{"", ""})
	conf.resetStore(2, ranges)

	// Verify both stores are in the config
	re.Contains(conf.StoreIDWithRanges, uint64(1))
	re.Contains(conf.StoreIDWithRanges, uint64(2))
	keyRanges := conf.StoreIDWithRanges[1]

	// Try to delete store 1, save should fail
	resp, err := conf.delete(1)
	re.Error(err)
	re.Contains(err.Error(), "fail to persist")

	// After failed save, store 1 should be restored
	re.Contains(conf.StoreIDWithRanges, uint64(1), "store should be restored after save failure")
	re.Equal(keyRanges, conf.StoreIDWithRanges[1], "key ranges should be restored")
	re.Empty(resp)
}

// TestControllerAddSchedulerRollbackOnSaveFailure covers the non-microservice
// creation path: Controller.AddScheduler must undo PrepareConfig's already-
// applied leader-transfer pause when the following SaveSchedulerConfig fails,
// and must not leave the scheduler registered or running.
func TestControllerAddSchedulerRollbackOnSaveFailure(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.AddLeaderStore(1, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)

	c := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail"))
	}()

	re.Error(c.AddScheduler(sl, "1"))

	exist, _ := c.IsSchedulerExisted(sl.GetName())
	re.False(exist, "a scheduler must not be registered when its creation fails to persist")
	re.True(tc.GetStore(1).AllowLeaderTransferIn(), "the leader-transfer pause applied by PrepareConfig must be undone")
}

// TestControllerAddSchedulerHandlerRollbackOnPrepareFailure covers the
// microservice creation path: Controller.AddSchedulerHandler must remove the
// scheduler config it just saved, and undo PrepareConfig's already-applied
// leader-transfer pause, when PrepareConfig itself fails on a genuinely new
// scheduler (no config existed for it before this call).
func TestControllerAddSchedulerHandlerRollbackOnPrepareFailure(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	// Store 999 is intentionally never registered, so PrepareConfig
	// (pauseLeaderTransfer) fails on it after store 1 has already been
	// paused.
	tc.AddLeaderStore(1, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1,999"}), func(string) error { return nil })
	re.NoError(err)

	testStorage := storage.NewStorageWithMemoryBackend()
	c := NewController(context.Background(), tc, testStorage, oc)

	re.Error(c.AddSchedulerHandler(sl, "1,999"))

	exist, _ := c.IsSchedulerExisted(sl.GetName())
	re.False(exist, "a scheduler must not be registered when PrepareConfig fails")
	re.True(tc.GetStore(1).AllowLeaderTransferIn(), "the leader-transfer pause applied by PrepareConfig must be undone")
	cfg, err := testStorage.LoadSchedulerConfig(sl.GetName())
	re.NoError(err)
	re.Empty(cfg, "the config saved by this call before the failed PrepareConfig must be removed, since nothing existed before it")
}

// TestControllerAddSchedulerHandlerPreservesExistingConfigOnPrepareFailure
// covers the restore path (InitSchedulers): when AddSchedulerHandler is
// called for a scheduler whose config was already persisted before this
// call (e.g. accumulated across earlier config updates), a PrepareConfig
// failure must not delete that pre-existing config — only a config this
// call itself created is an orphan safe to remove.
func TestControllerAddSchedulerHandlerPreservesExistingConfigOnPrepareFailure(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	// Store 999 is intentionally never registered, so PrepareConfig
	// (pauseLeaderTransfer) fails on it after store 1 has already been
	// paused.
	tc.AddLeaderStore(1, 0)

	testStorage := storage.NewStorageWithMemoryBackend()

	// Simulate a config that already existed before this call, e.g.
	// restored from a previous run or accumulated via config updates.
	preexisting, err := CreateScheduler(types.EvictLeaderScheduler, oc, testStorage,
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1,999"}), func(string) error { return nil })
	re.NoError(err)
	re.NoError(SaveSchedulerConfig(testStorage, preexisting))
	existingCfg, err := testStorage.LoadSchedulerConfig(preexisting.GetName())
	re.NoError(err)
	re.NotEmpty(existingCfg)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, testStorage,
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1,999"}), func(string) error { return nil })
	re.NoError(err)
	c := NewController(context.Background(), tc, testStorage, oc)

	re.Error(c.AddSchedulerHandler(sl, "1,999"))

	exist, _ := c.IsSchedulerExisted(sl.GetName())
	re.False(exist, "a scheduler must not be registered when PrepareConfig fails")
	re.True(tc.GetStore(1).AllowLeaderTransferIn(), "the leader-transfer pause applied by PrepareConfig must be undone")
	cfg, err := testStorage.LoadSchedulerConfig(sl.GetName())
	re.NoError(err)
	re.Equal(existingCfg, cfg, "a config that existed before this call must survive a failed PrepareConfig")
}

// TestControllerAddSchedulerRollbackOnPrepareFailure covers the
// non-microservice creation path when PrepareConfig itself fails (rather
// than the later SaveSchedulerConfig): AddScheduler must still undo
// PrepareConfig's already-applied leader-transfer pause and must not leave
// the scheduler registered or running.
func TestControllerAddSchedulerRollbackOnPrepareFailure(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	// Store 999 is intentionally never registered, so PrepareConfig
	// (pauseLeaderTransfer) fails on it after store 1 has already been
	// paused.
	tc.AddLeaderStore(1, 0)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1,999"}), func(string) error { return nil })
	re.NoError(err)

	c := NewController(context.Background(), tc, storage.NewStorageWithMemoryBackend(), oc)

	re.Error(c.AddScheduler(sl, "1,999"))

	exist, _ := c.IsSchedulerExisted(sl.GetName())
	re.False(exist, "a scheduler must not be registered when PrepareConfig fails")
	re.True(tc.GetStore(1).AllowLeaderTransferIn(), "the leader-transfer pause applied by PrepareConfig must be undone")
}
