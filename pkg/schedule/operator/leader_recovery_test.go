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

package operator

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/types"
)

type recoveryTest struct {
	*testing.T
	*Controller
	tc            *mockcluster.Cluster
	round         RecoveryRound
	now           int64
	storeInterval int64
}

func newRecoveryTest(t *testing.T) *recoveryTest {
	tc := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(t.Context(), tc.GetBasicCluster(), false)
	t.Cleanup(stream.Close)
	x := &recoveryTest{T: t, tc: tc, Controller: NewController(t.Context(), tc.GetBasicCluster(), tc.GetSharedConfig(), stream), now: time.Now().Unix(), storeInterval: 10}
	for _, s := range []uint64{1, 8, 12} {
		tc.AddLeaderStore(s, 1)
		x.store(s, x.now-120, 100)
	}
	x.round = x.CurrentRecoveryRound()
	x.round.ID = 1
	return x
}
func (x *recoveryTest) store(id uint64, start int64, rate uint64) {
	end := uint64(x.now)
	x.cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, StartTimestamp: start}, core.SetLastHeartbeatTS(time.Now()),
		core.SetStoreStats(&pdpb.StoreStats{Interval: &pdpb.TimeInterval{StartTimestamp: end - uint64(x.storeInterval), EndTimestamp: end}, KeysRead: rate * uint64(x.storeInterval)})))
}
func (x *recoveryTest) report(id, leader, term, rate uint64, end int64) *core.RegionInfo {
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 8, StoreId: 8}, {Id: 12, StoreId: 12}}
	r := core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{Region: &metapb.Region{Id: id, StartKey: []byte{byte(id)}, EndKey: []byte{byte(id + 1)}, Peers: peers,
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, Leader: &metapb.Peer{Id: leader, StoreId: leader}, Term: term, KeysRead: rate * 10, Interval: &pdpb.TimeInterval{StartTimestamp: uint64(end) - 10, EndTimestamp: uint64(end)}}, 0)
	x.cluster.CheckAndPutRegion(r)
	return r
}
func transfer(r *core.RegionInfo, desc string, to uint64, kind OpKind) *Operator {
	return NewOperator(desc, "", r.GetID(), r.GetRegionEpoch(), kind, 1, TransferLeader{FromStore: r.GetLeader().GetStoreId(), ToStore: to})
}
func (x *recoveryTest) finish(op *Operator, region *core.RegionInfo) {
	x.Helper()
	x.observeRecovery(op)
	require.True(x, op.Start())
	require.Nil(x, op.Check(region))
	x.finishRecovery(op, region)
}

func (x *recoveryTest) drain(store uint64) {
	x.Helper()
	require.NoError(x, x.UpdateRecoveryRound(x.round, store, x.now-120))
}
func (x *recoveryTest) evacuate(id, successor, rate uint64) *core.RegionInfo {
	x.Helper()
	r := x.report(id, 1, 7, rate, x.now-40)
	op := transfer(r, types.EvictLeaderScheduler.String(), successor, OpLeader)
	x.PlanEvictions(x.tc, []*Operator{op})
	require.True(x, x.AddOperator(op))
	r = x.report(id, successor, 8, rate, x.now-30)
	x.Dispatch(r, DispatchFromHeartBeat, func(uint64) {})
	require.Equal(x, SUCCESS, op.Status())
	return r
}

type gatedHeat struct {
	sche.SchedulerCluster
	checking, resume chan struct{}
}

func (g *gatedHeat) LeaderReturnEligibility(r []*core.RegionInfo, degree int) []bool {
	close(g.checking)
	<-g.resume
	return g.SchedulerCluster.LeaderReturnEligibility(r, degree)
}

func TestRecoveryRoundFencesRetriesAndDispatch(t *testing.T) {
	x := newRecoveryTest(t)
	x.evacuate(1, 12, 20)
	require.Empty(t, x.recovery.history, "untagged eviction")
	x.drain(1)
	x.drain(1)
	r := x.evacuate(2, 8, 40)
	x.store(1, x.now-20, 40)
	x.store(8, x.now-120, 100)
	ordinary := x.report(3, 8, 5, 10, x.now)
	op := transfer(r, types.BalanceLeaderScheduler.String(), 1, OpLeader)
	require.True(t, x.PreferReturn(x.tc, op, ordinary.GetID(), nil))
	gate := &gatedHeat{x.tc, make(chan struct{}), make(chan struct{})}
	op.recovery.cluster = gate
	result := make(chan bool, 1)
	go func() { result <- x.canSendRecovery(op, r) }()
	<-gate.checking
	closed := x.round
	closed.Finished = true
	require.NoError(t, x.UpdateRecoveryRound(closed, 0, 0))
	close(gate.resume)
	require.False(t, <-result, "close wins while first dispatch is checking hot peers")
	require.Empty(t, x.recovery.history)
	require.NoError(t, x.UpdateRecoveryRound(closed, 0, 0))
	require.Error(t, x.UpdateRecoveryRound(x.round, 1, x.now-120))
	next := x.round
	next.ID++
	require.NoError(t, x.UpdateRecoveryRound(next, 8, x.now-120))
	require.Error(t, x.UpdateRecoveryRound(closed, 0, 0))
	next.Epoch = "old primary"
	require.Error(t, x.UpdateRecoveryRound(next, 8, x.now-120))
}
func TestReturnRechecksSamplesAndIncomingHotOperator(t *testing.T) {
	x := newRecoveryTest(t)
	x.drain(1)
	r := x.evacuate(1, 8, 40)
	x.store(1, x.now-2, 0)
	ordinary := x.report(2, 8, 9, 10, x.now)
	op := transfer(r, types.BalanceLeaderScheduler.String(), 1, OpLeader)
	require.False(t, x.PreferReturn(x.tc, op, ordinary.GetID(), nil), "sample straddles restart")
	x.now += 15
	x.store(1, x.now-17, 40)
	x.store(8, x.now-135, 100)
	x.report(1, 8, 8, 0, x.now)
	require.Same(t, ordinary, x.BestReturn(x.tc, x.cluster.GetStore(8), x.cluster.GetStore(1), ordinary, nil, func(*core.RegionInfo) bool { return true }))
	r = x.report(1, 8, 8, 40, x.now)
	require.Same(t, r, x.BestReturn(x.tc, x.cluster.GetStore(8), x.cluster.GetStore(1), ordinary, nil, func(*core.RegionInfo) bool { return true }))
	require.True(t, x.PreferReturn(x.tc, op, ordinary.GetID(), nil))
	l := x.tc.GetRegionLabeler()
	require.NoError(t, l.SetLabelRule(&labeler.LabelRule{ID: "deny-return", RuleType: labeler.KeyRange,
		Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}}, Data: []any{map[string]any{"start_key": "01", "end_key": "02"}}}))
	require.False(t, x.canSendRecovery(op, r))
	require.NoError(t, l.DeleteLabelRule("deny-return"))
	x.report(2, 8, 9, 30, x.now)
	require.False(t, x.canSendRecovery(op, x.cluster.GetRegion(op.RegionID())), "default now equalizes reads")
	x.report(2, 8, 9, 10, x.now)
	hot := x.report(4, 12, 8, 80, x.now)
	in := NewOperator("move-hot-read-peer", "", hot.GetID(), hot.GetRegionEpoch(), OpHotRegion|OpRegion, 1, AddLearner{ToStore: 1}, TransferLeader{FromStore: 12, ToStore: 1})
	x.observeRecovery(in)
	require.False(t, x.canSendRecovery(op, x.cluster.GetRegion(op.RegionID())), "multi-step incoming Leader consumes receiver headroom")
	x.finishRecovery(in, nil)
	require.True(t, x.canSendRecovery(op, x.cluster.GetRegion(op.RegionID())))
	require.Same(t, ordinary, x.BestReturn(x.tc, x.cluster.GetStore(8), x.cluster.GetStore(1), ordinary, nil, func(*core.RegionInfo) bool { return true }))
}

// Both Regions came from the restarting Store. Read demand can switch between
// scheduling and sending the first command.
func TestBalanceReturnRechecksChangedDemandBeforeSending(t *testing.T) {
	x := newRecoveryTest(t)
	x.drain(1)
	preferred := x.evacuate(11, 8, 40)
	ordinary := x.evacuate(12, 8, 60)
	x.now += 20
	x.store(1, x.now-20, 40)
	x.store(8, x.now-140, 100)
	x.report(12, 8, 8, 10, x.now)
	op := transfer(preferred, "balance", 1, OpLeader)
	require.True(t, x.PreferReturn(x.tc, op, ordinary.GetID(), nil))
	sent := x.GetHBStreams().MsgLength()
	// A heartbeat with unchanged rounded bytes can reach Dispatch without
	// replacing the scheduling cache. The new key rate would overload home.
	fresh := preferred.Clone(core.SetReadKeys(900), core.SetReportInterval(uint64(x.now-10), uint64(x.now)))
	require.False(t, x.canSendRecovery(op, fresh))
	require.False(t, op.recovery.sent[0])

	// The native choice now returns more useful reads. Both moves pass the
	// read-imbalance guard, so that guard alone cannot detect the reversal.
	x.report(11, 8, 8, 10, x.now)
	x.report(12, 8, 8, 60, x.now)
	require.False(t, x.AddOperator(op))
	require.Equal(t, sent, x.GetHBStreams().MsgLength())

	// The next slot can issue immediately, without waiting for a Store report.
	fallback := x.report(13, 8, 8, 0, x.now)
	x.report(12, 8, 8, 50, x.now)
	next := transfer(ordinary, "balance", 1, OpLeader)
	require.True(t, x.PreferReturn(x.tc, next, fallback.GetID(), nil))
	require.True(t, x.AddOperator(next))
	require.Equal(t, sent+1, x.GetHBStreams().MsgLength())
	require.True(t, x.RemoveOperator(next))
	retry := transfer(preferred, "balance", 1, OpLeader)
	require.False(t, x.PreferReturn(x.tc, retry, fallback.GetID(), nil), "a sent command may still take effect after cancellation")
}

func TestReturnAfterConsecutivePlannedDrains(t *testing.T) {
	for _, tc := range []struct {
		name                                                         string
		planSecond, longReport, election, evictHistory, alreadyMoved bool
		wantReturn                                                   bool
	}{
		{name: "two confirmed drains", planSecond: true, wantReturn: true},
		{name: "no fresh read rate on temporary host", planSecond: true, longReport: true, wantReturn: true},
		{name: "hot scheduler changes leader"},
		{name: "intervening election", planSecond: true, election: true},
		{name: "leader already changed before dispatch", planSecond: true, alreadyMoved: true},
		{name: "origin evicted before confirmation", planSecond: true, evictHistory: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newRecoveryTest(t)
			x.drain(1)
			middle := x.evacuate(101, 12, 40)
			x.store(1, x.now-20, 10)
			x.drain(12)
			if tc.election {
				middle = x.report(101, 12, middle.GetTerm()+2, 12, x.now)
			}
			if tc.longReport {
				middle = middle.Clone(core.SetReportInterval(uint64(x.now-501), uint64(x.now)))
				x.cluster.CheckAndPutRegion(middle)
			}
			op := NewOperator("evict", "", 101, middle.GetRegionEpoch(), OpLeader, 1,
				TransferLeader{FromStore: 12, ToStore: 1, ToStores: []uint64{1, 8}})
			if tc.planSecond {
				x.PlanEvictions(x.tc, []*Operator{op})
			} else {
				op = transfer(middle, "move-hot-read-leader", 8, OpHotRegion|OpLeader)
			}
			if tc.alreadyMoved {
				x.report(101, 8, middle.GetTerm()+1, 25, x.now)
			}
			require.True(t, x.AddOperator(op))
			if tc.evictHistory {
				x.recovery.forget(101)
			}
			x.Dispatch(x.report(101, 8, middle.GetTerm()+1, 25, x.now), DispatchFromHeartBeat, func(uint64) {})
			require.Equal(t, SUCCESS, op.Status())
			if tc.evictHistory {
				require.NotContains(t, x.recovery.history, uint64(101), "the temporary host must not become the origin")
			}
			x.now += 20
			x.store(1, x.now-40, 10)
			x.store(8, x.now-140, 100)
			r := x.report(101, 8, middle.GetTerm()+1, 25, x.now)
			ordinary := x.report(102, 8, 7, 5, x.now)
			got := x.BestReturn(x.tc, x.cluster.GetStore(8), x.cluster.GetStore(1), ordinary, nil, func(*core.RegionInfo) bool { return true })
			if tc.wantReturn {
				require.Same(t, r, got)
				require.Equal(t, 40.0, x.recovery.history[101].rate)
				back := transfer(r, "balance", 1, OpLeader)
				x.store(1, x.now-40, 110)
				require.False(t, x.PreferReturn(x.tc, back, ordinary.GetID(), nil), "the recovered Store is busier")
				x.store(1, x.now-40, 10)
				require.True(t, x.PreferReturn(x.tc, back, ordinary.GetID(), nil))
				require.True(t, x.AddOperator(back))
				x.Dispatch(x.report(101, 1, r.GetTerm()+1, 25, x.now), DispatchFromHeartBeat, func(uint64) {})
				require.Equal(t, SUCCESS, back.Status())
				require.Equal(t, departure{}, x.recovery.history[101], "return consumed the original record")
			} else {
				require.Same(t, ordinary, got)
			}
		})
	}
}

func TestPlannedDrainReplacingAHotTransferCannotSeedAReturn(t *testing.T) {
	for _, source := range []uint64{1, 12} {
		x := newRecoveryTest(t)
		x.drain(1)
		r := x.report(101, 1, 7, 40, x.now-40)
		if source == 12 {
			r = x.evacuate(101, 12, 40)
			x.drain(12)
		}
		drain := transfer(r, "evict", 8, OpLeader)
		drain.SetPriorityLevel(constant.Urgent)
		x.PlanEvictions(x.tc, []*Operator{drain})
		hot := transfer(r, "move-hot-read-leader", 8, OpHotRegion|OpLeader)
		require.True(t, x.AddOperator(hot))
		require.Zero(t, x.recovery.history[101].id, "once hot scheduling takes the Region, its origin is spent")
		require.True(t, x.AddOperator(drain))
		x.Dispatch(x.report(101, 8, r.GetTerm()+1, 40, x.now), DispatchFromHeartBeat, func(uint64) {})
		require.Equal(t, SUCCESS, drain.Status())
		require.Zero(t, x.recovery.history[101].id, "a shared destination does not establish a cache return")
	}
}

func TestEvictionSwapsWithinBatchAndFollowsActualReceiver(t *testing.T) {
	x := newRecoveryTest(t)
	x.drain(1)
	x.drain(12)
	r := x.report(1, 1, 7, 40, x.now-40)
	ev := NewOperator("evict", "", r.GetID(), r.GetRegionEpoch(), OpLeader, 1, TransferLeader{FromStore: 1, ToStore: 8, ToStores: []uint64{8, 12}})
	x.PlanEvictions(x.tc, []*Operator{ev})
	require.True(t, x.AddOperator(ev))
	r = x.report(1, 12, 8, 40, x.now-30)
	x.Dispatch(r, DispatchFromHeartBeat, func(uint64) {})
	require.Equal(t, uint64(12), x.recovery.history[1].successor)
	x.store(1, x.now-20, 40)
	x.store(8, x.now-120, 100)
	other := x.report(2, 12, 3, 10, x.now)
	evict := func(r *core.RegionInfo, to uint64) *Operator {
		op := NewOperator("evict", "", r.GetID(), r.GetRegionEpoch(), OpLeader, 1, TransferLeader{FromStore: 12, ToStore: to, ToStores: []uint64{8, 1}})
		op.SetPriorityLevel(constant.Urgent)
		return op
	}
	x.observeRecovery(transfer(x.report(9, 8, 1, 5, x.now), "hot", 12, OpHotRegion))
	batch := func() []*Operator {
		ops := []*Operator{evict(r, 8), evict(other, 1)}
		x.PlanEvictions(x.tc, ops)
		return ops
	}
	hot := transfer(r, "hot", 8, OpHotRegion)
	hot.SetPriorityLevel(constant.High)
	x.operators.Store(r.GetID(), hot)
	ops := batch()
	require.Nil(t, ops[0].recovery, "keep ordinary urgent eviction while hot scheduling owns the Region")
	admit, _ := x.checkAddOperator(false, ops[0])
	require.True(t, admit, "the mandatory drain keeps its original priority")
	x.operators.Delete(r.GetID())
	require.Nil(t, batch()[0].recovery, "if only the companion succeeds it adds reads to the Store shedding heat")
	other = x.report(2, 12, 3, 0, x.now)
	x.recovery.history[other.GetID()] = departure{}
	require.Nil(t, batch()[0].recovery, "a spent origin cannot be redirected as the companion either")
	delete(x.recovery.history, other.GetID())
	ops = batch()
	require.NotNil(t, ops[0].recovery)
	x.operators.Store(r.GetID(), hot)
	admit, _ = x.checkAddOperator(false, ops[0])
	require.False(t, admit, "a recovery exchange yields to an already published hot move")
	x.operators.Store(r.GetID(), ops[0])
	admit, _ = x.checkAddOperator(false, hot)
	require.True(t, admit, "a later hot move can replace the voluntary exchange")
	x.operators.Delete(r.GetID())
	a, _ := primary(ops[0])
	b, _ := primary(ops[1])
	require.Equal(t, uint64(1), a.ToStore)
	require.Equal(t, uint64(8), b.ToStore)
	aborted := batch()
	require.True(t, aborted[0].Cancel())
	require.False(t, x.canSendRecovery(aborted[1], other))
	queued := batch()
	require.NotNil(t, queued[0].recovery)
	require.True(t, x.canSendRecovery(ops[1], other), "the compensating command can dispatch first")
	require.False(t, x.canSendRecovery(queued[1], other), "another exchange cannot claim the same origin")
	require.False(t, x.canSendRecovery(queued[0], r))
	require.True(t, x.canSendRecovery(ops[0], r))
	x.finish(ops[0], x.report(1, 1, 9, 40, x.now))
	x.store(12, x.now-20, 0)
	require.Same(t, other, x.BestReturn(x.tc, x.cluster.GetStore(1), x.cluster.GetStore(12), other, nil, func(*core.RegionInfo) bool { return true }), "temporary host must not pull the same Region back")
}

func TestHandoffPreservesAdmissionAndReportedReadBound(t *testing.T) {
	for _, tt := range []struct {
		name                                            string
		admitted, received                              uint64
		senderInterval, receiverInterval, version, term uint64
		allow                                           bool
	}{
		{"cold transfer after a long time as follower", 0, 0, 10, 493, 1, 8, true},
		{"retain known cost", 1, 0, 10, 301, 1, 8, true},
		{"retain high cost", 40, 0, 10, 301, 1, 8, false},
		{"unknown admission", 0, 0, 0, 301, 1, 8, false},
		{"split during handoff", 1, 0, 10, 301, 2, 8, false},
		{"another election intervened", 1, 0, 10, 301, 1, 9, false},
		{"one key over five minutes", 1, 1, 10, 301, 1, 8, true},
		{"long report may contain a burst", 1, 600, 10, 301, 1, 8, false},
		{"missing reporting interval", 1, 0, 10, 0, 1, 8, false},
		{"heartbeat too short", 1, 0, 10, 1, 1, 8, false},
		{"invalid counter on new leader", 1, core.ImpossibleFlowSize, 10, 493, 1, 8, false},
		{"new leader reports a burst", 1, 400, 10, 10, 1, 8, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			x := newRecoveryTest(t)
			x.drain(1)
			home := x.evacuate(1, 8, 40)
			x.store(1, x.now-20, 40)
			x.store(8, x.now-120, 100)
			ordinary := x.report(2, 8, 7, 10, x.now)
			canReturn := func() bool {
				return x.PreferReturn(x.tc, transfer(home, "balance", 1, OpLeader), ordinary.GetID(), nil)
			}
			require.True(t, canReturn())
			end := uint64(x.now)
			sender := x.report(731, 8, 7, tt.admitted, x.now).Clone(core.SetReportInterval(end-tt.senderInterval, end))
			x.cluster.CheckAndPutRegion(sender)
			op := transfer(sender, "balance", 12, OpLeader)
			x.observeRecovery(op)
			require.True(t, op.Start())
			metadata := sender.Clone(core.SetRegionVersion(tt.version))
			receiver := core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
				Region: metadata.GetMeta(), Leader: metadata.GetStorePeer(12), Term: tt.term,
				KeysRead: tt.received, Interval: &pdpb.TimeInterval{StartTimestamp: end - tt.receiverInterval, EndTimestamp: end},
			}, 0)
			require.Nil(t, op.Check(receiver))
			x.finishRecovery(op, receiver)
			require.Equal(t, tt.allow, canReturn())
		})
	}
}

func TestBalanceDefaultIsFreshlyEvacuated(t *testing.T) {
	x := newRecoveryTest(t)
	x.drain(1)
	home := x.evacuate(101, 8, 40)
	homeStart, sourceStart := x.now-20, x.now-120
	for i, tc := range []struct {
		admitted, keys, span, sourceRate uint64
		wantReturn                       bool
	}{
		{0, 0, 500, 100, true},   // A confirmed idle default, gap 60.
		{0, 90, 500, 100, false}, // The default could equalize the two Stores.
		{50, 0, 500, 100, false}, // A rate inside [0,50] beats returning 40.
		{50, 0, 500, 120, true},  // Returning 40 equalizes the two Stores for every default.
		// An unrateable short report leaves every native read rate possible.
		{0, 0, 1, 119, false},
		{0, 0, 1, 120, true},
		{0, 0, 1, 121, false},
	} {
		r := x.report(uint64(i+200), 12, 7, tc.admitted, x.now)
		ev := transfer(r, "evict", 8, OpLeader)
		require.True(t, x.AddOperator(ev))
		end := uint64(x.now)
		r = core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{Region: r.GetMeta(), Leader: r.GetStorePeer(8),
			Term: 8, KeysRead: tc.keys, Interval: &pdpb.TimeInterval{StartTimestamp: end - tc.span, EndTimestamp: end}}, 0)
		x.cluster.CheckAndPutRegion(r)
		x.Dispatch(r, DispatchFromHeartBeat, func(uint64) {})
		require.Equal(t, SUCCESS, ev.Status())
		x.now += 20
		x.store(1, homeStart, 40)
		x.store(8, sourceStart, tc.sourceRate)
		preferred := transfer(home, "balance", 1, OpLeader)
		got := x.BestReturn(x.tc, x.cluster.GetStore(8), x.cluster.GetStore(1), r, nil, func(*core.RegionInfo) bool {
			return x.PreferReturn(x.tc, preferred, r.GetID(), nil)
		})
		require.Equal(t, tc.wantReturn, got == home, "case %d", i)
		if tc.wantReturn {
			x.store(1, homeStart, tc.sourceRate)
			require.False(t, x.canSendRecovery(preferred, home), "recheck the gap before sending")
		}
	}
}

func TestUnknownTransferOnlyBlocksTheUnsafeDirection(t *testing.T) {
	x := newRecoveryTest(t)
	x.drain(1)
	home := x.evacuate(1, 8, 40)
	x.store(1, x.now-20, 40)
	x.store(8, x.now-120, 100)
	ordinary := x.report(2, 8, 7, 10, x.now)
	from := x.report(3, 12, 7, 0, x.now).Clone(core.SetReportInterval(0, 0))
	x.cluster.CheckAndPutRegion(from)
	for _, to := range []uint64{8, 1} {
		op := transfer(from, "balance", to, OpLeader)
		x.observeRecovery(op)
		got := x.PreferReturn(x.tc, transfer(home, "balance", 1, OpLeader), ordinary.GetID(), nil)
		require.Equal(t, to == 8, got, "unmeasured traffic entering Store %d", to)
		x.finishRecovery(op, nil)
	}
	require.False(t, noWiderGap(1, math.Inf(1), math.SmallestNonzeroFloat64, 2),
		"a rounded distance must not turn a worse allocation into a tie")
	require.True(t, noWiderGap(math.Inf(-1), 1, 2, 0))
}

// A former follower can immediately become the source of another transfer.
// Only the confirmed reporting interval carries the previous Leader's cost.
func TestConsecutiveTransfersCarryConfirmedReadCost(t *testing.T) {
	for _, initial := range []uint64{0, 40} {
		x := newRecoveryTest(t)
		x.drain(1)
		source := x.report(1863, 12, 7, initial, x.now)
		first := transfer(source, "balance", 8, OpLeader)
		x.observeRecovery(first)
		require.True(t, first.Start())

		end := uint64(x.now)
		middle := core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region: source.GetMeta(), Leader: source.GetStorePeer(8), Term: 8,
			Interval: &pdpb.TimeInterval{StartTimestamp: end - 493, EndTimestamp: end},
		}, 0)
		x.cluster.CheckAndPutRegion(middle)
		bounds, ok := x.loadBounds(x.cluster.GetStore(8), &recoveryChoice{})
		require.True(t, ok)
		require.Equal(t, 100+float64(initial), bounds.high, "confirmation already visible before dispatch")
		require.Nil(t, first.Check(middle))
		x.finishRecovery(first, middle)

		second := transfer(middle, "balance", 1, OpLeader)
		x.observeRecovery(second)
		require.True(t, second.Start())
		bounds, ok = x.loadBounds(x.cluster.GetStore(1), &recoveryChoice{})
		require.True(t, ok)
		require.Equal(t, 100+float64(initial), bounds.high, "second receiver reserves the first sender's rate")
		last := x.report(1863, 1, 9, 0, x.now)
		require.Nil(t, second.Check(last))
		x.finishRecovery(second, last)
		bounds, ok = x.loadBounds(x.cluster.GetStore(1), &recoveryChoice{})
		require.True(t, ok)
		require.Equal(t, 100+float64(initial), bounds.high, "keep the reservation until the Store reports")

		require.True(t, math.IsInf(x.recovery.readCostBounds(middle.Clone(core.SetReportInterval(end-494, end))).high, 1))
		require.True(t, math.IsInf(x.recovery.readCostBounds(middle.Clone(core.SetRegionConfVer(2))).high, 1))
		require.True(t, math.IsInf(readCost(middle), 1), "a reservation is never a measured candidate rate")
		next := x.round
		next.ID++
		require.NoError(t, x.UpdateRecoveryRound(next, 8, x.now-120))
		require.True(t, math.IsInf(x.recovery.readCostBounds(middle).high, 1))
	}
}

// Compare the indexed search with scoring every candidate independently. Both the
// hottest original and the hottest current Region may be inadmissible.
func TestBestReturnMatchesExhaustiveSelection(t *testing.T) {
	x := newRecoveryTest(t)
	x.drain(1)
	rng := rand.New(rand.NewPCG(731, 2975))
	const count = 32
	origin := make([]uint64, count)
	for i := range origin {
		origin[i] = rng.Uint64N(100) + 1
		x.evacuate(uint64(i+1), 8, origin[i])
	}
	restart, sourceStart := x.now-20, x.now-120
	x.now += 20
	weights := []float64{0.25, 0.5, 1, 2, 4}
	for trial := range 512 {
		sourceRate, homeRate := rng.Uint64N(400), rng.Uint64N(400)
		x.store(8, sourceStart, sourceRate)
		x.store(1, restart, homeRate)
		sw, hw := weights[rng.IntN(len(weights))], weights[rng.IntN(len(weights))]
		source := x.cluster.GetStore(8).Clone(core.SetLeaderWeight(sw))
		home := x.cluster.GetStore(1).Clone(core.SetLeaderWeight(hw))
		x.cluster.PutStore(source)
		x.cluster.PutStore(home)
		var current [count]uint64
		var veto [count]bool
		for i := range current {
			current[i] = rng.Uint64N(150)
			veto[i] = rng.IntN(3) == 0
			x.report(uint64(i+1), 8, 8, current[i], x.now)
		}
		ordinary := x.report(100, 8, 5, rng.Uint64N(100), x.now)
		var best uint64
		if trial%2 == 0 {
			i := rng.IntN(count)
			ordinary = x.cluster.GetRegion(uint64(i + 1))
			best = min(origin[i], current[i])
		}
		pending := rng.Uint64N(80)
		incoming := transfer(x.report(101, 12, 5, pending, x.now), "balance", 1, OpLeader)
		defaultRate, _ := ordinary.ReadKeysPerSecond()
		homeYield, sourceYield := trial%5 == 0, trial%7 == 0
		x.recovery.yielded[1], x.recovery.yielded[8] = homeYield, sourceYield
		for i, n := range current {
			if veto[i] || n == 0 || homeYield && float64(n) > defaultRate || sourceYield && float64(n) < defaultRate {
				continue
			}
			allowed := true
			for _, extra := range []uint64{0, pending} {
				h, s := float64(homeRate+extra), float64(sourceRate)
				gap := func(moved float64) float64 { return math.Abs((s-moved)/sw - (h+moved)/hw) }
				if gap(float64(n)) > min(gap(0), gap(defaultRate)) {
					allowed = false
				}
			}
			if allowed {
				best = max(best, min(origin[i], n))
			}
		}
		got := x.BestReturn(x.tc, source, home, ordinary, []*Operator{incoming}, func(r *core.RegionInfo) bool { return !veto[r.GetID()-1] })
		var score uint64
		if id := got.GetID(); id <= count {
			score = min(origin[id-1], current[id-1])
		}
		require.Equal(t, best, score, "trial %d", trial)
		if id := ordinary.GetID(); id <= count && min(origin[id-1], current[id-1]) == best {
			require.Same(t, ordinary, got, "keep an equally good default")
		}
	}
}
