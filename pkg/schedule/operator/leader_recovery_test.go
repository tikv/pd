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
	x := &recoveryTest{T: t, tc: tc, Controller: NewController(t.Context(), tc.GetBasicCluster(), tc.GetSharedConfig(), nil), now: time.Now().Unix(), storeInterval: 10}
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
	r = x.report(id, successor, 8, rate, x.now-30)
	x.finish(op, r)
	return r
}

type gatedHeat struct {
	sche.SchedulerCluster
	checking, resume chan struct{}
}

func (g *gatedHeat) AllowsLeaderReturn(r *core.RegionInfo, degree int) bool {
	close(g.checking)
	<-g.resume
	return g.SchedulerCluster.AllowsLeaderReturn(r, degree)
}

func TestRecoveryRoundFencesRetriesAndDispatch(t *testing.T) {
	x := newRecoveryTest(t)
	x.evacuate(1, 8, 20)
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
	require.Empty(t, x.ReturnCandidates(x.cluster.GetStore(8), x.cluster.GetStore(1)))
	r = x.report(1, 8, 8, 40, x.now)
	require.Equal(t, []*core.RegionInfo{r}, x.ReturnCandidates(x.cluster.GetStore(8), x.cluster.GetStore(1)))
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
	require.Empty(t, x.ReturnCandidates(x.cluster.GetStore(8), x.cluster.GetStore(1)))
}

func TestEvictionSwapsWithinBatchAndFollowsActualReceiver(t *testing.T) {
	x := newRecoveryTest(t)
	x.drain(1)
	x.drain(12)
	r := x.report(1, 1, 7, 40, x.now-40)
	ev := NewOperator("evict", "", r.GetID(), r.GetRegionEpoch(), OpLeader, 1, TransferLeader{FromStore: 1, ToStore: 8, ToStores: []uint64{8, 12}})
	x.PlanEvictions(x.tc, []*Operator{ev})
	r = x.report(1, 12, 8, 40, x.now-30)
	x.finish(ev, r)
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
	x.operators.Store(r.GetID(), hot)
	ops := batch()
	require.Nil(t, ops[0].recovery, "keep ordinary urgent eviction while hot scheduling owns the Region")
	require.True(t, isHigherPriorityOperator(ops[0], hot), "the mandatory drain keeps its original priority")
	x.operators.Delete(r.GetID())
	ops = batch()
	require.NotNil(t, ops[0].recovery)
	require.False(t, isHigherPriorityOperator(ops[0], hot), "a hot move arriving after selection keeps its operator")
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
	require.Empty(t, x.ReturnCandidates(x.cluster.GetStore(1), x.cluster.GetStore(12)), "temporary host must not pull the same Region back")
}
