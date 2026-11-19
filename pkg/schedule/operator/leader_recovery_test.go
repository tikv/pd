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
	"github.com/tikv/pd/pkg/schedule/types"
)

type recoveryTest struct {
	*testing.T
	*Controller
	now           int64
	storeInterval int64
}

func newRecoveryTest(t *testing.T) *recoveryTest {
	x := &recoveryTest{T: t, Controller: &Controller{cluster: core.NewBasicCluster()}, now: time.Now().Unix(), storeInterval: 10}
	for _, s := range []uint64{1, 8, 12} {
		x.store(s, x.now-120, 100)
	}
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
	require.True(x, op.Start())
	require.Nil(x, op.Check(region))
	x.finishRecovery(op, region)
}

func (x *recoveryTest) evac(id, to, old, new uint64) *core.RegionInfo {
	r := x.report(id, 1, 7, old, x.now-20)
	op := transfer(r, types.EvictLeaderScheduler.String(), to, OpLeader)
	require.True(x, x.admitRecovery(op))
	r = x.report(id, to, 8, new, x.now-11)
	x.finish(op, r)
	return r
}
func TestLeaderRecoveryRanksCompletedEvacuationsAndYieldsToHotScheduler(t *testing.T) {
	x := newRecoveryTest(t)
	x.evac(1, 8, 20, 10)
	best := x.evac(2, 8, 25, 26)
	x.evac(3, 8, 5, 5)
	stale := x.evac(4, 8, 200, 200)
	x.report(stale.GetID(), 8, 10, 200, x.now-1)
	x.store(1, x.now-5, 0)
	x.store(8, x.now-120, 200)
	src, home := x.cluster.GetStore(8), x.cluster.GetStore(1)
	regions, tracked := x.FormerLeaders(src, home, best)
	require.True(t, tracked)
	require.Equal(t, []*core.RegionInfo{best}, regions)
	op := transfer(best, types.BalanceLeaderScheduler.String(), 1, OpLeader)
	op.MarkLeaderReturn(x.cluster.GetStore(1), nil)
	staleReturn := transfer(stale, types.BalanceLeaderScheduler.String(), 1, OpLeader)
	staleReturn.MarkLeaderReturn(x.cluster.GetStore(1), nil)
	require.False(t, x.admitRecovery(staleReturn))
	x.store(1, x.now-5, 2000)
	require.False(t, x.admitRecovery(op))
	x.store(1, x.now-5, 0)
	require.True(t, x.admitRecovery(op))
	x.finishRecovery(op, nil)
	require.False(t, x.admitRecovery(op), "a canceled return has already spent its chance")

	inflight := transfer(x.cluster.GetRegion(1), types.BalanceLeaderScheduler.String(), 1, OpLeader)
	inflight.MarkLeaderReturn(x.cluster.GetStore(1), nil)
	require.True(t, x.admitRecovery(inflight))
	require.True(t, x.canSendRecovery(inflight))
	selected := transfer(x.cluster.GetRegion(3), types.BalanceLeaderScheduler.String(), 1, OpLeader)
	selected.MarkLeaderReturn(x.cluster.GetStore(1), nil)
	require.True(t, x.admitRecovery(selected))
	queued := transfer(x.cluster.GetRegion(1), types.BalanceLeaderScheduler.String(), 1, OpLeader)
	queued.MarkLeaderReturn(x.cluster.GetStore(1), nil)
	hot := transfer(x.report(8, 1, 11, 50, x.now), "transfer-hot-read-leader", 8, OpHotRegion|OpLeader)
	require.True(t, x.admitRecovery(hot))
	require.False(t, x.admitRecovery(queued))
	require.False(t, x.canSendRecovery(selected))
	require.True(t, x.canSendRecovery(inflight))
	regions, tracked = x.FormerLeaders(src, home, best)
	require.True(t, tracked)
	require.Empty(t, regions)
}

func TestLeaderRecoveryBoundsPendingAndCompletedEvictionChanges(t *testing.T) {
	x := newRecoveryTest(t)
	for id := uint64(1); id <= 6; id++ {
		x.evac(id, 12, 8, 8)
	}
	first := x.evac(20, 12, 2, 2)
	x.store(1, x.now-5, 10)
	x.store(8, x.now-120, 10)
	x.store(12, x.now-120, 1000)
	targets := []uint64{1, 8}
	target := func(r *core.RegionInfo, original uint64, ids []uint64) uint64 {
		dst, _, _ := x.recovery.evictionTarget(x.cluster, r, original, ids, 3)
		return dst
	}
	require.Equal(t, uint64(8), target(first, 8, targets), "a restart alone does not redirect evictions")
	back := transfer(first, types.BalanceLeaderScheduler.String(), 1, OpLeader)
	require.True(t, x.admitRecovery(back))
	x.finish(back, x.report(20, 1, 9, 2, x.now))
	choose := func(id, original, want uint64) *Operator {
		r := x.cluster.GetRegion(id)
		require.Equal(t, want, target(r, original, targets))
		op := transfer(r, types.EvictLeaderScheduler.String(), want, OpLeader)
		if original != want {
			op.recovery = &recoveryChoice{window: x.recovery.homes[1], original: original, targets: targets, batch: 3}
		}
		require.True(t, x.admitRecovery(op))
		return op
	}
	a := choose(1, 8, 1)
	b := choose(2, 8, 1)
	choose(3, 8, 1)
	require.Equal(t, uint64(8), target(x.cluster.GetRegion(4), 8, targets))
	// A reverse preference can still fail; it does not free a forward slot yet.
	x.report(7, 12, 7, 8, x.now)
	reverse := choose(7, 1, 8)
	require.Equal(t, uint64(8), target(x.cluster.GetRegion(4), 8, targets))
	x.finishRecovery(reverse, nil)
	x.finish(a, x.report(1, 1, 9, 8, x.now))
	require.Equal(t, uint64(8), target(x.cluster.GetRegion(4), 8, targets))
	x.finishRecovery(b, nil)
	choose(4, 8, 1)
	// One successful reverse transfer frees exactly one forward slot.
	reverse = choose(7, 1, 8)
	x.finish(reverse, x.report(7, 8, 8, 8, x.now))
	choose(5, 8, 1)
	require.Equal(t, uint64(8), target(x.cluster.GetRegion(6), 8, targets))
	require.Equal(t, uint64(8), target(x.cluster.GetRegion(6), 8, []uint64{8}))
}

func TestLeaderRecoveryEndsAfterAQuietStoreHeartbeat(t *testing.T) {
	x := newRecoveryTest(t)
	x.storeInterval = 30 // The cutoff follows the reported intervals.
	first := x.evac(1, 8, 4, 4)
	second := x.evac(2, 12, 4, 4)
	x.evac(3, 12, 4, 4)
	start := x.now - 5
	x.store(1, start, 0)
	returnToHome := func(r *core.RegionInfo) *Operator {
		op := transfer(r, types.BalanceLeaderScheduler.String(), 1, OpLeader)
		op.MarkLeaderReturn(x.cluster.GetStore(1), nil)
		require.True(t, x.admitRecovery(op))
		return op
	}
	x.finish(returnToHome(first), x.report(1, 1, 9, 4, x.now))
	pending := returnToHome(second)
	x.now += 2 * x.storeInterval
	x.store(1, start, 0)
	require.True(t, x.canSendRecovery(pending), "an outstanding return spans the heartbeat")
	x.finish(pending, x.report(2, 1, 9, 4, x.now))
	checkTarget := func(want uint64) {
		dst, _, _ := x.recovery.evictionTarget(x.cluster, x.cluster.GetRegion(3), 8, []uint64{1, 8}, 3)
		require.Equal(t, want, dst)
	}
	x.now += x.storeInterval
	x.store(1, start, 0)
	checkTarget(1) // the latest interval may still straddle the last return
	x.now += x.storeInterval
	x.store(1, start, 0)
	checkTarget(8)
	regions, planned := x.FormerLeaders(x.cluster.GetStore(12), x.cluster.GetStore(1), second)
	require.True(t, planned, "retirement must also suppress the generic restart fallback")
	require.Empty(t, regions)
	x.now -= 2 * x.storeInterval
	x.store(1, start, 0)
	checkTarget(8) // a regressing heartbeat cannot reopen the window
}

func TestLeaderRecoveryChargesActualReceiverUntilAFreshHeartbeat(t *testing.T) {
	x := newRecoveryTest(t)
	r := x.report(1, 1, 7, 5, x.now-20)
	op := NewOperator(types.EvictLeaderScheduler.String(), "", r.GetID(), r.GetRegionEpoch(), OpLeader, 1, TransferLeader{FromStore: 1, ToStore: 8, ToStores: []uint64{8, 12}})
	require.True(t, x.admitRecovery(op))
	load := func(id uint64) float64 {
		v, ok := x.recovery.load(x.cluster.GetStore(id))
		require.True(t, ok)
		return v
	}
	require.Equal(t, 105.0, load(8))
	x.finish(op, x.report(1, 12, 8, 5, x.now))
	require.Equal(t, 100.0, load(8))
	require.Equal(t, 105.0, load(12))
	require.Equal(t, 95.0, load(1))
	x.now += 20
	x.store(12, x.now-140, 105)
	require.Equal(t, 105.0, load(12), "the new interval already includes the transfer")
}

func TestLeaderRecoveryLimitsHistoryAcrossStores(t *testing.T) {
	s := &leaderRecovery{}
	a := s.draining(core.NewStoreInfo(&metapb.Store{Id: 1, StartTimestamp: 1}))
	b := s.draining(core.NewStoreInfo(&metapb.Store{Id: 8, StartTimestamp: 1}))
	for id := uint64(1); id <= 8192; id++ {
		s.remember(a, departure{StoreID: 1, RegionID: id, rate: float64(id)})
	}
	s.remember(b, departure{StoreID: 8, RegionID: 1, rate: 10000})
	require.NotContains(t, a.regions, uint64(1))
	require.Contains(t, b.regions, uint64(1))
	require.Equal(t, 8192, s.coldest.Len())
	s.forgetStore(1)
	require.Equal(t, 1, s.coldest.Len())
}
