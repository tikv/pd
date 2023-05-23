// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockhbstream"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

func newTestOperator(regionID uint64, regionEpoch *metapb.RegionEpoch, kind operator.OpKind, steps ...operator.OpStep) *operator.Operator {
	return operator.NewTestOperator(regionID, regionEpoch, kind, steps...)
}

func (c *testCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	id, err := c.GetAllocator().Alloc()
	if err != nil {
		return nil, err
	}
	return &metapb.Peer{Id: id, StoreId: storeID}, nil
}

func (c *testCluster) addRegionStore(storeID uint64, regionCount int, regionSizes ...uint64) error {
	var regionSize uint64
	if len(regionSizes) == 0 {
		regionSize = uint64(regionCount) * 10
	} else {
		regionSize = regionSizes[0]
	}

	stats := &pdpb.StoreStats{}
	stats.Capacity = 100 * units.GiB
	stats.UsedSize = regionSize * units.MiB
	stats.Available = stats.Capacity - stats.UsedSize
	newStore := core.NewStoreInfo(&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionSize)),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	c.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderRegion(regionID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) error {
	region := newTestRegionMeta(regionID)
	leader, _ := c.AllocPeer(leaderStoreID)
	region.Peers = []*metapb.Peer{leader}
	for _, followerStoreID := range followerStoreIDs {
		peer, _ := c.AllocPeer(followerStoreID)
		region.Peers = append(region.Peers, peer)
	}
	regionInfo := core.NewRegionInfo(region, leader, core.SetApproximateSize(10), core.SetApproximateKeys(10))
	return c.putRegion(regionInfo)
}

func (c *testCluster) updateLeaderCount(storeID uint64, leaderCount int) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderStore(storeID uint64, leaderCount int) error {
	stats := &pdpb.StoreStats{}
	newStore := core.NewStoreInfo(&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	c.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreDown(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(typeutil.ZeroTime),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreOffline(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(core.OfflineStore(false))
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) LoadRegion(regionID uint64, followerStoreIDs ...uint64) error {
	//  regions load from etcd will have no leader
	region := newTestRegionMeta(regionID)
	region.Peers = []*metapb.Peer{}
	for _, id := range followerStoreIDs {
		peer, _ := c.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	return c.putRegion(core.NewRegionInfo(region, nil))
}

func TestBasic(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.opController

	re.NoError(tc.addLeaderRegion(1, 1))

	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	oc.AddWaitingOperator(op1)
	re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader))
	re.Equal(op1.RegionID(), oc.GetOperator(1).RegionID())

	// Region 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op2)
	re.Equal(uint64(0), oc.OperatorCount(operator.OpRegion))

	// Remove the operator manually, then we can add a new operator.
	re.True(oc.RemoveOperator(op1))
	op3 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op3)
	re.Equal(uint64(1), oc.OperatorCount(operator.OpRegion))
	re.Equal(op3.RegionID(), oc.GetOperator(1).RegionID())
}

func TestDispatch(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	co.prepareChecker.prepared = true
	// Transfer peer from store 4 to store 1.
	re.NoError(tc.addRegionStore(4, 40))
	re.NoError(tc.addRegionStore(3, 30))
	re.NoError(tc.addRegionStore(2, 20))
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))

	// Transfer leader from store 4 to store 2.
	re.NoError(tc.updateLeaderCount(4, 50))
	re.NoError(tc.updateLeaderCount(3, 50))
	re.NoError(tc.updateLeaderCount(2, 20))
	re.NoError(tc.updateLeaderCount(1, 10))
	re.NoError(tc.addLeaderRegion(2, 4, 3, 2))

	go co.runUntilStop()

	// Wait for schedule and turn off balance.
	waitOperator(re, co, 1)
	operatorutil.CheckTransferPeer(re, co.opController.GetOperator(1), operator.OpKind(0), 4, 1)
	re.NoError(co.removeScheduler(schedulers.BalanceRegionName))
	waitOperator(re, co, 2)
	operatorutil.CheckTransferLeader(re, co.opController.GetOperator(2), operator.OpKind(0), 4, 2)
	re.NoError(co.removeScheduler(schedulers.BalanceLeaderName))

	stream := mockhbstream.NewHeartbeatStream()

	// Transfer peer.
	region := tc.GetRegion(1).Clone()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitRemovePeer(re, stream, region, 4)
	re.NoError(dispatchHeartbeat(co, region, stream))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Transfer leader.
	region = tc.GetRegion(2).Clone()
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitTransferLeader(re, stream, region, 2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
}

func dispatchHeartbeat(co *coordinator, region *core.RegionInfo, stream hbstream.HeartbeatStream) error {
	co.hbStreams.BindStream(region.GetLeader().GetStoreId(), stream)
	if err := co.cluster.putRegion(region.Clone()); err != nil {
		return err
	}
	co.opController.Dispatch(region, operator.DispatchFromHeartBeat)
	return nil
}

func TestCollectMetricsConcurrent(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(tc.GetOpts(), nil, tc.storeConfigManager)
	}, func(co *coordinator) { co.run() }, re)
	defer cleanup()

	// Make sure there are no problem when concurrent write and read
	var wg sync.WaitGroup
	count := 10
	wg.Add(count + 1)
	for i := 0; i <= count; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				re.NoError(tc.addRegionStore(uint64(i%5), rand.Intn(200)))
			}
		}(i)
	}
	for i := 0; i < 1000; i++ {
		co.collectHotSpotMetrics()
		co.collectSchedulerMetrics()
		co.cluster.collectClusterMetrics()
	}
	co.resetHotSpotMetrics()
	co.resetSchedulerMetrics()
	co.cluster.resetClusterMetrics()
	wg.Wait()
}

func TestCollectMetrics(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(tc.GetOpts(), nil, tc.storeConfigManager)
	}, func(co *coordinator) { co.run() }, re)
	defer cleanup()
	count := 10
	for i := 0; i <= count; i++ {
		for k := 0; k < 200; k++ {
			item := &statistics.HotPeerStat{
				StoreID:   uint64(i % 5),
				RegionID:  uint64(i*1000 + k),
				Loads:     []float64{10, 20, 30},
				HotDegree: 10,
				AntiCount: statistics.HotRegionAntiCount, // for write
			}
			tc.hotStat.HotCache.Update(item, statistics.Write)
		}
	}
	for i := 0; i < 1000; i++ {
		co.collectHotSpotMetrics()
		co.collectSchedulerMetrics()
		co.cluster.collectClusterMetrics()
	}
	stores := co.cluster.GetStores()
	regionStats := co.cluster.RegionWriteStats()
	status1 := statistics.CollectHotPeerInfos(stores, regionStats)
	status2 := statistics.GetHotStatus(stores, co.cluster.GetStoresLoads(), regionStats, statistics.Write, co.cluster.GetOpts().IsTraceRegionFlow())
	for _, s := range status2.AsLeader {
		s.Stats = nil
	}
	for _, s := range status2.AsPeer {
		s.Stats = nil
	}
	re.Equal(status1, status2)
	co.resetHotSpotMetrics()
	co.resetSchedulerMetrics()
	co.cluster.resetClusterMetrics()
}

func prepare(setCfg func(*config.ScheduleConfig), setTc func(*testCluster), run func(*coordinator), re *require.Assertions) (*testCluster, *coordinator, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg, opt, err := newTestScheduleConfig()
	re.NoError(err)
	if setCfg != nil {
		setCfg(cfg)
	}
	tc := newTestCluster(ctx, opt)
	hbStreams := hbstream.NewTestHeartbeatStreams(ctx, tc.meta.GetId(), tc, true /* need to run */)
	if setTc != nil {
		setTc(tc)
	}
	co := newCoordinator(ctx, tc.RaftCluster, hbStreams)
	if run != nil {
		run(co)
	}
	return tc, co, func() {
		co.stop()
		co.wg.Wait()
		hbStreams.Close()
		cancel()
	}
}

func checkRegionAndOperator(re *require.Assertions, tc *testCluster, co *coordinator, regionID uint64, expectAddOperator int) {
	ops := co.checkers.CheckRegion(tc.GetRegion(regionID))
	if ops == nil {
		re.Equal(0, expectAddOperator)
	} else {
		re.Equal(expectAddOperator, co.opController.AddWaitingOperator(ops...))
	}
}

func TestCheckRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(nil, nil, nil, re)
	hbStreams, opt := co.hbStreams, tc.opt
	defer cleanup()

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	checkRegionAndOperator(re, tc, co, 1, 1)
	operatorutil.CheckAddPeer(re, co.opController.GetOperator(1), operator.OpReplica, 1)
	checkRegionAndOperator(re, tc, co, 1, 0)

	r := tc.GetRegion(1)
	p := &metapb.Peer{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner}
	r = r.Clone(
		core.WithAddPeer(p),
		core.WithPendingPeers(append(r.GetPendingPeers(), p)),
	)
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 0)

	tc = newTestCluster(ctx, opt)
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 0)
	r = r.Clone(core.WithPendingPeers(nil))
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 1)
	op := co.opController.GetOperator(1)
	re.Equal(1, op.Len())
	re.Equal(uint64(1), op.Step(0).(operator.PromoteLearner).ToStore)
	checkRegionAndOperator(re, tc, co, 1, 0)
}

func TestCheckRegionWithScheduleDeny(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)

	defer cleanup()

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	region := tc.GetRegion(1)
	re.NotNil(region)
	// test with label schedule=deny
	labelerManager := tc.GetRegionLabeler()
	labelerManager.SetLabelRule(&labeler.LabelRule{
		ID:       "schedulelabel",
		Labels:   []labeler.RegionLabel{{Key: "schedule", Value: "deny"}},
		RuleType: labeler.KeyRange,
		Data:     []interface{}{map[string]interface{}{"start_key": "", "end_key": ""}},
	})

	// should allow to do rule checker
	re.True(labelerManager.ScheduleDisabled(region))
	checkRegionAndOperator(re, tc, co, 1, 1)

	// should not allow to merge
	tc.opt.SetSplitMergeInterval(time.Duration(0))

	re.NoError(tc.addLeaderRegion(2, 2, 3, 4))
	re.NoError(tc.addLeaderRegion(3, 2, 3, 4))
	region = tc.GetRegion(2)
	re.True(labelerManager.ScheduleDisabled(region))
	checkRegionAndOperator(re, tc, co, 2, 0)

	// delete label rule, should allow to do merge
	labelerManager.DeleteLabelRule("schedulelabel")
	re.False(labelerManager.ScheduleDisabled(region))
	checkRegionAndOperator(re, tc, co, 2, 2)
}

func TestCheckerIsBusy(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0 // ensure replica checker is busy
		cfg.MergeScheduleLimit = 10
	}, nil, nil, re)
	defer cleanup()

	re.NoError(tc.addRegionStore(1, 0))
	num := 1 + typeutil.MaxUint64(tc.opt.GetReplicaScheduleLimit(), tc.opt.GetMergeScheduleLimit())
	var operatorKinds = []operator.OpKind{
		operator.OpReplica, operator.OpRegion | operator.OpMerge,
	}
	for i, operatorKind := range operatorKinds {
		for j := uint64(0); j < num; j++ {
			regionID := j + uint64(i+1)*num
			re.NoError(tc.addLeaderRegion(regionID, 1))
			switch operatorKind {
			case operator.OpReplica:
				op := newTestOperator(regionID, tc.GetRegion(regionID).GetRegionEpoch(), operatorKind)
				re.Equal(1, co.opController.AddWaitingOperator(op))
			case operator.OpRegion | operator.OpMerge:
				if regionID%2 == 1 {
					ops, err := operator.CreateMergeRegionOperator("merge-region", co.cluster, tc.GetRegion(regionID), tc.GetRegion(regionID-1), operator.OpMerge)
					re.NoError(err)
					re.Len(ops, co.opController.AddWaitingOperator(ops...))
				}
			}
		}
	}
	checkRegionAndOperator(re, tc, co, num, 0)
}

func TestReplica(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		// Turn off balance.
		cfg.LeaderScheduleLimit = 0
		cfg.RegionScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() }, re)
	defer cleanup()

	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(4, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Add peer to store 1.
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	region := tc.GetRegion(1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Peer in store 3 is down, remove peer in store 3 and add peer to store 4.
	re.NoError(tc.setStoreDown(3))
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(3),
		DownSeconds: 24 * 60 * 60,
	}
	region = region.Clone(
		core.WithDownPeers(append(region.GetDownPeers(), downPeer)),
	)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 4)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 4)
	region = region.Clone(core.WithDownPeers(nil))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Remove peer from store 4.
	re.NoError(tc.addLeaderRegion(2, 1, 2, 3, 4))
	region = tc.GetRegion(2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitRemovePeer(re, stream, region, 4)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)

	// Remove offline peer directly when it's pending.
	re.NoError(tc.addLeaderRegion(3, 1, 2, 3))
	re.NoError(tc.setStoreOffline(3))
	region = tc.GetRegion(3)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
}

func TestCheckCache(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		// Turn off replica scheduling.
		cfg.ReplicaScheduleLimit = 0
	}, nil, nil, re)
	defer cleanup()

	re.NoError(tc.addRegionStore(1, 0))
	re.NoError(tc.addRegionStore(2, 0))
	re.NoError(tc.addRegionStore(3, 0))

	// Add a peer with two replicas.
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/break-patrol", `return`))

	// case 1: operator cannot be created due to replica-schedule-limit restriction
	co.wg.Add(1)
	co.patrolRegions()
	re.Len(co.checkers.GetWaitingRegions(), 1)

	// cancel the replica-schedule-limit restriction
	cfg := tc.GetScheduleConfig()
	cfg.ReplicaScheduleLimit = 10
	tc.SetScheduleConfig(cfg)
	co.wg.Add(1)
	co.patrolRegions()
	oc := co.opController
	re.Len(oc.GetOperators(), 1)
	re.Empty(co.checkers.GetWaitingRegions())

	// case 2: operator cannot be created due to store limit restriction
	oc.RemoveOperator(oc.GetOperator(1))
	tc.SetStoreLimit(1, storelimit.AddPeer, 0)
	co.wg.Add(1)
	co.patrolRegions()
	re.Len(co.checkers.GetWaitingRegions(), 1)

	// cancel the store limit restriction
	tc.SetStoreLimit(1, storelimit.AddPeer, 10)
	time.Sleep(time.Second)
	co.wg.Add(1)
	co.patrolRegions()
	re.Len(oc.GetOperators(), 1)
	re.Empty(co.checkers.GetWaitingRegions())

	co.wg.Wait()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/break-patrol"))
}

func TestPeerState(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, func(co *coordinator) { co.run() }, re)
	defer cleanup()

	// Transfer peer from store 4 to store 1.
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addRegionStore(2, 10))
	re.NoError(tc.addRegionStore(3, 10))
	re.NoError(tc.addRegionStore(4, 40))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Wait for schedulers.
	waitOperator(re, co, 1)
	operatorutil.CheckTransferPeer(re, co.opController.GetOperator(1), operator.OpKind(0), 4, 1)

	region := tc.GetRegion(1).Clone()

	// Add new peer.
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 1)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 1)

	// If the new peer is pending, the operator will not finish.
	region = region.Clone(core.WithPendingPeers(append(region.GetPendingPeers(), region.GetStorePeer(1))))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
	re.NotNil(co.opController.GetOperator(region.GetID()))

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in store 4.
	region = region.Clone(core.WithPendingPeers(nil))
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitRemovePeer(re, stream, region, 4)
	re.NoError(tc.addLeaderRegion(1, 1, 2, 3))
	region = tc.GetRegion(1).Clone()
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitNoResponse(re, stream)
}

func TestShouldRun(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	tc.RaftCluster.coordinator = co
	defer cleanup()

	re.NoError(tc.addLeaderStore(1, 5))
	re.NoError(tc.addLeaderStore(2, 2))
	re.NoError(tc.addLeaderStore(3, 0))
	re.NoError(tc.addLeaderStore(4, 0))
	re.NoError(tc.LoadRegion(1, 1, 2, 3))
	re.NoError(tc.LoadRegion(2, 1, 2, 3))
	re.NoError(tc.LoadRegion(3, 1, 2, 3))
	re.NoError(tc.LoadRegion(4, 1, 2, 3))
	re.NoError(tc.LoadRegion(5, 1, 2, 3))
	re.NoError(tc.LoadRegion(6, 2, 1, 4))
	re.NoError(tc.LoadRegion(7, 2, 1, 4))
	re.False(co.shouldRun())
	re.Equal(2, tc.GetStoreRegionCount(4))

	testCases := []struct {
		regionID  uint64
		shouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		// store4 needs collect two region
		{6, false},
		{7, true},
	}

	for _, testCase := range testCases {
		r := tc.GetRegion(testCase.regionID)
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]))
		re.NoError(tc.processRegionHeartbeat(nr))
		re.Equal(testCase.shouldRun, co.shouldRun())
	}
	nr := &metapb.Region{Id: 6, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil)
	re.Error(tc.processRegionHeartbeat(newRegion))
	re.Equal(7, co.prepareChecker.sum)
}

func TestShouldRunWithNonLeaderRegions(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	tc.RaftCluster.coordinator = co
	defer cleanup()

	re.NoError(tc.addLeaderStore(1, 10))
	re.NoError(tc.addLeaderStore(2, 0))
	re.NoError(tc.addLeaderStore(3, 0))
	for i := 0; i < 10; i++ {
		re.NoError(tc.LoadRegion(uint64(i+1), 1, 2, 3))
	}
	re.False(co.shouldRun())
	re.Equal(10, tc.GetStoreRegionCount(1))

	testCases := []struct {
		regionID  uint64
		shouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		{6, false},
		{7, false},
		{8, false},
		{9, true},
	}

	for _, testCase := range testCases {
		r := tc.GetRegion(testCase.regionID)
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]))
		re.NoError(tc.processRegionHeartbeat(nr))
		re.Equal(testCase.shouldRun, co.shouldRun())
	}
	nr := &metapb.Region{Id: 9, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil)
	re.Error(tc.processRegionHeartbeat(newRegion))
	re.Equal(9, co.prepareChecker.sum)

	// Now, after server is prepared, there exist some regions with no leader.
	re.Equal(uint64(0), tc.GetRegion(10).GetLeader().GetStoreId())
}

func TestAddScheduler(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, func(co *coordinator) { co.run() }, re)
	defer cleanup()

	re.Len(co.schedulers, len(config.DefaultSchedulers))
	re.NoError(co.removeScheduler(schedulers.BalanceLeaderName))
	re.NoError(co.removeScheduler(schedulers.BalanceRegionName))
	re.NoError(co.removeScheduler(schedulers.HotRegionName))
	re.NoError(co.removeScheduler(schedulers.BalanceWitnessName))
	re.NoError(co.removeScheduler(schedulers.TransferWitnessLeaderName))
	re.Empty(co.schedulers)

	stream := mockhbstream.NewHeartbeatStream()

	// Add stores 1,2,3
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))
	re.NoError(tc.addLeaderStore(3, 1))
	// Add regions 1 with leader in store 1 and followers in stores 2,3
	re.NoError(tc.addLeaderRegion(1, 1, 2, 3))
	// Add regions 2 with leader in store 2 and followers in stores 1,3
	re.NoError(tc.addLeaderRegion(2, 2, 1, 3))
	// Add regions 3 with leader in store 3 and followers in stores 1,2
	re.NoError(tc.addLeaderRegion(3, 3, 1, 2))

	oc := co.opController

	// test ConfigJSONDecoder create
	bl, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigJSONDecoder([]byte("{}")))
	re.NoError(err)
	conf, err := bl.EncodeConfig()
	re.NoError(err)
	data := make(map[string]interface{})
	err = json.Unmarshal(conf, &data)
	re.NoError(err)
	batch := data["batch"].(float64)
	re.Equal(4, int(batch))

	gls, err := schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"0"}))
	re.NoError(err)
	re.NotNil(co.addScheduler(gls))
	re.NotNil(co.removeScheduler(gls.GetName()))

	gls, err = schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}))
	re.NoError(err)
	re.NoError(co.addScheduler(gls))

	hb, err := schedulers.CreateScheduler(schedulers.HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigJSONDecoder([]byte("{}")))
	re.NoError(err)
	conf, err = hb.EncodeConfig()
	re.NoError(err)
	data = make(map[string]interface{})
	re.NoError(json.Unmarshal(conf, &data))
	re.Contains(data, "enable-for-tiflash")
	re.Equal("true", data["enable-for-tiflash"].(string))

	// Transfer all leaders to store 1.
	waitOperator(re, co, 2)
	region2 := tc.GetRegion(2)
	re.NoError(dispatchHeartbeat(co, region2, stream))
	region2 = waitTransferLeader(re, stream, region2, 1)
	re.NoError(dispatchHeartbeat(co, region2, stream))
	waitNoResponse(re, stream)

	waitOperator(re, co, 3)
	region3 := tc.GetRegion(3)
	re.NoError(dispatchHeartbeat(co, region3, stream))
	region3 = waitTransferLeader(re, stream, region3, 1)
	re.NoError(dispatchHeartbeat(co, region3, stream))
	waitNoResponse(re, stream)
}

func TestPersistScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(nil, nil, func(co *coordinator) { co.run() }, re)
	hbStreams := co.hbStreams
	defer cleanup()
	defaultCount := len(config.DefaultSchedulers)
	// Add stores 1,2
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))

	re.Len(co.schedulers, defaultCount)
	oc := co.opController
	storage := tc.RaftCluster.storage

	gls1, err := schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}))
	re.NoError(err)
	re.NoError(co.addScheduler(gls1, "1"))
	evict, err := schedulers.CreateScheduler(schedulers.EvictLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.EvictLeaderType, []string{"2"}))
	re.NoError(err)
	re.NoError(co.addScheduler(evict, "2"))
	re.Len(co.schedulers, defaultCount+2)
	sches, _, err := storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Len(sches, defaultCount+2)

	// remove 5 schedulers
	re.NoError(co.removeScheduler(schedulers.BalanceLeaderName))
	re.NoError(co.removeScheduler(schedulers.BalanceRegionName))
	re.NoError(co.removeScheduler(schedulers.HotRegionName))
	re.NoError(co.removeScheduler(schedulers.BalanceWitnessName))
	re.NoError(co.removeScheduler(schedulers.TransferWitnessLeaderName))
	re.Len(co.schedulers, defaultCount-3)
	re.NoError(co.cluster.opt.Persist(storage))
	co.stop()
	co.wg.Wait()
	// make a new coordinator for testing
	// whether the schedulers added or removed in dynamic way are recorded in opt
	_, newOpt, err := newTestScheduleConfig()
	re.NoError(err)
	_, err = schedulers.CreateScheduler(schedulers.ShuffleRegionType, oc, storage, schedulers.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	// suppose we add a new default enable scheduler
	config.DefaultSchedulers = append(config.DefaultSchedulers, config.SchedulerConfig{Type: "shuffle-region"})
	defer func() {
		config.DefaultSchedulers = config.DefaultSchedulers[:len(config.DefaultSchedulers)-1]
	}()
	re.Len(newOpt.GetSchedulers(), defaultCount)
	re.NoError(newOpt.Reload(storage))
	// only remains 3 items with independent config.
	sches, _, err = storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Len(sches, 3)

	// option have 6 items because the default scheduler do not remove.
	re.Len(newOpt.GetSchedulers(), defaultCount+3)
	re.NoError(newOpt.Persist(storage))
	tc.RaftCluster.opt = newOpt

	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()
	re.Len(co.schedulers, 3)
	co.stop()
	co.wg.Wait()
	// suppose restart PD again
	_, newOpt, err = newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(storage))
	tc.RaftCluster.opt = newOpt
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()
	re.Len(co.schedulers, 3)
	bls, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	re.NoError(co.addScheduler(bls))
	brs, err := schedulers.CreateScheduler(schedulers.BalanceRegionType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	re.NoError(co.addScheduler(brs))
	re.Len(co.schedulers, defaultCount)

	// the scheduler option should contain 6 items
	// the `hot scheduler` are disabled
	re.Len(co.cluster.opt.GetSchedulers(), defaultCount+3)
	re.NoError(co.removeScheduler(schedulers.GrantLeaderName))
	// the scheduler that is not enable by default will be completely deleted
	re.Len(co.cluster.opt.GetSchedulers(), defaultCount+2)
	re.Len(co.schedulers, 4)
	re.NoError(co.cluster.opt.Persist(co.cluster.storage))
	co.stop()
	co.wg.Wait()
	_, newOpt, err = newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(co.cluster.storage))
	tc.RaftCluster.opt = newOpt
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)

	co.run()
	re.Len(co.schedulers, defaultCount-1)
	re.NoError(co.removeScheduler(schedulers.EvictLeaderName))
	re.Len(co.schedulers, defaultCount-2)
}

func TestDenyScheduler(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, func(co *coordinator) {
		labelerManager := co.cluster.GetRegionLabeler()
		labelerManager.SetLabelRule(&labeler.LabelRule{
			ID:       "schedulelabel",
			Labels:   []labeler.RegionLabel{{Key: "schedule", Value: "deny"}},
			RuleType: labeler.KeyRange,
			Data:     []interface{}{map[string]interface{}{"start_key": "", "end_key": ""}},
		})
		co.run()
	}, re)
	defer cleanup()

	re.Len(co.schedulers, len(config.DefaultSchedulers))

	// Transfer peer from store 4 to store 1 if not set deny.
	re.NoError(tc.addRegionStore(4, 40))
	re.NoError(tc.addRegionStore(3, 30))
	re.NoError(tc.addRegionStore(2, 20))
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))

	// Transfer leader from store 4 to store 2 if not set deny.
	re.NoError(tc.updateLeaderCount(4, 1000))
	re.NoError(tc.updateLeaderCount(3, 50))
	re.NoError(tc.updateLeaderCount(2, 20))
	re.NoError(tc.updateLeaderCount(1, 10))
	re.NoError(tc.addLeaderRegion(2, 4, 3, 2))

	// there should no balance leader/region operator
	for i := 0; i < 10; i++ {
		re.Nil(co.opController.GetOperator(1))
		re.Nil(co.opController.GetOperator(2))
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRemoveScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() }, re)
	hbStreams := co.hbStreams
	defer cleanup()

	// Add stores 1,2
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))
	defaultCount := len(config.DefaultSchedulers)

	re.Len(co.schedulers, defaultCount)
	oc := co.opController
	storage := tc.RaftCluster.storage

	gls1, err := schedulers.CreateScheduler(schedulers.GrantLeaderType, oc, storage, schedulers.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}))
	re.NoError(err)
	re.NoError(co.addScheduler(gls1, "1"))
	re.Len(co.schedulers, defaultCount+1)
	sches, _, err := storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Len(sches, defaultCount+1)

	// remove all schedulers
	re.NoError(co.removeScheduler(schedulers.BalanceLeaderName))
	re.NoError(co.removeScheduler(schedulers.BalanceRegionName))
	re.NoError(co.removeScheduler(schedulers.HotRegionName))
	re.NoError(co.removeScheduler(schedulers.GrantLeaderName))
	re.NoError(co.removeScheduler(schedulers.BalanceWitnessName))
	re.NoError(co.removeScheduler(schedulers.TransferWitnessLeaderName))
	// all removed
	sches, _, err = storage.LoadAllScheduleConfig()
	re.NoError(err)
	re.Empty(sches)
	re.Empty(co.schedulers)
	re.NoError(co.cluster.opt.Persist(co.cluster.storage))
	co.stop()
	co.wg.Wait()

	// suppose restart PD again
	_, newOpt, err := newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(tc.storage))
	tc.RaftCluster.opt = newOpt
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()
	re.Empty(co.schedulers)
	// the option remains default scheduler

	re.Len(co.cluster.opt.GetSchedulers(), defaultCount)
	co.stop()
	co.wg.Wait()
}

func TestRestart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		// Turn off balance, we test add replica only.
		cfg.LeaderScheduleLimit = 0
		cfg.RegionScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() }, re)
	hbStreams := co.hbStreams
	defer cleanup()

	// Add 3 stores (1, 2, 3) and a region with 1 replica on store 1.
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addLeaderRegion(1, 1))
	region := tc.GetRegion(1)
	co.prepareChecker.collect(region)

	// Add 1 replica on store 2.
	stream := mockhbstream.NewHeartbeatStream()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 2)
	co.stop()
	co.wg.Wait()

	// Recreate coordinator then add another replica on store 3.
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.prepareChecker.collect(region)
	co.run()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 3)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitPromoteLearner(re, stream, region, 3)
}

func TestPauseScheduler(t *testing.T) {
	re := require.New(t)

	_, co, cleanup := prepare(nil, nil, func(co *coordinator) { co.run() }, re)
	defer cleanup()
	_, err := co.isSchedulerAllowed("test")
	re.Error(err)
	co.pauseOrResumeScheduler(schedulers.BalanceLeaderName, 60)
	paused, _ := co.isSchedulerPaused(schedulers.BalanceLeaderName)
	re.True(paused)
	pausedAt, err := co.getPausedSchedulerDelayAt(schedulers.BalanceLeaderName)
	re.NoError(err)
	resumeAt, err := co.getPausedSchedulerDelayUntil(schedulers.BalanceLeaderName)
	re.NoError(err)
	re.Equal(int64(60), resumeAt-pausedAt)
	allowed, _ := co.isSchedulerAllowed(schedulers.BalanceLeaderName)
	re.False(allowed)
}

func BenchmarkPatrolRegion(b *testing.B) {
	re := require.New(b)

	mergeLimit := uint64(4100)
	regionNum := 10000

	tc, co, cleanup := prepare(func(cfg *config.ScheduleConfig) {
		cfg.MergeScheduleLimit = mergeLimit
	}, nil, nil, re)
	defer cleanup()

	tc.opt.SetSplitMergeInterval(time.Duration(0))
	for i := 1; i < 4; i++ {
		if err := tc.addRegionStore(uint64(i), regionNum, 96); err != nil {
			return
		}
	}
	for i := 0; i < regionNum; i++ {
		if err := tc.addLeaderRegion(uint64(i), 1, 2, 3); err != nil {
			return
		}
	}

	listen := make(chan int)
	go func() {
		oc := co.opController
		listen <- 0
		for {
			if oc.OperatorCount(operator.OpMerge) == mergeLimit {
				co.cancel()
				return
			}
		}
	}()
	<-listen

	co.wg.Add(1)
	b.ResetTimer()
	co.patrolRegions()
}

func waitOperator(re *require.Assertions, co *coordinator, regionID uint64) {
	testutil.Eventually(re, func() bool {
		return co.opController.GetOperator(regionID) != nil
	})
}

func TestOperatorCount(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.opController
	re.Equal(uint64(0), oc.OperatorCount(operator.OpLeader))
	re.Equal(uint64(0), oc.OperatorCount(operator.OpRegion))

	re.NoError(tc.addLeaderRegion(1, 1))
	re.NoError(tc.addLeaderRegion(2, 2))
	{
		op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op1)
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader)) // 1:leader
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op2)
		re.Equal(uint64(2), oc.OperatorCount(operator.OpLeader)) // 1:leader, 2:leader
		re.True(oc.RemoveOperator(op1))
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader)) // 2:leader
	}

	{
		op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
		oc.AddWaitingOperator(op1)
		re.Equal(uint64(1), oc.OperatorCount(operator.OpRegion)) // 1:region 2:leader
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader))
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion)
		op2.SetPriorityLevel(constant.High)
		oc.AddWaitingOperator(op2)
		re.Equal(uint64(2), oc.OperatorCount(operator.OpRegion)) // 1:region 2:region
		re.Equal(uint64(0), oc.OperatorCount(operator.OpLeader))
	}
}

func TestStoreOverloaded(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.opController
	lb, err := schedulers.CreateScheduler(schedulers.BalanceRegionType, oc, tc.storage, schedulers.ConfigSliceDecoder(schedulers.BalanceRegionType, []string{"", ""}))
	re.NoError(err)
	opt := tc.GetOpts()
	re.NoError(tc.addRegionStore(4, 100))
	re.NoError(tc.addRegionStore(3, 100))
	re.NoError(tc.addRegionStore(2, 100))
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(60))
	tc.putRegion(region)
	start := time.Now()
	{
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.Len(ops, 1)
		op1 := ops[0]
		re.NotNil(op1)
		re.True(oc.AddOperator(op1))
		re.True(oc.RemoveOperator(op1))
	}
	for {
		time.Sleep(time.Millisecond * 10)
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		if time.Since(start) > time.Second {
			break
		}
		re.Empty(ops)
	}

	// reset all stores' limit
	// scheduling one time needs 1/10 seconds
	opt.SetAllStoresLimit(storelimit.AddPeer, 600)
	opt.SetAllStoresLimit(storelimit.RemovePeer, 600)
	time.Sleep(time.Second)
	for i := 0; i < 10; i++ {
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.Len(ops, 1)
		op := ops[0]
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
	// sleep 1 seconds to make sure that the token is filled up
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.Greater(len(ops), 0)
	}
}

func TestStoreOverloadedWithReplace(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.opController
	lb, err := schedulers.CreateScheduler(schedulers.BalanceRegionType, oc, tc.storage, schedulers.ConfigSliceDecoder(schedulers.BalanceRegionType, []string{"", ""}))
	re.NoError(err)

	re.NoError(tc.addRegionStore(4, 100))
	re.NoError(tc.addRegionStore(3, 100))
	re.NoError(tc.addRegionStore(2, 100))
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))
	re.NoError(tc.addLeaderRegion(2, 1, 3, 4))
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(60))
	tc.putRegion(region)
	region = tc.GetRegion(2).Clone(core.SetApproximateSize(60))
	tc.putRegion(region)
	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 1})
	re.True(oc.AddOperator(op1))
	op2 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 2})
	op2.SetPriorityLevel(constant.High)
	re.True(oc.AddOperator(op2))
	op3 := newTestOperator(1, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 3})
	re.False(oc.AddOperator(op3))
	ops, _ := lb.Schedule(tc, false /* dryRun */)
	re.Empty(ops)
	// sleep 2 seconds to make sure that token is filled up
	time.Sleep(2 * time.Second)
	ops, _ = lb.Schedule(tc, false /* dryRun */)
	re.Greater(len(ops), 0)
}

func TestDownStoreLimit(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.opController
	rc := co.checkers.GetRuleChecker()

	tc.addRegionStore(1, 100)
	tc.addRegionStore(2, 100)
	tc.addRegionStore(3, 100)
	tc.addLeaderRegion(1, 1, 2, 3)

	region := tc.GetRegion(1)
	tc.setStoreDown(1)
	tc.SetStoreLimit(1, storelimit.RemovePeer, 1)

	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        region.GetStorePeer(1),
			DownSeconds: 24 * 60 * 60,
		},
	}), core.SetApproximateSize(1))
	tc.putRegion(region)
	for i := uint64(1); i < 20; i++ {
		tc.addRegionStore(i+3, 100)
		op := rc.Check(region)
		re.NotNil(op)
		re.True(oc.AddOperator(op))
		oc.RemoveOperator(op)
	}

	region = region.Clone(core.SetApproximateSize(100))
	tc.putRegion(region)
	for i := uint64(20); i < 25; i++ {
		tc.addRegionStore(i+3, 100)
		op := rc.Check(region)
		re.NotNil(op)
		re.True(oc.AddOperator(op))
		oc.RemoveOperator(op)
	}
}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedulers.Scheduler
	limit   uint64
	counter *operator.Controller
	kind    operator.OpKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster sche.ClusterInformer) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func TestController(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.opController

	re.NoError(tc.addLeaderRegion(1, 1))
	re.NoError(tc.addLeaderRegion(2, 2))
	scheduler, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := newScheduleController(co, lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		re.Equal(i, sc.GetInterval())
		re.Empty(sc.Schedule(false))
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	{
		re.True(sc.AllowSchedule(false))
		op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op1))
		// count = 1
		re.True(sc.AllowSchedule(false))
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op2))
		// count = 2
		re.False(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op1))
		// count = 1
		re.True(sc.AllowSchedule(false))
	}

	op11 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	// add a PriorityKind operator will remove old operator
	{
		op3 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpHotRegion)
		op3.SetPriorityLevel(constant.High)
		re.Equal(1, oc.AddWaitingOperator(op11))
		re.False(sc.AllowSchedule(false))
		re.Equal(1, oc.AddWaitingOperator(op3))
		re.True(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op3))
	}

	// add a admin operator will remove old operator
	{
		op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op2))
		re.False(sc.AllowSchedule(false))
		op4 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpAdmin)
		op4.SetPriorityLevel(constant.High)
		re.Equal(1, oc.AddWaitingOperator(op4))
		re.True(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op4))
	}

	// test wrong region id.
	{
		op5 := newTestOperator(3, &metapb.RegionEpoch{}, operator.OpHotRegion)
		re.Equal(0, oc.AddWaitingOperator(op5))
	}

	// test wrong region epoch.
	re.True(oc.RemoveOperator(op11))
	epoch := &metapb.RegionEpoch{
		Version: tc.GetRegion(1).GetRegionEpoch().GetVersion() + 1,
		ConfVer: tc.GetRegion(1).GetRegionEpoch().GetConfVer(),
	}
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		re.Equal(0, oc.AddWaitingOperator(op6))
	}
	epoch.Version--
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op6))
		re.True(oc.RemoveOperator(op6))
	}
}

func TestInterval(t *testing.T) {
	re := require.New(t)

	_, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()

	lb, err := schedulers.CreateScheduler(schedulers.BalanceLeaderType, co.opController, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"", ""}))
	re.NoError(err)
	sc := newScheduleController(co, lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.nextInterval = schedulers.MinScheduleInterval
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			re.Empty(sc.Schedule(false))
		}
		re.Less(sc.GetInterval(), time.Second*time.Duration(n/2))
	}
}

func waitAddLearner(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_AddLearnerNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithAddPeer(res.GetChangePeer().GetPeer()),
		core.WithIncConfVer(),
	)
}

func waitPromoteLearner(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_AddNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	// Remove learner than add voter.
	return region.Clone(
		core.WithRemoveStorePeer(storeID),
		core.WithAddPeer(res.GetChangePeer().GetPeer()),
	)
}

func waitRemovePeer(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_RemoveNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithRemoveStorePeer(storeID),
		core.WithIncConfVer(),
	)
}

func waitTransferLeader(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if res = stream.Recv(); res != nil {
			if res.GetRegionId() == region.GetID() {
				for _, peer := range append(res.GetTransferLeader().GetPeers(), res.GetTransferLeader().GetPeer()) {
					if peer.GetStoreId() == storeID {
						return true
					}
				}
			}
		}
		return false
	})
	return region.Clone(
		core.WithLeader(region.GetStorePeer(storeID)),
	)
}

func waitNoResponse(re *require.Assertions, stream mockhbstream.HeartbeatStream) {
	testutil.Eventually(re, func() bool {
		res := stream.Recv()
		return res == nil
	})
}
