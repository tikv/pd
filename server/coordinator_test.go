// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/mock/mockhbstream"
	"github.com/pingcap/pd/pkg/mock/mockid"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/config"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/id"
	"github.com/pingcap/pd/server/kv"
	"github.com/pingcap/pd/server/namespace"
	syncer "github.com/pingcap/pd/server/region_syncer"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedulers"
)

func newTestScheduleConfig() (*config.ScheduleConfig, *config.ScheduleOption, error) {
	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	cfg.Schedule.StoreBalanceRate = 60
	if err := cfg.Adjust(nil); err != nil {
		return nil, nil, err
	}
	opt := config.NewScheduleOption(cfg)
	opt.SetClusterVersion(MinSupportedVersion(Version2_0))
	return &cfg.Schedule, opt, nil
}

func newTestOperator(regionID uint64, regionEpoch *metapb.RegionEpoch, kind operator.OpKind, steps ...operator.OpStep) *operator.Operator {
	return operator.NewOperator("test", "test", regionID, regionEpoch, kind, steps...)
}

type testCluster struct {
	*RaftCluster
}

func newTestCluster(opt *config.ScheduleOption) *testCluster {
	cluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))
	return &testCluster{RaftCluster: cluster}
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:      []byte(fmt.Sprintf("%20d", regionID+1)),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
	}
}

func (c *testCluster) addRegionStore(storeID uint64, regionCount int) error {
	stats := &pdpb.StoreStats{}
	stats.Capacity = 1000 * (1 << 20)
	stats.Available = stats.Capacity - uint64(regionCount)*10
	newStore := core.NewStoreInfo(&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) error {
	region := newTestRegionMeta(regionID)
	leader, _ := c.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := c.AllocPeer(id)
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
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreDown(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreOffline(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) LoadRegion(regionID uint64, followerIds ...uint64) error {
	//  regions load from etcd will have no leader
	region := newTestRegionMeta(regionID)
	region.Peers = []*metapb.Peer{}
	for _, id := range followerIds {
		peer, _ := c.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	return c.putRegion(core.NewRegionInfo(region, nil))
}

var _ = Suite(&testCoordinatorSuite{})

type testCoordinatorSuite struct{}

func (s *testCoordinatorSuite) TestBasic(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	oc := co.opController

	c.Assert(tc.addLeaderRegion(1, 1), IsNil)

	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	oc.AddWaitingOperator(op1)
	c.Assert(oc.OperatorCount(op1.Kind()), Equals, uint64(1))
	c.Assert(oc.GetOperator(1).RegionID(), Equals, op1.RegionID())

	// Region 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op2)
	c.Assert(oc.OperatorCount(op2.Kind()), Equals, uint64(0))

	// Remove the operator manually, then we can add a new operator.
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	oc.AddWaitingOperator(op2)
	c.Assert(oc.OperatorCount(op2.Kind()), Equals, uint64(1))
	c.Assert(oc.GetOperator(1).RegionID(), Equals, op2.RegionID())
}

func (s *testCoordinatorSuite) TestDispatch(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	c.Assert(tc.addRegionStore(4, 40), IsNil)
	c.Assert(tc.addRegionStore(3, 30), IsNil)
	c.Assert(tc.addRegionStore(2, 20), IsNil)
	c.Assert(tc.addRegionStore(1, 10), IsNil)
	c.Assert(tc.addLeaderRegion(1, 2, 3, 4), IsNil)

	// Transfer leader from store 4 to store 2.
	c.Assert(tc.updateLeaderCount(4, 50), IsNil)
	c.Assert(tc.updateLeaderCount(3, 30), IsNil)
	c.Assert(tc.updateLeaderCount(2, 20), IsNil)
	c.Assert(tc.updateLeaderCount(1, 10), IsNil)
	c.Assert(tc.addLeaderRegion(2, 4, 3, 2), IsNil)

	// Wait for schedule and turn off balance.
	waitOperator(c, co, 1)
	testutil.CheckTransferPeer(c, co.opController.GetOperator(1), operator.OpBalance, 4, 1)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	waitOperator(c, co, 2)
	testutil.CheckTransferLeader(c, co.opController.GetOperator(2), operator.OpBalance, 4, 2)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)

	stream := mockhbstream.NewHeartbeatStream()

	// Transfer peer.
	region := tc.GetRegion(1).Clone()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddLearner(c, stream, region, 1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitPromoteLearner(c, stream, region, 1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitRemovePeer(c, stream, region, 4)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)

	// Transfer leader.
	region = tc.GetRegion(2).Clone()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitTransferLeader(c, stream, region, 2)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
}

func dispatchHeartbeat(c *C, co *coordinator, region *core.RegionInfo, stream mockhbstream.HeartbeatStream) error {
	co.hbStreams.bindStream(region.GetLeader().GetStoreId(), stream)
	if err := co.cluster.putRegion(region.Clone()); err != nil {
		return err
	}
	co.opController.Dispatch(region, schedule.DispatchFromHeartBeat)
	return nil
}

func (s *testCoordinatorSuite) TestCollectMetrics(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	// Make sure there are no problem when concurrent write and read
	for i := 0; i <= 10; i++ {
		go func(i int) {
			for j := 0; j < 10000; j++ {
				c.Assert(tc.addRegionStore(uint64(i%5), rand.Intn(200)), IsNil)
			}
		}(i)
	}
	for i := 0; i < 1000; i++ {
		co.collectHotSpotMetrics()
		co.collectSchedulerMetrics()
		co.cluster.collectClusterMetrics()
	}
}

func (s *testCoordinatorSuite) TestCheckRegion(c *C) {
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.DisableLearner = false
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()

	c.Assert(tc.addRegionStore(4, 4), IsNil)
	c.Assert(tc.addRegionStore(3, 3), IsNil)
	c.Assert(tc.addRegionStore(2, 2), IsNil)
	c.Assert(tc.addRegionStore(1, 1), IsNil)
	c.Assert(tc.addLeaderRegion(1, 2, 3), IsNil)
	c.Assert(co.checkRegion(tc.GetRegion(1)), IsTrue)
	waitOperator(c, co, 1)
	testutil.CheckAddPeer(c, co.opController.GetOperator(1), operator.OpReplica, 1)
	c.Assert(co.checkRegion(tc.GetRegion(1)), IsFalse)

	r := tc.GetRegion(1)
	p := &metapb.Peer{Id: 1, StoreId: 1, IsLearner: true}
	r = r.Clone(
		core.WithAddPeer(p),
		core.WithPendingPeers(append(r.GetPendingPeers(), p)),
	)
	c.Assert(tc.putRegion(r), IsNil)
	c.Assert(co.checkRegion(tc.GetRegion(1)), IsFalse)
	co.stop()
	co.wg.Wait()

	// new cluster with learner disabled
	cfg.DisableLearner = true
	tc = newTestCluster(opt)
	co = newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	c.Assert(tc.addRegionStore(4, 4), IsNil)
	c.Assert(tc.addRegionStore(3, 3), IsNil)
	c.Assert(tc.addRegionStore(2, 2), IsNil)
	c.Assert(tc.addRegionStore(1, 1), IsNil)
	c.Assert(tc.putRegion(r), IsNil)
	c.Assert(co.checkRegion(tc.GetRegion(1)), IsFalse)
	r = r.Clone(core.WithPendingPeers(nil))
	c.Assert(tc.putRegion(r), IsNil)
	c.Assert(co.checkRegion(tc.GetRegion(1)), IsTrue)
	waitOperator(c, co, 1)
	op := co.opController.GetOperator(1)
	c.Assert(op.Len(), Equals, 1)
	c.Assert(op.Step(0).(operator.PromoteLearner).ToStore, Equals, uint64(1))
	c.Assert(co.checkRegion(tc.GetRegion(1)), IsFalse)
}

func (s *testCoordinatorSuite) TestReplica(c *C) {
	// Turn off balance.
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.LeaderScheduleLimit = 0
	cfg.RegionScheduleLimit = 0

	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	c.Assert(tc.addRegionStore(1, 1), IsNil)
	c.Assert(tc.addRegionStore(2, 2), IsNil)
	c.Assert(tc.addRegionStore(3, 3), IsNil)
	c.Assert(tc.addRegionStore(4, 4), IsNil)

	stream := mockhbstream.NewHeartbeatStream()

	// Add peer to store 1.
	c.Assert(tc.addLeaderRegion(1, 2, 3), IsNil)
	region := tc.GetRegion(1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddLearner(c, stream, region, 1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitPromoteLearner(c, stream, region, 1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)

	// Peer in store 3 is down, remove peer in store 3 and add peer to store 4.
	c.Assert(tc.setStoreDown(3), IsNil)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(3),
		DownSeconds: 24 * 60 * 60,
	}
	region = region.Clone(
		core.WithDownPeers(append(region.GetDownPeers(), downPeer)),
	)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddLearner(c, stream, region, 4)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitPromoteLearner(c, stream, region, 4)
	region = region.Clone(core.WithDownPeers(nil))
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)

	// Remove peer from store 4.
	c.Assert(tc.addLeaderRegion(2, 1, 2, 3, 4), IsNil)
	region = tc.GetRegion(2)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitRemovePeer(c, stream, region, 4)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)

	// Remove offline peer directly when it's pending.
	c.Assert(tc.addLeaderRegion(3, 1, 2, 3), IsNil)
	c.Assert(tc.setStoreOffline(3), IsNil)
	region = tc.GetRegion(3)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestPeerState(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	c.Assert(tc.addRegionStore(1, 10), IsNil)
	c.Assert(tc.addRegionStore(2, 20), IsNil)
	c.Assert(tc.addRegionStore(3, 30), IsNil)
	c.Assert(tc.addRegionStore(4, 40), IsNil)
	c.Assert(tc.addLeaderRegion(1, 2, 3, 4), IsNil)

	stream := mockhbstream.NewHeartbeatStream()

	// Wait for schedule.
	waitOperator(c, co, 1)
	testutil.CheckTransferPeer(c, co.opController.GetOperator(1), operator.OpBalance, 4, 1)

	region := tc.GetRegion(1).Clone()

	// Add new peer.
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddLearner(c, stream, region, 1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitPromoteLearner(c, stream, region, 1)

	// If the new peer is pending, the operator will not finish.
	region = region.Clone(core.WithPendingPeers(append(region.GetPendingPeers(), region.GetStorePeer(1))))
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
	c.Assert(co.opController.GetOperator(region.GetID()), NotNil)

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in store 4.
	region = region.Clone(core.WithPendingPeers(nil))
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitRemovePeer(c, stream, region, 4)
	c.Assert(tc.addLeaderRegion(1, 1, 2, 3), IsNil)
	region = tc.GetRegion(1).Clone()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestShouldRun(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)

	c.Assert(tc.addLeaderStore(1, 5), IsNil)
	c.Assert(tc.addLeaderStore(2, 2), IsNil)
	c.Assert(tc.addLeaderStore(3, 0), IsNil)
	c.Assert(tc.addLeaderStore(4, 0), IsNil)
	c.Assert(tc.LoadRegion(1, 1, 2, 3), IsNil)
	c.Assert(tc.LoadRegion(2, 1, 2, 3), IsNil)
	c.Assert(tc.LoadRegion(3, 1, 2, 3), IsNil)
	c.Assert(tc.LoadRegion(4, 1, 2, 3), IsNil)
	c.Assert(tc.LoadRegion(5, 1, 2, 3), IsNil)
	c.Assert(tc.LoadRegion(6, 2, 1, 4), IsNil)
	c.Assert(tc.LoadRegion(7, 2, 1, 4), IsNil)
	c.Assert(co.shouldRun(), IsFalse)
	c.Assert(tc.core.Regions.GetStoreRegionCount(4), Equals, 2)

	tbl := []struct {
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

	for _, t := range tbl {
		r := tc.GetRegion(t.regionID)
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]))
		c.Assert(tc.processRegionHeartbeat(nr), IsNil)
		c.Assert(co.shouldRun(), Equals, t.shouldRun)
	}
	nr := &metapb.Region{Id: 6, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil)
	c.Assert(tc.processRegionHeartbeat(newRegion), NotNil)
	c.Assert(co.cluster.prepareChecker.sum, Equals, 7)

}
func (s *testCoordinatorSuite) TestShouldRunWithNonLeaderRegions(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)

	c.Assert(tc.addLeaderStore(1, 10), IsNil)
	c.Assert(tc.addLeaderStore(2, 0), IsNil)
	c.Assert(tc.addLeaderStore(3, 0), IsNil)
	for i := 0; i < 10; i++ {
		c.Assert(tc.LoadRegion(uint64(i+1), 1, 2, 3), IsNil)
	}
	c.Assert(co.shouldRun(), IsFalse)
	c.Assert(tc.core.Regions.GetStoreRegionCount(1), Equals, 10)

	tbl := []struct {
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
		{8, true},
	}

	for _, t := range tbl {
		r := tc.GetRegion(t.regionID)
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]))
		c.Assert(tc.processRegionHeartbeat(nr), IsNil)
		c.Assert(co.shouldRun(), Equals, t.shouldRun)
	}
	nr := &metapb.Region{Id: 8, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil)
	c.Assert(tc.processRegionHeartbeat(newRegion), NotNil)
	c.Assert(co.cluster.prepareChecker.sum, Equals, 8)

	// Now, after server is prepared, there exist some regions with no leader.
	c.Assert(tc.GetRegion(9).GetLeader().GetStoreId(), Equals, uint64(0))
	c.Assert(tc.GetRegion(10).GetLeader().GetStoreId(), Equals, uint64(0))
}

func (s *testCoordinatorSuite) TestAddScheduler(c *C) {
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.ReplicaScheduleLimit = 0

	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()
	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	c.Assert(co.schedulers, HasLen, 4)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-hot-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("label-scheduler"), IsNil)
	c.Assert(co.schedulers, HasLen, 0)

	stream := mockhbstream.NewHeartbeatStream()

	// Add stores 1,2,3
	c.Assert(tc.addLeaderStore(1, 1), IsNil)
	c.Assert(tc.addLeaderStore(2, 1), IsNil)
	c.Assert(tc.addLeaderStore(3, 1), IsNil)
	// Add regions 1 with leader in store 1 and followers in stores 2,3
	c.Assert(tc.addLeaderRegion(1, 1, 2, 3), IsNil)
	// Add regions 2 with leader in store 2 and followers in stores 1,3
	c.Assert(tc.addLeaderRegion(2, 2, 1, 3), IsNil)
	// Add regions 3 with leader in store 3 and followers in stores 1,2
	c.Assert(tc.addLeaderRegion(3, 3, 1, 2), IsNil)

	oc := co.opController
	gls, err := schedule.CreateScheduler("grant-leader", oc, "0")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls), NotNil)
	c.Assert(co.removeScheduler(gls.GetName()), NotNil)

	gls, err = schedule.CreateScheduler("grant-leader", oc, "1")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls), IsNil)

	// Transfer all leaders to store 1.
	waitOperator(c, co, 2)
	region2 := tc.GetRegion(2)
	c.Assert(dispatchHeartbeat(c, co, region2, stream), IsNil)
	region2 = waitTransferLeader(c, stream, region2, 1)
	c.Assert(dispatchHeartbeat(c, co, region2, stream), IsNil)
	waitNoResponse(c, stream)

	waitOperator(c, co, 3)
	region3 := tc.GetRegion(3)
	c.Assert(dispatchHeartbeat(c, co, region3, stream), IsNil)
	region3 = waitTransferLeader(c, stream, region3, 1)
	c.Assert(dispatchHeartbeat(c, co, region3, stream), IsNil)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestPersistScheduler(c *C) {
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.ReplicaScheduleLimit = 0

	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()

	// Add stores 1,2
	c.Assert(tc.addLeaderStore(1, 1), IsNil)
	c.Assert(tc.addLeaderStore(2, 1), IsNil)

	c.Assert(co.schedulers, HasLen, 4)
	oc := co.opController
	gls1, err := schedule.CreateScheduler("grant-leader", oc, "1")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls1, "1"), IsNil)
	gls2, err := schedule.CreateScheduler("grant-leader", oc, "2")
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(gls2, "2"), IsNil)
	c.Assert(co.schedulers, HasLen, 6)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-hot-region-scheduler"), IsNil)
	c.Assert(co.removeScheduler("label-scheduler"), IsNil)
	c.Assert(co.schedulers, HasLen, 2)
	c.Assert(co.cluster.opt.Persist(co.cluster.storage), IsNil)
	co.stop()
	co.wg.Wait()
	// make a new coordinator for testing
	// whether the schedulers added or removed in dynamic way are recorded in opt
	_, newOpt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	_, err = schedule.CreateScheduler("adjacent-region", oc)
	c.Assert(err, IsNil)
	// suppose we add a new default enable scheduler
	newOpt.AddSchedulerCfg("adjacent-region", []string{})
	c.Assert(newOpt.GetSchedulers(), HasLen, 5)
	c.Assert(newOpt.Reload(co.cluster.storage), IsNil)
	c.Assert(newOpt.GetSchedulers(), HasLen, 7)
	tc.RaftCluster.opt = newOpt

	co = newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	c.Assert(co.schedulers, HasLen, 3)
	co.stop()
	co.wg.Wait()
	// suppose restart PD again
	_, newOpt, err = newTestScheduleConfig()
	c.Assert(err, IsNil)
	c.Assert(newOpt.Reload(tc.storage), IsNil)
	tc.RaftCluster.opt = newOpt
	co = newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	c.Assert(co.schedulers, HasLen, 3)
	bls, err := schedule.CreateScheduler("balance-leader", oc)
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(bls), IsNil)
	brs, err := schedule.CreateScheduler("balance-region", oc)
	c.Assert(err, IsNil)
	c.Assert(co.addScheduler(brs), IsNil)
	c.Assert(co.schedulers, HasLen, 5)
	// the scheduler option should contain 7 items
	// the `hot scheduler` and `label scheduler` are disabled
	c.Assert(co.cluster.opt.GetSchedulers(), HasLen, 7)
	c.Assert(co.removeScheduler("grant-leader-scheduler-1"), IsNil)
	// the scheduler that is not enable by default will be completely deleted
	c.Assert(co.cluster.opt.GetSchedulers(), HasLen, 6)
	c.Assert(co.schedulers, HasLen, 4)
	c.Assert(co.cluster.opt.Persist(co.cluster.storage), IsNil)
	co.stop()
	co.wg.Wait()

	_, newOpt, err = newTestScheduleConfig()
	c.Assert(err, IsNil)
	c.Assert(newOpt.Reload(co.cluster.storage), IsNil)
	tc.RaftCluster.opt = newOpt
	co = newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)

	co.run()
	defer co.wg.Wait()
	defer co.stop()
	c.Assert(co.schedulers, HasLen, 4)
	c.Assert(co.removeScheduler("grant-leader-scheduler-2"), IsNil)
	c.Assert(co.schedulers, HasLen, 3)
}

func (s *testCoordinatorSuite) TestRestart(c *C) {
	// Turn off balance, we test add replica only.
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.LeaderScheduleLimit = 0
	cfg.RegionScheduleLimit = 0

	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	// Add 3 stores (1, 2, 3) and a region with 1 replica on store 1.
	c.Assert(tc.addRegionStore(1, 1), IsNil)
	c.Assert(tc.addRegionStore(2, 2), IsNil)
	c.Assert(tc.addRegionStore(3, 3), IsNil)
	c.Assert(tc.addLeaderRegion(1, 1), IsNil)
	region := tc.GetRegion(1)
	tc.prepareChecker.collect(region)

	// Add 1 replica on store 2.
	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	stream := mockhbstream.NewHeartbeatStream()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddLearner(c, stream, region, 2)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitPromoteLearner(c, stream, region, 2)
	co.stop()
	co.wg.Wait()

	// Recreate coodinator then add another replica on store 3.
	co = newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	co.run()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddLearner(c, stream, region, 3)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitPromoteLearner(c, stream, region, 3)
	co.stop()
	co.wg.Wait()
}

func waitOperator(c *C, co *coordinator, regionID uint64) {
	testutil.WaitUntil(c, func(c *C) bool {
		return co.opController.GetOperator(regionID) != nil
	})
}

var _ = Suite(&testOperatorControllerSuite{})

type testOperatorControllerSuite struct{}

func (s *testOperatorControllerSuite) TestOperatorCount(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := mockhbstream.NewHeartbeatStreams(tc.RaftCluster.getClusterID())

	oc := schedule.NewOperatorController(tc.RaftCluster, hbStreams)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(0))
	c.Assert(oc.OperatorCount(operator.OpRegion), Equals, uint64(0))

	c.Assert(tc.addLeaderRegion(1, 1), IsNil)
	c.Assert(tc.addLeaderRegion(2, 2), IsNil)
	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	oc.AddWaitingOperator(op1)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(1)) // 1:leader
	op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
	oc.AddWaitingOperator(op2)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(2)) // 1:leader, 2:leader
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(1)) // 2:leader

	op1 = newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op1)
	c.Assert(oc.OperatorCount(operator.OpRegion), Equals, uint64(1)) // 1:region 2:leader
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(1))
	op2 = newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion)
	op2.SetPriorityLevel(core.HighPriority)
	oc.AddWaitingOperator(op2)
	c.Assert(oc.OperatorCount(operator.OpRegion), Equals, uint64(2)) // 1:region 2:region
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(0))
}

func (s *testOperatorControllerSuite) TestStoreOverloaded(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()
	oc := schedule.NewOperatorController(tc.RaftCluster, hbStreams)
	lb, err := schedule.CreateScheduler("balance-region", oc)
	c.Assert(err, IsNil)

	c.Assert(tc.addRegionStore(4, 40), IsNil)
	c.Assert(tc.addRegionStore(3, 40), IsNil)
	c.Assert(tc.addRegionStore(2, 40), IsNil)
	c.Assert(tc.addRegionStore(1, 10), IsNil)
	c.Assert(tc.addLeaderRegion(1, 2, 3, 4), IsNil)
	op1 := lb.Schedule(tc)[0]
	c.Assert(op1, NotNil)
	c.Assert(oc.AddOperator(op1), IsTrue)
	for i := 0; i < 10; i++ {
		c.Assert(lb.Schedule(tc), IsNil)
	}
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	time.Sleep(1 * time.Second)
	for i := 0; i < 100; i++ {
		c.Assert(lb.Schedule(tc), NotNil)
	}
}

func (s *testOperatorControllerSuite) TestStoreOverloadedWithReplace(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()
	oc := schedule.NewOperatorController(tc.RaftCluster, hbStreams)
	lb, err := schedule.CreateScheduler("balance-region", oc)
	c.Assert(err, IsNil)

	c.Assert(tc.addRegionStore(4, 40), IsNil)
	c.Assert(tc.addRegionStore(3, 40), IsNil)
	c.Assert(tc.addRegionStore(2, 40), IsNil)
	c.Assert(tc.addRegionStore(1, 10), IsNil)
	c.Assert(tc.addLeaderRegion(1, 2, 3, 4), IsNil)
	c.Assert(tc.addLeaderRegion(2, 1, 3, 4), IsNil)
	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 1})
	c.Assert(oc.AddOperator(op1), IsTrue)
	op2 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 2})
	op2.SetPriorityLevel(core.HighPriority)
	c.Assert(oc.AddOperator(op2), IsTrue)
	op3 := newTestOperator(1, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 3})
	c.Assert(oc.AddOperator(op3), IsFalse)
	c.Assert(lb.Schedule(tc), IsNil)
	time.Sleep(1 * time.Second)
	c.Assert(lb.Schedule(tc), NotNil)
}

var _ = Suite(&testScheduleControllerSuite{})

type testScheduleControllerSuite struct{}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedule.Scheduler
	limit   uint64
	counter *schedule.OperatorController
	kind    operator.OpKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func (s *testScheduleControllerSuite) TestController(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	c.Assert(tc.addLeaderRegion(1, 1), IsNil)
	c.Assert(tc.addLeaderRegion(2, 2), IsNil)

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	oc := co.opController
	scheduler, err := schedule.CreateScheduler("balance-leader", oc)
	c.Assert(err, IsNil)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := newScheduleController(co, lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		c.Assert(sc.GetInterval(), Equals, i)
		c.Assert(sc.Schedule(), IsNil)
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	c.Assert(sc.AllowSchedule(), IsTrue)
	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op1), IsTrue)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)
	op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op2), IsTrue)
	// count = 2
	c.Assert(sc.AllowSchedule(), IsFalse)
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)

	// add a PriorityKind operator will remove old operator
	op3 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpHotRegion)
	op3.SetPriorityLevel(core.HighPriority)
	c.Assert(oc.AddWaitingOperator(op1), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	c.Assert(oc.AddWaitingOperator(op3), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
	c.Assert(oc.RemoveOperator(op3), IsTrue)

	// add a admin operator will remove old operator
	c.Assert(oc.AddWaitingOperator(op2), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	op4 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpAdmin)
	op4.SetPriorityLevel(core.HighPriority)
	c.Assert(oc.AddWaitingOperator(op4), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
	c.Assert(oc.RemoveOperator(op4), IsTrue)

	// test wrong region id.
	op5 := newTestOperator(3, &metapb.RegionEpoch{}, operator.OpHotRegion)
	c.Assert(oc.AddWaitingOperator(op5), IsFalse)

	// test wrong region epoch.
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	epoch := &metapb.RegionEpoch{
		Version: tc.GetRegion(1).GetRegionEpoch().GetVersion() + 1,
		ConfVer: tc.GetRegion(1).GetRegionEpoch().GetConfVer(),
	}
	op6 := newTestOperator(1, epoch, operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op6), IsFalse)
	epoch.Version--
	op6 = newTestOperator(1, epoch, operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op6), IsTrue)
	c.Assert(oc.RemoveOperator(op6), IsTrue)
}

func (s *testScheduleControllerSuite) TestInterval(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := getHeartBeatStreams(c, tc)
	defer hbStreams.Close()

	co := newCoordinator(tc.RaftCluster, hbStreams, namespace.DefaultClassifier)
	lb, err := schedule.CreateScheduler("balance-leader", co.opController)
	c.Assert(err, IsNil)
	sc := newScheduleController(co, lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.nextInterval = schedulers.MinScheduleInterval
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			c.Assert(sc.Schedule(), IsNil)
		}
		c.Assert(sc.GetInterval(), Less, time.Second*time.Duration(n/2))
	}
}

func waitAddLearner(c *C, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
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

func waitPromoteLearner(c *C, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
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

func waitRemovePeer(c *C, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
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

func waitTransferLeader(c *C, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() && res.GetTransferLeader().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithLeader(res.GetTransferLeader().GetPeer()),
	)
}

func waitNoResponse(c *C, stream mockhbstream.HeartbeatStream) {
	testutil.WaitUntil(c, func(c *C) bool {
		res := stream.Recv()
		return res == nil
	})
}

func getHeartBeatStreams(c *C, tc *testCluster) *heartbeatStreams {
	config := NewTestSingleConfig(c)
	svr, err := CreateServer(config, nil)
	c.Assert(err, IsNil)
	kvBase := kv.NewEtcdKVBase(svr.client, svr.rootPath)
	path := filepath.Join(svr.cfg.DataDir, "region-meta")
	regionStorage, err := core.NewRegionStorage(path)
	c.Assert(err, IsNil)
	svr.storage = core.NewStorage(kvBase).SetRegionStorage(regionStorage)
	cluster := tc.RaftCluster
	cluster.s = svr
	cluster.running = false
	cluster.clusterID = tc.getClusterID()
	cluster.clusterRoot = svr.getClusterRootPath()
	cluster.regionSyncer = syncer.NewRegionSyncer(svr)
	hbStreams := newHeartbeatStreams(tc.getClusterID(), cluster)
	return hbStreams
}

func createTestRaftCluster(id id.Allocator, opt *config.ScheduleOption, storage *core.Storage) *RaftCluster {
	cluster := &RaftCluster{}
	cluster.initCluster(id, opt, storage)
	return cluster
}
