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
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cluster"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/mock/mockhbstream"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/progress"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/checker"
	sc "github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
)

func TestStoreHeartbeat(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	opt.GetScheduleConfig().StoreLimitVersion = "v2"
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())

	n, np := uint64(3), uint64(3)
	stores := newTestStores(n, "2.0.0")
	storeMetasAfterHeartbeat := make([]*metapb.Store, 0, n)
	regions := newTestRegions(n, n, np)

	for _, region := range regions {
		re.NoError(cluster.putRegion(region))
	}
	re.Equal(int(n), cluster.GetTotalRegionCount())

	for i, store := range stores {
		req := &pdpb.StoreHeartbeatRequest{}
		resp := &pdpb.StoreHeartbeatResponse{}
		req.Stats = &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		re.Error(cluster.HandleStoreHeartbeat(req, resp))

		re.NoError(cluster.setStore(store))
		re.Equal(i+1, cluster.GetStoreCount())

		re.Equal(int64(0), store.GetLastHeartbeatTS().UnixNano())

		re.NoError(cluster.HandleStoreHeartbeat(req, resp))

		s := cluster.GetStore(store.GetID())
		re.NotEqual(int64(0), s.GetLastHeartbeatTS().UnixNano())
		re.Equal(req.GetStats(), s.GetStoreStats())
		re.Equal("v2", cluster.GetStore(1).GetStoreLimit().Version())

		storeMetasAfterHeartbeat = append(storeMetasAfterHeartbeat, s.GetMeta())
	}

	re.Equal(int(n), cluster.GetStoreCount())

	for i, store := range stores {
		tmp := &metapb.Store{}
		ok, err := cluster.storage.LoadStoreMeta(store.GetID(), tmp)
		re.True(ok)
		re.NoError(err)
		re.Equal(storeMetasAfterHeartbeat[i], tmp)
	}
	hotReq := &pdpb.StoreHeartbeatRequest{}
	hotResp := &pdpb.StoreHeartbeatResponse{}
	hotReq.Stats = &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10,
		},
		PeerStats: []*pdpb.PeerStat{
			{
				RegionId:  1,
				ReadKeys:  9999999,
				ReadBytes: 9999998,
				QueryStats: &pdpb.QueryStats{
					Get: 9999997,
				},
			},
		},
	}
	hotHeartBeat := hotReq.GetStats()
	coldReq := &pdpb.StoreHeartbeatRequest{}
	coldResp := &pdpb.StoreHeartbeatResponse{}
	coldReq.Stats = &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10,
		},
		PeerStats: []*pdpb.PeerStat{},
	}
	scheCfg := cluster.opt.GetScheduleConfig().Clone()
	scheCfg.StoreLimitVersion = "v1"
	cluster.opt.SetScheduleConfig(scheCfg)
	re.NoError(cluster.HandleStoreHeartbeat(hotReq, hotResp))
	re.NoError(cluster.HandleStoreHeartbeat(hotReq, hotResp))
	re.NoError(cluster.HandleStoreHeartbeat(hotReq, hotResp))
	re.Equal("v1", cluster.GetStore(1).GetStoreLimit().Version())
	time.Sleep(20 * time.Millisecond)
	storeStats := cluster.hotStat.GetHotPeerStats(utils.Read, 3)
	re.Len(storeStats[1], 1)
	re.Equal(uint64(1), storeStats[1][0].RegionID)
	interval := float64(hotHeartBeat.Interval.EndTimestamp - hotHeartBeat.Interval.StartTimestamp)
	re.Len(storeStats[1][0].Loads, utils.DimLen)
	re.Equal(float64(hotHeartBeat.PeerStats[0].ReadBytes)/interval, storeStats[1][0].Loads[utils.ByteDim])
	re.Equal(float64(hotHeartBeat.PeerStats[0].ReadKeys)/interval, storeStats[1][0].Loads[utils.KeyDim])
	re.Equal(float64(hotHeartBeat.PeerStats[0].QueryStats.Get)/interval, storeStats[1][0].Loads[utils.QueryDim])
	// After cold heartbeat, we won't find region 1 peer in HotPeerStats
	re.NoError(cluster.HandleStoreHeartbeat(coldReq, coldResp))
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.GetHotPeerStats(utils.Read, 1)
	re.Empty(storeStats[1])
	// After hot heartbeat, we can find region 1 peer again
	re.NoError(cluster.HandleStoreHeartbeat(hotReq, hotResp))
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.GetHotPeerStats(utils.Read, 3)
	re.Len(storeStats[1], 1)
	re.Equal(uint64(1), storeStats[1][0].RegionID)
	//  after several cold heartbeats, and one hot heartbeat, we also can't find region 1 peer
	re.NoError(cluster.HandleStoreHeartbeat(coldReq, coldResp))
	re.NoError(cluster.HandleStoreHeartbeat(coldReq, coldResp))
	re.NoError(cluster.HandleStoreHeartbeat(coldReq, coldResp))
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.GetHotPeerStats(utils.Read, 0)
	re.Empty(storeStats[1])
	re.NoError(cluster.HandleStoreHeartbeat(hotReq, hotResp))
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.GetHotPeerStats(utils.Read, 1)
	re.Empty(storeStats[1])
	storeStats = cluster.hotStat.GetHotPeerStats(utils.Read, 3)
	re.Empty(storeStats[1])
	// after 2 hot heartbeats, wo can find region 1 peer again
	re.NoError(cluster.HandleStoreHeartbeat(hotReq, hotResp))
	re.NoError(cluster.HandleStoreHeartbeat(hotReq, hotResp))
	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.GetHotPeerStats(utils.Read, 3)
	re.Len(storeStats[1], 1)
	re.Equal(uint64(1), storeStats[1][0].RegionID)
}

func TestFilterUnhealthyStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())

	stores := newTestStores(3, "2.0.0")
	req := &pdpb.StoreHeartbeatRequest{}
	resp := &pdpb.StoreHeartbeatResponse{}
	for _, store := range stores {
		req.Stats = &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		re.NoError(cluster.setStore(store))
		re.NoError(cluster.HandleStoreHeartbeat(req, resp))
		re.NotNil(cluster.hotStat.GetRollingStoreStats(store.GetID()))
	}

	for _, store := range stores {
		req.Stats = &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
		re.NoError(cluster.setStore(newStore))
		re.NoError(cluster.HandleStoreHeartbeat(req, resp))
		re.Nil(cluster.hotStat.GetRollingStoreStats(store.GetID()))
	}
}

func TestSetOfflineStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(ctx, storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels(), opt.GetIsolationLevel())
		if err != nil {
			panic(err)
		}
	}

	// Put 6 stores.
	for _, store := range newTestStores(6, "2.0.0") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}

	// store 1: up -> offline
	re.NoError(cluster.RemoveStore(1, false))
	store := cluster.GetStore(1)
	re.True(store.IsRemoving())
	re.False(store.IsPhysicallyDestroyed())

	// store 1: set physically to true success
	re.NoError(cluster.RemoveStore(1, true))
	store = cluster.GetStore(1)
	re.True(store.IsRemoving())
	re.True(store.IsPhysicallyDestroyed())

	// store 2:up -> offline & physically destroyed
	re.NoError(cluster.RemoveStore(2, true))
	// store 2: set physically destroyed to false failed
	re.Error(cluster.RemoveStore(2, false))
	re.NoError(cluster.RemoveStore(2, true))

	// store 3: up to offline
	re.NoError(cluster.RemoveStore(3, false))
	re.NoError(cluster.RemoveStore(3, false))

	cluster.checkStores()
	// store 1,2,3 should be to tombstone
	for storeID := uint64(1); storeID <= 3; storeID++ {
		re.True(cluster.GetStore(storeID).IsRemoved())
	}
	// test bury store
	for storeID := uint64(0); storeID <= 4; storeID++ {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsUp() {
			re.Error(cluster.BuryStore(storeID, false))
		} else {
			re.NoError(cluster.BuryStore(storeID, false))
		}
	}
	// test clean up tombstone store
	toCleanStore := cluster.GetStore(1).Clone().GetMeta()
	toCleanStore.LastHeartbeat = time.Now().Add(-40 * 24 * time.Hour).UnixNano()
	cluster.PutMetaStore(toCleanStore)
	cluster.checkStores()
	re.Nil(cluster.GetStore(1))
}

func TestSetOfflineWithReplica(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}

	re.NoError(cluster.RemoveStore(2, false))
	// should be failed since no enough store to accommodate the extra replica.
	err = cluster.RemoveStore(3, false)
	re.Contains(err.Error(), string(errs.ErrStoresNotEnough.RFCCode()))
	re.Error(cluster.RemoveStore(3, false))
	// should be success since physically-destroyed is true.
	re.NoError(cluster.RemoveStore(3, true))
}

func addEvictLeaderScheduler(cluster *RaftCluster, storeID uint64) (evictScheduler schedulers.Scheduler, err error) {
	args := []string{fmt.Sprintf("%d", storeID)}
	evictScheduler, err = schedulers.CreateScheduler(types.EvictLeaderScheduler, cluster.GetOperatorController(), cluster.storage, schedulers.ConfigSliceDecoder(types.EvictLeaderScheduler, args), cluster.GetCoordinator().GetSchedulersController().RemoveScheduler)
	if err != nil {
		return
	}
	if err = cluster.AddScheduler(evictScheduler, args...); err != nil {
		return
	} else if err = cluster.opt.Persist(cluster.GetStorage()); err != nil {
		return
	}
	return
}

func TestSetOfflineStoreWithEvictLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	opt.SetMaxReplicas(1)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	// Put 3 stores.
	for _, store := range newTestStores(3, "2.0.0") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	_, err = addEvictLeaderScheduler(cluster, 1)

	re.NoError(err)
	re.NoError(cluster.RemoveStore(2, false))

	// should be failed since there is only 1 store left and it is the evict-leader store.
	err = cluster.RemoveStore(3, false)
	re.Error(err)
	re.Contains(err.Error(), string(errs.ErrNoStoreForRegionLeader.RFCCode()))
	re.NoError(cluster.RemoveScheduler(types.EvictLeaderScheduler.String()))
	re.NoError(cluster.RemoveStore(3, false))
}

func TestForceBuryStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	// Put 2 stores.
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	re.NoError(cluster.BuryStore(uint64(1), true))
	re.Error(cluster.BuryStore(uint64(2), true))
	re.True(errors.ErrorEqual(cluster.BuryStore(uint64(3), true), errs.ErrStoreNotFound.FastGenByArgs(uint64(3))))
}

func TestReuseAddress(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	// store 1: up
	// store 2: offline
	re.NoError(cluster.RemoveStore(2, false))
	// store 3: offline and physically destroyed
	re.NoError(cluster.RemoveStore(3, true))
	// store 4: tombstone
	re.NoError(cluster.RemoveStore(4, true))
	re.NoError(cluster.BuryStore(4, false))

	for id := uint64(1); id <= 4; id++ {
		storeInfo := cluster.GetStore(id)
		storeID := storeInfo.GetID() + 1000
		newStore := &metapb.Store{
			Id:         storeID,
			Address:    storeInfo.GetAddress(),
			State:      metapb.StoreState_Up,
			Version:    storeInfo.GetVersion(),
			DeployPath: getTestDeployPath(storeID),
		}

		if storeInfo.IsPhysicallyDestroyed() || storeInfo.IsRemoved() {
			// try to start a new store with the same address with store which is physically destroyed or tombstone should be success
			re.NoError(cluster.PutMetaStore(newStore))
		} else {
			re.Error(cluster.PutMetaStore(newStore))
		}
	}
}

func getTestDeployPath(storeID uint64) string {
	return fmt.Sprintf("test/store%d", storeID)
}

func TestUpStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(ctx, storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels(), opt.GetIsolationLevel())
		if err != nil {
			panic(err)
		}
	}

	// Put 5 stores.
	for _, store := range newTestStores(5, "5.0.0") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}

	// set store 1 offline
	re.NoError(cluster.RemoveStore(1, false))
	// up a offline store should be success.
	re.NoError(cluster.UpStore(1))

	// set store 2 offline and physically destroyed
	re.NoError(cluster.RemoveStore(2, true))
	re.Error(cluster.UpStore(2))

	// bury store 2
	cluster.checkStores()
	// store is tombstone
	err = cluster.UpStore(2)
	re.True(errors.ErrorEqual(err, errs.ErrStoreRemoved.FastGenByArgs(2)))

	// store 3 is up
	re.NoError(cluster.UpStore(3))

	// store 4 not exist
	err = cluster.UpStore(10)
	re.True(errors.ErrorEqual(err, errs.ErrStoreNotFound.FastGenByArgs(4)))
}

func TestRemovingProcess(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.SetPrepared()

	// Put 5 stores.
	stores := newTestStores(5, "5.0.0")
	for _, store := range stores {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	regions := newTestRegions(100, 5, 1)
	var regionInStore1 []*core.RegionInfo
	for _, region := range regions {
		if region.GetPeers()[0].GetStoreId() == 1 {
			region = region.Clone(core.SetApproximateSize(100))
			regionInStore1 = append(regionInStore1, region)
		}
		re.NoError(cluster.putRegion(region))
	}
	re.Len(regionInStore1, 20)
	cluster.progressManager = progress.NewManager()
	cluster.RemoveStore(1, false)
	cluster.checkStores()
	process := "removing-1"
	// no region moving
	p, l, cs, err := cluster.progressManager.Status(process)
	re.NoError(err)
	re.Equal(0.0, p)
	re.Equal(math.MaxFloat64, l)
	re.Equal(0.0, cs)
	i := 0
	// simulate region moving by deleting region from store 1
	for _, region := range regionInStore1 {
		if i >= 5 {
			break
		}
		cluster.RemoveRegionIfExist(region.GetID())
		i++
	}
	cluster.checkStores()
	p, l, cs, err = cluster.progressManager.Status(process)
	re.NoError(err)
	// In above we delete 5 region from store 1, the total count of region in store 1 is 20.
	// process = 5 / 20 = 0.25
	re.Equal(0.25, p)
	// Each region is 100MB, we use more than 1s to move 5 region.
	// speed = 5 * 100MB / 20s = 25MB/s
	re.Equal(25.0, cs)
	// left second = 15 * 100MB / 25s = 60s
	re.Equal(60.0, l)
}

func TestDeleteStoreUpdatesClusterVersion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(ctx, storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels(), opt.GetIsolationLevel())
		if err != nil {
			panic(err)
		}
	}

	// Put 3 new 4.0.9 stores.
	for _, store := range newTestStores(3, "4.0.9") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	re.Equal("4.0.9", cluster.GetClusterVersion())

	// Upgrade 2 stores to 5.0.0.
	for _, store := range newTestStores(2, "5.0.0") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	re.Equal("4.0.9", cluster.GetClusterVersion())

	// Bury the other store.
	re.NoError(cluster.RemoveStore(3, true))
	cluster.checkStores()
	re.Equal("5.0.0", cluster.GetClusterVersion())
}

func TestStoreClusterVersion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	stores := newTestStores(3, "5.0.0")
	s1, s2, s3 := stores[0].GetMeta(), stores[1].GetMeta(), stores[2].GetMeta()
	s1.Version = "5.0.1"
	s2.Version = "5.0.3"
	s3.Version = "5.0.5"
	re.NoError(cluster.PutMetaStore(s2))
	re.Equal(s2.Version, cluster.GetClusterVersion())

	re.NoError(cluster.PutMetaStore(s1))
	// the cluster version should be 5.0.1(the min one)
	re.Equal(s1.Version, cluster.GetClusterVersion())

	re.NoError(cluster.PutMetaStore(s3))
	// the cluster version should be 5.0.1(the min one)
	re.Equal(s1.Version, cluster.GetClusterVersion())
}

func TestRegionHeartbeatHotStat(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	stores := newTestStores(4, "2.0.0")
	for _, store := range stores {
		cluster.PutStore(store)
	}
	peers := []*metapb.Peer{
		{
			Id:      1,
			StoreId: 1,
		},
		{
			Id:      2,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
	}
	leader := &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	regionMeta := &metapb.Region{
		Id:          1,
		Peers:       peers,
		StartKey:    []byte{byte(1)},
		EndKey:      []byte{byte(1 + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
	}
	region := core.NewRegionInfo(regionMeta, leader, core.WithInterval(&pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: utils.RegionHeartBeatReportInterval}),
		core.SetWrittenBytes(30000*10),
		core.SetWrittenKeys(300000*10))
	err = cluster.processRegionHeartbeat(core.ContextTODO(), region)
	re.NoError(err)
	// wait HotStat to update items
	time.Sleep(time.Second)
	stats := cluster.hotStat.GetHotPeerStats(utils.Write, 0)
	re.Len(stats[1], 1)
	re.Len(stats[2], 1)
	re.Len(stats[3], 1)
	newPeer := &metapb.Peer{
		Id:      4,
		StoreId: 4,
	}
	region = region.Clone(core.WithRemoveStorePeer(2), core.WithAddPeer(newPeer))
	err = cluster.processRegionHeartbeat(core.ContextTODO(), region)
	re.NoError(err)
	// wait HotStat to update items
	time.Sleep(time.Second)
	stats = cluster.hotStat.GetHotPeerStats(utils.Write, 0)
	re.Len(stats[1], 1)
	re.Empty(stats[2])
	re.Len(stats[3], 1)
	re.Len(stats[4], 1)
}

func TestBucketHeartbeat(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	// case1: region is not exist
	buckets := &metapb.Buckets{
		RegionId: 1,
		Version:  1,
		Keys:     [][]byte{{'1'}, {'2'}},
	}
	re.Error(cluster.processReportBuckets(buckets))

	// case2: bucket can be processed after the region update.
	stores := newTestStores(3, "2.0.0")
	n, np := uint64(2), uint64(2)
	regions := newTestRegions(n, n, np)
	for _, store := range stores {
		re.NoError(cluster.setStore(store))
	}

	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), regions[0]))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), regions[1]))
	re.Nil(cluster.GetRegion(uint64(1)).GetBuckets())
	re.NoError(cluster.processReportBuckets(buckets))
	re.Equal(buckets, cluster.GetRegion(uint64(1)).GetBuckets())

	// case3: the bucket version is same.
	re.NoError(cluster.processReportBuckets(buckets))
	// case4: the bucket version is changed.
	newBuckets := &metapb.Buckets{
		RegionId: 1,
		Version:  3,
		Keys:     [][]byte{{'1'}, {'2'}},
	}
	re.NoError(cluster.processReportBuckets(newBuckets))
	re.Equal(newBuckets, cluster.GetRegion(uint64(1)).GetBuckets())

	// case5: region update should inherit buckets.
	newRegion := regions[1].Clone(core.WithIncConfVer(), core.SetBuckets(nil))
	opt.SetRegionBucketEnabled(true)
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newRegion))
	re.Len(cluster.GetRegion(uint64(1)).GetBuckets().GetKeys(), 2)

	// case6: disable region bucket in
	opt.SetRegionBucketEnabled(false)
	newRegion2 := regions[1].Clone(core.WithIncConfVer(), core.SetBuckets(nil))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newRegion2))
	re.Nil(cluster.GetRegion(uint64(1)).GetBuckets())
	re.Empty(cluster.GetRegion(uint64(1)).GetBuckets().GetKeys())
}

func TestRegionHeartbeat(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	n, np := uint64(3), uint64(3)
	cluster.wg.Add(1)
	go cluster.runUpdateStoreStats()
	stores := newTestStores(3, "2.0.0")
	regions := newTestRegions(n, n, np)

	for _, store := range stores {
		re.NoError(cluster.setStore(store))
	}

	for i, region := range regions {
		// region does not exist.
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])

		// region is the same, not updated.
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])
		origin := region
		// region is updated.
		region = origin.Clone(core.WithIncVersion())
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])

		// region is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		re.Error(cluster.processRegionHeartbeat(core.ContextTODO(), stale))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])

		// region is updated
		region = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])

		// region is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		re.Error(cluster.processRegionHeartbeat(core.ContextTODO(), stale))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])

		// Add a down peer.
		region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
			{
				Peer:        region.GetPeers()[rand.Intn(len(region.GetPeers()))],
				DownSeconds: 42,
			},
		}))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Add a pending peer.
		region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[rand.Intn(len(region.GetPeers()))]}))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Clear down peers.
		region = region.Clone(core.WithDownPeers(nil))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Clear pending peers.
		region = region.Clone(core.WithPendingPeers(nil))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Remove peers.
		origin = region
		region = origin.Clone(core.SetPeers(region.GetPeers()[:1]))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])
		// Add peers.
		region = origin
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		checkRegionsKV(re, cluster.storage, regions[:i+1])

		// Change one peer to witness
		region = region.Clone(
			core.WithWitnesses([]*metapb.Peer{region.GetPeers()[rand.Intn(len(region.GetPeers()))]}),
			core.WithIncConfVer(),
		)
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Change leader.
		region = region.Clone(core.WithLeader(region.GetPeers()[1]))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Change ApproximateSize.
		region = region.Clone(core.SetApproximateSize(144))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Change ApproximateKeys.
		region = region.Clone(core.SetApproximateKeys(144000))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Change bytes written.
		region = region.Clone(core.SetWrittenBytes(24000))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Change bytes read.
		region = region.Clone(core.SetReadBytes(1080000))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])

		// Flashback
		region = region.Clone(core.WithFlashback(true, 1))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
		region = region.Clone(core.WithFlashback(false, 0))
		regions[i] = region
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region))
		checkRegions(re, cluster.BasicCluster, regions[:i+1])
	}

	regionCounts := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCounts[peer.GetStoreId()]++
		}
	}
	for id, count := range regionCounts {
		re.Equal(count, cluster.GetStoreRegionCount(id))
	}

	for _, region := range cluster.GetRegions() {
		checkRegion(re, region, regions[region.GetID()])
	}
	for _, region := range cluster.GetMetaRegions() {
		re.Equal(regions[region.GetId()].GetMeta(), region)
	}

	for _, region := range regions {
		for _, store := range cluster.GetRegionStores(region) {
			re.NotNil(region.GetStorePeer(store.GetID()))
		}
		for _, store := range cluster.GetFollowerStores(region) {
			peer := region.GetStorePeer(store.GetID())
			re.NotEqual(region.GetLeader().GetId(), peer.GetId())
		}
	}

	time.Sleep(50 * time.Millisecond)
	for _, store := range cluster.GetStores() {
		re.Equal(cluster.GetStoreLeaderCount(store.GetID()), store.GetLeaderCount())
		re.Equal(cluster.GetStoreRegionCount(store.GetID()), store.GetRegionCount())
		re.Equal(cluster.GetStoreLeaderRegionSize(store.GetID()), store.GetLeaderSize())
		re.Equal(cluster.GetStoreRegionSize(store.GetID()), store.GetRegionSize())
	}

	// Test with storage.
	if storage := cluster.storage; storage != nil {
		for _, region := range regions {
			tmp := &metapb.Region{}
			ok, err := storage.LoadRegion(region.GetID(), tmp)
			re.True(ok)
			re.NoError(err)
			re.Equal(region.GetMeta(), tmp)
		}

		// Check overlap with stale version
		overlapRegion := regions[n-1].Clone(
			core.WithStartKey([]byte("")),
			core.WithEndKey([]byte("")),
			core.WithNewRegionID(10000),
			core.WithDecVersion(),
		)
		re.Error(cluster.processRegionHeartbeat(core.ContextTODO(), overlapRegion))
		region := &metapb.Region{}
		ok, err := storage.LoadRegion(regions[n-1].GetID(), region)
		re.True(ok)
		re.NoError(err)
		re.Equal(regions[n-1].GetMeta(), region)
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		re.True(ok)
		re.NoError(err)
		re.Equal(regions[n-2].GetMeta(), region)
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		re.False(ok)
		re.NoError(err)

		// Check overlap
		rm := cluster.GetRuleManager()
		rm.SetPlaceholderRegionFitCache(regions[n-1])
		rm.SetPlaceholderRegionFitCache(regions[n-2])
		re.True(rm.CheckIsCachedDirectly(regions[n-1].GetID()))
		re.True(rm.CheckIsCachedDirectly(regions[n-2].GetID()))
		overlapRegion = regions[n-1].Clone(
			core.WithStartKey(regions[n-2].GetStartKey()),
			core.WithNewRegionID(regions[n-1].GetID()+1),
		)
		tracer := core.NewHeartbeatProcessTracer()
		tracer.Begin()
		ctx := core.ContextTODO()
		ctx.Tracer = tracer
		re.NoError(cluster.processRegionHeartbeat(ctx, overlapRegion))
		tracer.OnAllStageFinished()
		re.Condition(func() bool {
			fields := tracer.LogFields()
			return slice.AllOf(fields, func(i int) bool { return fields[i].Integer > 0 })
		}, "should have stats")
		region = &metapb.Region{}
		ok, err = storage.LoadRegion(regions[n-1].GetID(), region)
		re.False(ok)
		re.NoError(err)
		re.False(rm.CheckIsCachedDirectly(regions[n-1].GetID()))
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		re.False(ok)
		re.NoError(err)
		re.False(rm.CheckIsCachedDirectly(regions[n-2].GetID()))
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		re.True(ok)
		re.NoError(err)
		re.Equal(overlapRegion.GetMeta(), region)
	}
}

func TestRegionFlowChanged(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	regions := []*core.RegionInfo{core.NewTestRegionInfo(1, 1, []byte{}, []byte{})}
	processRegions := func(regions []*core.RegionInfo) {
		for _, r := range regions {
			mctx := core.ContextTODO()
			mctx.Context = ctx
			cluster.processRegionHeartbeat(mctx, r)
		}
	}
	regions = core.SplitRegions(regions)
	processRegions(regions)
	// update region
	region := regions[0]
	regions[0] = region.Clone(core.SetReadBytes(1000))
	processRegions(regions)
	newRegion := cluster.GetRegion(region.GetID())
	re.Equal(uint64(1000), newRegion.GetBytesRead())
}

func TestRegionSizeChanged(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager)
	region := newTestRegions(1, 3, 3)[0]
	cluster.opt.GetMaxMergeRegionKeys()
	curMaxMergeSize := int64(cluster.opt.GetMaxMergeRegionSize())
	curMaxMergeKeys := int64(cluster.opt.GetMaxMergeRegionKeys())
	region = region.Clone(
		core.WithLeader(region.GetPeers()[2]),
		core.SetApproximateSize(curMaxMergeSize-1),
		core.SetApproximateKeys(curMaxMergeKeys-1),
		core.SetSource(core.Heartbeat),
	)
	cluster.processRegionHeartbeat(core.ContextTODO(), region)
	regionID := region.GetID()
	re.True(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
	// Test ApproximateSize and ApproximateKeys change.
	region = region.Clone(
		core.WithLeader(region.GetPeers()[2]),
		core.SetApproximateSize(curMaxMergeSize+1),
		core.SetApproximateKeys(curMaxMergeKeys+1),
		core.SetSource(core.Heartbeat),
	)
	cluster.processRegionHeartbeat(core.ContextTODO(), region)
	re.False(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
	// Test MaxMergeRegionSize and MaxMergeRegionKeys change.
	cluster.opt.SetMaxMergeRegionSize(uint64(curMaxMergeSize + 2))
	cluster.opt.SetMaxMergeRegionKeys(uint64(curMaxMergeKeys + 2))
	cluster.processRegionHeartbeat(core.ContextTODO(), region)
	re.True(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
	cluster.opt.SetMaxMergeRegionSize(uint64(curMaxMergeSize))
	cluster.opt.SetMaxMergeRegionKeys(uint64(curMaxMergeKeys))
	cluster.processRegionHeartbeat(core.ContextTODO(), region)
	re.False(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
}

func TestConcurrentReportBucket(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo(1, 1, []byte{}, []byte{})}
	heartbeatRegions(re, cluster, regions)
	re.NotNil(cluster.GetRegion(1))

	bucket1 := &metapb.Buckets{RegionId: 1, Version: 3}
	bucket2 := &metapb.Buckets{RegionId: 1, Version: 2}
	var wg sync.WaitGroup
	wg.Add(1)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat", "return(true)"))
	go func() {
		defer wg.Done()
		cluster.processReportBuckets(bucket1)
	}()
	time.Sleep(100 * time.Millisecond)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat"))
	re.NoError(cluster.processReportBuckets(bucket2))
	wg.Wait()
	re.Equal(bucket1, cluster.GetRegion(1).GetBuckets())
}

func TestConcurrentRegionHeartbeat(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo(1, 1, []byte{}, []byte{})}
	regions = core.SplitRegions(regions)
	heartbeatRegions(re, cluster, regions)

	// Merge regions manually
	source, target := regions[0], regions[1]
	target.GetMeta().StartKey = []byte{}
	target.GetMeta().EndKey = []byte{}
	source.GetMeta().GetRegionEpoch().Version++
	if source.GetMeta().GetRegionEpoch().GetVersion() > target.GetMeta().GetRegionEpoch().GetVersion() {
		target.GetMeta().GetRegionEpoch().Version = source.GetMeta().GetRegionEpoch().GetVersion()
	}
	target.GetMeta().GetRegionEpoch().Version++

	var wg sync.WaitGroup
	wg.Add(1)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat", "return(true)"))
	go func() {
		defer wg.Done()
		cluster.processRegionHeartbeat(core.ContextTODO(), source)
	}()
	time.Sleep(100 * time.Millisecond)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat"))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), target))
	wg.Wait()
	checkRegion(re, cluster.GetRegionByKey([]byte{}), target)
}

func TestRegionLabelIsolationLevel(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	cfg := opt.GetReplicationConfig()
	cfg.LocationLabels = []string{"zone"}
	opt.SetReplicationConfig(cfg)
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	for i := uint64(1); i <= 4; i++ {
		var labels []*metapb.StoreLabel
		if i == 4 {
			labels = []*metapb.StoreLabel{{Key: "zone", Value: fmt.Sprintf("%d", 3)}, {Key: "engine", Value: "tiflash"}}
		} else {
			labels = []*metapb.StoreLabel{{Key: "zone", Value: fmt.Sprintf("%d", i)}}
		}
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("127.0.0.1:%d", i),
			State:   metapb.StoreState_Up,
			Labels:  labels,
		}
		re.NoError(cluster.setStore(core.NewStoreInfo(store)))
	}

	peers := make([]*metapb.Peer, 0, 4)
	for i := uint64(1); i <= 4; i++ {
		peer := &metapb.Peer{
			Id: i + 4,
		}
		peer.StoreId = i
		if i == 8 {
			peer.Role = metapb.PeerRole_Learner
		}
		peers = append(peers, peer)
	}
	region := &metapb.Region{
		Id:       9,
		Peers:    peers,
		StartKey: []byte{byte(1)},
		EndKey:   []byte{byte(2)},
	}
	r1 := core.NewRegionInfo(region, peers[0])
	re.NoError(cluster.putRegion(r1))

	cluster.UpdateRegionsLabelLevelStats([]*core.RegionInfo{r1})
	counter := cluster.labelStats.GetLabelCounter()
	re.Equal(0, counter["none"])
	re.Equal(1, counter["zone"])

	region = &metapb.Region{
		Id:       10,
		Peers:    peers,
		StartKey: []byte{byte(2)},
		EndKey:   []byte{byte(3)},
	}
	r2 := core.NewRegionInfo(region, peers[0])
	re.NoError(cluster.putRegion(r2))

	cluster.UpdateRegionsLabelLevelStats([]*core.RegionInfo{r2})
	counter = cluster.labelStats.GetLabelCounter()
	re.Equal(0, counter["none"])
	re.Equal(2, counter["zone"])

	// issue: https://github.com/tikv/pd/issues/8700
	// step1: heartbeat a overlap region, which is used to simulate the case that the region is merged.
	// step2: update region 9 and region 10, which is used to simulate the case that patrol is triggered.
	// We should only count region 9.
	overlapRegion := r1.Clone(
		core.WithStartKey(r1.GetStartKey()),
		core.WithEndKey(r2.GetEndKey()),
		core.WithLeader(r2.GetPeer(8)),
	)
	re.NoError(cluster.HandleRegionHeartbeat(overlapRegion))
	cluster.UpdateRegionsLabelLevelStats([]*core.RegionInfo{r1, r2})
	counter = cluster.labelStats.GetLabelCounter()
	re.Equal(0, counter["none"])
	re.Equal(1, counter["zone"])
}

func heartbeatRegions(re *require.Assertions, cluster *RaftCluster, regions []*core.RegionInfo) {
	// Heartbeat and check region one by one.
	for _, r := range regions {
		re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), r))

		checkRegion(re, cluster.GetRegion(r.GetID()), r)
		checkRegion(re, cluster.GetRegionByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(re, cluster.GetRegionByKey([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, r := range regions {
		checkRegion(re, cluster.GetRegion(r.GetID()), r)
		checkRegion(re, cluster.GetRegionByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(re, cluster.GetRegionByKey([]byte{end - 1}), r)
			result := cluster.GetRegionByKey([]byte{end + 1})
			re.NotEqual(r.GetID(), result.GetID())
		}
	}
}

func TestHeartbeatSplit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	// 1: [nil, nil)
	region1 := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region1))
	checkRegion(re, cluster.GetRegionByKey([]byte("foo")), region1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	region2 := core.NewRegionInfo(&metapb.Region{Id: 2, EndKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region2))
	checkRegion(re, cluster.GetRegionByKey([]byte("a")), region2)
	// [m, nil) is missing before r1's heartbeat.
	re.Nil(cluster.GetRegionByKey([]byte("z")))

	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region1))
	checkRegion(re, cluster.GetRegionByKey([]byte("z")), region1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	region3 := core.NewRegionInfo(&metapb.Region{Id: 3, StartKey: []byte("m"), EndKey: []byte("q"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region1))
	checkRegion(re, cluster.GetRegionByKey([]byte("z")), region1)
	checkRegion(re, cluster.GetRegionByKey([]byte("a")), region2)
	// [m, q) is missing before r3's heartbeat.
	re.Nil(cluster.GetRegionByKey([]byte("n")))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), region3))
	checkRegion(re, cluster.GetRegionByKey([]byte("n")), region3)
}

func TestRegionSplitAndMerge(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo(1, 1, []byte{}, []byte{})}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for range n {
		regions = core.SplitRegions(regions)
		heartbeatRegions(re, cluster, regions)
	}

	// Merge.
	for range n {
		regions = core.MergeRegions(regions)
		heartbeatRegions(re, cluster, regions)
	}

	// Split twice and merge once.
	for i := range n * 2 {
		if (i+1)%3 == 0 {
			regions = core.MergeRegions(regions)
		} else {
			regions = core.SplitRegions(regions)
		}
		heartbeatRegions(re, cluster, regions)
	}
}

func TestOfflineAndMerge(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(ctx, storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels(), opt.GetIsolationLevel())
		if err != nil {
			panic(err)
		}
	}
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager)
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)

	// Put 4 stores.
	for _, store := range newTestStores(4, "5.0.0") {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}

	peers := []*metapb.Peer{
		{
			Id:      4,
			StoreId: 1,
		}, {
			Id:      5,
			StoreId: 2,
		}, {
			Id:      6,
			StoreId: 3,
		},
	}
	origin := core.NewRegionInfo(
		&metapb.Region{
			StartKey:    []byte{},
			EndKey:      []byte{},
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
			Id:          1,
			Peers:       peers}, peers[0])
	regions := []*core.RegionInfo{origin}

	// store 1: up -> offline
	re.NoError(cluster.RemoveStore(1, false))
	store := cluster.GetStore(1)
	re.True(store.IsRemoving())

	// Split.
	n := 7
	for range n {
		regions = core.SplitRegions(regions)
	}
	heartbeatRegions(re, cluster, regions)
	re.Len(cluster.GetRegionStatsByType(statistics.OfflinePeer), len(regions))

	// Merge.
	for range n {
		regions = core.MergeRegions(regions)
		heartbeatRegions(re, cluster, regions)
		re.Len(cluster.GetRegionStatsByType(statistics.OfflinePeer), len(regions))
	}
}

func TestStoreConfigUpdate(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestCluster(ctx, opt)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		re.NoError(tc.setStore(s))
	}
	re.Len(tc.getUpStores(), 5)
	// Case1: big region.
	{
		body := `{ "coprocessor": {
        "split-region-on-table": false,
        "batch-split-limit": 2,
        "region-max-size": "15GiB",
        "region-split-size": "10GiB",
        "region-max-keys": 144000000,
        "region-split-keys": 96000000,
        "consistency-check-method": "mvcc",
        "perf-level": 2
    	}}`
		var config sc.StoreConfig
		re.NoError(json.Unmarshal([]byte(body), &config))
		tc.updateStoreConfig(opt.GetStoreConfig(), &config)
		re.Equal(uint64(144000000), opt.GetRegionMaxKeys())
		re.Equal(uint64(96000000), opt.GetRegionSplitKeys())
		re.Equal(uint64(15*units.GiB/units.MiB), opt.GetRegionMaxSize())
		re.Equal(uint64(10*units.GiB/units.MiB), opt.GetRegionSplitSize())
	}
	// Case2: empty config.
	{
		body := `{}`
		var config sc.StoreConfig
		re.NoError(json.Unmarshal([]byte(body), &config))
		tc.updateStoreConfig(opt.GetStoreConfig(), &config)
		re.Equal(uint64(1440000), opt.GetRegionMaxKeys())
		re.Equal(uint64(960000), opt.GetRegionSplitKeys())
		re.Equal(uint64(144), opt.GetRegionMaxSize())
		re.Equal(uint64(96), opt.GetRegionSplitSize())
	}
	// Case3: raft-kv2 config.
	{
		body := `{ "coprocessor": {
		"split-region-on-table":false,
		"batch-split-limit":10,
		"region-max-size":"384MiB",
		"region-split-size":"256MiB",
		"region-max-keys":3840000,
		"region-split-keys":2560000,
		"consistency-check-method":"mvcc",
		"enable-region-bucket":true,
		"region-bucket-size":"96MiB",
		"region-size-threshold-for-approximate":"384MiB",
		"region-bucket-merge-size-ratio":0.33
		},
		"storage":{
			"engine":"raft-kv2"
		}}`
		var config sc.StoreConfig
		re.NoError(json.Unmarshal([]byte(body), &config))
		tc.updateStoreConfig(opt.GetStoreConfig(), &config)
		re.Equal(uint64(96), opt.GetRegionBucketSize())
		re.True(opt.IsRaftKV2())
	}
}

func TestSyncConfigContext(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestCluster(ctx, opt)
	tc.httpClient = &http.Client{}

	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, _ *http.Request) {
		time.Sleep(time.Second * 100)
		cfg := &sc.StoreConfig{}
		b, err := json.Marshal(cfg)
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			res.Write([]byte(fmt.Sprintf("failed setting up test server: %s", err)))
			return
		}

		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	stores := newTestStores(1, "2.0.0")
	for _, s := range stores {
		re.NoError(tc.setStore(s))
	}
	// trip schema header
	now := time.Now()
	stores[0].GetMeta().StatusAddress = server.URL[7:]
	synced, switchRaftV2, needPersist := tc.syncStoreConfig(tc.GetStores())
	re.False(synced)
	re.False(switchRaftV2)
	re.False(needPersist)
	re.Less(time.Since(now), clientTimeout*2)
}

func TestStoreConfigSync(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestCluster(ctx, opt)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		re.NoError(tc.setStore(s))
	}
	re.Len(tc.getUpStores(), 5)

	re.Equal(uint64(144), tc.GetStoreConfig().GetRegionMaxSize())
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/mockFetchStoreConfigFromTiKV", `return("10MiB")`))
	// switchRaftV2 will be true.
	synced, switchRaftV2, needPersist := tc.syncStoreConfig(tc.GetStores())
	re.True(synced)
	re.True(switchRaftV2)
	re.True(needPersist)
	re.EqualValues(512, tc.opt.GetMaxMovableHotPeerSize())
	re.Equal(uint64(10), tc.GetStoreConfig().GetRegionMaxSize())
	// switchRaftV2 will be false this time.
	synced, switchRaftV2, needPersist = tc.syncStoreConfig(tc.GetStores())
	re.True(synced)
	re.False(switchRaftV2)
	re.False(needPersist)
	re.Equal(uint64(10), tc.GetStoreConfig().GetRegionMaxSize())
	re.NoError(opt.Persist(tc.GetStorage()))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/mockFetchStoreConfigFromTiKV"))

	// Check the persistence of the store config.
	opt = config.NewPersistOptions(&config.Config{})
	re.Empty(opt.GetStoreConfig())
	err = opt.Reload(tc.GetStorage())
	re.NoError(err)
	re.Equal(tc.GetOpts().(*config.PersistOptions).GetStoreConfig(), opt.GetStoreConfig())

	re.Equal("v1", opt.GetScheduleConfig().StoreLimitVersion)
	re.NoError(opt.SwitchRaftV2(tc.GetStorage()))
	re.Equal("v2", opt.GetScheduleConfig().StoreLimitVersion)
}

func TestUpdateStorePendingPeerCount(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestCluster(ctx, opt)
	tc.RaftCluster.coordinator = schedule.NewCoordinator(ctx, tc.RaftCluster, nil)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		re.NoError(tc.setStore(s))
	}
	tc.RaftCluster.wg.Add(1)
	go tc.RaftCluster.runUpdateStoreStats()
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
		{
			Id:      4,
			StoreId: 4,
		},
	}
	origin := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[:3]}, peers[0], core.WithPendingPeers(peers[1:3]))
	re.NoError(tc.processRegionHeartbeat(core.ContextTODO(), origin))
	time.Sleep(50 * time.Millisecond)
	checkPendingPeerCount([]int{0, 1, 1, 0}, tc.RaftCluster, re)
	newRegion := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[1:]}, peers[1], core.WithPendingPeers(peers[3:4]))
	re.NoError(tc.processRegionHeartbeat(core.ContextTODO(), newRegion))
	time.Sleep(50 * time.Millisecond)
	checkPendingPeerCount([]int{0, 0, 0, 1}, tc.RaftCluster, re)
}

func TestTopologyWeight(t *testing.T) {
	re := require.New(t)

	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3", "h4"}

	var stores []*core.StoreInfo
	var testStore *core.StoreInfo
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				storeID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				storeLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				store := core.NewStoreInfoWithLabel(storeID, storeLabels)
				if i == 0 && j == 0 && k == 0 {
					testStore = store
				}
				stores = append(stores, store)
			}
		}
	}

	re.Equal(1.0/3/3/4, getStoreTopoWeight(testStore, stores, labels, 3))
}

func TestTopologyWeight1(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, map[string]string{"dc": "dc2", "zone": "zone2", "host": "host2"})
	store3 := core.NewStoreInfoWithLabel(3, map[string]string{"dc": "dc3", "zone": "zone3", "host": "host3"})
	store4 := core.NewStoreInfoWithLabel(4, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store5 := core.NewStoreInfoWithLabel(5, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host2"})
	store6 := core.NewStoreInfoWithLabel(6, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host3"})
	stores := []*core.StoreInfo{store1, store2, store3, store4, store5, store6}

	re.Equal(1.0/3, getStoreTopoWeight(store2, stores, labels, 3))
	re.Equal(1.0/3/4, getStoreTopoWeight(store1, stores, labels, 3))
	re.Equal(1.0/3/4, getStoreTopoWeight(store6, stores, labels, 3))
}

func TestTopologyWeight2(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, map[string]string{"dc": "dc2"})
	store3 := core.NewStoreInfoWithLabel(3, map[string]string{"dc": "dc3"})
	store4 := core.NewStoreInfoWithLabel(4, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host1"})
	store5 := core.NewStoreInfoWithLabel(5, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host1"})
	stores := []*core.StoreInfo{store1, store2, store3, store4, store5}

	re.Equal(1.0/3, getStoreTopoWeight(store2, stores, labels, 3))
	re.Equal(1.0/3/3, getStoreTopoWeight(store1, stores, labels, 3))
}

func TestTopologyWeight3(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host2"})
	store3 := core.NewStoreInfoWithLabel(3, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host3"})
	store4 := core.NewStoreInfoWithLabel(4, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host4"})
	store5 := core.NewStoreInfoWithLabel(5, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host5"})
	store6 := core.NewStoreInfoWithLabel(6, map[string]string{"dc": "dc2", "zone": "zone5", "host": "host6"})

	store7 := core.NewStoreInfoWithLabel(7, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host7"})
	store8 := core.NewStoreInfoWithLabel(8, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host8"})
	store9 := core.NewStoreInfoWithLabel(9, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host9"})
	store10 := core.NewStoreInfoWithLabel(10, map[string]string{"dc": "dc2", "zone": "zone5", "host": "host10"})
	stores := []*core.StoreInfo{store1, store2, store3, store4, store5, store6, store7, store8, store9, store10}

	re.Equal(1.0/5/2, getStoreTopoWeight(store7, stores, labels, 5))
	re.Equal(1.0/5/4, getStoreTopoWeight(store8, stores, labels, 5))
	re.Equal(1.0/5/4, getStoreTopoWeight(store9, stores, labels, 5))
	re.Equal(1.0/5/2, getStoreTopoWeight(store10, stores, labels, 5))
}

func TestTopologyWeight4(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := core.NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := core.NewStoreInfoWithLabel(2, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host2"})
	store3 := core.NewStoreInfoWithLabel(3, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host3"})
	store4 := core.NewStoreInfoWithLabel(4, map[string]string{"dc": "dc2", "zone": "zone1", "host": "host4"})

	stores := []*core.StoreInfo{store1, store2, store3, store4}

	re.Equal(1.0/3/2, getStoreTopoWeight(store1, stores, labels, 3))
	re.Equal(1.0/3, getStoreTopoWeight(store3, stores, labels, 3))
	re.Equal(1.0/3, getStoreTopoWeight(store4, stores, labels, 3))
}

func TestCalculateStoreSize1(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cfg := opt.GetReplicationConfig()
	cfg.EnablePlacementRules = true
	opt.SetReplicationConfig(cfg)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager)

	// Put 10 stores.
	for i, store := range newTestStores(10, "6.0.0") {
		var labels []*metapb.StoreLabel
		if i%3 == 0 {
			// zone 1 has 1, 4, 7, 10
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone1"})
		} else if i%3 == 1 {
			// zone 2 has 2, 5, 8
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone2"})
		} else {
			// zone 3 has 3, 6, 9
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone3"})
		}
		labels = append(labels, []*metapb.StoreLabel{
			{
				Key:   "rack",
				Value: fmt.Sprintf("rack-%d", i%2+1),
			},
			{
				Key:   "host",
				Value: fmt.Sprintf("host-%d", i),
			},
		}...)
		s := store.Clone(core.SetStoreLabels(labels))
		re.NoError(cluster.PutMetaStore(s.GetMeta()))
	}

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: placement.DefaultGroupID, ID: "zone1", StartKey: []byte(""), EndKey: []byte(""), Role: placement.Voter, Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone1"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: placement.DefaultGroupID, ID: "zone2", StartKey: []byte(""), EndKey: []byte(""), Role: placement.Voter, Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone2"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: placement.DefaultGroupID, ID: "zone3", StartKey: []byte(""), EndKey: []byte(""), Role: placement.Follower, Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone3"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)
	cluster.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)

	regions := newTestRegions(100, 10, 5)
	for _, region := range regions {
		re.NoError(cluster.putRegion(region))
	}

	stores := cluster.GetStores()
	store := cluster.GetStore(1)
	// 100 * 100 * 2 (placement rule) / 4 (host) * 0.9 = 4500
	re.Equal(4500.0, cluster.getThreshold(stores, store))

	cluster.opt.SetPlacementRuleEnabled(false)
	cluster.opt.SetLocationLabels([]string{"zone", "rack", "host"})
	// 30000 (total region size) / 3 (zone) / 4 (host) * 0.9 = 2250
	re.Equal(2250.0, cluster.getThreshold(stores, store))
}

func TestCalculateStoreSize2(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cfg := opt.GetReplicationConfig()
	cfg.EnablePlacementRules = true
	opt.SetReplicationConfig(cfg)
	opt.SetMaxReplicas(3)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.coordinator = schedule.NewCoordinator(ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(
		cluster.GetBasicCluster(),
		cluster.GetOpts(),
		cluster.ruleManager)

	// Put 10 stores.
	for i, store := range newTestStores(10, "6.0.0") {
		var labels []*metapb.StoreLabel
		if i%2 == 0 {
			// dc 1 has 1, 3, 5, 7, 9
			labels = append(labels, &metapb.StoreLabel{Key: "dc", Value: "dc1"})
			if i%4 == 0 {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic1"})
			} else {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic2"})
			}
		} else {
			// dc 2 has 2, 4, 6, 8, 10
			labels = append(labels, &metapb.StoreLabel{Key: "dc", Value: "dc2"})
			if i%3 == 0 {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic3"})
			} else {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic4"})
			}
		}
		labels = append(labels, []*metapb.StoreLabel{{Key: "rack", Value: "r1"}, {Key: "host", Value: "h1"}}...)
		s := store.Clone(core.SetStoreLabels(labels))
		re.NoError(cluster.PutMetaStore(s.GetMeta()))
	}

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: placement.DefaultGroupID, ID: "dc1", StartKey: []byte(""), EndKey: []byte(""), Role: placement.Voter, Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "dc", Op: "in", Values: []string{"dc1"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: placement.DefaultGroupID, ID: "logic3", StartKey: []byte(""), EndKey: []byte(""), Role: placement.Voter, Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "logic", Op: "in", Values: []string{"logic3"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: placement.DefaultGroupID, ID: "logic4", StartKey: []byte(""), EndKey: []byte(""), Role: placement.Learner, Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "logic", Op: "in", Values: []string{"logic4"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)
	cluster.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)

	regions := newTestRegions(100, 10, 5)
	for _, region := range regions {
		re.NoError(cluster.putRegion(region))
	}

	stores := cluster.GetStores()
	store := cluster.GetStore(1)

	// 100 * 100 * 4 (total region size) / 2 (dc) / 2 (logic) / 3 (host) * 0.9 = 3000
	re.Equal(3000.0, cluster.getThreshold(stores, store))
}

func TestStores(t *testing.T) {
	re := require.New(t)
	n := uint64(10)
	cache := core.NewStoresInfo()
	stores := newTestStores(n, "2.0.0")

	for i, store := range stores {
		id := store.GetID()
		re.Nil(cache.GetStore(id))
		re.Error(cache.PauseLeaderTransfer(id, constant.In))
		cache.PutStore(store)
		re.Equal(store, cache.GetStore(id))
		re.Equal(i+1, cache.GetStoreCount())
		re.NoError(cache.PauseLeaderTransfer(id, constant.In))
		re.False(cache.GetStore(id).AllowLeaderTransferIn())
		re.Error(cache.PauseLeaderTransfer(id, constant.In))
		cache.ResumeLeaderTransfer(id, constant.In)
		re.True(cache.GetStore(id).AllowLeaderTransferIn())
	}
	re.Equal(int(n), cache.GetStoreCount())

	for _, store := range cache.GetStores() {
		re.Equal(stores[store.GetID()-1], store)
	}
	for _, store := range cache.GetMetaStores() {
		re.Equal(stores[store.GetId()-1].GetMeta(), store)
	}

	re.Equal(int(n), cache.GetStoreCount())
}

func Test(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n, np := uint64(10), uint64(3)
	regions := newTestRegions(n, n, np)
	_, opts, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opts, storage.NewStorageWithMemoryBackend())
	cache := tc.BasicCluster

	for i := range n {
		region := regions[i]
		regionKey := []byte(fmt.Sprintf("a%20d", i+1))

		re.Nil(cache.GetRegion(i))
		re.Nil(cache.GetRegionByKey(regionKey))
		checkRegions(re, cache, regions[0:i])

		origin, overlaps, rangeChanged := cache.SetRegion(region)
		cache.UpdateSubTree(region, origin, overlaps, rangeChanged)
		checkRegion(re, cache.GetRegion(i), region)
		checkRegion(re, cache.GetRegionByKey(regionKey), region)
		checkRegions(re, cache, regions[0:(i+1)])
		// previous region
		if i == 0 {
			re.Nil(cache.GetPrevRegionByKey(regionKey))
		} else {
			checkRegion(re, cache.GetPrevRegionByKey(regionKey), regions[i-1])
		}
		// Update leader to peer np-1.
		newRegion := region.Clone(core.WithLeader(region.GetPeers()[np-1]))
		regions[i] = newRegion
		origin, overlaps, rangeChanged = cache.SetRegion(newRegion)
		cache.UpdateSubTree(newRegion, origin, overlaps, rangeChanged)
		checkRegion(re, cache.GetRegion(i), newRegion)
		checkRegion(re, cache.GetRegionByKey(regionKey), newRegion)
		checkRegions(re, cache, regions[0:(i+1)])

		cache.RemoveRegion(region)
		cache.RemoveRegionFromSubTree(region)
		re.Nil(cache.GetRegion(i))
		re.Nil(cache.GetRegionByKey(regionKey))
		checkRegions(re, cache, regions[0:i])

		// Reset leader to peer 0.
		newRegion = region.Clone(core.WithLeader(region.GetPeers()[0]))
		regions[i] = newRegion
		origin, overlaps, rangeChanged = cache.SetRegion(newRegion)
		cache.UpdateSubTree(newRegion, origin, overlaps, rangeChanged)
		checkRegion(re, cache.GetRegion(i), newRegion)
		checkRegions(re, cache, regions[0:(i+1)])
		checkRegion(re, cache.GetRegionByKey(regionKey), newRegion)
	}

	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	for i := range n {
		region := filter.SelectOneRegion(tc.RandLeaderRegions(i, []core.KeyRange{core.NewKeyRange("", "")}), nil, pendingFilter, downFilter)
		re.Equal(i, region.GetLeader().GetStoreId())

		region = filter.SelectOneRegion(tc.RandFollowerRegions(i, []core.KeyRange{core.NewKeyRange("", "")}), nil, pendingFilter, downFilter)
		re.NotEqual(i, region.GetLeader().GetStoreId())

		re.NotNil(region.GetStorePeer(i))
	}

	// check overlaps
	// clone it otherwise there are two items with the same key in the tree
	overlapRegion := regions[n-1].Clone(core.WithStartKey(regions[n-2].GetStartKey()))
	origin, overlaps, rangeChanged := cache.SetRegion(overlapRegion)
	cache.UpdateSubTree(overlapRegion, origin, overlaps, rangeChanged)
	re.Nil(cache.GetRegion(n - 2))
	re.NotNil(cache.GetRegion(n - 1))

	// All regions will be filtered out if they have pending peers.
	for i := range n {
		for range cache.GetStoreLeaderCount(i) {
			region := filter.SelectOneRegion(tc.RandLeaderRegions(i, []core.KeyRange{core.NewKeyRange("", "")}), nil, pendingFilter, downFilter)
			newRegion := region.Clone(core.WithPendingPeers(region.GetPeers()))
			origin, overlaps, rangeChanged = cache.SetRegion(newRegion)
			cache.UpdateSubTree(newRegion, origin, overlaps, rangeChanged)
		}
		re.Nil(filter.SelectOneRegion(tc.RandLeaderRegions(i, []core.KeyRange{core.NewKeyRange("", "")}), nil, pendingFilter, downFilter))
	}
	for i := range n {
		re.Nil(filter.SelectOneRegion(tc.RandFollowerRegions(i, []core.KeyRange{core.NewKeyRange("", "")}), nil, pendingFilter, downFilter))
	}
}

func TestCheckStaleRegion(t *testing.T) {
	re := require.New(t)

	// (0, 0) v.s. (0, 0)
	region := core.NewTestRegionInfo(1, 1, []byte{}, []byte{})
	origin := core.NewTestRegionInfo(1, 1, []byte{}, []byte{})
	re.NoError(checkStaleRegion(region.GetMeta(), origin.GetMeta()))
	re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))

	// (1, 0) v.s. (0, 0)
	region.GetRegionEpoch().Version++
	re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))
	re.Error(checkStaleRegion(region.GetMeta(), origin.GetMeta()))

	// (1, 1) v.s. (0, 0)
	region.GetRegionEpoch().ConfVer++
	re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))
	re.Error(checkStaleRegion(region.GetMeta(), origin.GetMeta()))

	// (0, 1) v.s. (0, 0)
	region.GetRegionEpoch().Version--
	re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))
	re.Error(checkStaleRegion(region.GetMeta(), origin.GetMeta()))
}

func TestAwakenStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	n := uint64(3)
	stores := newTestStores(n, "6.5.0")
	re.True(stores[0].NeedAwakenStore())
	for _, store := range stores {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	for i := uint64(1); i <= n; i++ {
		re.False(cluster.slowStat.ExistsSlowStores())
		needAwaken, _ := cluster.NeedAwakenAllRegionsInStore(i)
		re.False(needAwaken)
	}

	now := time.Now()
	store4 := stores[0].Clone(core.SetLastHeartbeatTS(now), core.SetLastAwakenTime(now.Add(-11*time.Minute)))
	re.NoError(cluster.setStore(store4))
	store1 := cluster.GetStore(1)
	re.True(store1.NeedAwakenStore())

	// Test slowStore heartbeat by marking Store-1 already slow.
	slowStoreReq := &pdpb.StoreHeartbeatRequest{}
	slowStoreResp := &pdpb.StoreHeartbeatResponse{}
	slowStoreReq.Stats = &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10,
		},
		PeerStats: []*pdpb.PeerStat{},
		SlowScore: 80,
	}
	re.NoError(cluster.HandleStoreHeartbeat(slowStoreReq, slowStoreResp))
	time.Sleep(20 * time.Millisecond)
	re.True(cluster.slowStat.ExistsSlowStores())
	{
		// Store 1 cannot be awaken.
		needAwaken, _ := cluster.NeedAwakenAllRegionsInStore(1)
		re.False(needAwaken)
	}
	{
		// Other stores can be awaken.
		needAwaken, _ := cluster.NeedAwakenAllRegionsInStore(2)
		re.True(needAwaken)
	}
}

func TestUpdateAndDeleteLabel(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	stores := newTestStores(1, "6.5.1")
	for _, store := range stores {
		re.NoError(cluster.PutMetaStore(store.GetMeta()))
	}
	re.Empty(cluster.GetStore(1).GetLabels())
	// Update label.
	cluster.UpdateStoreLabels(
		1,
		[]*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: "host", Value: "host1"},
		},
		false,
	)
	re.Equal(
		[]*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: "host", Value: "host1"},
		},
		cluster.GetStore(1).GetLabels(),
	)
	// Update label again.
	cluster.UpdateStoreLabels(
		1,
		[]*metapb.StoreLabel{
			{Key: "mode", Value: "readonly"},
		},
		false,
	)
	// Update label with empty value.
	cluster.UpdateStoreLabels(
		1,
		[]*metapb.StoreLabel{},
		false,
	)
	re.Equal(
		[]*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: "host", Value: "host1"},
			{Key: "mode", Value: "readonly"},
		},
		cluster.GetStore(1).GetLabels(),
	)
	// Delete label.
	err = cluster.DeleteStoreLabel(1, "mode")
	re.NoError(err)
	re.Equal(
		[]*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: "host", Value: "host1"},
		},
		cluster.GetStore(1).GetLabels(),
	)
	// Delete a non-exist label.
	err = cluster.DeleteStoreLabel(1, "mode")
	re.Error(err)
	re.Equal(
		[]*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: "host", Value: "host1"},
		},
		cluster.GetStore(1).GetLabels(),
	)
	// Update label without force.
	cluster.UpdateStoreLabels(
		1,
		[]*metapb.StoreLabel{},
		false,
	)
	re.Equal(
		[]*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: "host", Value: "host1"},
		},
		cluster.GetStore(1).GetLabels(),
	)
	// Update label with force.
	cluster.UpdateStoreLabels(
		1,
		[]*metapb.StoreLabel{},
		true,
	)
	re.Empty(cluster.GetStore(1).GetLabels())
	// Update label first and then reboot the store.
	cluster.UpdateStoreLabels(
		1,
		[]*metapb.StoreLabel{{Key: "mode", Value: "readonly"}},
		false,
	)
	re.Equal([]*metapb.StoreLabel{{Key: "mode", Value: "readonly"}}, cluster.GetStore(1).GetLabels())
	// Mock the store doesn't have any label configured.
	newStore := typeutil.DeepClone(cluster.GetStore(1).GetMeta(), core.StoreFactory)
	newStore.Labels = nil
	// Store rebooting will call PutStore.
	err = cluster.PutMetaStore(newStore)
	re.NoError(err)
	// Check the label after rebooting.
	re.Equal([]*metapb.StoreLabel{{Key: "mode", Value: "readonly"}}, cluster.GetStore(1).GetLabels())
}

type testCluster struct {
	*RaftCluster
}

func newTestScheduleConfig() (*sc.ScheduleConfig, *config.PersistOptions, error) {
	schedulers.Register()
	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	if err := cfg.Adjust(nil, false); err != nil {
		return nil, nil, err
	}
	opt := config.NewPersistOptions(cfg)
	opt.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version2_0))
	return &cfg.Schedule, opt, nil
}

func newTestCluster(ctx context.Context, opt *config.PersistOptions) *testCluster {
	rc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	storage := storage.NewStorageWithMemoryBackend()
	rc.regionLabeler, _ = labeler.NewRegionLabeler(ctx, storage, time.Second*5)

	return &testCluster{RaftCluster: rc}
}

func newTestRaftCluster(
	ctx context.Context,
	id id.Allocator,
	opt *config.PersistOptions,
	s storage.Storage,
) *RaftCluster {
	opt.GetScheduleConfig().EnableHeartbeatConcurrentRunner = false
	rc := &RaftCluster{serverCtx: ctx, BasicCluster: core.NewBasicCluster(), storage: s}
	rc.InitCluster(id, opt, nil, nil)
	rc.ruleManager = placement.NewRuleManager(ctx, storage.NewStorageWithMemoryBackend(), rc, opt)
	if opt.IsPlacementRulesEnabled() {
		err := rc.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels(), opt.GetIsolationLevel())
		if err != nil {
			panic(err)
		}
	}
	rc.schedulingController = newSchedulingController(rc.ctx, rc.BasicCluster, rc.opt, rc.ruleManager)
	return rc
}

// Create n stores (0..n).
func newTestStores(n uint64, version string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id:            i,
			Address:       fmt.Sprintf("127.0.0.1:%d", i),
			StatusAddress: fmt.Sprintf("127.0.0.1:%d", i),
			State:         metapb.StoreState_Up,
			Version:       version,
			DeployPath:    getTestDeployPath(i),
			NodeState:     metapb.NodeState_Serving,
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

// Create n regions (0..n) of m stores (0..m).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, m, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := range n {
		peers := make([]*metapb.Peer, 0, np)
		for j := range np {
			peer := &metapb.Peer{
				Id: 100000000 + i*np + j,
			}
			peer.StoreId = (i + j) % m
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte(fmt.Sprintf("a%20d", i+1)),
			EndKey:      []byte(fmt.Sprintf("a%20d", i+2)),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0], core.SetApproximateSize(100), core.SetApproximateKeys(1000)))
	}
	return regions
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:      []byte(fmt.Sprintf("%20d", regionID+1)),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
	}
}

func checkRegion(re *require.Assertions, a *core.RegionInfo, b *core.RegionInfo) {
	re.Equal(b, a)
	re.Equal(b.GetMeta(), a.GetMeta())
	re.Equal(b.GetLeader(), a.GetLeader())
	re.Equal(b.GetPeers(), a.GetPeers())
	if len(a.GetDownPeers()) > 0 || len(b.GetDownPeers()) > 0 {
		re.Equal(b.GetDownPeers(), a.GetDownPeers())
	}
	if len(a.GetPendingPeers()) > 0 || len(b.GetPendingPeers()) > 0 {
		re.Equal(b.GetPendingPeers(), a.GetPendingPeers())
	}
}

func checkRegionsKV(re *require.Assertions, s storage.Storage, regions []*core.RegionInfo) {
	if s != nil {
		for _, region := range regions {
			var meta metapb.Region
			ok, err := s.LoadRegion(region.GetID(), &meta)
			re.True(ok)
			re.NoError(err)
			re.Equal(region.GetMeta(), &meta)
		}
	}
}

func checkRegions(re *require.Assertions, cache *core.BasicCluster, regions []*core.RegionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	witnessCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCount[peer.StoreId]++
			if peer.Id == region.GetLeader().Id {
				leaderCount[peer.StoreId]++
				checkRegion(re, cache.GetLeader(peer.StoreId, region), region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(re, cache.GetFollower(peer.StoreId, region), region)
			}
			if peer.IsWitness {
				witnessCount[peer.StoreId]++
			}
		}
	}

	re.Len(regions, cache.GetTotalRegionCount())
	for id, count := range regionCount {
		re.Equal(count, cache.GetStoreRegionCount(id))
	}
	for id, count := range leaderCount {
		re.Equal(count, cache.GetStoreLeaderCount(id))
	}
	for id, count := range followerCount {
		re.Equal(count, cache.GetStoreFollowerCount(id))
	}
	for id, count := range witnessCount {
		re.Equal(count, cache.GetStoreWitnessCount(id))
	}

	for _, region := range cache.GetRegions() {
		checkRegion(re, region, regions[region.GetID()])
	}
	for _, region := range cache.GetMetaRegions() {
		re.Equal(regions[region.GetId()].GetMeta(), region)
	}
}

func checkPendingPeerCount(expect []int, cluster *RaftCluster, re *require.Assertions) {
	for i, e := range expect {
		s := cluster.GetStore(uint64(i + 1))
		re.Equal(e, s.GetPendingPeerCount())
	}
}

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}

	return nil
}

func (c *testCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	id, err := c.AllocID()
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
	return c.setStore(newStore)
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
	return c.setStore(newStore)
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
	return c.setStore(newStore)
}

func (c *testCluster) setStoreDown(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(typeutil.ZeroTime),
	)
	c.Lock()
	defer c.Unlock()
	return c.setStore(newStore)
}

func (c *testCluster) setStoreOffline(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	c.Lock()
	defer c.Unlock()
	return c.setStore(newStore)
}

func (c *testCluster) LoadRegion(regionID uint64, followerStoreIDs ...uint64) error {
	//  regions load from etcd will have no leader
	region := newTestRegionMeta(regionID)
	region.Peers = []*metapb.Peer{}
	for _, id := range followerStoreIDs {
		peer, _ := c.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	return c.putRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
}

func TestBasic(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()

	re.NoError(tc.addLeaderRegion(1, 1))

	op1 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	oc.AddWaitingOperator(op1)
	re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader))
	re.Equal(op1.RegionID(), oc.GetOperator(1).RegionID())

	// Region 1 already has an operator, cannot add another one.
	op2 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op2)
	re.Equal(uint64(0), oc.OperatorCount(operator.OpRegion))

	// Remove the operator manually, then we can add a new operator.
	re.True(oc.RemoveOperator(op1))
	op3 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddWaitingOperator(op3)
	re.Equal(uint64(1), oc.OperatorCount(operator.OpRegion))
	re.Equal(op3.RegionID(), oc.GetOperator(1).RegionID())
}

func TestDispatch(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	co.GetPrepareChecker().SetPrepared()
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

	go co.RunUntilStop()

	// Wait for schedule and turn off balance.
	waitOperator(re, co, 1)
	controller := co.GetSchedulersController()
	operatorutil.CheckTransferPeer(re, co.GetOperatorController().GetOperator(1), operator.OpKind(0), 4, 1)
	re.NoError(controller.RemoveScheduler(types.BalanceRegionScheduler.String()))
	waitOperator(re, co, 2)
	operatorutil.CheckTransferLeader(re, co.GetOperatorController().GetOperator(2), operator.OpKind(0), 4, 2)
	re.NoError(controller.RemoveScheduler(types.BalanceLeaderScheduler.String()))

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

func dispatchHeartbeat(co *schedule.Coordinator, region *core.RegionInfo, stream hbstream.HeartbeatStream) error {
	co.GetHeartbeatStreams().BindStream(region.GetLeader().GetStoreId(), stream)
	if err := co.GetCluster().(*RaftCluster).putRegion(region.Clone(core.SetSource(core.Heartbeat))); err != nil {
		return err
	}
	co.GetOperatorController().Dispatch(region, operator.DispatchFromHeartBeat, nil)
	return nil
}

func TestCollectMetricsConcurrent(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(
			tc.GetBasicCluster(),
			tc.GetOpts(),
			nil)
	}, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()
	rc := co.GetCluster().(*RaftCluster)
	rc.schedulingController = newSchedulingController(rc.serverCtx, rc.GetBasicCluster(), rc.GetOpts(), rc.GetRuleManager())
	rc.schedulingController.coordinator = co
	controller := co.GetSchedulersController()
	// Make sure there are no problem when concurrent write and read
	var wg sync.WaitGroup
	count := 10
	wg.Add(count + 1)
	for i := 0; i <= count; i++ {
		go func(i int) {
			defer wg.Done()
			for range 1000 {
				re.NoError(tc.addRegionStore(uint64(i%5), rand.Intn(200)))
			}
		}(i)
	}
	for range 1000 {
		co.CollectHotSpotMetrics()
		controller.CollectSchedulerMetrics()
		rc.collectSchedulingMetrics()
	}
	schedule.ResetHotSpotMetrics()
	schedulers.ResetSchedulerMetrics()
	resetSchedulingMetrics()
	wg.Wait()
}

func TestCollectMetrics(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(
			tc.GetBasicCluster(),
			tc.GetOpts(),
			nil)
	}, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()

	rc := co.GetCluster().(*RaftCluster)
	rc.schedulingController = newSchedulingController(rc.serverCtx, rc.GetBasicCluster(), rc.GetOpts(), rc.GetRuleManager())
	rc.schedulingController.coordinator = co
	controller := co.GetSchedulersController()
	count := 10
	for i := 0; i <= count; i++ {
		for k := range 200 {
			item := &statistics.HotPeerStat{
				StoreID:   uint64(i % 5),
				RegionID:  uint64(i*1000 + k),
				Loads:     []float64{10, 20, 30},
				HotDegree: 10,
				AntiCount: utils.HotRegionAntiCount, // for write
			}
			tc.hotStat.HotCache.Update(item, utils.Write)
		}
	}

	for range 1000 {
		co.CollectHotSpotMetrics()
		controller.CollectSchedulerMetrics()
		rc.collectSchedulingMetrics()
	}
	stores := co.GetCluster().GetStores()
	regionStats := co.GetCluster().GetHotPeerStats(utils.Write)
	status1 := statistics.CollectHotPeerInfos(stores, regionStats)
	status2 := statistics.GetHotStatus(stores, co.GetCluster().GetStoresLoads(), regionStats, utils.Write, co.GetCluster().GetSchedulerConfig().IsTraceRegionFlow())
	for _, s := range status2.AsLeader {
		s.Stats = nil
	}
	for _, s := range status2.AsPeer {
		s.Stats = nil
	}
	re.Equal(status1, status2)
	schedule.ResetHotSpotMetrics()
	schedulers.ResetSchedulerMetrics()
	resetSchedulingMetrics()
}

func prepare(setCfg func(*sc.ScheduleConfig), setTc func(*testCluster), run func(*schedule.Coordinator), re *require.Assertions) (*testCluster, *schedule.Coordinator, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg, opt, err := newTestScheduleConfig()
	re.NoError(err)
	if setCfg != nil {
		setCfg(cfg)
	}
	tc := newTestCluster(ctx, opt)
	hbStreams := hbstream.NewTestHeartbeatStreams(ctx, tc, true /* need to run */)
	if setTc != nil {
		setTc(tc)
	}
	co := schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	if run != nil {
		run(co)
	}
	return tc, co, func() {
		co.Stop()
		co.GetSchedulersController().Wait()
		co.GetWaitGroup().Wait()
		hbStreams.Close()
		cancel()
	}
}

func checkRegionAndOperator(re *require.Assertions, tc *testCluster, co *schedule.Coordinator, regionID uint64, expectAddOperator int) {
	ops := co.GetCheckerController().CheckRegion(tc.GetRegion(regionID))
	if ops == nil {
		re.Equal(0, expectAddOperator)
	} else {
		re.Equal(expectAddOperator, co.GetOperatorController().AddWaitingOperator(ops...))
	}
}

func TestCheckRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(nil, nil, nil, re)
	hbStreams, opt := co.GetHeartbeatStreams(), tc.opt
	defer cleanup()

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	checkRegionAndOperator(re, tc, co, 1, 1)
	operatorutil.CheckAddPeer(re, co.GetOperatorController().GetOperator(1), operator.OpReplica, 1)
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
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)

	re.NoError(tc.addRegionStore(4, 4))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 0)
	r = r.Clone(core.WithPendingPeers(nil))
	re.NoError(tc.putRegion(r))
	checkRegionAndOperator(re, tc, co, 1, 1)
	op := co.GetOperatorController().GetOperator(1)
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
		Data:     []any{map[string]any{"start_key": "", "end_key": ""}},
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

	tc, co, cleanup := prepare(func(cfg *sc.ScheduleConfig) {
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
		for j := range num {
			regionID := j + uint64(i+1)*num
			re.NoError(tc.addLeaderRegion(regionID, 1))
			switch operatorKind {
			case operator.OpReplica:
				op := operator.NewTestOperator(regionID, tc.GetRegion(regionID).GetRegionEpoch(), operatorKind)
				re.Equal(1, co.GetOperatorController().AddWaitingOperator(op))
			case operator.OpRegion | operator.OpMerge:
				if regionID%2 == 1 {
					ops, err := operator.CreateMergeRegionOperator("merge-region", co.GetCluster(), tc.GetRegion(regionID), tc.GetRegion(regionID-1), operator.OpMerge)
					re.NoError(err)
					re.Len(ops, co.GetOperatorController().AddWaitingOperator(ops...))
				}
			}
		}
	}
	checkRegionAndOperator(re, tc, co, num, 0)
}

func TestMergeRegionCancelOneOperator(t *testing.T) {
	re := require.New(t)
	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()

	source := core.NewRegionInfo(
		&metapb.Region{
			Id:       1,
			StartKey: []byte(""),
			EndKey:   []byte("a"),
		},
		nil,
	)
	target := core.NewRegionInfo(
		&metapb.Region{
			Id:       2,
			StartKey: []byte("a"),
			EndKey:   []byte("t"),
		},
		nil,
	)
	re.NoError(tc.putRegion(source))
	re.NoError(tc.putRegion(target))

	// Cancel source region.
	ops, err := operator.CreateMergeRegionOperator("merge-region", tc, source, target, operator.OpMerge)
	re.NoError(err)
	re.Len(ops, co.GetOperatorController().AddWaitingOperator(ops...))
	// Cancel source operator.
	co.GetOperatorController().RemoveOperator(co.GetOperatorController().GetOperator(source.GetID()))
	re.Empty(co.GetOperatorController().GetOperators())

	// Cancel target region.
	ops, err = operator.CreateMergeRegionOperator("merge-region", tc, source, target, operator.OpMerge)
	re.NoError(err)
	re.Len(ops, co.GetOperatorController().AddWaitingOperator(ops...))
	// Cancel target operator.
	co.GetOperatorController().RemoveOperator(co.GetOperatorController().GetOperator(target.GetID()))
	re.Empty(co.GetOperatorController().GetOperators())
}

func TestReplica(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(func(cfg *sc.ScheduleConfig) {
		// Turn off balance.
		cfg.LeaderScheduleLimit = 0
		cfg.RegionScheduleLimit = 0
	}, nil, func(co *schedule.Coordinator) { co.Run() }, re)
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

	// Remove peer from store 3.
	re.NoError(tc.addLeaderRegion(2, 1, 2, 3, 4))
	region = tc.GetRegion(2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitRemovePeer(re, stream, region, 3) // store3 is down, we should remove it firstly.
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

	tc, co, cleanup := prepare(func(cfg *sc.ScheduleConfig) {
		// Turn off replica scheduling.
		cfg.ReplicaScheduleLimit = 0
	}, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	checker := co.GetCheckerController()

	re.NoError(tc.addRegionStore(1, 0))
	re.NoError(tc.addRegionStore(2, 0))
	re.NoError(tc.addRegionStore(3, 0))

	// Add a peer with two replicas.
	re.NoError(tc.addLeaderRegion(1, 2, 3))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/breakPatrol", `return`))

	// case 1: operator cannot be created due to replica-schedule-limit restriction
	checker.PatrolRegions()
	re.Empty(oc.GetOperators())
	re.Len(checker.GetPendingProcessedRegions(), 1)

	// cancel the replica-schedule-limit restriction
	cfg := tc.GetScheduleConfig()
	cfg.ReplicaScheduleLimit = 10
	tc.SetScheduleConfig(cfg)
	checker.PatrolRegions()
	re.Len(oc.GetOperators(), 1)
	re.Empty(checker.GetPendingProcessedRegions())

	// case 2: operator cannot be created due to store limit restriction
	oc.RemoveOperator(oc.GetOperator(1))
	tc.SetStoreLimit(1, storelimit.AddPeer, 0)
	checker.PatrolRegions()
	re.Len(checker.GetPendingProcessedRegions(), 1)

	// cancel the store limit restriction
	tc.SetStoreLimit(1, storelimit.AddPeer, 10)
	time.Sleep(time.Second)
	checker.PatrolRegions()
	re.Len(oc.GetOperators(), 1)
	re.Empty(checker.GetPendingProcessedRegions())

	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/breakPatrol"))
}

func TestPatrolRegionConcurrency(t *testing.T) {
	re := require.New(t)

	regionNum := 10000
	mergeScheduleLimit := 15

	tc, co, cleanup := prepare(func(cfg *sc.ScheduleConfig) {
		cfg.PatrolRegionWorkerCount = 8
		cfg.MergeScheduleLimit = uint64(mergeScheduleLimit)
	}, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	checker := co.GetCheckerController()

	tc.opt.SetSplitMergeInterval(time.Duration(0))
	for i := range 3 {
		if err := tc.addRegionStore(uint64(i+1), regionNum); err != nil {
			return
		}
	}
	for i := range regionNum {
		if err := tc.addLeaderRegion(uint64(i), 1, 2, 3); err != nil {
			return
		}
	}

	// test patrol region concurrency
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/breakPatrol", `return`))
	checker.PatrolRegions()
	testutil.Eventually(re, func() bool {
		return len(oc.GetOperators()) >= mergeScheduleLimit
	})
	checkOperatorDuplicate(re, oc.GetOperators())

	// test patrol region concurrency with suspect regions
	suspectRegions := make([]uint64, 0)
	for i := range 10 {
		suspectRegions = append(suspectRegions, uint64(i))
	}
	checker.AddPendingProcessedRegions(false, suspectRegions...)
	checker.PatrolRegions()
	testutil.Eventually(re, func() bool {
		return len(oc.GetOperators()) >= mergeScheduleLimit
	})
	checkOperatorDuplicate(re, oc.GetOperators())
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/breakPatrol"))
}

func checkOperatorDuplicate(re *require.Assertions, ops []*operator.Operator) {
	regionMap := make(map[uint64]struct{})
	for _, op := range ops {
		if _, ok := regionMap[op.RegionID()]; ok {
			re.Fail("duplicate operator")
		}
		regionMap[op.RegionID()] = struct{}{}
	}
}

func TestScanLimit(t *testing.T) {
	re := require.New(t)

	checkScanLimit(re, 1000, checker.MinPatrolRegionScanLimit)
	checkScanLimit(re, 10000)
	checkScanLimit(re, 100000)
	checkScanLimit(re, 1000000)
	checkScanLimit(re, 10000000, checker.MaxPatrolScanRegionLimit)
}

func checkScanLimit(re *require.Assertions, regionCount int, expectScanLimit ...int) {
	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/breakPatrol", `return`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/regionCount", fmt.Sprintf("return(\"%d\")", regionCount)))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/breakPatrol"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/regionCount"))
	}()

	re.NoError(tc.addRegionStore(1, 0))
	re.NoError(tc.addRegionStore(2, 0))
	re.NoError(tc.addRegionStore(3, 0))
	regions := newTestRegions(10, 3, 3)
	for i, region := range regions {
		if i == 0 {
			region.GetMeta().StartKey = []byte("")
		}
		if i == len(regions)-1 {
			region.GetMeta().EndKey = []byte("")
		}
		re.NoError(tc.putRegion(region))
	}

	co.GetCheckerController().PatrolRegions()
	defer func() {
		co.GetSchedulersController().Wait()
		co.GetWaitGroup().Wait()
	}()

	limit := co.GetCheckerController().GetPatrolRegionScanLimit()
	re.LessOrEqual(checker.MinPatrolRegionScanLimit, limit)
	re.GreaterOrEqual(checker.MaxPatrolScanRegionLimit, limit)
	if len(expectScanLimit) > 0 {
		re.Equal(expectScanLimit[0], limit)
	}
}

func TestPeerState(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()

	// Transfer peer from store 4 to store 1.
	re.NoError(tc.addRegionStore(1, 10))
	re.NoError(tc.addRegionStore(2, 10))
	re.NoError(tc.addRegionStore(3, 10))
	re.NoError(tc.addRegionStore(4, 40))
	re.NoError(tc.addLeaderRegion(1, 2, 3, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Wait for schedule.
	waitOperator(re, co, 1)
	operatorutil.CheckTransferPeer(re, co.GetOperatorController().GetOperator(1), operator.OpKind(0), 4, 1)

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
	re.NotNil(co.GetOperatorController().GetOperator(region.GetID()))

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
	re.False(co.ShouldRun())
	re.Equal(2, tc.GetStoreRegionCount(4))

	testCases := []struct {
		regionID  uint64
		ShouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		// store4 needs Collect two region
		{6, false},
		{7, true},
	}

	for _, testCase := range testCases {
		r := tc.GetRegion(testCase.regionID)
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]), core.SetSource(core.Heartbeat))
		re.NoError(tc.processRegionHeartbeat(core.ContextTODO(), nr))
		re.Equal(testCase.ShouldRun, co.ShouldRun())
	}
	nr := &metapb.Region{Id: 6, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil, core.SetSource(core.Heartbeat))
	re.Error(tc.processRegionHeartbeat(core.ContextTODO(), newRegion))
	re.Equal(7, tc.GetClusterNotFromStorageRegionsCnt())
}

func TestShouldRunWithNonLeaderRegions(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	tc.RaftCluster.coordinator = co
	defer cleanup()

	re.NoError(tc.addLeaderStore(1, 10))
	re.NoError(tc.addLeaderStore(2, 0))
	re.NoError(tc.addLeaderStore(3, 0))
	for i := range 10 {
		re.NoError(tc.LoadRegion(uint64(i+1), 1, 2, 3))
	}
	re.False(co.ShouldRun())
	re.Equal(10, tc.GetStoreRegionCount(1))

	testCases := []struct {
		regionID  uint64
		ShouldRun bool
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
		nr := r.Clone(core.WithLeader(r.GetPeers()[0]), core.SetSource(core.Heartbeat))
		re.NoError(tc.processRegionHeartbeat(core.ContextTODO(), nr))
		re.Equal(testCase.ShouldRun, co.ShouldRun())
	}
	nr := &metapb.Region{Id: 9, Peers: []*metapb.Peer{}}
	newRegion := core.NewRegionInfo(nr, nil, core.SetSource(core.Heartbeat))
	re.Error(tc.processRegionHeartbeat(core.ContextTODO(), newRegion))
	re.Equal(9, tc.GetClusterNotFromStorageRegionsCnt())

	// Now, after server is prepared, there exist some regions with no leader.
	re.Equal(uint64(0), tc.GetRegion(10).GetLeader().GetStoreId())
}

func TestAddScheduler(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()
	controller := co.GetSchedulersController()
	re.Len(controller.GetSchedulerNames(), len(sc.DefaultSchedulers))
	re.NoError(controller.RemoveScheduler(types.BalanceLeaderScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.BalanceRegionScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.BalanceHotRegionScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.EvictSlowStoreScheduler.String()))
	re.Empty(controller.GetSchedulerNames())

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

	oc := co.GetOperatorController()

	// test ConfigJSONDecoder create
	bl, err := schedulers.CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigJSONDecoder([]byte("{}")))
	re.NoError(err)
	conf, err := bl.EncodeConfig()
	re.NoError(err)
	data := make(map[string]any)
	err = json.Unmarshal(conf, &data)
	re.NoError(err)
	batch := data["batch"].(float64)
	re.Equal(4, int(batch))
	gls, err := schedulers.CreateScheduler(types.GrantLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(types.GrantLeaderScheduler, []string{"0"}), controller.RemoveScheduler)
	re.NoError(err)
	re.Error(controller.AddScheduler(gls))
	re.Error(controller.RemoveScheduler(gls.GetName()))

	gls, err = schedulers.CreateScheduler(types.GrantLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(types.GrantLeaderScheduler, []string{"1"}), controller.RemoveScheduler)
	re.NoError(err)
	re.NoError(controller.AddScheduler(gls))

	hb, err := schedulers.CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigJSONDecoder([]byte("{}")))
	re.NoError(err)
	conf, err = hb.EncodeConfig()
	re.NoError(err)
	data = make(map[string]any)
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

	tc, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	hbStreams := co.GetHeartbeatStreams()
	defer cleanup()
	defaultCount := len(sc.DefaultSchedulers)
	// Add stores 1,2
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))

	controller := co.GetSchedulersController()
	re.Len(controller.GetSchedulerNames(), defaultCount)
	oc := co.GetOperatorController()
	storage := tc.RaftCluster.storage

	gls1, err := schedulers.CreateScheduler(types.GrantLeaderScheduler, oc, storage, schedulers.ConfigSliceDecoder(types.GrantLeaderScheduler, []string{"1"}), controller.RemoveScheduler)
	re.NoError(err)
	re.NoError(controller.AddScheduler(gls1, "1"))
	evict, err := schedulers.CreateScheduler(types.EvictLeaderScheduler, oc, storage, schedulers.ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"2"}), controller.RemoveScheduler)
	re.NoError(err)
	re.NoError(controller.AddScheduler(evict, "2"))
	re.Len(controller.GetSchedulerNames(), defaultCount+2)
	sches, _, err := storage.LoadAllSchedulerConfigs()
	re.NoError(err)
	re.Len(sches, defaultCount+2)

	// remove all default schedulers
	re.NoError(controller.RemoveScheduler(types.BalanceLeaderScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.BalanceRegionScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.BalanceHotRegionScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.EvictSlowStoreScheduler.String()))
	// only remains 2 items with independent config.
	re.Len(controller.GetSchedulerNames(), 2)
	re.NoError(co.GetCluster().GetSchedulerConfig().Persist(storage))
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	// make a new coordinator for testing
	// whether the schedulers added or removed in dynamic way are recorded in opt
	_, newOpt, err := newTestScheduleConfig()
	re.NoError(err)
	shuffle, err := schedulers.CreateScheduler(types.ShuffleRegionScheduler, oc, storage, schedulers.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	re.NoError(controller.AddScheduler(shuffle))
	// suppose we add a new default enable scheduler
	sc.DefaultSchedulers = append(sc.DefaultSchedulers, sc.SchedulerConfig{
		Type: types.SchedulerTypeCompatibleMap[types.ShuffleRegionScheduler],
	})
	defer func() {
		sc.DefaultSchedulers = sc.DefaultSchedulers[:len(sc.DefaultSchedulers)-1]
	}()
	re.Len(newOpt.GetSchedulers(), defaultCount)
	re.NoError(newOpt.Reload(storage))
	// only remains 3 items with independent config.
	sches, _, err = storage.LoadAllSchedulerConfigs()
	re.NoError(err)
	re.Len(sches, 3)

	// option have 9 items because the default scheduler do not remove.
	re.Len(newOpt.GetSchedulers(), defaultCount+3)
	re.NoError(newOpt.Persist(storage))
	tc.RaftCluster.SetScheduleConfig(newOpt.GetScheduleConfig())

	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.Run()
	controller = co.GetSchedulersController()
	re.Len(controller.GetSchedulerNames(), 3)
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	// suppose restart PD again
	_, newOpt, err = newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(storage))
	tc.RaftCluster.SetScheduleConfig(newOpt.GetScheduleConfig())
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.Run()
	controller = co.GetSchedulersController()
	re.Len(controller.GetSchedulerNames(), 3)
	bls, err := schedulers.CreateScheduler(types.BalanceLeaderScheduler, oc, storage, schedulers.ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	re.NoError(controller.AddScheduler(bls))
	brs, err := schedulers.CreateScheduler(types.BalanceRegionScheduler, oc, storage, schedulers.ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
	re.NoError(err)
	re.NoError(controller.AddScheduler(brs))
	re.Len(controller.GetSchedulerNames(), 5)

	// the scheduler option should contain 9 items
	// the `hot scheduler` are disabled
	re.Len(co.GetCluster().GetSchedulerConfig().(*config.PersistOptions).GetSchedulers(), defaultCount+3)
	re.NoError(controller.RemoveScheduler(types.GrantLeaderScheduler.String()))
	// the scheduler that is not enable by default will be completely deleted
	re.Len(co.GetCluster().GetSchedulerConfig().(*config.PersistOptions).GetSchedulers(), defaultCount+2)
	re.Len(controller.GetSchedulerNames(), 4)
	re.NoError(co.GetCluster().GetSchedulerConfig().Persist(co.GetCluster().GetStorage()))
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
	_, newOpt, err = newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(co.GetCluster().GetStorage()))
	tc.RaftCluster.SetScheduleConfig(newOpt.GetScheduleConfig())
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)

	co.Run()
	controller = co.GetSchedulersController()
	re.Len(controller.GetSchedulerNames(), 4)
	re.NoError(controller.RemoveScheduler(types.EvictLeaderScheduler.String()))
	re.Len(controller.GetSchedulerNames(), 3)
}

func TestRemoveScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(func(cfg *sc.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0
	}, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	hbStreams := co.GetHeartbeatStreams()
	defer cleanup()

	// Add stores 1,2
	re.NoError(tc.addLeaderStore(1, 1))
	re.NoError(tc.addLeaderStore(2, 1))
	defaultCount := len(sc.DefaultSchedulers)
	controller := co.GetSchedulersController()
	re.Len(controller.GetSchedulerNames(), defaultCount)
	oc := co.GetOperatorController()
	storage := tc.RaftCluster.storage

	gls1, err := schedulers.CreateScheduler(types.GrantLeaderScheduler, oc, storage, schedulers.ConfigSliceDecoder(types.GrantLeaderScheduler, []string{"1"}), controller.RemoveScheduler)
	re.NoError(err)
	re.NoError(controller.AddScheduler(gls1, "1"))
	re.Len(controller.GetSchedulerNames(), defaultCount+1)
	sches, _, err := storage.LoadAllSchedulerConfigs()
	re.NoError(err)
	re.Len(sches, defaultCount+1)

	// remove all schedulers
	re.NoError(controller.RemoveScheduler(types.BalanceLeaderScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.BalanceRegionScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.BalanceHotRegionScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.GrantLeaderScheduler.String()))
	re.NoError(controller.RemoveScheduler(types.EvictSlowStoreScheduler.String()))
	// all removed
	sches, _, err = storage.LoadAllSchedulerConfigs()
	re.NoError(err)
	re.Empty(sches)
	re.Empty(controller.GetSchedulerNames())
	re.NoError(co.GetCluster().GetSchedulerConfig().Persist(co.GetCluster().GetStorage()))
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()

	// suppose restart PD again
	_, newOpt, err := newTestScheduleConfig()
	re.NoError(err)
	re.NoError(newOpt.Reload(tc.storage))
	tc.RaftCluster.SetScheduleConfig(newOpt.GetScheduleConfig())
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.Run()
	re.Empty(controller.GetSchedulerNames())
	// the option remains default scheduler
	re.Len(co.GetCluster().GetSchedulerConfig().(*config.PersistOptions).GetSchedulers(), defaultCount)
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()
}

func TestRestart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(func(cfg *sc.ScheduleConfig) {
		// Turn off balance, we test add replica only.
		cfg.LeaderScheduleLimit = 0
		cfg.RegionScheduleLimit = 0
	}, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	hbStreams := co.GetHeartbeatStreams()
	defer cleanup()

	// Add 3 stores (1, 2, 3) and a region with 1 replica on store 1.
	re.NoError(tc.addRegionStore(1, 1))
	re.NoError(tc.addRegionStore(2, 2))
	re.NoError(tc.addRegionStore(3, 3))
	re.NoError(tc.addLeaderRegion(1, 1))
	region := tc.GetRegion(1)

	// Add 1 replica on store 2.
	stream := mockhbstream.NewHeartbeatStream()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 2)
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitPromoteLearner(re, stream, region, 2)
	co.Stop()
	co.GetSchedulersController().Wait()
	co.GetWaitGroup().Wait()

	// Recreate coordinator then add another replica on store 3.
	co = schedule.NewCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.Run()
	re.NoError(dispatchHeartbeat(co, region, stream))
	region = waitAddLearner(re, stream, region, 3)
	re.NoError(dispatchHeartbeat(co, region, stream))
	waitPromoteLearner(re, stream, region, 3)
}

func TestPauseScheduler(t *testing.T) {
	re := require.New(t)

	_, co, cleanup := prepare(nil, nil, func(co *schedule.Coordinator) { co.Run() }, re)
	defer cleanup()
	controller := co.GetSchedulersController()
	_, err := controller.IsSchedulerAllowed("test")
	re.Error(err)
	controller.PauseOrResumeScheduler(types.BalanceLeaderScheduler.String(), 60)
	paused, _ := controller.IsSchedulerPaused(types.BalanceLeaderScheduler.String())
	re.True(paused)
	pausedAt, err := controller.GetPausedSchedulerDelayAt(types.BalanceLeaderScheduler.String())
	re.NoError(err)
	resumeAt, err := controller.GetPausedSchedulerDelayUntil(types.BalanceLeaderScheduler.String())
	re.NoError(err)
	re.Equal(int64(60), resumeAt-pausedAt)
	allowed, _ := controller.IsSchedulerAllowed(types.BalanceLeaderScheduler.String())
	re.False(allowed)
}

func BenchmarkPatrolRegion(b *testing.B) {
	re := require.New(b)

	mergeLimit := uint64(4100)
	regionNum := 10000

	tc, co, cleanup := prepare(func(cfg *sc.ScheduleConfig) {
		cfg.MergeScheduleLimit = mergeLimit
	}, nil, nil, re)
	defer cleanup()

	tc.opt.SetSplitMergeInterval(time.Duration(0))
	for i := 1; i < 4; i++ {
		if err := tc.addRegionStore(uint64(i), regionNum, 96); err != nil {
			return
		}
	}
	for i := range regionNum {
		if err := tc.addLeaderRegion(uint64(i), 1, 2, 3); err != nil {
			return
		}
	}

	listen := make(chan int)
	go func() {
		oc := co.GetOperatorController()
		listen <- 0
		for {
			if oc.OperatorCount(operator.OpMerge) == mergeLimit {
				co.Stop()
				return
			}
		}
	}()
	<-listen

	b.ResetTimer()
	checker := co.GetCheckerController()
	checker.PatrolRegions()
}

func waitOperator(re *require.Assertions, co *schedule.Coordinator, regionID uint64) {
	testutil.Eventually(re, func() bool {
		return co.GetOperatorController().GetOperator(regionID) != nil
	})
}

func TestOperatorCount(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	re.Equal(uint64(0), oc.OperatorCount(operator.OpLeader))
	re.Equal(uint64(0), oc.OperatorCount(operator.OpRegion))

	re.NoError(tc.addLeaderRegion(1, 1))
	re.NoError(tc.addLeaderRegion(2, 2))
	{
		op1 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op1)
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader)) // 1:leader
		op2 := operator.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op2)
		re.Equal(uint64(2), oc.OperatorCount(operator.OpLeader)) // 1:leader, 2:leader
		re.True(oc.RemoveOperator(op1))
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader)) // 2:leader
	}

	{
		op1 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
		oc.AddWaitingOperator(op1)
		re.Equal(uint64(1), oc.OperatorCount(operator.OpRegion)) // 1:region 2:leader
		re.Equal(uint64(1), oc.OperatorCount(operator.OpLeader))
		op2 := operator.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion)
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
	oc := co.GetOperatorController()
	lb, err := schedulers.CreateScheduler(types.BalanceRegionScheduler, oc, tc.storage, schedulers.ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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
	for range 10 {
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.Len(ops, 1)
		op := ops[0]
		re.True(oc.AddOperator(op))
		re.True(oc.RemoveOperator(op))
	}
	// sleep 1 seconds to make sure that the token is filled up
	time.Sleep(time.Second)
	for range 100 {
		ops, _ := lb.Schedule(tc, false /* dryRun */)
		re.NotEmpty(ops)
	}
}

func TestStoreOverloadedWithReplace(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	lb, err := schedulers.CreateScheduler(types.BalanceRegionScheduler, oc, tc.storage, schedulers.ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""}))
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
	op1 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 1})
	re.True(oc.AddOperator(op1))
	op2 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 2})
	op2.SetPriorityLevel(constant.High)
	re.True(oc.AddOperator(op2))
	op3 := operator.NewTestOperator(1, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion, operator.AddPeer{ToStore: 1, PeerID: 3})
	re.False(oc.AddOperator(op3))
	ops, _ := lb.Schedule(tc, false /* dryRun */)
	re.Empty(ops)
	// make sure that token is filled up
	testutil.Eventually(re, func() bool {
		ops, _ = lb.Schedule(tc, false /* dryRun */)
		return len(ops) != 0
	})
}

func TestDownStoreLimit(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()
	rc := co.GetCheckerController().GetRuleChecker()

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

func (s *mockLimitScheduler) IsScheduleAllowed(sche.SchedulerCluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func TestController(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()
	oc := co.GetOperatorController()

	re.NoError(tc.addLeaderRegion(1, 1))
	re.NoError(tc.addLeaderRegion(2, 2))
	scheduler, err := schedulers.CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := schedulers.NewScheduleController(tc.ctx, co.GetCluster(), co.GetOperatorController(), lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		re.Equal(i, sc.GetInterval())
		re.Empty(sc.Schedule(false))
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	{
		re.True(sc.AllowSchedule(false))
		op1 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op1))
		// count = 1
		re.True(sc.AllowSchedule(false))
		op2 := operator.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op2))
		// count = 2
		re.False(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op1))
		// count = 1
		re.True(sc.AllowSchedule(false))
	}

	op11 := operator.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	// add a PriorityKind operator will remove old operator
	{
		op3 := operator.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpHotRegion)
		op3.SetPriorityLevel(constant.High)
		re.Equal(1, oc.AddWaitingOperator(op11))
		re.False(sc.AllowSchedule(false))
		re.Equal(1, oc.AddWaitingOperator(op3))
		re.True(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op3))
	}

	// add a admin operator will remove old operator
	{
		op2 := operator.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op2))
		re.False(sc.AllowSchedule(false))
		op4 := operator.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpAdmin)
		op4.SetPriorityLevel(constant.High)
		re.Equal(1, oc.AddWaitingOperator(op4))
		re.True(sc.AllowSchedule(false))
		re.True(oc.RemoveOperator(op4))
	}

	// test wrong region id.
	{
		op5 := operator.NewTestOperator(3, &metapb.RegionEpoch{}, operator.OpHotRegion)
		re.Equal(0, oc.AddWaitingOperator(op5))
	}

	// test wrong region epoch.
	re.True(oc.RemoveOperator(op11))
	epoch := &metapb.RegionEpoch{
		Version: tc.GetRegion(1).GetRegionEpoch().GetVersion() + 1,
		ConfVer: tc.GetRegion(1).GetRegionEpoch().GetConfVer(),
	}
	{
		op6 := operator.NewTestOperator(1, epoch, operator.OpLeader)
		re.Equal(0, oc.AddWaitingOperator(op6))
	}
	epoch.Version--
	{
		op6 := operator.NewTestOperator(1, epoch, operator.OpLeader)
		re.Equal(1, oc.AddWaitingOperator(op6))
		re.True(oc.RemoveOperator(op6))
	}
}

func TestInterval(t *testing.T) {
	re := require.New(t)

	tc, co, cleanup := prepare(nil, nil, nil, re)
	defer cleanup()

	lb, err := schedulers.CreateScheduler(types.BalanceLeaderScheduler, co.GetOperatorController(), storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	sc := schedulers.NewScheduleController(tc.ctx, co.GetCluster(), co.GetOperatorController(), lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.SetInterval(schedulers.MinScheduleInterval)
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			re.Empty(sc.Schedule(false))
		}
		re.Less(sc.GetInterval(), time.Second*time.Duration(n/2))
	}
}

func waitAddLearner(re *require.Assertions, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *pdpb.RegionHeartbeatResponse
	testutil.Eventually(re, func() bool {
		if r := stream.Recv(); r != nil {
			res = r.(*pdpb.RegionHeartbeatResponse)
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
		if r := stream.Recv(); r != nil {
			res = r.(*pdpb.RegionHeartbeatResponse)
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
		if r := stream.Recv(); r != nil {
			res = r.(*pdpb.RegionHeartbeatResponse)
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
		if r := stream.Recv(); r != nil {
			res = r.(*pdpb.RegionHeartbeatResponse)
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

func BenchmarkHandleStatsAsync(b *testing.B) {
	// Setup: create a new instance of Cluster
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, opt, _ := newTestScheduleConfig()
	c := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	c.coordinator = schedule.NewCoordinator(ctx, c, nil)
	c.SetPrepared()
	region := core.NewRegionInfo(&metapb.Region{
		Id: 1,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte{byte(2)},
		EndKey:   []byte{byte(3)},
		Peers:    []*metapb.Peer{{Id: 11, StoreId: uint64(1)}},
	}, nil,
		core.SetApproximateSize(10),
		core.SetReportInterval(0, 10),
	)
	// Reset timer after setup
	b.ResetTimer()
	// Run HandleStatsAsync b.N times
	for i := 0; i < b.N; i++ {
		cluster.HandleStatsAsync(c, region)
	}
}

func BenchmarkHandleRegionHeartbeat(b *testing.B) {
	// Setup: create a new instance of Cluster
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, _ := newTestScheduleConfig()
	c := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	c.coordinator = schedule.NewCoordinator(ctx, c, nil)
	c.SetPrepared()
	log.SetLevel(logutil.StringToZapLogLevel("fatal"))
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 22, StoreId: 2},
		{Id: 33, StoreId: 3},
	}
	queryStats := &pdpb.QueryStats{
		Get:                    5,
		Coprocessor:            6,
		Scan:                   7,
		Put:                    8,
		Delete:                 9,
		DeleteRange:            10,
		AcquirePessimisticLock: 11,
		Rollback:               12,
		Prewrite:               13,
		Commit:                 14,
	}
	interval := &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10}
	downPeers := []*pdpb.PeerStats{{Peer: peers[1], DownSeconds: 100}, {Peer: peers[2], DownSeconds: 100}}
	pendingPeers := []*metapb.Peer{peers[1], peers[2]}

	var requests []*pdpb.RegionHeartbeatRequest
	for i := range 1000000 {
		request := &pdpb.RegionHeartbeatRequest{
			Region:          &metapb.Region{Id: 10, Peers: peers, StartKey: []byte{byte(i)}, EndKey: []byte{byte(i + 1)}},
			Leader:          peers[0],
			DownPeers:       downPeers,
			PendingPeers:    pendingPeers,
			BytesWritten:    10,
			BytesRead:       20,
			KeysWritten:     100,
			KeysRead:        200,
			ApproximateSize: 30 * units.MiB,
			ApproximateKeys: 300,
			Interval:        interval,
			QueryStats:      queryStats,
			Term:            1,
			CpuUsage:        100,
		}
		requests = append(requests, request)
	}
	flowRoundDivisor := opt.GetPDServerConfig().FlowRoundByDigit

	// Reset timer after setup
	b.ResetTimer()
	// Run HandleRegionHeartbeat b.N times
	for i := 0; i < b.N; i++ {
		region := core.RegionFromHeartbeat(requests[i], flowRoundDivisor)
		c.HandleRegionHeartbeat(region)
	}
}
