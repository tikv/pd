// Copyright 2019 TiKV Project Authors.
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

package hot_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

type hotTestSuite struct {
	suite.Suite
	env *pdTests.SchedulingTestEnvironment
}

func TestHotTestSuite(t *testing.T) {
	suite.Run(t, new(hotTestSuite))
}

func (suite *hotTestSuite) SetupSuite() {
	suite.env = pdTests.NewSchedulingTestEnvironment(suite.T(),
		func(conf *config.Config, _ string) {
			conf.Schedule.MaxStoreDownTime.Duration = time.Hour
			conf.Schedule.HotRegionCacheHitsThreshold = 0
		},
	)
}

func (suite *hotTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *hotTestSuite) TearDownTest() {
	cleanFunc := func(cluster *pdTests.TestCluster) {
		leader := cluster.GetLeaderServer()
		hotStat := leader.GetRaftCluster().GetHotStat()
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			hotStat = sche.GetCluster().GetHotStat()
		}
		hotStat.HotCache.CleanCache()
	}
	suite.env.RunTest(cleanFunc)
}

func (suite *hotTestSuite) TestHot() {
	suite.env.RunTest(suite.checkHot)
}

func (suite *hotTestSuite) checkHot(cluster *pdTests.TestCluster) {
	re := suite.Require()
	statistics.Denoising = false
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	store1 := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	store2 := &metapb.Store{
		Id:            2,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
		Labels:        []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}},
	}

	pdTests.MustPutStore(re, cluster, store1)
	pdTests.MustPutStore(re, cluster, store2)

	// test hot store
	leaderServer := cluster.GetLeaderServer()
	ss := leaderServer.GetStore(1)
	now := time.Now().Unix()

	newStats := typeutil.DeepClone(ss.GetStoreStats(), core.StoreStatsFactory)
	bytesWritten := uint64(8 * units.MiB)
	bytesRead := uint64(16 * units.MiB)
	keysWritten := uint64(2000)
	keysRead := uint64(4000)
	newStats.BytesWritten = bytesWritten
	newStats.BytesRead = bytesRead
	newStats.KeysWritten = keysWritten
	newStats.KeysRead = keysRead

	rc := leaderServer.GetRaftCluster()
	stats := rc.GetStoresStats()
	hotStat := rc.GetHotStat()
	getHotPeerStat := rc.GetHotPeerStat
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		stats = sche.GetCluster().GetStoresStats()
		hotStat = sche.GetCluster().GetHotStat()
		getHotPeerStat = sche.GetCluster().GetHotPeerStat
	}

	for i := utils.DefaultWriteMfSize; i > 0; i-- {
		start := uint64(now - utils.StoreHeartBeatReportInterval*int64(i))
		end := start + utils.StoreHeartBeatReportInterval
		newStats.Interval = &pdpb.TimeInterval{StartTimestamp: start, EndTimestamp: end}
		stats.Observe(ss.GetID(), newStats)
	}

	for i := statistics.RegionsStatsRollingWindowsSize; i > 0; i-- {
		stats.ObserveRegionsStats([]uint64{2},
			[]float64{float64(bytesWritten)},
			[]float64{float64(keysWritten)})
	}

	args := []string{"-u", pdAddr, "hot", "store"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	hotStores := handler.HotStoreStats{}
	re.NoError(json.Unmarshal(output, &hotStores))
	re.Equal(float64(bytesWritten)/utils.StoreHeartBeatReportInterval, hotStores.BytesWriteStats[1])
	re.Equal(float64(bytesRead)/utils.StoreHeartBeatReportInterval, hotStores.BytesReadStats[1])
	re.Equal(float64(keysWritten)/utils.StoreHeartBeatReportInterval, hotStores.KeysWriteStats[1])
	re.Equal(float64(keysRead)/utils.StoreHeartBeatReportInterval, hotStores.KeysReadStats[1])
	re.Equal(float64(bytesWritten), hotStores.BytesWriteStats[2])
	re.Equal(float64(keysWritten), hotStores.KeysWriteStats[2])

	// test hot region
	args = []string{"-u", pdAddr, "config", "set", "hot-region-cache-hits-threshold", "0"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	hotStoreID := store1.Id
	count := 0
	testHot := func(hotRegionID, hotStoreID uint64, hotType string) {
		args = []string{"-u", pdAddr, "hot", hotType}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		hotRegion := statistics.StoreHotPeersInfos{}
		re.NoError(json.Unmarshal(output, &hotRegion))
		re.Contains(hotRegion.AsLeader, hotStoreID)
		re.Equal(count, hotRegion.AsLeader[hotStoreID].Count)
		if count > 0 {
			re.Equal(hotRegionID, hotRegion.AsLeader[hotStoreID].Stats[count-1].RegionID)
		}
	}

	regionIDCounter := uint64(1)
	testCommand := func(reportIntervals []uint64, hotType string) {
		for _, reportInterval := range reportIntervals {
			hotRegionID := regionIDCounter
			regionIDCounter++
			switch hotType {
			case "read":
				loads := []float64{
					utils.RegionReadBytes:     float64(1000000000 * reportInterval),
					utils.RegionReadKeys:      float64(1000000000 * reportInterval),
					utils.RegionReadQueryNum:  float64(1000000000 * reportInterval),
					utils.RegionWriteBytes:    0,
					utils.RegionWriteKeys:     0,
					utils.RegionWriteQueryNum: 0,
				}
				leader := &metapb.Peer{
					Id:      100 + regionIDCounter,
					StoreId: hotStoreID,
				}
				region := core.NewRegionInfo(&metapb.Region{
					Id: hotRegionID,
				}, leader)
				checkReadPeerTask := func(cache *statistics.HotPeerCache) {
					stats := cache.CheckPeerFlow(region, []*metapb.Peer{leader}, loads, reportInterval)
					for _, stat := range stats {
						cache.UpdateStat(stat)
					}
				}
				hotStat.CheckReadAsync(checkReadPeerTask)
				testutil.Eventually(re, func() bool {
					hotPeerStat := getHotPeerStat(utils.Read, hotRegionID, hotStoreID)
					return hotPeerStat != nil
				})
				if reportInterval >= utils.StoreHeartBeatReportInterval {
					count++
				}
				testHot(hotRegionID, hotStoreID, "read")
			case "write":
				pdTests.MustPutRegion(
					re, cluster,
					hotRegionID, hotStoreID,
					[]byte("c"), []byte("d"),
					core.SetWrittenBytes(1000000000*reportInterval), core.SetReportInterval(0, reportInterval))
				testutil.Eventually(re, func() bool {
					hotPeerStat := getHotPeerStat(utils.Write, hotRegionID, hotStoreID)
					return hotPeerStat != nil
				})
				if reportInterval >= utils.RegionHeartBeatReportInterval {
					count++
				}
				testHot(hotRegionID, hotStoreID, "write")
			}
		}
	}
	reportIntervals := []uint64{
		statistics.HotRegionReportMinInterval,
		statistics.HotRegionReportMinInterval + 1,
		utils.RegionHeartBeatReportInterval,
		utils.RegionHeartBeatReportInterval + 1,
		utils.RegionHeartBeatReportInterval * 2,
		utils.RegionHeartBeatReportInterval*2 + 1,
	}
	testCommand(reportIntervals, "write")
	count = 0
	reportIntervals = []uint64{
		statistics.HotRegionReportMinInterval,
		statistics.HotRegionReportMinInterval + 1,
		utils.StoreHeartBeatReportInterval,
		utils.StoreHeartBeatReportInterval + 1,
		utils.StoreHeartBeatReportInterval * 2,
		utils.StoreHeartBeatReportInterval*2 + 1,
	}
	testCommand(reportIntervals, "read")
}

func (suite *hotTestSuite) TestHotWithStoreID() {
	suite.env.RunTest(suite.checkHotWithStoreID)
}

func (suite *hotTestSuite) checkHotWithStoreID(cluster *pdTests.TestCluster) {
	re := suite.Require()
	statistics.Denoising = false
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()
	leaderServer := cluster.GetLeaderServer()

	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store)
	}
	s := &server.GrpcServer{Server: leaderServer.GetServer()}
	for _, store := range stores {
		resp1, err := s.StoreHeartbeat(
			context.Background(), &pdpb.StoreHeartbeatRequest{
				Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
				Stats: &pdpb.StoreStats{
					StoreId:   store.Id,
					Capacity:  1000 * units.MiB,
					Available: 1000 * units.MiB,
				},
			},
		)
		re.NoError(err)
		re.Empty(resp1.GetHeader().GetError())
	}

	pdTests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))
	pdTests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))
	pdTests.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"), core.SetWrittenBytes(9000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))

	getHotPeerStat := leaderServer.GetRaftCluster().GetHotPeerStat
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		getHotPeerStat = sche.GetCluster().GetHotPeerStat
	}

	testutil.Eventually(re, func() bool {
		return getHotPeerStat(utils.Write, 1, 1) != nil &&
			getHotPeerStat(utils.Write, 2, 2) != nil &&
			getHotPeerStat(utils.Write, 3, 1) != nil
	})
	args := []string{"-u", pdAddr, "hot", "write", "1"}
	output, err := tests.ExecuteCommand(cmd, args...)
	hotRegion := statistics.StoreHotPeersInfos{}
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &hotRegion))
	re.Len(hotRegion.AsLeader, 1)
	re.Equal(2, hotRegion.AsLeader[1].Count)
	re.Equal(float64(200000000), hotRegion.AsLeader[1].TotalBytesRate)
}

func (suite *hotTestSuite) TestHotWithoutHotPeer() {
	suite.env.RunTest(suite.checkHotWithoutHotPeer)
}

func (suite *hotTestSuite) checkHotWithoutHotPeer(cluster *pdTests.TestCluster) {
	re := suite.Require()
	statistics.Denoising = false

	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	leaderServer := cluster.GetLeaderServer()
	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store)
	}
	timestamp := uint64(time.Now().UnixNano())
	load := 1024.0
	s := &server.GrpcServer{Server: leaderServer.GetServer()}
	for _, store := range stores {
		for i := range 5 {
			resp1, err := s.StoreHeartbeat(
				context.Background(), &pdpb.StoreHeartbeatRequest{
					Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
					Stats: &pdpb.StoreStats{
						StoreId:      store.Id,
						BytesRead:    uint64(load * utils.StoreHeartBeatReportInterval),
						KeysRead:     uint64(load * utils.StoreHeartBeatReportInterval),
						BytesWritten: uint64(load * utils.StoreHeartBeatReportInterval),
						KeysWritten:  uint64(load * utils.StoreHeartBeatReportInterval),
						Capacity:     1000 * units.MiB,
						Available:    1000 * units.MiB,
						Interval: &pdpb.TimeInterval{
							StartTimestamp: timestamp + uint64(i*utils.StoreHeartBeatReportInterval),
							EndTimestamp:   timestamp + uint64((i+1)*utils.StoreHeartBeatReportInterval)},
					},
				},
			)
			re.NoError(err)
			re.Empty(resp1.GetHeader().GetError())
		}
	}

	{
		args := []string{"-u", pdAddr, "hot", "read"}
		output, err := tests.ExecuteCommand(cmd, args...)
		hotRegion := statistics.StoreHotPeersInfos{}
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &hotRegion))
		re.NotNil(hotRegion.AsPeer[1])
		re.Zero(hotRegion.AsPeer[1].Count)
		re.Zero(hotRegion.AsPeer[1].TotalBytesRate)
		re.Equal(load, hotRegion.AsPeer[1].StoreByteRate)
		re.Zero(hotRegion.AsLeader[1].Count)
		re.Zero(hotRegion.AsLeader[1].TotalBytesRate)
		re.Equal(load, hotRegion.AsLeader[1].StoreByteRate)
	}
	{
		args := []string{"-u", pdAddr, "hot", "write"}
		output, err := tests.ExecuteCommand(cmd, args...)
		hotRegion := statistics.StoreHotPeersInfos{}
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &hotRegion))
		re.Zero(hotRegion.AsPeer[1].Count)
		re.Zero(hotRegion.AsPeer[1].TotalBytesRate)
		re.Equal(load, hotRegion.AsPeer[1].StoreByteRate)
		re.Zero(hotRegion.AsLeader[1].Count)
		re.Zero(hotRegion.AsLeader[1].TotalBytesRate)
		re.Zero(hotRegion.AsLeader[1].StoreByteRate) // write leader sum
	}
}

func TestHistoryHotRegions(t *testing.T) {
	// TODO: support history hotspot in scheduling server with stateless in the future.
	// Ref: https://github.com/tikv/pd/pull/7183
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1,
		func(cfg *config.Config, _ string) {
			cfg.Schedule.HotRegionCacheHitsThreshold = 0
			cfg.Schedule.HotRegionsWriteInterval.Duration = 1000 * time.Millisecond
			cfg.Schedule.HotRegionsReservedDays = 1
		},
	)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store)
	}
	defer cluster.Destroy()
	startTime := time.Now().Unix()
	pdTests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	pdTests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	pdTests.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"), core.SetWrittenBytes(9000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	pdTests.MustPutRegion(re, cluster, 4, 3, []byte("g"), []byte("h"), core.SetWrittenBytes(9000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	// wait hot scheduler starts
	testutil.Eventually(re, func() bool {
		hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
		iter := hotRegionStorage.NewIterator([]string{utils.Write.String()}, startTime*1000, time.Now().UnixNano()/int64(time.Millisecond))
		next, err := iter.Next()
		return err == nil && next != nil
	})
	endTime := time.Now().UnixNano() / int64(time.Millisecond)
	start := strconv.FormatInt(startTime*1000, 10)
	end := strconv.FormatInt(endTime, 10)
	args := []string{"-u", pdAddr, "hot", "history",
		start, end,
		"hot_region_type", "write",
		"region_id", "1,2",
		"store_id", "1,4",
		"is_learner", "false",
	}
	output, err := tests.ExecuteCommand(cmd, args...)
	hotRegions := storage.HistoryHotRegions{}
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &hotRegions))
	regions := hotRegions.HistoryHotRegion
	re.Len(regions, 1)
	re.Equal(uint64(1), regions[0].RegionID)
	re.Equal(uint64(1), regions[0].StoreID)
	re.Equal("write", regions[0].HotRegionType)
	args = []string{"-u", pdAddr, "hot", "history",
		start, end,
		"hot_region_type", "write",
		"region_id", "1,2",
		"store_id", "1,2",
	}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &hotRegions))
	regions = hotRegions.HistoryHotRegion
	re.Len(regions, 2)
	isSort := regions[0].UpdateTime > regions[1].UpdateTime || regions[0].RegionID < regions[1].RegionID
	re.True(isSort)
	args = []string{"-u", pdAddr, "hot", "history",
		start, end,
		"hot_region_type", "read",
		"is_leader", "false",
		"peer_id", "12",
	}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &hotRegions))
	re.Empty(hotRegions.HistoryHotRegion)
	args = []string{"-u", pdAddr, "hot", "history"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Error(json.Unmarshal(output, &hotRegions))
	args = []string{"-u", pdAddr, "hot", "history",
		start, end,
		"region_id", "dada",
	}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Error(json.Unmarshal(output, &hotRegions))
	args = []string{"-u", pdAddr, "hot", "history",
		start, end,
		"region_ids", "12323",
	}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Error(json.Unmarshal(output, &hotRegions))
}

func TestBuckets(t *testing.T) {
	// TODO: support forward bucket request in scheduling server in the future.
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1, func(cfg *config.Config, _ string) { cfg.Schedule.HotRegionCacheHitsThreshold = 0 })
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store)
	}
	defer cluster.Destroy()

	pdTests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))
	pdTests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))
	pdTests.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"), core.SetWrittenBytes(9000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))

	stats := &metapb.BucketStats{
		ReadBytes:  []uint64{10 * units.MiB},
		ReadKeys:   []uint64{11 * units.MiB},
		ReadQps:    []uint64{0},
		WriteKeys:  []uint64{12 * units.MiB},
		WriteBytes: []uint64{13 * units.MiB},
		WriteQps:   []uint64{0},
	}
	buckets := pdTests.MustReportBuckets(re, cluster, 1, []byte("a"), []byte("b"), stats)
	args := []string{"-u", pdAddr, "hot", "buckets", "1"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	hotBuckets := handler.HotBucketsResponse{}
	re.NoError(json.Unmarshal(output, &hotBuckets))
	re.Len(hotBuckets, 1)
	re.Len(hotBuckets[1], 1)
	item := hotBuckets[1][0]
	re.Equal(core.HexRegionKeyStr(buckets.GetKeys()[0]), item.StartKey)
	re.Equal(core.HexRegionKeyStr(buckets.GetKeys()[1]), item.EndKey)
	re.Equal(1, item.HotDegree)
	interval := buckets.GetPeriodInMs() / 1000
	re.Equal(buckets.GetStats().ReadBytes[0]/interval, item.ReadBytes)
	re.Equal(buckets.GetStats().ReadKeys[0]/interval, item.ReadKeys)
	re.Equal(buckets.GetStats().WriteBytes[0]/interval, item.WriteBytes)
	re.Equal(buckets.GetStats().WriteKeys[0]/interval, item.WriteKeys)

	args = []string{"-u", pdAddr, "hot", "buckets", "2"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	hotBuckets = handler.HotBucketsResponse{}
	re.NoError(json.Unmarshal(output, &hotBuckets))
	re.Nil(hotBuckets[2])
}
