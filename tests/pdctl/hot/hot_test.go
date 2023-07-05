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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestHot(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

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

	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	pdctl.MustPutStore(re, leaderServer.GetServer(), store1)
	pdctl.MustPutStore(re, leaderServer.GetServer(), store2)
	defer cluster.Destroy()

	// test hot store
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
	for i := statistics.DefaultWriteMfSize; i > 0; i-- {
		start := uint64(now - statistics.StoreHeartBeatReportInterval*int64(i))
		end := start + statistics.StoreHeartBeatReportInterval
		newStats.Interval = &pdpb.TimeInterval{StartTimestamp: start, EndTimestamp: end}
		rc.GetStoresStats().Observe(ss.GetID(), newStats)
	}

	for i := statistics.RegionsStatsRollingWindowsSize; i > 0; i-- {
		rc.GetStoresStats().ObserveRegionsStats([]uint64{2}, []float64{float64(bytesWritten)}, []float64{float64(keysWritten)})
	}

	args := []string{"-u", pdAddr, "hot", "store"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	hotStores := api.HotStoreStats{}
	re.NoError(json.Unmarshal(output, &hotStores))
	re.Equal(float64(bytesWritten)/statistics.StoreHeartBeatReportInterval, hotStores.BytesWriteStats[1])
	re.Equal(float64(bytesRead)/statistics.StoreHeartBeatReportInterval, hotStores.BytesReadStats[1])
	re.Equal(float64(keysWritten)/statistics.StoreHeartBeatReportInterval, hotStores.KeysWriteStats[1])
	re.Equal(float64(keysRead)/statistics.StoreHeartBeatReportInterval, hotStores.KeysReadStats[1])
	re.Equal(float64(bytesWritten), hotStores.BytesWriteStats[2])
	re.Equal(float64(keysWritten), hotStores.KeysWriteStats[2])

	// test hot region
	args = []string{"-u", pdAddr, "config", "set", "hot-region-cache-hits-threshold", "0"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)

	hotStoreID := store1.Id
	count := 0
	testHot := func(hotRegionID, hotStoreID uint64, hotType string) {
		args = []string{"-u", pdAddr, "hot", hotType}
		output, err := pdctl.ExecuteCommand(cmd, args...)
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
					statistics.RegionReadBytes:     float64(1000000000 * reportInterval),
					statistics.RegionReadKeys:      float64(1000000000 * reportInterval),
					statistics.RegionReadQueryNum:  float64(1000000000 * reportInterval),
					statistics.RegionWriteBytes:    0,
					statistics.RegionWriteKeys:     0,
					statistics.RegionWriteQueryNum: 0,
				}
				leader := &metapb.Peer{
					Id:      100 + regionIDCounter,
					StoreId: hotStoreID,
				}
				peerInfo := core.NewPeerInfo(leader, loads, reportInterval)
				region := core.NewRegionInfo(&metapb.Region{
					Id: hotRegionID,
				}, leader)
				rc.GetHotStat().CheckReadAsync(statistics.NewCheckPeerTask(peerInfo, region))
				testutil.Eventually(re, func() bool {
					hotPeerStat := rc.GetHotPeerStat(statistics.Read, hotRegionID, hotStoreID)
					return hotPeerStat != nil
				})
				if reportInterval >= statistics.ReadReportInterval {
					count++
				}
				testHot(hotRegionID, hotStoreID, "read")
			case "write":
				pdctl.MustPutRegion(
					re, cluster,
					hotRegionID, hotStoreID,
					[]byte("c"), []byte("d"),
					core.SetWrittenBytes(1000000000*reportInterval), core.SetReportInterval(0, reportInterval))
				testutil.Eventually(re, func() bool {
					hotPeerStat := rc.GetHotPeerStat(statistics.Write, hotRegionID, hotStoreID)
					return hotPeerStat != nil
				})
				if reportInterval >= statistics.WriteReportInterval {
					count++
				}
				testHot(hotRegionID, hotStoreID, "write")
			}
		}
	}
	reportIntervals := []uint64{
		statistics.HotRegionReportMinInterval,
		statistics.HotRegionReportMinInterval + 1,
		statistics.WriteReportInterval,
		statistics.WriteReportInterval + 1,
		statistics.WriteReportInterval * 2,
		statistics.WriteReportInterval*2 + 1,
	}
	testCommand(reportIntervals, "write")
	count = 0
	reportIntervals = []uint64{
		statistics.HotRegionReportMinInterval,
		statistics.HotRegionReportMinInterval + 1,
		statistics.ReadReportInterval,
		statistics.ReadReportInterval + 1,
		statistics.ReadReportInterval * 2,
		statistics.ReadReportInterval*2 + 1,
	}
	testCommand(reportIntervals, "read")
}

func TestHotWithStoreID(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(cfg *config.Config, serverName string) { cfg.Schedule.HotRegionCacheHitsThreshold = 0 })
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

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

	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	for _, store := range stores {
		pdctl.MustPutStore(re, leaderServer.GetServer(), store)
	}
	defer cluster.Destroy()

	pdctl.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(0, statistics.WriteReportInterval))
	pdctl.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000), core.SetReportInterval(0, statistics.WriteReportInterval))
	pdctl.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"), core.SetWrittenBytes(9000000000), core.SetReportInterval(0, statistics.WriteReportInterval))
	// wait hot scheduler starts
	rc := leaderServer.GetRaftCluster()
	testutil.Eventually(re, func() bool {
		return rc.GetHotPeerStat(statistics.Write, 1, 1) != nil &&
			rc.GetHotPeerStat(statistics.Write, 2, 2) != nil &&
			rc.GetHotPeerStat(statistics.Write, 3, 1) != nil
	})
	args := []string{"-u", pdAddr, "hot", "write", "1"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	hotRegion := statistics.StoreHotPeersInfos{}
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &hotRegion))
	re.Len(hotRegion.AsLeader, 1)
	re.Equal(2, hotRegion.AsLeader[1].Count)
	re.Equal(float64(200000000), hotRegion.AsLeader[1].TotalBytesRate)

	args = []string{"-u", pdAddr, "hot", "write", "1", "2"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	hotRegion = statistics.StoreHotPeersInfos{}
	re.NoError(json.Unmarshal(output, &hotRegion))
	re.Len(hotRegion.AsLeader, 2)
	re.Equal(2, hotRegion.AsLeader[1].Count)
	re.Equal(1, hotRegion.AsLeader[2].Count)
	re.Equal(float64(200000000), hotRegion.AsLeader[1].TotalBytesRate)
	re.Equal(float64(100000000), hotRegion.AsLeader[2].TotalBytesRate)

	buckets := pdctl.MustReportBuckets(re, cluster, 1, []byte("a"), []byte("b"),
		[]uint64{10 * units.MiB, 11 * units.MiB, 12 * units.MiB, 13 * units.MiB})
	args = []string{"-u", pdAddr, "hot", "buckets", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	hotBuckets := make(map[uint64][]*api.HotBucketItem, 1)
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
}

func TestHistoryHotRegions(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1,
		func(cfg *config.Config, serverName string) {
			cfg.Schedule.HotRegionCacheHitsThreshold = 0
			cfg.Schedule.HotRegionsWriteInterval.Duration = 1000 * time.Millisecond
			cfg.Schedule.HotRegionsReservedDays = 1
		},
	)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

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

	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	for _, store := range stores {
		pdctl.MustPutStore(re, leaderServer.GetServer(), store)
	}
	defer cluster.Destroy()
	startTime := time.Now().Unix()
	pdctl.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000),
		core.SetReportInterval(uint64(startTime-statistics.RegionHeartBeatReportInterval), uint64(startTime)))
	pdctl.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000),
		core.SetReportInterval(uint64(startTime-statistics.RegionHeartBeatReportInterval), uint64(startTime)))
	pdctl.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"), core.SetWrittenBytes(9000000000),
		core.SetReportInterval(uint64(startTime-statistics.RegionHeartBeatReportInterval), uint64(startTime)))
	pdctl.MustPutRegion(re, cluster, 4, 3, []byte("g"), []byte("h"), core.SetWrittenBytes(9000000000),
		core.SetReportInterval(uint64(startTime-statistics.RegionHeartBeatReportInterval), uint64(startTime)))
	// wait hot scheduler starts
	testutil.Eventually(re, func() bool {
		hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
		iter := hotRegionStorage.NewIterator([]string{storage.WriteType.String()}, startTime*1000, time.Now().UnixNano()/int64(time.Millisecond))
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
	output, err := pdctl.ExecuteCommand(cmd, args...)
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
	output, err = pdctl.ExecuteCommand(cmd, args...)
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
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &hotRegions))
	re.Empty(hotRegions.HistoryHotRegion)
	args = []string{"-u", pdAddr, "hot", "history"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Error(json.Unmarshal(output, &hotRegions))
	args = []string{"-u", pdAddr, "hot", "history",
		start, end,
		"region_id", "dada",
	}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Error(json.Unmarshal(output, &hotRegions))
	args = []string{"-u", pdAddr, "hot", "history",
		start, end,
		"region_ids", "12323",
	}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Error(json.Unmarshal(output, &hotRegions))
}

func TestHotWithoutHotPeer(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(cfg *config.Config, serverName string) { cfg.Schedule.HotRegionCacheHitsThreshold = 0 })
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

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

	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	for _, store := range stores {
		pdctl.MustPutStore(re, leaderServer.GetServer(), store)
	}
	timestamp := uint64(time.Now().UnixNano())
	load := 1024.0
	for _, store := range stores {
		for i := 0; i < 5; i++ {
			err := leaderServer.GetServer().GetRaftCluster().HandleStoreHeartbeat(&pdpb.StoreHeartbeatRequest{
				Stats: &pdpb.StoreStats{
					StoreId:      store.Id,
					BytesRead:    uint64(load * statistics.StoreHeartBeatReportInterval),
					KeysRead:     uint64(load * statistics.StoreHeartBeatReportInterval),
					BytesWritten: uint64(load * statistics.StoreHeartBeatReportInterval),
					KeysWritten:  uint64(load * statistics.StoreHeartBeatReportInterval),
					Capacity:     1000 * units.MiB,
					Available:    1000 * units.MiB,
					Interval: &pdpb.TimeInterval{
						StartTimestamp: timestamp + uint64(i*statistics.StoreHeartBeatReportInterval),
						EndTimestamp:   timestamp + uint64((i+1)*statistics.StoreHeartBeatReportInterval)},
				},
			}, &pdpb.StoreHeartbeatResponse{})
			re.NoError(err)
		}
	}
	defer cluster.Destroy()

	{
		args := []string{"-u", pdAddr, "hot", "read"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		hotRegion := statistics.StoreHotPeersInfos{}
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &hotRegion))
		re.Equal(hotRegion.AsPeer[1].Count, 0)
		re.Equal(0.0, hotRegion.AsPeer[1].TotalBytesRate)
		re.Equal(load, hotRegion.AsPeer[1].StoreByteRate)
		re.Equal(hotRegion.AsLeader[1].Count, 0)
		re.Equal(0.0, hotRegion.AsLeader[1].TotalBytesRate)
		re.Equal(load, hotRegion.AsLeader[1].StoreByteRate)
	}
	{
		args := []string{"-u", pdAddr, "hot", "write"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		hotRegion := statistics.StoreHotPeersInfos{}
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &hotRegion))
		re.Equal(hotRegion.AsPeer[1].Count, 0)
		re.Equal(0.0, hotRegion.AsPeer[1].TotalBytesRate)
		re.Equal(load, hotRegion.AsPeer[1].StoreByteRate)
		re.Equal(hotRegion.AsLeader[1].Count, 0)
		re.Equal(0.0, hotRegion.AsLeader[1].TotalBytesRate)
		re.Equal(0.0, hotRegion.AsLeader[1].StoreByteRate) // write leader sum
	}
}
