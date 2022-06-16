// Copyright 2021 TiKV Project Authors.
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

package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
)

func TestHotRegionStorage(t *testing.T) {
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
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	pdctl.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(statistics.WriteReportInterval))
	pdctl.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000), core.SetReportInterval(statistics.WriteReportInterval))
	pdctl.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"))
	pdctl.MustPutRegion(re, cluster, 4, 2, []byte("g"), []byte("h"))
	storeStats := []*pdpb.StoreStats{
		{
			StoreId:  1,
			Interval: &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: statistics.ReadReportInterval},
			PeerStats: []*pdpb.PeerStat{
				{
					RegionId:  3,
					ReadBytes: 9000000000,
				},
			},
		},
		{
			StoreId:  2,
			Interval: &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: statistics.ReadReportInterval},
			PeerStats: []*pdpb.PeerStat{
				{
					RegionId:  4,
					ReadBytes: 9000000000,
				},
			},
		},
	}
	for _, storeStats := range storeStats {
		leaderServer.GetRaftCluster().HandleStoreHeartbeat(storeStats)
	}
	// wait hot scheduler starts
	time.Sleep(5000 * time.Millisecond)
	endTime := time.Now().UnixNano() / int64(time.Millisecond)
	hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter := hotRegionStorage.NewIterator([]string{storage.WriteType.String()}, startTime, endTime)
	next, err := iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(2), next.RegionID)
	re.Equal(uint64(2), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	iter = hotRegionStorage.NewIterator([]string{storage.ReadType.String()}, startTime, endTime)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(3), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(storage.ReadType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(4), next.RegionID)
	re.Equal(uint64(2), next.StoreID)
	re.Equal(storage.ReadType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
}

func TestHotRegionStorageReservedDayConfigChange(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	interval := 100 * time.Millisecond
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1,
		func(cfg *config.Config, serverName string) {
			cfg.Schedule.HotRegionCacheHitsThreshold = 0
			cfg.Schedule.HotRegionsWriteInterval.Duration = interval
			cfg.Schedule.HotRegionsReservedDays = 1
		},
	)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
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
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	pdctl.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(statistics.WriteReportInterval))
	// wait hot scheduler starts
	time.Sleep(5000 * time.Millisecond)
	endTime := time.Now().UnixNano() / int64(time.Millisecond)
	hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter := hotRegionStorage.NewIterator([]string{storage.WriteType.String()}, startTime, endTime)
	next, err := iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	schedule := leaderServer.GetConfig().Schedule
	// set reserved day to zero,close hot region storage
	schedule.HotRegionsReservedDays = 0
	leaderServer.GetServer().SetScheduleConfig(schedule)
	time.Sleep(3 * interval)
	pdctl.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000), core.SetReportInterval(statistics.WriteReportInterval))
	time.Sleep(10 * interval)
	endTime = time.Now().UnixNano() / int64(time.Millisecond)
	hotRegionStorage = leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter = hotRegionStorage.NewIterator([]string{storage.WriteType.String()}, startTime, endTime)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	// set reserved day to one,open hot region storage
	schedule.HotRegionsReservedDays = 1
	leaderServer.GetServer().SetScheduleConfig(schedule)
	time.Sleep(3 * interval)
	endTime = time.Now().UnixNano() / int64(time.Millisecond)
	hotRegionStorage = leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter = hotRegionStorage.NewIterator([]string{storage.WriteType.String()}, startTime, endTime)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(2), next.RegionID)
	re.Equal(uint64(2), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
}

func TestHotRegionStorageWriteIntervalConfigChange(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	interval := 100 * time.Millisecond
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1,
		func(cfg *config.Config, serverName string) {
			cfg.Schedule.HotRegionCacheHitsThreshold = 0
			cfg.Schedule.HotRegionsWriteInterval.Duration = interval
			cfg.Schedule.HotRegionsReservedDays = 1
		},
	)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
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
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	pdctl.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(statistics.WriteReportInterval))
	// wait hot scheduler starts
	time.Sleep(5000 * time.Millisecond)
	endTime := time.Now().UnixNano() / int64(time.Millisecond)
	hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter := hotRegionStorage.NewIterator([]string{storage.WriteType.String()}, startTime, endTime)
	next, err := iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	schedule := leaderServer.GetConfig().Schedule
	// set the time to 20 times the interval
	schedule.HotRegionsWriteInterval.Duration = 20 * interval
	leaderServer.GetServer().SetScheduleConfig(schedule)
	time.Sleep(3 * interval)
	pdctl.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000), core.SetReportInterval(statistics.WriteReportInterval))
	time.Sleep(10 * interval)
	endTime = time.Now().UnixNano() / int64(time.Millisecond)
	// it cant get new hot region because wait time smaller than hot region write interval
	hotRegionStorage = leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter = hotRegionStorage.NewIterator([]string{storage.WriteType.String()}, startTime, endTime)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(storage.WriteType.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
}
