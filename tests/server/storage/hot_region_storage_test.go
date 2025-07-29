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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type hotRegionStorageTestSuite struct {
	suite.Suite
	interval time.Duration
	env      *tests.SchedulingTestEnvironment
}

func TestHotRegionStorageTestSuite(t *testing.T) {
	suite.Run(t, new(hotRegionStorageTestSuite))
}

func (s *hotRegionStorageTestSuite) SetupSuite() {
	statistics.DisableDenoising()
	s.interval = 100 * time.Millisecond
	s.env = tests.NewSchedulingTestEnvironment(s.T(),
		func(cfg *config.Config, _ string) {
			cfg.Schedule.HotRegionCacheHitsThreshold = 0
			cfg.Schedule.HotRegionsWriteInterval.Duration = s.interval
			cfg.Schedule.HotRegionsReservedDays = 1
		},
	)
}

func (s *hotRegionStorageTestSuite) TearDownSuite() {
	s.env.Cleanup()
}

func (s *hotRegionStorageTestSuite) SetupTest() {
	re := s.Require()
	s.env.RunFunc(func(cluster *tests.TestCluster) {
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
			tests.MustPutStore(re, cluster, store)
		}
	})
}

func (s *hotRegionStorageTestSuite) TearDownTest() {
	s.env.Reset(s.Require())
}

func (s *hotRegionStorageTestSuite) TestHotRegionStorage() {
	s.env.RunTestInNonMicroserviceEnv(s.checkHotRegionStorage)
}

func (s *hotRegionStorageTestSuite) checkHotRegionStorage(cluster *tests.TestCluster) {
	re := s.Require()
	startTime := time.Now().Unix()
	leaderServer := cluster.GetLeaderServer()
	tests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	tests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	tests.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	tests.MustPutRegion(re, cluster, 4, 2, []byte("g"), []byte("h"),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	storeStats := []*pdpb.StoreStats{
		{
			StoreId:  1,
			Interval: &pdpb.TimeInterval{StartTimestamp: uint64(startTime - utils.StoreHeartBeatReportInterval), EndTimestamp: uint64(startTime)},
			PeerStats: []*pdpb.PeerStat{
				{
					RegionId:  3,
					ReadBytes: 9000000000,
				},
			},
		},
		{
			StoreId:  2,
			Interval: &pdpb.TimeInterval{StartTimestamp: uint64(startTime - utils.StoreHeartBeatReportInterval), EndTimestamp: uint64(startTime)},
			PeerStats: []*pdpb.PeerStat{
				{
					RegionId:  4,
					ReadBytes: 9000000000,
				},
			},
		},
	}
	for _, storeStats := range storeStats {
		err := leaderServer.GetRaftCluster().HandleStoreHeartbeat(&pdpb.StoreHeartbeatRequest{Stats: storeStats}, &pdpb.StoreHeartbeatResponse{})
		re.NoError(err)
	}
	var (
		iter storage.HotRegionStorageIterator
		next *storage.HistoryHotRegion
		err  error
	)
	hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
	testutil.Eventually(re, func() bool { // wait for the history hot region to be written to the storage
		iter = hotRegionStorage.NewIterator([]string{utils.Write.String()}, startTime*1000, time.Now().UnixMilli())
		next, err = iter.Next()
		return err == nil && next != nil
	})
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(2), next.RegionID)
	re.Equal(uint64(2), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	iter = hotRegionStorage.NewIterator([]string{utils.Read.String()}, startTime*1000, time.Now().UnixMilli())
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(3), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(utils.Read.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(4), next.RegionID)
	re.Equal(uint64(2), next.StoreID)
	re.Equal(utils.Read.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
}

func (s *hotRegionStorageTestSuite) TestHotRegionStorageReservedDayConfigChange() {
	s.env.RunTestInNonMicroserviceEnv(s.checkHotRegionStorageReservedDayConfigChange)
}

func (s *hotRegionStorageTestSuite) checkHotRegionStorageReservedDayConfigChange(cluster *tests.TestCluster) {
	re := s.Require()
	startTime := time.Now().Unix()
	leaderServer := cluster.GetLeaderServer()
	tests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	var (
		iter storage.HotRegionStorageIterator
		next *storage.HistoryHotRegion
		err  error
	)
	testutil.Eventually(re, func() bool { // wait for the history hot region to be written to the storage
		hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
		iter = hotRegionStorage.NewIterator([]string{utils.Write.String()}, startTime*1000, time.Now().UnixMilli())
		next, err = iter.Next()
		return err == nil && next != nil
	})
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	schedule := leaderServer.GetConfig().Schedule
	// set reserved day to zero, close hot region storage
	schedule.HotRegionsReservedDays = 0
	err = leaderServer.GetServer().SetScheduleConfig(schedule)
	re.NoError(err)
	time.Sleep(3 * s.interval)
	tests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000),
		core.SetReportInterval(uint64(time.Now().Unix()-utils.RegionHeartBeatReportInterval), uint64(time.Now().Unix())))
	time.Sleep(10 * s.interval)
	hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter = hotRegionStorage.NewIterator([]string{utils.Write.String()}, startTime*1000, time.Now().UnixMilli())
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	// set reserved day to one, open hot region storage
	schedule.HotRegionsReservedDays = 1
	err = leaderServer.GetServer().SetScheduleConfig(schedule)
	re.NoError(err)
	time.Sleep(3 * s.interval)
	hotRegionStorage = leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter = hotRegionStorage.NewIterator([]string{utils.Write.String()}, startTime*1000, time.Now().UnixMilli())
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(2), next.RegionID)
	re.Equal(uint64(2), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
}

func (s *hotRegionStorageTestSuite) TestHotRegionStorageWriteIntervalConfigChange() {
	s.env.RunTestInNonMicroserviceEnv(s.checkHotRegionStorageWriteIntervalConfigChange)
}

func (s *hotRegionStorageTestSuite) checkHotRegionStorageWriteIntervalConfigChange(cluster *tests.TestCluster) {
	re := s.Require()
	startTime := time.Now().Unix()
	leaderServer := cluster.GetLeaderServer()
	tests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"),
		core.SetWrittenBytes(3000000000),
		core.SetReportInterval(uint64(startTime-utils.RegionHeartBeatReportInterval), uint64(startTime)))
	var (
		iter storage.HotRegionStorageIterator
		next *storage.HistoryHotRegion
		err  error
	)
	testutil.Eventually(re, func() bool { // wait for the history hot region to be written to the storage
		hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
		iter = hotRegionStorage.NewIterator([]string{utils.Write.String()}, startTime*1000, time.Now().UnixMilli())
		next, err = iter.Next()
		return err == nil && next != nil
	})
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
	schedule := leaderServer.GetConfig().Schedule
	// set the time to 20 times the interval
	schedule.HotRegionsWriteInterval.Duration = 20 * s.interval
	err = leaderServer.GetServer().SetScheduleConfig(schedule)
	re.NoError(err)
	time.Sleep(3 * s.interval)
	tests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"), core.SetWrittenBytes(6000000000),
		core.SetReportInterval(uint64(time.Now().Unix()-utils.RegionHeartBeatReportInterval), uint64(time.Now().Unix())))
	time.Sleep(10 * s.interval)
	// it cant get new hot region because wait time smaller than hot region write interval
	hotRegionStorage := leaderServer.GetServer().GetHistoryHotRegionStorage()
	iter = hotRegionStorage.NewIterator([]string{utils.Write.String()}, startTime*1000, time.Now().UnixMilli())
	next, err = iter.Next()
	re.NoError(err)
	re.NotNil(next)
	re.Equal(uint64(1), next.RegionID)
	re.Equal(uint64(1), next.StoreID)
	re.Equal(utils.Write.String(), next.HotRegionType)
	next, err = iter.Next()
	re.NoError(err)
	re.Nil(next)
}
