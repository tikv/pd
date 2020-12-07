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
// See the License for the specific language governing permissions and
// limitations under the License.

package hottest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&hotTestSuite{})

type hotTestSuite struct{}

func (s *hotTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *hotTestSuite) TestHot(c *C) {
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctl.InitCommand()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdctl.MustPutStore(c, leaderServer.GetServer(), store)
	defer cluster.Destroy()

	// test hot store
	ss := leaderServer.GetStore(1)
	now := time.Now().Second()
	newStats := proto.Clone(ss.GetStoreStats()).(*pdpb.StoreStats)
	bytesWritten := uint64(8 * 1024 * 1024)
	bytesRead := uint64(16 * 1024 * 1024)
	keysWritten := uint64(2000)
	keysRead := uint64(4000)
	newStats.BytesWritten = bytesWritten
	newStats.BytesRead = bytesRead
	newStats.KeysWritten = keysWritten
	newStats.KeysRead = keysRead
	rc := leaderServer.GetRaftCluster()
	for i := statistics.DefaultWriteMfSize; i > 0; i-- {
		start := uint64(now - statistics.StoreHeartBeatReportInterval*i)
		end := start + statistics.StoreHeartBeatReportInterval
		newStats.Interval = &pdpb.TimeInterval{StartTimestamp: start, EndTimestamp: end}
		rc.GetStoresStats().Observe(ss.GetID(), newStats)
	}

	args := []string{"-u", pdAddr, "hot", "store"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	hotStores := api.HotStoreStats{}
	c.Assert(json.Unmarshal(output, &hotStores), IsNil)
	c.Assert(hotStores.BytesWriteStats[1], Equals, float64(bytesWritten)/statistics.StoreHeartBeatReportInterval)
	c.Assert(hotStores.BytesReadStats[1], Equals, float64(bytesRead)/statistics.StoreHeartBeatReportInterval)
	c.Assert(hotStores.KeysWriteStats[1], Equals, float64(keysWritten)/statistics.StoreHeartBeatReportInterval)
	c.Assert(hotStores.KeysReadStats[1], Equals, float64(keysRead)/statistics.StoreHeartBeatReportInterval)

	// test hot region
	args = []string{"-u", pdAddr, "config", "set", "hot-region-cache-hits-threshold", "0"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)

	hotStoreID := uint64(1)
	count := 0

	testHot := func(hotRegionID, hotStoreID uint64, hotType string) {
		args = []string{"-u", pdAddr, "hot", hotType}
		output, e := pdctl.ExecuteCommand(cmd, args...)
		hotRegion := statistics.StoreHotPeersInfos{}
		c.Assert(e, IsNil)
		c.Assert(json.Unmarshal(output, &hotRegion), IsNil)
		c.Assert(hotRegion.AsLeader, HasKey, hotStoreID)
		c.Assert(hotRegion.AsLeader[hotStoreID].Count, Equals, count)
		if count > 0 {
			c.Assert(hotRegion.AsLeader[hotStoreID].Stats[count-1].RegionID, Equals, hotRegionID)
		}
	}

	regionIDCounter := uint64(1)
	testCommand := func(reportIntervals []uint64, hotType string) {
		for _, reportInterval := range reportIntervals {
			hotRegionID := regionIDCounter
			regionIDCounter++
			switch hotType {
			case "read":
				pdctl.MustPutRegion(c, cluster, hotRegionID, hotStoreID, []byte("b"), []byte("c"), core.SetReadBytes(1000000000), core.SetReportInterval(reportInterval))
				time.Sleep(5000 * time.Millisecond)
				if reportInterval >= statistics.RegionHeartBeatReportInterval {
					count++
				}
				testHot(hotRegionID, hotStoreID, "read")
			case "write":
				pdctl.MustPutRegion(c, cluster, hotRegionID, hotStoreID, []byte("c"), []byte("d"), core.SetWrittenBytes(1000000000), core.SetReportInterval(reportInterval))
				time.Sleep(5000 * time.Millisecond)
				if reportInterval >= statistics.RegionHeartBeatReportInterval {
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
