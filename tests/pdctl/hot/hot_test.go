// Copyright 2019 PingCAP, Inc.
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

package hot_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/statistics"
	"github.com/pingcap/pd/tests"
	"github.com/pingcap/pd/tests/pdctl"
	"github.com/stretchr/testify/assert"
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
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:      1,
		Address: "tikv1",
		State:   metapb.StoreState_Up,
		Version: "2.0.0",
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	// test hot region
	statistics.Denoising = false
	checkInMap := assert.New(c)
	reportInterval := uint64(3) // need to be minHotRegionReportInterval < reportInterval < 3*RegionHeartBeatReportInterval
	args := []string{"-u", pdAddr, "config", "set", "hot-region-cache-hits-threshold", "0"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	testHot := func(hotRegionID, hotStoreId uint64, hotType string, option core.RegionCreateOption) {
		pdctl.MustPutRegion(c, cluster, hotRegionID, hotStoreId, []byte("b"), []byte("c"), option, core.SetReportInterval(reportInterval))
		time.Sleep(5 * time.Second)
		args = []string{"-u", pdAddr, "hot", hotType}
		_, output, err := pdctl.ExecuteCommandC(cmd, args...)
		hotRegion := statistics.StoreHotRegionInfos{}
		c.Assert(err, IsNil)
		c.Assert(json.Unmarshal(output, &hotRegion), IsNil)
		checkInMap.Contains(hotRegion.AsLeader, hotStoreId)
		c.Assert(hotRegion.AsLeader[hotStoreId].RegionsCount, Equals, 1)
		c.Assert(hotRegion.AsLeader[hotStoreId].RegionsStat[0].RegionID, Equals, hotRegionID)
	}

	testHot(3, 1, "read", core.SetReadBytes(1000000000))
	testHot(2, 1, "write", core.SetWrittenBytes(1000000000))

	//test hot store
	ss := leaderServer.GetStore(1)
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - 10), EndTimestamp: uint64(now)}
	newStats := proto.Clone(ss.GetStoreStats()).(*pdpb.StoreStats)
	bytesWritten := uint64(8 * 1024 * 1024)
	bytesRead := uint64(16 * 1024 * 1024)
	keysWritten := uint64(2000)
	keysRead := uint64(4000)
	newStats.BytesWritten = bytesWritten
	newStats.BytesRead = bytesRead
	newStats.KeysWritten = keysWritten
	newStats.KeysRead = keysRead
	newStats.Interval = interval
	rc := leaderServer.GetRaftCluster()
	rc.GetStoresStats().Observe(ss.GetID(), newStats)
	args = []string{"-u", pdAddr, "hot", "store"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	hotStores := api.HotStoreStats{}
	c.Assert(json.Unmarshal(output, &hotStores), IsNil)
	c.Assert(hotStores.BytesWriteStats[1], Equals, bytesWritten/10)
	c.Assert(hotStores.BytesReadStats[1], Equals, bytesRead/10)
	c.Assert(hotStores.KeysWriteStats[1], Equals, keysWritten/10)
	c.Assert(hotStores.KeysReadStats[1], Equals, keysRead/10)
}
