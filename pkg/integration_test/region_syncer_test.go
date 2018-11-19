// Copyright 2018 PingCAP, Inc.
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

package integration

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
)

type idAllocator struct {
	id uint64
}

func (alloc *idAllocator) Alloc() uint64 {
	alloc.id++
	return alloc.id
}

func (s *integrationTestSuite) TestRegionSyncer(c *C) {
	c.Parallel()
	cluster, err := newTestCluster(3, func(conf *server.Config) { conf.PDServerCfg.UseRegionStorage = true })
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	s.bootstrapCluster(leaderServer, c)
	rc := leaderServer.server.GetRaftCluster()
	c.Assert(rc, NotNil)
	regionLen := 110
	id := &idAllocator{}
	regions := make([]*core.RegionInfo, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		r := &metapb.Region{
			Id: id.Alloc(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    []*metapb.Peer{{Id: id.Alloc(), StoreId: uint64(0)}},
		}
		regions = append(regions, core.NewRegionInfo(r, r.Peers[0]))
	}
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}
	// ensure flush to region kv
	time.Sleep(3 * time.Second)
	err = leaderServer.Stop()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer, NotNil)
	loadRegions := leaderServer.server.GetRaftCluster().GetRegions()
	c.Assert(len(loadRegions), Equals, regionLen)
}
