// Copyright 2017 PingCAP, Inc.
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

package api

import (
	"fmt"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/apiutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/statistics"
)

var _ = Suite(&testClusterStatsSuite{})

type testClusterStatsSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testClusterStatsSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testClusterStatsSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterStatsSuite) TestStatsInfo(c *C) {
	statsURL := s.urlPrefix + "/cluster_stats"
	epoch := &metapb.RegionEpoch{
		ConfVer: 1,
		Version: 1,
	}
	regions := []*core.RegionInfo{
		core.NewRegionInfo(&metapb.Region{
			Id:       1,
			StartKey: []byte(""),
			EndKey:   []byte("a"),
			Peers: []*metapb.Peer{
				{Id: 101, StoreId: 1},
				{Id: 102, StoreId: 2},
				{Id: 103, StoreId: 3},
			},
			RegionEpoch: epoch,
		},
			&metapb.Peer{Id: 101, StoreId: 1},
			core.SetApproximateSize(100),
			core.SetApproximateKeys(50),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       2,
				StartKey: []byte("a"),
				EndKey:   []byte("t"),
				Peers: []*metapb.Peer{
					{Id: 104, StoreId: 1},
					{Id: 105, StoreId: 4},
					{Id: 106, StoreId: 5},
				},
				RegionEpoch: epoch,
			},
			&metapb.Peer{Id: 105, StoreId: 4},
			core.SetApproximateSize(200),
			core.SetApproximateKeys(150),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       3,
				StartKey: []byte("t"),
				EndKey:   []byte("x"),
				Peers: []*metapb.Peer{
					{Id: 106, StoreId: 1},
					{Id: 107, StoreId: 5},
				},
				RegionEpoch: epoch,
			},
			&metapb.Peer{Id: 107, StoreId: 5},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       4,
				StartKey: []byte("x"),
				EndKey:   []byte(""),
				Peers: []*metapb.Peer{
					{Id: 108, StoreId: 4},
				},
				RegionEpoch: epoch,
			},
			&metapb.Peer{Id: 108, StoreId: 4},
			core.SetApproximateSize(50),
			core.SetApproximateKeys(20),
		),
	}

	for _, r := range regions {
		mustRegionHeartbeat(c, s.svr, r)
	}

	res, err := http.Get(statsURL)
	c.Assert(err, IsNil)
	stats := &statistics.RegionStats{}
	err = apiutil.ReadJSON(res.Body, stats)
	c.Assert(err, IsNil)
}
