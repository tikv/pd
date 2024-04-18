// Copyright 2017 TiKV Project Authors.
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

package api

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type statsTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   testutil.CleanupFunc
	urlPrefix string
}

func TestStatsTestSuite(t *testing.T) {
	suite.Run(t, new(statsTestSuite))
}

func (suite *statsTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *statsTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *statsTestSuite) TestRegionStats() {
	statsURL := suite.urlPrefix + "/stats/region"
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
			core.SetApproximateKvSize(80),
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
			core.SetApproximateKvSize(180),
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
			core.SetApproximateKvSize(1),
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
			core.SetApproximateKvSize(30),
			core.SetApproximateKeys(20),
		),
	}

	re := suite.Require()
	for _, r := range regions {
		mustRegionHeartbeat(re, suite.svr, r)
	}

	// Distribution (L for leader, F for follower):
	// region range       size  rows store1 store2 store3 store4 store5
	// 1      ["", "a")   100   50 	  L      F      F
	// 2      ["a", "t")  200   150	  F                    L      F
	// 3      ["t", "x")  1     1	  F                           L
	// 4      ["x", "")   50    20                   	   L

	statsAll := &statistics.RegionStats{
		Count:            4,
		EmptyCount:       1,
		StorageSize:      351,
		UserStorageSize:  291,
		StorageKeys:      221,
		StoreLeaderCount: map[uint64]int{1: 1, 4: 2, 5: 1},
		StorePeerCount:   map[uint64]int{1: 3, 2: 1, 3: 1, 4: 2, 5: 2},
		StoreLeaderSize:  map[uint64]int64{1: 100, 4: 250, 5: 1},
		StoreLeaderKeys:  map[uint64]int64{1: 50, 4: 170, 5: 1},
		StorePeerSize:    map[uint64]int64{1: 301, 2: 100, 3: 100, 4: 250, 5: 201},
		StorePeerKeys:    map[uint64]int64{1: 201, 2: 50, 3: 50, 4: 170, 5: 151},
	}

	stats23 := &statistics.RegionStats{
		Count:            2,
		EmptyCount:       1,
		StorageSize:      201,
		UserStorageSize:  181,
		StorageKeys:      151,
		StoreLeaderCount: map[uint64]int{4: 1, 5: 1},
		StorePeerCount:   map[uint64]int{1: 2, 4: 1, 5: 2},
		StoreLeaderSize:  map[uint64]int64{4: 200, 5: 1},
		StoreLeaderKeys:  map[uint64]int64{4: 150, 5: 1},
		StorePeerSize:    map[uint64]int64{1: 201, 4: 200, 5: 201},
		StorePeerKeys:    map[uint64]int64{1: 151, 4: 150, 5: 151},
	}

	testdata := []struct {
		startKey string
		endKey   string
		expect   *statistics.RegionStats
	}{
		{
			startKey: "",
			endKey:   "",
			expect:   statsAll,
		}, {
			startKey: url.QueryEscape("\x01\x02"),
			endKey:   url.QueryEscape("xyz\x00\x00"),
			expect:   statsAll,
		},
		{
			startKey: url.QueryEscape("a"),
			endKey:   url.QueryEscape("x"),
			expect:   stats23,
		},
	}

	for _, data := range testdata {
		for _, query := range []string{"", "count"} {
			args := fmt.Sprintf("?start_key=%s&end_key=%s&%s", data.startKey, data.endKey, query)
			res, err := testDialClient.Get(statsURL + args)
			re.NoError(err)
			stats := &statistics.RegionStats{}
			err = apiutil.ReadJSON(res.Body, stats)
			res.Body.Close()
			re.NoError(err)
			re.Equal(data.expect.Count, stats.Count)
			if query != "count" {
				re.Equal(data.expect, stats)
			}
		}
	}
}
