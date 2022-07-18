// Copyright 2022 TiKV Project Authors.
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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

type minResolvedTSTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func TestMinResolvedTSTestSuite(t *testing.T) {
	suite.Run(t, new(minResolvedTSTestSuite))
}

func (suite *minResolvedTSTestSuite) SetupSuite() {
	re := suite.Require()
	cluster.DefaultMinResolvedTSPersistenceInterval = time.Microsecond
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	r1 := newTestRegionInfo(7, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(re, suite.svr, r1)
	r2 := newTestRegionInfo(8, 1, []byte("b"), []byte("c"))
	mustRegionHeartbeat(re, suite.svr, r2)
}

func (suite *minResolvedTSTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *minResolvedTSTestSuite) TestMinResolvedTS() {
	url := suite.urlPrefix + "/min-resolved-ts"
	rc := suite.svr.GetRaftCluster()
	ts := uint64(233)
	rc.SetMinResolvedTS(1, ts)

	// no run job
	result := &minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      false,
		PersistInterval: typeutil.Duration{Duration: 0},
	}
	res, err := testDialClient.Get(url)
	suite.NoError(err)
	defer res.Body.Close()
	listResp := &minResolvedTS{}
	err = apiutil.ReadJSON(res.Body, listResp)
	suite.NoError(err)
	suite.Equal(result, listResp)

	// run job
	interval := typeutil.NewDuration(time.Microsecond)
	cfg := suite.svr.GetRaftCluster().GetOpts().GetPDServerConfig().Clone()
	cfg.MinResolvedTSPersistenceInterval = interval
	suite.svr.GetRaftCluster().GetOpts().SetPDServerConfig(cfg)
	time.Sleep(time.Millisecond)
	result = &minResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      true,
		PersistInterval: interval,
	}
	res, err = testDialClient.Get(url)
	suite.NoError(err)
	defer res.Body.Close()
	listResp = &minResolvedTS{}
	err = apiutil.ReadJSON(res.Body, listResp)
	suite.NoError(err)
	suite.Equal(result, listResp)
}
