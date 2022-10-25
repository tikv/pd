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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
)

type ExternalTSTestSuite struct {
	suite.Suite
	svr     *server.Server
	cleanup cleanUpFunc
	url     string
}

func TestExternalTSTestSuite(t *testing.T) {
	suite.Run(t, new(ExternalTSTestSuite))
}

func (suite *ExternalTSTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.url = fmt.Sprintf("%s%s/api/v1/external-timestamp", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	r1 := newTestRegionInfo(7, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(re, suite.svr, r1)
	r2 := newTestRegionInfo(8, 1, []byte("b"), []byte("c"))
	mustRegionHeartbeat(re, suite.svr, r2)
}

func (suite *ExternalTSTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *ExternalTSTestSuite) TestExternalTimestamp() {
	// case: set external timestamp
	ts := uint64(233)
	err := suite.svr.SetExternalTS(ts)
	suite.NoError(err)
	suite.checkExternalTS(ts)
	// case2: set smaller external timestamp is invalid
	err = suite.svr.SetExternalTS(ts - 1)
	suite.Error(err)
	suite.checkExternalTS(ts)
	// case3: external ts should be less than global ts.
	globalTS, err := suite.svr.GetGlobalTS()
	suite.NoError(err)
	err = suite.svr.SetExternalTS(globalTS + 1)
	suite.Error(err)
	suite.checkExternalTS(ts)
}

func (suite *ExternalTSTestSuite) checkExternalTS(timestamp uint64) {
	res, err := testDialClient.Get(suite.url)
	suite.NoError(err)
	defer res.Body.Close()
	resp := &externalTimestamp{}
	err = apiutil.ReadJSON(res.Body, resp)
	suite.NoError(err)
	suite.Equal(timestamp, resp.ExternalTimestamp)
	ts, err := suite.svr.GetRaftCluster().GetStorage().LoadExternalTS()
	suite.NoError(err)
	suite.Equal(timestamp, ts)
}
