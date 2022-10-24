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

type ExternalTimestampTestSuite struct {
	suite.Suite
	svr     *server.Server
	cleanup cleanUpFunc
	url     string
}

func TestExternalTimestampTestSuite(t *testing.T) {
	suite.Run(t, new(ExternalTimestampTestSuite))
}

func (suite *ExternalTimestampTestSuite) SetupSuite() {
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

func (suite *ExternalTimestampTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *ExternalTimestampTestSuite) TestExternalTimestamp() {
	// case: set external timestamp
	rc := suite.svr.GetRaftCluster()
	ts := uint64(233)
	err := rc.SetExternalTimestamp(ts)
	suite.NoError(err)
	suite.checkExternalTimestamp(ts)
	// case2: set external smaller timestamp
	err = rc.SetExternalTimestamp(ts - 1)
	suite.Error(err)
	suite.checkExternalTimestamp(ts)
}

func (suite *ExternalTimestampTestSuite) checkExternalTimestamp(timestamp uint64) {
	res, err := testDialClient.Get(suite.url)
	suite.NoError(err)
	defer res.Body.Close()
	resp := &externalTimestamp{}
	err = apiutil.ReadJSON(res.Body, resp)
	suite.NoError(err)
	suite.Equal(timestamp, resp.ExternalTimestamp)
}
