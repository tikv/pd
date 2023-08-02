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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/apiutil"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type diagnosticTestSuite struct {
	suite.Suite
	svr             *server.Server
	cleanup         tu.CleanupFunc
	urlPrefix       string
	configPrefix    string
	schedulerPrifex string
}

func TestDiagnosticTestSuite(t *testing.T) {
	suite.Run(t, new(diagnosticTestSuite))
}

func (suite *diagnosticTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/schedulers/diagnostic", addr, apiPrefix)
	suite.schedulerPrifex = fmt.Sprintf("%s%s/api/v1/schedulers", addr, apiPrefix)
	suite.configPrefix = fmt.Sprintf("%s%s/api/v1/config", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	mustPutStore(re, suite.svr, 2, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (suite *diagnosticTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *diagnosticTestSuite) checkStatus(status string, url string) {
	re := suite.Require()
	suite.Eventually(func() bool {
		result := &schedulers.DiagnosticResult{}
		err := tu.ReadGetJSON(re, testDialClient, url, result)
		suite.NoError(err)
		return result.Status == status
	}, time.Second, time.Millisecond*50)
}

func (suite *diagnosticTestSuite) TestSchedulerDiagnosticAPI() {
	re := suite.Require()
	addr := suite.configPrefix
	cfg := &config.Config{}
	err := tu.ReadGetJSON(re, testDialClient, addr, cfg)
	suite.NoError(err)

	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, cfg))
	suite.True(cfg.Schedule.EnableDiagnostic)

	ms := map[string]interface{}{
		"enable-diagnostic": "true",
		"max-replicas":      1,
	}
	postData, err := json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	cfg = &config.Config{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, cfg))
	suite.True(cfg.Schedule.EnableDiagnostic)

	balanceRegionURL := suite.urlPrefix + "/" + schedulers.BalanceRegionName
	result := &schedulers.DiagnosticResult{}
	err = tu.ReadGetJSON(re, testDialClient, balanceRegionURL, result)
	suite.NoError(err)
	suite.Equal("disabled", result.Status)

	evictLeaderURL := suite.urlPrefix + "/" + schedulers.EvictLeaderName
	suite.NoError(tu.CheckGetJSON(testDialClient, evictLeaderURL, nil, tu.StatusNotOK(re)))

	input := make(map[string]interface{})
	input["name"] = schedulers.BalanceRegionName
	body, err := json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.schedulerPrifex, body, tu.StatusOK(suite.Require()))
	suite.NoError(err)
	suite.checkStatus("pending", balanceRegionURL)

	input = make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.schedulerPrifex+"/"+schedulers.BalanceRegionName, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	suite.checkStatus("paused", balanceRegionURL)

	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.schedulerPrifex+"/"+schedulers.BalanceRegionName, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	suite.checkStatus("pending", balanceRegionURL)

	mustPutRegion(re, suite.svr, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	suite.checkStatus("normal", balanceRegionURL)

	deleteURL := fmt.Sprintf("%s/%s", suite.schedulerPrifex, schedulers.BalanceRegionName)
	_, err = apiutil.DoDelete(testDialClient, deleteURL)
	suite.NoError(err)
	suite.checkStatus("disabled", balanceRegionURL)
}
