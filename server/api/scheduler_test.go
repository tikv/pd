// Copyright 2025 TiKV Project Authors.
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
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/schedule/types"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type schedulerTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(schedulerTestSuite))
}

func (suite *schedulerTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})
	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/", addr, apiPrefix)
	mustBootstrapCluster(re, suite.svr)
}

func (suite *schedulerTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *schedulerTestSuite) TestGetSchedulers() {
	input := map[string]any{
		"alias":     "test-sysbench-partition(p1)",
		"engine":    "tikv",
		"rule":      "leader",
		"name":      types.BalanceRangeScheduler.String(),
		"start-key": "100",
		"end-key":   "200",
	}
	data, err := json.Marshal(input)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.urlPrefix+"schedulers", data, func(_ []byte, i int, _ http.Header) {
		suite.Equal(http.StatusOK, i)
	}))

	// check
	suite.NoError(tu.CheckGetJSON(testDialClient,
		suite.urlPrefix+"scheduler-config"+"/"+types.BalanceRangeScheduler.String()+"/list", nil,
		func(data []byte, i int, _ http.Header) {
			suite.Equal(http.StatusOK, i)
			var resp []any
			suite.NoError(json.Unmarshal(data, &resp))
			suite.Len(resp, 1)
			suite.Equal(input["alias"], resp[0].(map[string]any)["alias"])
		}))

	// add more job
	input["alias"] = "test-sysbench-partition(p2)"
	data, err = json.Marshal(input)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.urlPrefix+"schedulers", data, func(_ []byte, i int, _ http.Header) {
		suite.Equal(http.StatusOK, i)
	}))
	suite.NoError(tu.CheckGetJSON(testDialClient,
		suite.urlPrefix+"scheduler-config"+"/"+types.BalanceRangeScheduler.String()+"/list", nil,
		func(data []byte, i int, _ http.Header) {
			suite.Equal(http.StatusOK, i)
			var resp []any
			suite.NoError(json.Unmarshal(data, &resp))
			suite.Len(resp, 2)
			suite.Equal(input["alias"], resp[1].(map[string]any)["alias"])
		}))

	// cancel job
	suite.NoError(tu.CheckDelete(testDialClient,
		suite.urlPrefix+"scheduler-config"+"/"+types.BalanceRangeScheduler.String()+"/job?job-id=1",
		func(_ []byte, i int, _ http.Header) {
			suite.Equal(http.StatusOK, i)
		}))
	// check job
	suite.NoError(tu.CheckGetJSON(testDialClient,
		suite.urlPrefix+"scheduler-config"+"/"+types.BalanceRangeScheduler.String()+"/list", nil,
		func(data []byte, i int, _ http.Header) {
			suite.Equal(http.StatusOK, i)
			var resp []map[string]any
			suite.NoError(json.Unmarshal(data, &resp))
			slices.SortFunc(resp, func(a, b map[string]any) int {
				aID := a["job-id"].(float64)
				bID := b["job-id"].(float64)
				if aID == bID {
					return 0
				}
				if aID > bID {
					return 1
				}
				return -1
			})
			suite.Len(resp, 2)
			suite.Equal("cancelled", resp[1]["status"])
		}))
}
