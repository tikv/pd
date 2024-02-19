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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/apiutil"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

const apiPrefix = "/pd"

type scheduleTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestScheduleTestSuite(t *testing.T) {
	suite.Run(t, new(scheduleTestSuite))
}

func (suite *scheduleTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/skipStoreConfigSync", `return(true)`))
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *scheduleTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.env.Cleanup()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/skipStoreConfigSync"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
}

func (suite *scheduleTestSuite) TestOriginAPI() {
	suite.env.RunTestInTwoModes(suite.checkOriginAPI)
}

func (suite *scheduleTestSuite) checkOriginAPI(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/schedulers", leaderAddr)
	for i := 1; i <= 4; i++ {
		store := &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		}
		tests.MustPutStore(re, cluster, store)
	}

	input := make(map[string]any)
	input["name"] = "evict-leader-scheduler"
	input["store_id"] = 1
	body, err := json.Marshal(input)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, urlPrefix, body, tu.StatusOK(re)))

	suite.assertSchedulerExists(urlPrefix, "evict-leader-scheduler")
	resp := make(map[string]any)
	listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, "evict-leader-scheduler")
	re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
	re.Len(resp["store-id-ranges"], 1)
	input1 := make(map[string]any)
	input1["name"] = "evict-leader-scheduler"
	input1["store_id"] = 2
	body, err = json.Marshal(input1)
	re.NoError(err)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail", "return(true)"))
	re.NoError(tu.CheckPostJSON(testDialClient, urlPrefix, body, tu.StatusNotOK(re)))
	suite.assertSchedulerExists(urlPrefix, "evict-leader-scheduler")
	resp = make(map[string]any)
	re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
	re.Len(resp["store-id-ranges"], 1)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/persistFail"))
	re.NoError(tu.CheckPostJSON(testDialClient, urlPrefix, body, tu.StatusOK(re)))
	suite.assertSchedulerExists(urlPrefix, "evict-leader-scheduler")
	resp = make(map[string]any)
	re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
	re.Len(resp["store-id-ranges"], 2)
	deleteURL := fmt.Sprintf("%s/%s", urlPrefix, "evict-leader-scheduler-1")
	err = tu.CheckDelete(testDialClient, deleteURL, tu.StatusOK(re))
	re.NoError(err)
	suite.assertSchedulerExists(urlPrefix, "evict-leader-scheduler")
	resp1 := make(map[string]any)
	re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp1))
	re.Len(resp1["store-id-ranges"], 1)
	deleteURL = fmt.Sprintf("%s/%s", urlPrefix, "evict-leader-scheduler-2")
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/config/persistFail", "return(true)"))
	err = tu.CheckDelete(testDialClient, deleteURL, tu.Status(re, http.StatusInternalServerError))
	re.NoError(err)
	suite.assertSchedulerExists(urlPrefix, "evict-leader-scheduler")
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/config/persistFail"))
	err = tu.CheckDelete(testDialClient, deleteURL, tu.StatusOK(re))
	re.NoError(err)
	suite.assertNoScheduler(re, urlPrefix, "evict-leader-scheduler")
	re.NoError(tu.CheckGetJSON(testDialClient, listURL, nil, tu.Status(re, http.StatusNotFound)))
	err = tu.CheckDelete(testDialClient, deleteURL, tu.Status(re, http.StatusNotFound))
	re.NoError(err)
}

func (suite *scheduleTestSuite) TestAPI() {
	suite.env.RunTestInTwoModes(suite.checkAPI)
}

func (suite *scheduleTestSuite) checkAPI(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/schedulers", leaderAddr)
	for i := 1; i <= 4; i++ {
		store := &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		}
		tests.MustPutStore(re, cluster, store)
	}

	type arg struct {
		opt   string
		value any
	}
	testCases := []struct {
		name          string
		createdName   string
		args          []arg
		extraTestFunc func(name string)
	}{
		{
			name:        "balance-leader-scheduler",
			createdName: "balance-leader-scheduler",
			extraTestFunc: func(name string) {
				resp := make(map[string]any)
				listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal(4.0, resp["batch"])
				dataMap := make(map[string]any)
				dataMap["batch"] = 3
				updateURL := fmt.Sprintf("%s%s%s/%s/config", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(dataMap)
				re.NoError(err)
				re.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				tu.Eventually(re, func() bool { // wait for scheduling server to be synced.
					resp = make(map[string]any)
					re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
					return resp["batch"] == 3.0
				})

				// update again
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.StatusOK(re),
					tu.StringEqual(re, "\"Config is the same with origin, so do nothing.\"\n"))
				re.NoError(err)
				// update invalidate batch
				dataMap = map[string]any{}
				dataMap["batch"] = 100
				body, err = json.Marshal(dataMap)
				re.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "\"invalid batch size which should be an integer between 1 and 10\"\n"))
				re.NoError(err)
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal(3.0, resp["batch"])
				// empty body
				err = tu.CheckPostJSON(testDialClient, updateURL, nil,
					tu.Status(re, http.StatusInternalServerError),
					tu.StringEqual(re, "\"unexpected end of JSON input\"\n"))
				re.NoError(err)
				// config item not found
				dataMap = map[string]any{}
				dataMap["error"] = 3
				body, err = json.Marshal(dataMap)
				re.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "\"Config item is not found.\"\n"))
				re.NoError(err)
			},
		},
		{
			name:        "balance-hot-region-scheduler",
			createdName: "balance-hot-region-scheduler",
			extraTestFunc: func(name string) {
				resp := make(map[string]any)
				listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				expectMap := map[string]any{
					"min-hot-byte-rate":          100.0,
					"min-hot-key-rate":           10.0,
					"min-hot-query-rate":         10.0,
					"max-zombie-rounds":          3.0,
					"max-peer-number":            1000.0,
					"byte-rate-rank-step-ratio":  0.05,
					"key-rate-rank-step-ratio":   0.05,
					"query-rate-rank-step-ratio": 0.05,
					"count-rank-step-ratio":      0.01,
					"great-dec-ratio":            0.95,
					"minor-dec-ratio":            0.99,
					"src-tolerance-ratio":        1.05,
					"dst-tolerance-ratio":        1.05,
					"split-thresholds":           0.2,
					"rank-formula-version":       "v2",
					"read-priorities":            []any{"byte", "key"},
					"write-leader-priorities":    []any{"key", "byte"},
					"write-peer-priorities":      []any{"byte", "key"},
					"enable-for-tiflash":         "true",
					"strict-picking-store":       "true",
				}
				re.Equal(len(expectMap), len(resp), "expect %v, got %v", expectMap, resp)
				for key := range expectMap {
					re.Equal(expectMap[key], resp[key])
				}
				dataMap := make(map[string]any)
				dataMap["max-zombie-rounds"] = 5.0
				expectMap["max-zombie-rounds"] = 5.0
				updateURL := fmt.Sprintf("%s%s%s/%s/config", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(dataMap)
				re.NoError(err)
				re.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))

				for key := range expectMap {
					re.Equal(expectMap[key], resp[key], "key %s", key)
				}

				// update again
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.StatusOK(re),
					tu.StringEqual(re, "Config is the same with origin, so do nothing."))
				re.NoError(err)
				// config item not found
				dataMap = map[string]any{}
				dataMap["error"] = 3
				body, err = json.Marshal(dataMap)
				re.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "Config item is not found."))
				re.NoError(err)
			},
		},
		{
			name:        "split-bucket-scheduler",
			createdName: "split-bucket-scheduler",
			extraTestFunc: func(name string) {
				resp := make(map[string]any)
				listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal(3.0, resp["degree"])
				re.Equal(0.0, resp["split-limit"])
				dataMap := make(map[string]any)
				dataMap["degree"] = 4
				updateURL := fmt.Sprintf("%s%s%s/%s/config", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(dataMap)
				re.NoError(err)
				re.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal(4.0, resp["degree"])
				// update again
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.StatusOK(re),
					tu.StringEqual(re, "Config is the same with origin, so do nothing."))
				re.NoError(err)
				// empty body
				err = tu.CheckPostJSON(testDialClient, updateURL, nil,
					tu.Status(re, http.StatusInternalServerError),
					tu.StringEqual(re, "\"unexpected end of JSON input\"\n"))
				re.NoError(err)
				// config item not found
				dataMap = map[string]any{}
				dataMap["error"] = 3
				body, err = json.Marshal(dataMap)
				re.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "Config item is not found."))
				re.NoError(err)
			},
		},
		{
			name:        "balance-region-scheduler",
			createdName: "balance-region-scheduler",
		},
		{
			name:        "shuffle-leader-scheduler",
			createdName: "shuffle-leader-scheduler",
		},
		{
			name:        "shuffle-region-scheduler",
			createdName: "shuffle-region-scheduler",
		},
		{
			name:        "transfer-witness-leader-scheduler",
			createdName: "transfer-witness-leader-scheduler",
		},
		{
			name:        "balance-witness-scheduler",
			createdName: "balance-witness-scheduler",
			extraTestFunc: func(name string) {
				resp := make(map[string]any)
				listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal(4.0, resp["batch"])
				dataMap := make(map[string]any)
				dataMap["batch"] = 3
				updateURL := fmt.Sprintf("%s%s%s/%s/config", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(dataMap)
				re.NoError(err)
				re.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal(3.0, resp["batch"])
				// update again
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.StatusOK(re),
					tu.StringEqual(re, "\"Config is the same with origin, so do nothing.\"\n"))
				re.NoError(err)
				// update invalidate batch
				dataMap = map[string]any{}
				dataMap["batch"] = 100
				body, err = json.Marshal(dataMap)
				re.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "\"invalid batch size which should be an integer between 1 and 10\"\n"))
				re.NoError(err)
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal(3.0, resp["batch"])
				// empty body
				err = tu.CheckPostJSON(testDialClient, updateURL, nil,
					tu.Status(re, http.StatusInternalServerError),
					tu.StringEqual(re, "\"unexpected end of JSON input\"\n"))
				re.NoError(err)
				// config item not found
				dataMap = map[string]any{}
				dataMap["error"] = 3
				body, err = json.Marshal(dataMap)
				re.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "\"Config item is not found.\"\n"))
				re.NoError(err)
			},
		},
		{
			name:        "grant-leader-scheduler",
			createdName: "grant-leader-scheduler",
			args:        []arg{{"store_id", 1}},
			extraTestFunc: func(name string) {
				resp := make(map[string]any)
				listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				exceptMap := make(map[string]any)
				exceptMap["1"] = []any{map[string]any{"end-key": "", "start-key": ""}}
				re.Equal(exceptMap, resp["store-id-ranges"])

				// using /pd/v1/schedule-config/grant-leader-scheduler/config to add new store to grant-leader-scheduler
				input := make(map[string]any)
				input["name"] = "grant-leader-scheduler"
				input["store_id"] = 2
				updateURL := fmt.Sprintf("%s%s%s/%s/config", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(input)
				re.NoError(err)
				re.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				exceptMap["2"] = []any{map[string]any{"end-key": "", "start-key": ""}}
				re.Equal(exceptMap, resp["store-id-ranges"])

				// using /pd/v1/schedule-config/grant-leader-scheduler/config to delete exists store from grant-leader-scheduler
				deleteURL := fmt.Sprintf("%s%s%s/%s/delete/%s", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name, "2")
				err = tu.CheckDelete(testDialClient, deleteURL, tu.StatusOK(re))
				re.NoError(err)
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				delete(exceptMap, "2")
				re.Equal(exceptMap, resp["store-id-ranges"])
				err = tu.CheckDelete(testDialClient, deleteURL, tu.Status(re, http.StatusNotFound))
				re.NoError(err)
			},
		},
		{
			name:        "scatter-range",
			createdName: "scatter-range-test",
			args:        []arg{{"start_key", ""}, {"end_key", ""}, {"range_name", "test"}},
			// Test the scheduler config handler.
			extraTestFunc: func(name string) {
				resp := make(map[string]any)
				listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal("", resp["start-key"])
				re.Equal("", resp["end-key"])
				re.Equal("test", resp["range-name"])
				resp["start-key"] = "a_00"
				resp["end-key"] = "a_99"
				updateURL := fmt.Sprintf("%s%s%s/%s/config", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(resp)
				re.NoError(err)
				re.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]any)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				re.Equal("a_00", resp["start-key"])
				re.Equal("a_99", resp["end-key"])
				re.Equal("test", resp["range-name"])
			},
		},
		{
			name:        "evict-leader-scheduler",
			createdName: "evict-leader-scheduler",
			args:        []arg{{"store_id", 3}},
			// Test the scheduler config handler.
			extraTestFunc: func(name string) {
				resp := make(map[string]any)
				listURL := fmt.Sprintf("%s%s%s/%s/list", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				exceptMap := make(map[string]any)
				exceptMap["3"] = []any{map[string]any{"end-key": "", "start-key": ""}}
				re.Equal(exceptMap, resp["store-id-ranges"])

				// using /pd/v1/schedule-config/evict-leader-scheduler/config to add new store to evict-leader-scheduler
				input := make(map[string]any)
				input["name"] = "evict-leader-scheduler"
				input["store_id"] = 4
				updateURL := fmt.Sprintf("%s%s%s/%s/config", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(input)
				re.NoError(err)
				re.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]any)
				tu.Eventually(re, func() bool {
					re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
					exceptMap["4"] = []any{map[string]any{"end-key": "", "start-key": ""}}
					return reflect.DeepEqual(exceptMap, resp["store-id-ranges"])
				})

				// using /pd/v1/schedule-config/evict-leader-scheduler/config to delete exist store from evict-leader-scheduler
				deleteURL := fmt.Sprintf("%s%s%s/%s/delete/%s", leaderAddr, apiPrefix, server.SchedulerConfigHandlerPath, name, "4")
				err = tu.CheckDelete(testDialClient, deleteURL, tu.StatusOK(re))
				re.NoError(err)
				resp = make(map[string]any)
				tu.Eventually(re, func() bool {
					re.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
					delete(exceptMap, "4")
					return reflect.DeepEqual(exceptMap, resp["store-id-ranges"])
				})
				err = tu.CheckDelete(testDialClient, deleteURL, tu.Status(re, http.StatusNotFound))
				re.NoError(err)
			},
		},
		{
			name:        "evict-slow-store-scheduler",
			createdName: "evict-slow-store-scheduler",
		},
	}
	for _, testCase := range testCases {
		input := make(map[string]any)
		input["name"] = testCase.name
		for _, a := range testCase.args {
			input[a.opt] = a.value
		}
		body, err := json.Marshal(input)
		re.NoError(err)
		suite.testPauseOrResume(re, urlPrefix, testCase.name, testCase.createdName, body)
		if testCase.extraTestFunc != nil {
			testCase.extraTestFunc(testCase.createdName)
		}
		suite.deleteScheduler(re, urlPrefix, testCase.createdName)
		suite.assertNoScheduler(re, urlPrefix, testCase.createdName)
	}

	// test pause and resume all schedulers.

	// add schedulers.
	for _, testCase := range testCases {
		input := make(map[string]any)
		input["name"] = testCase.name
		for _, a := range testCase.args {
			input[a.opt] = a.value
		}
		body, err := json.Marshal(input)
		re.NoError(err)
		suite.addScheduler(re, urlPrefix, body)
		suite.assertSchedulerExists(urlPrefix, testCase.createdName) // wait for scheduler to be synced.
		if testCase.extraTestFunc != nil {
			testCase.extraTestFunc(testCase.createdName)
		}
	}

	// test pause all schedulers.
	input := make(map[string]any)
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	re.NoError(err)

	for _, testCase := range testCases {
		createdName := testCase.createdName
		if createdName == "" {
			createdName = testCase.name
		}
		isPaused := suite.isSchedulerPaused(re, urlPrefix, createdName)
		re.True(isPaused)
	}
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	time.Sleep(time.Second)
	for _, testCase := range testCases {
		createdName := testCase.createdName
		if createdName == "" {
			createdName = testCase.name
		}
		isPaused := suite.isSchedulerPaused(re, urlPrefix, createdName)
		re.False(isPaused)
	}

	// test resume all schedulers.
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	for _, testCase := range testCases {
		createdName := testCase.createdName
		if createdName == "" {
			createdName = testCase.name
		}
		isPaused := suite.isSchedulerPaused(re, urlPrefix, createdName)
		re.False(isPaused)
	}

	// delete schedulers.
	for _, testCase := range testCases {
		createdName := testCase.createdName
		if createdName == "" {
			createdName = testCase.name
		}
		suite.deleteScheduler(re, urlPrefix, createdName)
		suite.assertNoScheduler(re, urlPrefix, createdName)
	}
}

func (suite *scheduleTestSuite) TestDisable() {
	suite.env.RunTestInTwoModes(suite.checkDisable)
}

func (suite *scheduleTestSuite) checkDisable(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/schedulers", leaderAddr)
	for i := 1; i <= 4; i++ {
		store := &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		}
		tests.MustPutStore(re, cluster, store)
	}

	name := "shuffle-leader-scheduler"
	input := make(map[string]any)
	input["name"] = name
	body, err := json.Marshal(input)
	re.NoError(err)
	suite.addScheduler(re, urlPrefix, body)

	u := fmt.Sprintf("%s%s/api/v1/config/schedule", leaderAddr, apiPrefix)
	var scheduleConfig sc.ScheduleConfig
	err = tu.ReadGetJSON(re, testDialClient, u, &scheduleConfig)
	re.NoError(err)

	originSchedulers := scheduleConfig.Schedulers
	scheduleConfig.Schedulers = sc.SchedulerConfigs{sc.SchedulerConfig{Type: "shuffle-leader", Disable: true}}
	body, err = json.Marshal(scheduleConfig)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, u, body, tu.StatusOK(re))
	re.NoError(err)

	suite.assertNoScheduler(re, urlPrefix, name)
	suite.assertSchedulerExists(fmt.Sprintf("%s?status=disabled", urlPrefix), name)

	// reset schedule config
	scheduleConfig.Schedulers = originSchedulers
	body, err = json.Marshal(scheduleConfig)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, u, body, tu.StatusOK(re))
	re.NoError(err)

	suite.deleteScheduler(re, urlPrefix, name)
	suite.assertNoScheduler(re, urlPrefix, name)
}

func (suite *scheduleTestSuite) addScheduler(re *require.Assertions, urlPrefix string, body []byte) {
	err := tu.CheckPostJSON(testDialClient, urlPrefix, body, tu.StatusOK(re))
	re.NoError(err)
}

func (suite *scheduleTestSuite) deleteScheduler(re *require.Assertions, urlPrefix string, createdName string) {
	deleteURL := fmt.Sprintf("%s/%s", urlPrefix, createdName)
	err := tu.CheckDelete(testDialClient, deleteURL, tu.StatusOK(re))
	re.NoError(err)
}

func (suite *scheduleTestSuite) testPauseOrResume(re *require.Assertions, urlPrefix string, name, createdName string, body []byte) {
	if createdName == "" {
		createdName = name
	}
	var schedulers []string
	tu.ReadGetJSON(re, testDialClient, urlPrefix, &schedulers)
	if !slice.Contains(schedulers, createdName) {
		err := tu.CheckPostJSON(testDialClient, urlPrefix, body, tu.StatusOK(re))
		re.NoError(err)
	}
	suite.assertSchedulerExists(urlPrefix, createdName) // wait for scheduler to be synced.

	// test pause.
	input := make(map[string]any)
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	isPaused := suite.isSchedulerPaused(re, urlPrefix, createdName)
	re.True(isPaused)
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	time.Sleep(time.Second * 2)
	isPaused = suite.isSchedulerPaused(re, urlPrefix, createdName)
	re.False(isPaused)

	// test resume.
	input = make(map[string]any)
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	isPaused = suite.isSchedulerPaused(re, urlPrefix, createdName)
	re.False(isPaused)
}

func (suite *scheduleTestSuite) TestEmptySchedulers() {
	suite.env.RunTestInTwoModes(suite.checkEmptySchedulers)
}

func (suite *scheduleTestSuite) checkEmptySchedulers(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/schedulers", leaderAddr)
	for i := 1; i <= 4; i++ {
		store := &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		}
		tests.MustPutStore(re, cluster, store)
	}
	for _, query := range []string{"", "?status=paused", "?status=disabled"} {
		schedulers := make([]string, 0)
		re.NoError(tu.ReadGetJSON(re, testDialClient, urlPrefix+query, &schedulers))
		for _, scheduler := range schedulers {
			if strings.Contains(query, "disable") {
				input := make(map[string]any)
				input["name"] = scheduler
				body, err := json.Marshal(input)
				re.NoError(err)
				suite.addScheduler(re, urlPrefix, body)
			} else {
				suite.deleteScheduler(re, urlPrefix, scheduler)
			}
		}
		tu.Eventually(re, func() bool {
			resp, err := apiutil.GetJSON(testDialClient, urlPrefix+query, nil)
			re.NoError(err)
			defer resp.Body.Close()
			re.Equal(http.StatusOK, resp.StatusCode)
			b, err := io.ReadAll(resp.Body)
			re.NoError(err)
			return strings.Contains(string(b), "[]") && !strings.Contains(string(b), "null")
		})
	}
}

func (suite *scheduleTestSuite) assertSchedulerExists(urlPrefix string, scheduler string) {
	var schedulers []string
	re := suite.Require()
	tu.Eventually(re, func() bool {
		err := tu.ReadGetJSON(re, testDialClient, urlPrefix, &schedulers,
			tu.StatusOK(re))
		re.NoError(err)
		return slice.Contains(schedulers, scheduler)
	})
}

func (suite *scheduleTestSuite) assertNoScheduler(re *require.Assertions, urlPrefix string, scheduler string) {
	var schedulers []string
	tu.Eventually(re, func() bool {
		err := tu.ReadGetJSON(re, testDialClient, urlPrefix, &schedulers,
			tu.StatusOK(re))
		re.NoError(err)
		return !slice.Contains(schedulers, scheduler)
	})
}

func (suite *scheduleTestSuite) isSchedulerPaused(re *require.Assertions, urlPrefix, name string) bool {
	var schedulers []string
	err := tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s?status=paused", urlPrefix), &schedulers,
		tu.StatusOK(re))
	re.NoError(err)
	for _, scheduler := range schedulers {
		if scheduler == name {
			return true
		}
	}
	return false
}
