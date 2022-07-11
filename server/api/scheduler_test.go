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
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/apiutil"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	_ "github.com/tikv/pd/server/schedulers"
)

type scheduleTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func TestScheduleTestSuite(t *testing.T) {
	suite.Run(t, new(scheduleTestSuite))
}

func (suite *scheduleTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/schedulers", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	mustPutStore(re, suite.svr, 2, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (suite *scheduleTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *scheduleTestSuite) TestOriginAPI() {
	addURL := suite.urlPrefix
	input := make(map[string]interface{})
	input["name"] = "evict-leader-scheduler"
	input["store_id"] = 1
	body, err := json.Marshal(input)
	suite.NoError(err)
	re := suite.Require()
	suite.NoError(tu.CheckPostJSON(testDialClient, addURL, body, tu.StatusOK(re)))
	rc := suite.svr.GetRaftCluster()
	suite.Len(rc.GetSchedulers(), 1)
	resp := make(map[string]interface{})
	listURL := fmt.Sprintf("%s%s%s/%s/list", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, "evict-leader-scheduler")
	suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
	suite.Len(resp["store-id-ranges"], 1)
	input1 := make(map[string]interface{})
	input1["name"] = "evict-leader-scheduler"
	input1["store_id"] = 2
	body, err = json.Marshal(input1)
	suite.NoError(err)
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/schedulers/persistFail", "return(true)"))
	suite.NoError(tu.CheckPostJSON(testDialClient, addURL, body, tu.StatusNotOK(re)))
	suite.Len(rc.GetSchedulers(), 1)
	resp = make(map[string]interface{})
	suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
	suite.Len(resp["store-id-ranges"], 1)
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/schedulers/persistFail"))
	suite.NoError(tu.CheckPostJSON(testDialClient, addURL, body, tu.StatusOK(re)))
	suite.Len(rc.GetSchedulers(), 1)
	resp = make(map[string]interface{})
	suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
	suite.Len(resp["store-id-ranges"], 2)
	deleteURL := fmt.Sprintf("%s/%s", suite.urlPrefix, "evict-leader-scheduler-1")
	_, err = apiutil.DoDelete(testDialClient, deleteURL)
	suite.NoError(err)
	suite.Len(rc.GetSchedulers(), 1)
	resp1 := make(map[string]interface{})
	suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp1))
	suite.Len(resp1["store-id-ranges"], 1)
	deleteURL = fmt.Sprintf("%s/%s", suite.urlPrefix, "evict-leader-scheduler-2")
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/config/persistFail", "return(true)"))
	statusCode, err := apiutil.DoDelete(testDialClient, deleteURL)
	suite.NoError(err)
	suite.Equal(500, statusCode)
	suite.Len(rc.GetSchedulers(), 1)
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/config/persistFail"))
	statusCode, err = apiutil.DoDelete(testDialClient, deleteURL)
	suite.NoError(err)
	suite.Equal(200, statusCode)
	suite.Empty(rc.GetSchedulers())
	suite.NoError(tu.CheckGetJSON(testDialClient, listURL, nil, tu.Status(re, 404)))
	statusCode, _ = apiutil.DoDelete(testDialClient, deleteURL)
	suite.Equal(404, statusCode)
}

func (suite *scheduleTestSuite) TestAPI() {
	re := suite.Require()
	type arg struct {
		opt   string
		value interface{}
	}
	cases := []struct {
		name          string
		createdName   string
		args          []arg
		extraTestFunc func(name string)
	}{
		{
			name: "balance-leader-scheduler",
			extraTestFunc: func(name string) {
				resp := make(map[string]interface{})
				listURL := fmt.Sprintf("%s%s%s/%s/list", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				suite.Equal(4.0, resp["batch"])
				dataMap := make(map[string]interface{})
				dataMap["batch"] = 3
				updateURL := fmt.Sprintf("%s%s%s/%s/config", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(dataMap)
				suite.NoError(err)
				suite.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				suite.Equal(3.0, resp["batch"])
				// update again
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.StatusOK(re),
					tu.StringEqual(re, "\"no changed\"\n"))
				suite.NoError(err)
				// update invalidate batch
				dataMap = map[string]interface{}{}
				dataMap["batch"] = 100
				body, err = json.Marshal(dataMap)
				suite.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "\"invalid batch size which should be an integer between 1 and 10\"\n"))
				suite.NoError(err)
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				suite.Equal(3.0, resp["batch"])
				// empty body
				err = tu.CheckPostJSON(testDialClient, updateURL, nil,
					tu.Status(re, http.StatusInternalServerError),
					tu.StringEqual(re, "\"unexpected end of JSON input\"\n"))
				suite.NoError(err)
				// config item not found
				dataMap = map[string]interface{}{}
				dataMap["error"] = 3
				body, err = json.Marshal(dataMap)
				suite.NoError(err)
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.Status(re, http.StatusBadRequest),
					tu.StringEqual(re, "\"config item not found\"\n"))
				suite.NoError(err)
			},
		},
		{
			name: "balance-hot-region-scheduler",
			extraTestFunc: func(name string) {
				resp := make(map[string]interface{})
				listURL := fmt.Sprintf("%s%s%s/%s/list", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				expectMap := map[string]float64{
					"min-hot-byte-rate":          100,
					"min-hot-key-rate":           10,
					"max-zombie-rounds":          3,
					"max-peer-number":            1000,
					"byte-rate-rank-step-ratio":  0.05,
					"key-rate-rank-step-ratio":   0.05,
					"query-rate-rank-step-ratio": 0.05,
					"count-rank-step-ratio":      0.01,
					"great-dec-ratio":            0.95,
					"minor-dec-ratio":            0.99,
				}
				for key := range expectMap {
					suite.Equal(expectMap[key], resp[key])
				}
				dataMap := make(map[string]interface{})
				dataMap["max-zombie-rounds"] = 5.0
				expectMap["max-zombie-rounds"] = 5.0
				updateURL := fmt.Sprintf("%s%s%s/%s/config", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(dataMap)
				suite.NoError(err)
				suite.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				for key := range expectMap {
					suite.Equal(expectMap[key], resp[key])
				}
				// update again
				err = tu.CheckPostJSON(testDialClient, updateURL, body,
					tu.StatusOK(re),
					tu.StringEqual(re, "no changed"))
				suite.NoError(err)
			},
		},
		{name: "balance-region-scheduler"},
		{name: "shuffle-leader-scheduler"},
		{name: "shuffle-region-scheduler"},
		{
			name:        "grant-leader-scheduler",
			createdName: "grant-leader-scheduler",
			args:        []arg{{"store_id", 1}},
			extraTestFunc: func(name string) {
				resp := make(map[string]interface{})
				listURL := fmt.Sprintf("%s%s%s/%s/list", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				exceptMap := make(map[string]interface{})
				exceptMap["1"] = []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}
				suite.Equal(exceptMap, resp["store-id-ranges"])

				// using /pd/v1/schedule-config/grant-leader-scheduler/config to add new store to grant-leader-scheduler
				input := make(map[string]interface{})
				input["name"] = "grant-leader-scheduler"
				input["store_id"] = 2
				updateURL := fmt.Sprintf("%s%s%s/%s/config", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(input)
				suite.NoError(err)
				suite.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				exceptMap["2"] = []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}
				suite.Equal(exceptMap, resp["store-id-ranges"])

				// using /pd/v1/schedule-config/grant-leader-scheduler/config to delete exists store from grant-leader-scheduler
				deleteURL := fmt.Sprintf("%s%s%s/%s/delete/%s", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name, "2")
				_, err = apiutil.DoDelete(testDialClient, deleteURL)
				suite.NoError(err)
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				delete(exceptMap, "2")
				suite.Equal(exceptMap, resp["store-id-ranges"])
				statusCode, err := apiutil.DoDelete(testDialClient, deleteURL)
				suite.NoError(err)
				suite.Equal(404, statusCode)
			},
		},
		{
			name:        "scatter-range",
			createdName: "scatter-range-test",
			args:        []arg{{"start_key", ""}, {"end_key", ""}, {"range_name", "test"}},
			// Test the scheduler config handler.
			extraTestFunc: func(name string) {
				resp := make(map[string]interface{})
				listURL := fmt.Sprintf("%s%s%s/%s/list", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				suite.Equal("", resp["start-key"])
				suite.Equal("", resp["end-key"])
				suite.Equal("test", resp["range-name"])
				resp["start-key"] = "a_00"
				resp["end-key"] = "a_99"
				updateURL := fmt.Sprintf("%s%s%s/%s/config", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(resp)
				suite.NoError(err)
				suite.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				suite.Equal("a_00", resp["start-key"])
				suite.Equal("a_99", resp["end-key"])
				suite.Equal("test", resp["range-name"])
			},
		},
		{
			name:        "evict-leader-scheduler",
			createdName: "evict-leader-scheduler",
			args:        []arg{{"store_id", 1}},
			// Test the scheduler config handler.
			extraTestFunc: func(name string) {
				resp := make(map[string]interface{})
				listURL := fmt.Sprintf("%s%s%s/%s/list", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				exceptMap := make(map[string]interface{})
				exceptMap["1"] = []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}
				suite.Equal(exceptMap, resp["store-id-ranges"])

				// using /pd/v1/schedule-config/evict-leader-scheduler/config to add new store to evict-leader-scheduler
				input := make(map[string]interface{})
				input["name"] = "evict-leader-scheduler"
				input["store_id"] = 2
				updateURL := fmt.Sprintf("%s%s%s/%s/config", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name)
				body, err := json.Marshal(input)
				suite.NoError(err)
				suite.NoError(tu.CheckPostJSON(testDialClient, updateURL, body, tu.StatusOK(re)))
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				exceptMap["2"] = []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}
				suite.Equal(exceptMap, resp["store-id-ranges"])

				// using /pd/v1/schedule-config/evict-leader-scheduler/config to delete exist store from evict-leader-scheduler
				deleteURL := fmt.Sprintf("%s%s%s/%s/delete/%s", suite.svr.GetAddr(), apiPrefix, server.SchedulerConfigHandlerPath, name, "2")
				_, err = apiutil.DoDelete(testDialClient, deleteURL)
				suite.NoError(err)
				resp = make(map[string]interface{})
				suite.NoError(tu.ReadGetJSON(re, testDialClient, listURL, &resp))
				delete(exceptMap, "2")
				suite.Equal(exceptMap, resp["store-id-ranges"])
				statusCode, err := apiutil.DoDelete(testDialClient, deleteURL)
				suite.NoError(err)
				suite.Equal(404, statusCode)
			},
		},
	}
	for _, ca := range cases {
		input := make(map[string]interface{})
		input["name"] = ca.name
		for _, a := range ca.args {
			input[a.opt] = a.value
		}
		body, err := json.Marshal(input)
		suite.NoError(err)
		suite.testPauseOrResume(ca.name, ca.createdName, body)
	}

	// test pause and resume all schedulers.

	// add schedulers.
	cases = cases[:3]
	for _, ca := range cases {
		input := make(map[string]interface{})
		input["name"] = ca.name
		for _, a := range ca.args {
			input[a.opt] = a.value
		}
		body, err := json.Marshal(input)
		suite.NoError(err)
		suite.addScheduler(body)
	}

	// test pause all schedulers.
	input := make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	handler := suite.svr.GetHandler()
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		isPaused, err := handler.IsSchedulerPaused(createdName)
		suite.NoError(err)
		suite.True(isPaused)
	}
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	time.Sleep(time.Second)
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		isPaused, err := handler.IsSchedulerPaused(createdName)
		suite.NoError(err)
		suite.False(isPaused)
	}

	// test resume all schedulers.
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/all", pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		isPaused, err := handler.IsSchedulerPaused(createdName)
		suite.NoError(err)
		suite.False(isPaused)
	}

	// delete schedulers.
	for _, ca := range cases {
		createdName := ca.createdName
		if createdName == "" {
			createdName = ca.name
		}
		suite.deleteScheduler(createdName)
	}
}

func (suite *scheduleTestSuite) TestDisable() {
	name := "shuffle-leader-scheduler"
	input := make(map[string]interface{})
	input["name"] = name
	body, err := json.Marshal(input)
	suite.NoError(err)
	suite.addScheduler(body)

	re := suite.Require()
	u := fmt.Sprintf("%s%s/api/v1/config/schedule", suite.svr.GetAddr(), apiPrefix)
	var scheduleConfig config.ScheduleConfig
	err = tu.ReadGetJSON(re, testDialClient, u, &scheduleConfig)
	suite.NoError(err)

	originSchedulers := scheduleConfig.Schedulers
	scheduleConfig.Schedulers = config.SchedulerConfigs{config.SchedulerConfig{Type: "shuffle-leader", Disable: true}}
	body, err = json.Marshal(scheduleConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, u, body, tu.StatusOK(re))
	suite.NoError(err)

	var schedulers []string
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix, &schedulers)
	suite.NoError(err)
	suite.Len(schedulers, 1)
	suite.Equal(name, schedulers[0])

	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s?status=disabled", suite.urlPrefix), &schedulers)
	suite.NoError(err)
	suite.Len(schedulers, 1)
	suite.Equal(name, schedulers[0])

	// reset schedule config
	scheduleConfig.Schedulers = originSchedulers
	body, err = json.Marshal(scheduleConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, u, body, tu.StatusOK(re))
	suite.NoError(err)

	suite.deleteScheduler(name)
}

func (suite *scheduleTestSuite) addScheduler(body []byte) {
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix, body, tu.StatusOK(suite.Require()))
	suite.NoError(err)
}

func (suite *scheduleTestSuite) deleteScheduler(createdName string) {
	deleteURL := fmt.Sprintf("%s/%s", suite.urlPrefix, createdName)
	_, err := apiutil.DoDelete(testDialClient, deleteURL)
	suite.NoError(err)
}

func (suite *scheduleTestSuite) testPauseOrResume(name, createdName string, body []byte) {
	if createdName == "" {
		createdName = name
	}
	re := suite.Require()
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix, body, tu.StatusOK(re))
	suite.NoError(err)
	handler := suite.svr.GetHandler()
	sches, err := handler.GetSchedulers()
	suite.NoError(err)
	suite.Equal(createdName, sches[0])

	// test pause.
	input := make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	isPaused, err := handler.IsSchedulerPaused(createdName)
	suite.NoError(err)
	suite.True(isPaused)
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	pausedAt, err := handler.GetPausedSchedulerDelayAt(createdName)
	suite.NoError(err)
	resumeAt, err := handler.GetPausedSchedulerDelayUntil(createdName)
	suite.NoError(err)
	suite.Equal(int64(1), resumeAt-pausedAt)
	time.Sleep(time.Second)
	isPaused, err = handler.IsSchedulerPaused(createdName)
	suite.NoError(err)
	suite.False(isPaused)

	// test resume.
	input = make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+createdName, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	isPaused, err = handler.IsSchedulerPaused(createdName)
	suite.NoError(err)
	suite.False(isPaused)
	suite.deleteScheduler(createdName)
}
