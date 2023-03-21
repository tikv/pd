// Copyright 2021 TiKV Project Authors.
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
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type checkerTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestCheckerTestSuite(t *testing.T) {
	suite.Run(t, new(checkerTestSuite))
}

func (suite *checkerTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/checker", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	mustPutStore(re, suite.svr, 2, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (suite *checkerTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *checkerTestSuite) TestAPI() {
	suite.testErrCases()

	testCases := []struct {
		name string
	}{
		{name: "learner"},
		{name: "replica"},
		{name: "rule"},
		{name: "split"},
		{name: "merge"},
		{name: "joint-state"},
	}
	for _, testCase := range testCases {
		suite.testGetStatus(testCase.name)
		suite.testPauseOrResume(testCase.name)
	}
}

func (suite *checkerTestSuite) testErrCases() {
	// missing args
	input := make(map[string]interface{})
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)

	// negative delay
	input["delay"] = -10
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)

	// wrong name
	name := "dummy"
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)
}

func (suite *checkerTestSuite) testGetStatus(name string) {
	handler := suite.svr.GetHandler()

	// normal run
	resp := make(map[string]interface{})
	re := suite.Require()
	err := tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", suite.urlPrefix, name), &resp)
	suite.NoError(err)
	suite.False(resp["paused"].(bool))
	// paused
	err = handler.PauseOrResumeChecker(name, 30)
	suite.NoError(err)
	resp = make(map[string]interface{})
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", suite.urlPrefix, name), &resp)
	suite.NoError(err)
	suite.True(resp["paused"].(bool))
	// resumed
	err = handler.PauseOrResumeChecker(name, 1)
	suite.NoError(err)
	time.Sleep(time.Second)
	resp = make(map[string]interface{})
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", suite.urlPrefix, name), &resp)
	suite.NoError(err)
	suite.False(resp["paused"].(bool))
}

func (suite *checkerTestSuite) testPauseOrResume(name string) {
	handler := suite.svr.GetHandler()
	input := make(map[string]interface{})

	// test pause.
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	isPaused, err := handler.IsCheckerPaused(name)
	suite.NoError(err)
	suite.True(isPaused)
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	time.Sleep(time.Second)
	isPaused, err = handler.IsCheckerPaused(name)
	suite.NoError(err)
	suite.False(isPaused)

	// test resume.
	input = make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	isPaused, err = handler.IsCheckerPaused(name)
	suite.NoError(err)
	suite.False(isPaused)
}
