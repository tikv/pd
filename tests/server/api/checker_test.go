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

	"github.com/stretchr/testify/suite"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type checkerTestSuite struct {
	suite.Suite
}

func TestCheckerTestSuite(t *testing.T) {
	suite.Run(t, new(checkerTestSuite))
}
func (suite *checkerTestSuite) TestAPI() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkAPI)
}

func (suite *checkerTestSuite) checkAPI(cluster *tests.TestCluster) {
	suite.testErrCases(cluster)

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
		suite.testGetStatus(cluster, testCase.name)
		suite.testPauseOrResume(cluster, testCase.name)
	}
}

func (suite *checkerTestSuite) testErrCases(cluster *tests.TestCluster) {
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/checker", cluster.GetLeaderServer().GetAddr())
	// missing args
	input := make(map[string]interface{})
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)

	// negative delay
	input["delay"] = -10
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)

	// wrong name
	name := "dummy"
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(re))
	suite.NoError(err)
}

func (suite *checkerTestSuite) testGetStatus(cluster *tests.TestCluster, name string) {
	input := make(map[string]interface{})
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/checker", cluster.GetLeaderServer().GetAddr())
	// normal run
	resp := make(map[string]interface{})
	re := suite.Require()
	err := tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	suite.NoError(err)
	suite.False(resp["paused"].(bool))
	// paused
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	resp = make(map[string]interface{})
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	suite.NoError(err)
	suite.True(resp["paused"].(bool))
	// resumed
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	time.Sleep(time.Second)
	resp = make(map[string]interface{})
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	suite.NoError(err)
	suite.False(resp["paused"].(bool))
}

func (suite *checkerTestSuite) testPauseOrResume(cluster *tests.TestCluster, name string) {
	input := make(map[string]interface{})
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/checker", cluster.GetLeaderServer().GetAddr())
	resp := make(map[string]interface{})

	// test pause.
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	suite.NoError(err)
	suite.True(resp["paused"].(bool))
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	time.Sleep(time.Second)
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	suite.NoError(err)
	suite.False(resp["paused"].(bool))

	// test resume.
	input = make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	suite.NoError(err)
	suite.False(resp["paused"].(bool))
}
