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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type checkerTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestCheckerTestSuite(t *testing.T) {
	suite.Run(t, new(checkerTestSuite))
}

func (suite *checkerTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *checkerTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *checkerTestSuite) TestAPI() {
	suite.env.RunTestBasedOnMode(suite.checkAPI)
}

func (suite *checkerTestSuite) checkAPI(cluster *tests.TestCluster) {
	re := suite.Require()
	testErrCases(re, cluster)

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
		testGetStatus(re, cluster, testCase.name)
		testPauseOrResume(re, cluster, testCase.name)
	}
}

func testErrCases(re *require.Assertions, cluster *tests.TestCluster) {
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/checker", cluster.GetLeaderServer().GetAddr())
	// missing args
	input := make(map[string]any)
	pauseArgs, err := json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(re))
	re.NoError(err)

	// negative delay
	input["delay"] = -10
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(re))
	re.NoError(err)

	// wrong name
	name := "dummy"
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(re))
	re.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(re))
	re.NoError(err)
}

func testGetStatus(re *require.Assertions, cluster *tests.TestCluster, name string) {
	input := make(map[string]any)
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/checker", cluster.GetLeaderServer().GetAddr())
	// normal run
	resp := make(map[string]any)
	err := tu.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	re.NoError(err)
	re.False(resp["paused"].(bool))
	// paused
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	resp = make(map[string]any)
	err = tu.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	re.NoError(err)
	re.True(resp["paused"].(bool))
	// resumed
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	time.Sleep(time.Second)
	resp = make(map[string]any)
	err = tu.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	re.NoError(err)
	re.False(resp["paused"].(bool))
}

func testPauseOrResume(re *require.Assertions, cluster *tests.TestCluster, name string) {
	input := make(map[string]any)
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/checker", cluster.GetLeaderServer().GetAddr())
	resp := make(map[string]any)

	// test pause.
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	re.NoError(err)
	re.True(resp["paused"].(bool))
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	time.Sleep(time.Second)
	err = tu.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	re.NoError(err)
	re.False(resp["paused"].(bool))

	// test resume.
	input = make(map[string]any)
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/"+name, pauseArgs, tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
	re.NoError(err)
	re.False(resp["paused"].(bool))
}
