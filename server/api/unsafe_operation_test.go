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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/unsaferecovery"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type unsafeOperationTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestUnsafeOperationTestSuite(t *testing.T) {
	suite.Run(t, new(unsafeOperationTestSuite))
}

func (suite *unsafeOperationTestSuite) SetupTest() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/admin/unsafe", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Offline, metapb.NodeState_Removing, nil)
}

func (suite *unsafeOperationTestSuite) TearDownTest() {
	suite.cleanup()
}

func (suite *unsafeOperationTestSuite) TestRemoveFailedStores() {
	re := suite.Require()

	input := map[string]any{"stores": []uint64{}}
	data, _ := json.Marshal(input)
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input no store specified\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []string{"abc", "def"}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"Store ids are invalid\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []uint64{1, 2}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input store 2 doesn't exist\"\n"))
	re.NoError(err)

	for _, input := range []map[string]any{
		{"stores": []uint64{1}, "timeout": -1},
		{"stores": []uint64{1}, "timeout": 1.5},
		{"stores": []uint64{1}, "timeout": maxPlanExecutionTimeoutSeconds + 1},
		{"stores": []uint64{1}, "timeout": "60"},
	} {
		data, _ = json.Marshal(input)
		err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
			tu.StringContain(re, "timeout is invalid"))
		re.NoError(err)
	}

	for _, input := range []map[string]any{
		{"stores": []uint64{1}, "plan-execution-timeout": -1},
		{"stores": []uint64{1}, "plan-execution-timeout": 1.5},
		{"stores": []uint64{1}, "plan-execution-timeout": maxPlanExecutionTimeoutSeconds + 1},
		{"stores": []uint64{1}, "plan_execution_timeout": "60"},
	} {
		data, _ = json.Marshal(input)
		err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
			tu.StringContain(re, "plan-execution-timeout is invalid"))
		re.NoError(err)
	}

	data, _ = json.Marshal(map[string]any{"stores": []uint64{1}, "disable-paranoid-check": "true"})
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringContain(re, "disable-paranoid-check is invalid"))
	re.NoError(err)

	data, _ = json.Marshal(map[string]any{
		"stores":                 []uint64{1},
		"plan-execution-timeout": 300,
		"plan_execution_timeout": 600,
	})
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringContain(re, "plan-execution-timeout is specified multiple times"))
	re.NoError(err)

	data, _ = json.Marshal(map[string]any{
		"stores":                 []uint64{1},
		"disable-paranoid-check": true,
		"disable_paranoid_check": false,
	})
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringContain(re, "disable-paranoid-check is specified multiple times"))
	re.NoError(err)

	input = map[string]any{"stores": []uint64{1}, "plan-execution-timeout": 300, "disable-paranoid-check": true}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusOK(re))
	re.NoError(err)

	// Test show
	var output []unsaferecovery.StageOutput
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/remove-failed-stores/show", &output)
	re.NoError(err)
	re.NotEmpty(output)
	re.Contains(output[0].Details, "paranoid check disabled")
	re.Contains(output[0].Details, "plan execution timeout 5m0s")
}

func (suite *unsafeOperationTestSuite) TestRemoveFailedStoresAutoDetect() {
	re := suite.Require()

	input := map[string]any{"auto-detect": false}
	data, _ := json.Marshal(input)
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"Store ids are invalid\"\n"))
	re.NoError(err)

	input = map[string]any{"auto-detect": true}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusOK(re))
	re.NoError(err)
}

func TestParsePlanExecutionTimeout(t *testing.T) {
	re := require.New(t)

	timeout, err := parsePlanExecutionTimeout(map[string]any{})
	re.NoError(err)
	re.Zero(timeout)

	timeout, err = parsePlanExecutionTimeout(map[string]any{"plan-execution-timeout": float64(300)})
	re.NoError(err)
	re.Equal(5*time.Minute, timeout)

	timeout, err = parsePlanExecutionTimeout(map[string]any{
		"plan-execution-timeout": maxPlanExecutionTimeoutSeconds,
	})
	re.NoError(err)
	re.Equal(time.Duration(int64(maxPlanExecutionTimeoutSeconds))*time.Second, timeout)

	for _, input := range []map[string]any{
		{"plan-execution-timeout": float64(0)},
		{"plan-execution-timeout": float64(-1)},
		{"plan-execution-timeout": 1.5},
		{"plan-execution-timeout": maxPlanExecutionTimeoutSeconds + 1},
		{"plan-execution-timeout": "60"},
	} {
		_, err = parsePlanExecutionTimeout(input)
		re.Error(err)
	}

	_, err = parsePlanExecutionTimeout(map[string]any{
		"plan-execution-timeout": float64(300),
		"plan_execution_timeout": float64(600),
	})
	re.EqualError(err, "plan-execution-timeout is specified multiple times")
}

func TestParseTimeout(t *testing.T) {
	re := require.New(t)

	timeout, err := parseTimeout(map[string]any{})
	re.NoError(err)
	re.Equal(uint64(600), timeout)

	timeout, err = parseTimeout(map[string]any{"timeout": float64(300)})
	re.NoError(err)
	re.Equal(uint64(300), timeout)

	timeout, err = parseTimeout(map[string]any{"timeout": maxPlanExecutionTimeoutSeconds})
	re.NoError(err)
	re.Equal(uint64(maxPlanExecutionTimeoutSeconds), timeout)

	for _, input := range []map[string]any{
		{"timeout": float64(0)},
		{"timeout": float64(-1)},
		{"timeout": 1.5},
		{"timeout": maxPlanExecutionTimeoutSeconds + 1},
		{"timeout": "60"},
	} {
		_, err = parseTimeout(input)
		re.EqualError(err, "timeout is invalid")
	}
}

func TestParseDisableParanoidCheck(t *testing.T) {
	re := require.New(t)

	disableParanoidCheck, err := parseDisableParanoidCheck(map[string]any{})
	re.NoError(err)
	re.False(disableParanoidCheck)

	disableParanoidCheck, err = parseDisableParanoidCheck(map[string]any{"disable-paranoid-check": true})
	re.NoError(err)
	re.True(disableParanoidCheck)

	disableParanoidCheck, err = parseDisableParanoidCheck(map[string]any{"disable_paranoid_check": false})
	re.NoError(err)
	re.False(disableParanoidCheck)

	for _, input := range []map[string]any{
		{"disable-paranoid-check": "true"},
		{"disable_paranoid_check": 1},
	} {
		_, err = parseDisableParanoidCheck(input)
		re.Error(err)
	}

	_, err = parseDisableParanoidCheck(map[string]any{
		"disable-paranoid-check": true,
		"disable_paranoid_check": false,
	})
	re.EqualError(err, "disable-paranoid-check is specified multiple times")
}
