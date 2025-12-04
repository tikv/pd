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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type diagnosticTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestDiagnosticTestSuite(t *testing.T) {
	suite.Run(t, new(diagnosticTestSuite))
}

func (suite *diagnosticTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *diagnosticTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *diagnosticTestSuite) TestSchedulerDiagnosticAPI() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkSchedulerDiagnosticAPI)
}

func (suite *diagnosticTestSuite) checkSchedulerDiagnosticAPI(cluster *tests.TestCluster) {
	re := suite.Require()

	for i := range 3 {
		tests.MustPutStore(re, cluster, &metapb.Store{
			Id:        uint64(i + 1),
			Address:   fmt.Sprintf("mock://tikv-%d:%d", i+1, i+1),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		})
	}

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	diagnosticPrefix := fmt.Sprintf("%s/schedulers/diagnostic", urlPrefix)
	schedulerPrefix := fmt.Sprintf("%s/schedulers", urlPrefix)
	configPrefix := fmt.Sprintf("%s/config", urlPrefix)

	cfg := &config.Config{}
	err := testutil.ReadGetJSON(re, tests.TestDialClient, configPrefix, cfg)
	re.NoError(err)

	re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, configPrefix, cfg))
	re.True(cfg.Schedule.EnableDiagnostic)

	ms := map[string]any{
		"enable-diagnostic": "true",
		"max-replicas":      1,
	}
	postData, err := json.Marshal(ms)
	re.NoError(err)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, configPrefix, postData, testutil.StatusOK(re)))
	cfg = &config.Config{}
	re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, configPrefix, cfg))
	re.True(cfg.Schedule.EnableDiagnostic)

	balanceRegionURL := diagnosticPrefix + "/" + types.BalanceRegionScheduler.String()
	result := &schedulers.DiagnosticResult{}
	err = testutil.ReadGetJSON(re, tests.TestDialClient, balanceRegionURL, result)
	re.NoError(err)
	re.Equal("disabled", result.Status)

	evictLeaderURL := diagnosticPrefix + "/" + types.EvictLeaderScheduler.String()
	re.NoError(testutil.CheckGetJSON(tests.TestDialClient, evictLeaderURL, nil, testutil.StatusNotOK(re)))

	input := make(map[string]any)
	input["name"] = types.BalanceRegionScheduler.String()
	body, err := json.Marshal(input)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, schedulerPrefix, body, testutil.StatusOK(re))
	re.NoError(err)
	checkStatus(re, "pending", balanceRegionURL)

	input = make(map[string]any)
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, schedulerPrefix+"/"+types.BalanceRegionScheduler.String(), pauseArgs, testutil.StatusOK(re))
	re.NoError(err)
	checkStatus(re, "paused", balanceRegionURL)

	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, schedulerPrefix+"/"+types.BalanceRegionScheduler.String(), pauseArgs, testutil.StatusOK(re))
	re.NoError(err)
	checkStatus(re, "pending", balanceRegionURL)

	tests.MustPutRegion(re, cluster, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	checkStatus(re, "normal", balanceRegionURL)

	deleteURL := fmt.Sprintf("%s/%s", schedulerPrefix, types.BalanceRegionScheduler.String())
	err = testutil.CheckDelete(tests.TestDialClient, deleteURL, testutil.StatusOK(re))
	re.NoError(err)
	checkStatus(re, "disabled", balanceRegionURL)
}

func checkStatus(re *require.Assertions, status string, url string) {
	err := testutil.CheckGetUntilStatusCode(re, tests.TestDialClient, url, http.StatusOK)
	re.NoError(err)
	re.Eventually(func() bool {
		result := &schedulers.DiagnosticResult{}
		err := testutil.ReadGetJSON(re, tests.TestDialClient, url, result)
		re.NoError(err)
		return result.Status == status
	}, time.Second, time.Millisecond*50)
}
