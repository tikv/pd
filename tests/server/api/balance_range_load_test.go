// Copyright 2026 TiKV Project Authors.
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
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func TestBalanceRangeMCSConfigWatchOrder(t *testing.T) {
	for _, jobsFirst := range []bool{true, false} {
		name := "scheduler-list-first"
		if jobsFirst {
			name = "jobs-first"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			for _, fp := range []string{
				"github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker",
				"github.com/tikv/pd/server/cluster/skipStoreConfigSync",
			} {
				re.NoError(failpoint.Enable(fp, "return(true)"))
				defer func() { re.NoError(failpoint.Disable(fp)) }()
			}
			env := tests.NewSchedulingTestEnvironment(t)
			env.Env, env.PDCount = tests.MicroserviceEnv, 1
			defer env.Cleanup()
			env.RunTest(func(cluster *tests.TestCluster) {
				entered := make(chan struct{}, 1)
				notReady := make(chan struct{}, 1)
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				fp := "github.com/tikv/pd/pkg/mcs/scheduling/server/beforeLoadBalanceRangeScheduler"
				if !jobsFirst {
					fp = "github.com/tikv/pd/pkg/mcs/scheduling/server/config/beforeUpdateBalanceRangeSchedulerConfig"
				}
				re.NoError(failpoint.EnableCall(fp, func() {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-release
				}))
				const missingFP = "github.com/tikv/pd/pkg/mcs/scheduling/server/balanceRangeSchedulerConfigNotReady"
				re.NoError(failpoint.EnableCall(missingFP, func() {
					select {
					case notReady <- struct{}{}:
					default:
					}
				}))
				defer func() {
					unblock()
					re.NoError(failpoint.Disable(fp))
					re.NoError(failpoint.Disable(missingFP))
				}()
				waitSignal := func(ch <-chan struct{}) {
					testutil.Eventually(re, func() bool {
						select {
						case <-ch:
							return true
						default:
							return false
						}
					})
				}
				prefix := cluster.GetLeaderServer().GetAddr() + "/pd/api/v1"
				const first = `{"name":"balance-range-scheduler","alias":"first","engine":"tikv","rule":"leader-scatter","start-key":"100","end-key":"200"}`
				const second = `{"name":"balance-range-scheduler","alias":"second","engine":"tikv","rule":"leader-scatter","start-key":"100","end-key":"200"}`
				re.NoError(testutil.CheckPostJSON(tests.TestDialClient, prefix+"/schedulers", []byte(first), testutil.StatusOK(re)))
				waitSignal(entered)
				service := cluster.GetSchedulingPrimaryServer().GetCluster()
				if !jobsFirst {
					waitSignal(notReady)
					re.Nil(service.GetCoordinator().GetSchedulersController().GetScheduler(types.BalanceRangeScheduler.String()))
					unblock()
					testutil.Eventually(re, func() bool {
						return service.GetCoordinator().GetSchedulersController().GetScheduler(types.BalanceRangeScheduler.String()) != nil
					})
				}
				re.NoError(testutil.CheckPutJSON(tests.TestDialClient, prefix+"/scheduler-config/balance-range-scheduler/job", []byte(second),
					func(body []byte, code int, _ http.Header) {
						re.Equal(http.StatusOK, code)
						re.JSONEq(`"1"`, string(body))
					}))
				re.NoError(testutil.CheckDelete(tests.TestDialClient,
					prefix+"/scheduler-config/balance-range-scheduler/job?job-id=0", testutil.StatusOK(re)))
				testutil.Eventually(re, func() bool {
					data, err := service.GetStorage().LoadSchedulerConfig(types.BalanceRangeScheduler.String())
					re.NoError(err)
					var jobs []map[string]any
					return json.Unmarshal([]byte(data), &jobs) == nil && len(jobs) == 2 && jobs[0]["status"] == "cancelled"
				})
				unblock()
				testutil.Eventually(re, func() bool {
					return service.GetCoordinator().GetSchedulersController().GetScheduler(types.BalanceRangeScheduler.String()) != nil
				})
				// An identical retry must not require another job-config event to
				// restore a job that was already acknowledged.
				re.NoError(testutil.CheckPostJSON(tests.TestDialClient, prefix+"/schedulers", []byte(second),
					func(body []byte, code int, _ http.Header) {
						re.Equal(http.StatusOK, code)
						re.JSONEq(`"1"`, string(body))
					}))
				var jobs []map[string]any
				re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient,
					prefix+"/scheduler-config/balance-range-scheduler/list", &jobs, testutil.StatusOK(re)))
				re.Len(jobs, 2)
				re.Equal("cancelled", jobs[0]["status"])
				re.Equal("second", jobs[1]["alias"])
			})
		})
	}
}
