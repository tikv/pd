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

package scheduling

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/stretchr/testify/require"

	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type balanceRangeJob struct {
	JobID  uint64     `json:"job-id"`
	Alias  string     `json:"alias"`
	Status string     `json:"status"`
	Start  *time.Time `json:"start,omitempty"`
	Finish *time.Time `json:"finish,omitempty"`
}

// listSchedulingBalanceRangeJobs returns the jobs in the memory of the scheduling server,
// or nil if the balance-range-scheduler doesn't exist there.
func listSchedulingBalanceRangeJobs(re *require.Assertions, server *scheduling.Server) []balanceRangeJob {
	cluster := server.GetCluster()
	if cluster == nil || cluster.GetCoordinator() == nil {
		return nil
	}
	handler, ok := cluster.GetCoordinator().GetSchedulersController().
		GetSchedulerHandlers()[types.BalanceRangeScheduler.String()]
	if !ok {
		return nil
	}
	req := httptest.NewRequest(http.MethodGet, "/list", http.NoBody)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	re.Equal(http.StatusOK, w.Code, w.Body.String())
	var jobs []balanceRangeJob
	re.NoError(json.Unmarshal(w.Body.Bytes(), &jobs))
	return jobs
}

// listPersistedBalanceRangeJobs returns the jobs persisted in etcd by the PD.
func (suite *serverTestSuite) listPersistedBalanceRangeJobs(re *require.Assertions) []balanceRangeJob {
	data, err := suite.pdLeader.GetServer().GetStorage().LoadSchedulerConfig(types.BalanceRangeScheduler.String())
	re.NoError(err)
	if len(data) == 0 {
		return nil
	}
	var jobs []balanceRangeJob
	re.NoError(json.Unmarshal([]byte(data), &jobs))
	return jobs
}

func (suite *serverTestSuite) mustAddBalanceRangeJob(re *require.Assertions, alias, timeout, startKey, endKey string) {
	tests.MustAddScheduler(re, suite.backendEndpoints, types.BalanceRangeScheduler.String(), map[string]any{
		"engine":    "tikv",
		"rule":      "leader-scatter",
		"alias":     alias,
		"timeout":   timeout,
		"start-key": url.QueryEscape(startKey),
		"end-key":   url.QueryEscape(endKey),
	})
}

func (suite *serverTestSuite) TestBalanceRangeJobStatusAfterPrimaryChange() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	primary := tc.WaitForPrimaryServing(re)
	testutil.Eventually(re, func() bool {
		cluster := primary.GetCluster()
		return cluster != nil && cluster.GetCoordinator() != nil
	})
	primary.GetCluster().SetPrepared()
	defer tests.MustDeleteScheduler(re, suite.backendEndpoints, types.BalanceRangeScheduler.String())

	// The first job times out quickly and becomes finished.
	suite.mustAddBalanceRangeJob(re, "job-0", "1s", "100", "200")
	testutil.Eventually(re, func() bool {
		jobs := listSchedulingBalanceRangeJobs(re, primary)
		return len(jobs) == 1 && jobs[0].Status == "finished"
	})
	// The second job keeps running.
	suite.mustAddBalanceRangeJob(re, "job-1", "1h", "300", "400")
	testutil.Eventually(re, func() bool {
		jobs := listSchedulingBalanceRangeJobs(re, primary)
		return len(jobs) == 2 && jobs[0].Status == "finished" && jobs[1].Status == "running"
	})

	// The job status must be persisted by the PD, otherwise it will be lost after the primary changes.
	var persisted []balanceRangeJob
	testutil.Eventually(re, func() bool {
		persisted = suite.listPersistedBalanceRangeJobs(re)
		return len(persisted) == 2 && persisted[0].Status == "finished" && persisted[1].Status == "running"
	})
	re.NotNil(persisted[0].Finish)
	re.NotNil(persisted[1].Start)

	// Change the primary.
	oldPrimaryAddr := primary.GetAddr()
	primary.Close()
	primary = tc.WaitForPrimaryServing(re)
	re.NotEqual(oldPrimaryAddr, primary.GetAddr())
	testutil.Eventually(re, func() bool {
		cluster := primary.GetCluster()
		return cluster != nil && cluster.GetCoordinator() != nil
	})
	primary.GetCluster().SetPrepared()

	// The new primary must keep the job status: the finished job is not executed again,
	// and the running job keeps its start time and key ranges.
	testutil.Eventually(re, func() bool {
		jobs := listSchedulingBalanceRangeJobs(re, primary)
		return len(jobs) == 2 && jobs[1].Status == "running" &&
			!primary.GetCluster().GetKeyRangeManager().IsEmpty()
	})
	// Give the scheduler some rounds to make sure the finished job is not executed again.
	time.Sleep(2 * time.Second)
	jobs := listSchedulingBalanceRangeJobs(re, primary)
	re.Len(jobs, 2)
	re.Equal("finished", jobs[0].Status)
	re.True(persisted[0].Finish.Equal(*jobs[0].Finish))
	re.Equal("running", jobs[1].Status)
	re.True(persisted[1].Start.Equal(*jobs[1].Start))
}

func (suite *serverTestSuite) TestBalanceRangeCancelRunningJob() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	primary := tc.WaitForPrimaryServing(re)
	testutil.Eventually(re, func() bool {
		cluster := primary.GetCluster()
		return cluster != nil && cluster.GetCoordinator() != nil
	})
	primary.GetCluster().SetPrepared()
	defer tests.MustDeleteScheduler(re, suite.backendEndpoints, types.BalanceRangeScheduler.String())

	suite.mustAddBalanceRangeJob(re, "job-0", "1h", "100", "200")
	km := primary.GetCluster().GetKeyRangeManager()
	testutil.Eventually(re, func() bool {
		jobs := listSchedulingBalanceRangeJobs(re, primary)
		return len(jobs) == 1 && jobs[0].Status == "running" && !km.IsEmpty()
	})

	// Cancel the running job through the PD, the scheduling service must stop it and clean its key ranges.
	tests.MustCallSchedulerConfigAPI(re, http.MethodDelete, suite.backendEndpoints,
		types.BalanceRangeScheduler.String(), []string{"job?job-id=0"}, nil)
	testutil.Eventually(re, func() bool {
		jobs := listSchedulingBalanceRangeJobs(re, primary)
		return len(jobs) == 1 && jobs[0].Status == "cancelled" && km.IsEmpty()
	})
	persisted := suite.listPersistedBalanceRangeJobs(re)
	re.Len(persisted, 1)
	re.Equal("cancelled", persisted[0].Status)
}

func (suite *serverTestSuite) TestBalanceRangeRemoveSchedulerWithRunningJob() {
	re := suite.Require()
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	primary := tc.WaitForPrimaryServing(re)
	testutil.Eventually(re, func() bool {
		cluster := primary.GetCluster()
		return cluster != nil && cluster.GetCoordinator() != nil
	})
	primary.GetCluster().SetPrepared()

	suite.mustAddBalanceRangeJob(re, "job-0", "1h", "100", "200")
	km := primary.GetCluster().GetKeyRangeManager()
	testutil.Eventually(re, func() bool {
		jobs := listSchedulingBalanceRangeJobs(re, primary)
		return len(jobs) == 1 && jobs[0].Status == "running" && !km.IsEmpty()
	})

	// Remove the scheduler with a running job, its key ranges must be cleaned.
	tests.MustDeleteScheduler(re, suite.backendEndpoints, types.BalanceRangeScheduler.String())
	testutil.Eventually(re, func() bool {
		return listSchedulingBalanceRangeJobs(re, primary) == nil && km.IsEmpty()
	})
}
