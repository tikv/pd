package scheduling_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"

	_ "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
)

type apiTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestAPI(t *testing.T) {
	suite.Run(t, new(apiTestSuite))
}

func (suite *apiTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/changeRunCollectWaitTime", `return(true)`))
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *apiTestSuite) TearDownSuite() {
	suite.env.Cleanup()
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/changeRunCollectWaitTime"))
}

func (suite *apiTestSuite) TestGetCheckerByName() {
	suite.env.RunTestInAPIMode(suite.checkGetCheckerByName)
}

func (suite *apiTestSuite) checkGetCheckerByName(cluster *tests.TestCluster) {
	re := suite.Require()
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

	s := cluster.GetSchedulingPrimaryServer()
	urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/checkers", s.GetAddr())
	co := s.GetCoordinator()

	for _, testCase := range testCases {
		name := testCase.name
		// normal run
		resp := make(map[string]any)
		err := testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		re.NoError(err)
		re.False(resp["paused"].(bool))
		// paused
		err = co.PauseOrResumeChecker(name, 30)
		re.NoError(err)
		resp = make(map[string]any)
		err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		re.NoError(err)
		re.True(resp["paused"].(bool))
		// resumed
		err = co.PauseOrResumeChecker(name, 1)
		re.NoError(err)
		time.Sleep(time.Second)
		resp = make(map[string]any)
		err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		re.NoError(err)
		re.False(resp["paused"].(bool))
	}
}

func (suite *apiTestSuite) TestAPIForward() {
	suite.env.RunTestInAPIMode(suite.checkAPIForward)
}

func (suite *apiTestSuite) checkAPIForward(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer().GetServer()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1", leader.GetAddr())
	var respSlice []string
	var resp map[string]any
	testutil.Eventually(re, func() bool {
		return leader.IsServiceIndependent(constant.SchedulingServiceName)
	})

	// Test operators
	err := testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), &respSlice,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	re.Empty(respSlice)

	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), []byte(``),
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/2"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/2"),
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/records"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test checker
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "checker/merge"), &resp,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	re.False(resp["paused"].(bool))

	// Test pause
	postChecker := func(delay int) {
		input := make(map[string]any)
		input["delay"] = delay
		pauseArgs, err := json.Marshal(input)
		re.NoError(err)
		err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "checker/merge"), pauseArgs,
			testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
		re.NoError(err)
	}
	postChecker(30)
	postChecker(0)

	// Test scheduler:
	// Need to redirect:
	//	"/schedulers", http.MethodGet
	//	"/schedulers/{name}", http.MethodPost
	//	"/schedulers/diagnostic/{name}", http.MethodGet
	// 	"/scheduler-config/", http.MethodGet
	// 	"/scheduler-config/{name}/list", http.MethodGet
	// 	"/scheduler-config/{name}/roles", http.MethodGet
	// Should not redirect:
	//	"/schedulers", http.MethodPost
	//	"/schedulers/{name}", http.MethodDelete
	testutil.Eventually(re, func() bool {
		err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), &respSlice,
			testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
		re.NoError(err)
		return slice.Contains(respSlice, "balance-leader-scheduler")
	})

	postScheduler := func(delay int) {
		input := make(map[string]any)
		input["delay"] = delay
		pauseArgs, err := json.Marshal(input)
		re.NoError(err)
		err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"), pauseArgs,
			testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
		re.NoError(err)
	}
	postScheduler(30)
	postScheduler(0)

	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/diagnostic/balance-leader-scheduler"), &resp,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "scheduler-config"), &resp,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	re.Contains(resp, "balance-leader-scheduler")
	re.Contains(resp, "balance-hot-region-scheduler")

	schedulers := []string{
		"balance-leader-scheduler",
		"balance-hot-region-scheduler",
	}
	for _, schedulerName := range schedulers {
		err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s/%s/%s", urlPrefix, "scheduler-config", schedulerName, "list"), &resp,
			testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
		re.NoError(err)
	}

	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), nil,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)

	err = testutil.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)

	input := make(map[string]any)
	input["name"] = "balance-leader-scheduler"
	b, err := json.Marshal(input)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), b,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)

	// Test hotspot
	var hotRegions statistics.StoreHotPeersInfos
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/write"), &hotRegions,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/read"), &hotRegions,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	var stores handler.HotStoreStats
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/stores"), &stores,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	var buckets handler.HotBucketsResponse
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/buckets"), &buckets,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	var history storage.HistoryHotRegions
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/history"), &history,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test region label
	var labelRules []*labeler.LabelRule
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rules"), &labelRules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSONWithBody(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rules/ids"), []byte(`["rule1", "rule3"]`),
		&labelRules, testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rule/rule1"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1"), nil,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1/label/key"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1/labels"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test Region
	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")))
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/accelerate-schedule"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`[{"start_key":"%s", "end_key": "%s"}, {"start_key":"%s", "end_key": "%s"}]`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")), hex.EncodeToString([]byte("a4")), hex.EncodeToString([]byte("a6")))
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/accelerate-schedule/batch"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("b1")), hex.EncodeToString([]byte("b3")))
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/scatter"), []byte(body),
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`{"retry_limit":%v, "split_keys": ["%s","%s","%s"]}`, 3,
		hex.EncodeToString([]byte("bbb")),
		hex.EncodeToString([]byte("ccc")),
		hex.EncodeToString([]byte("ddd")))
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/split"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a2"))), nil,
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	// Test rules: only forward `GET` request
	var rules []*placement.Rule
	tests.MustPutRegion(re, cluster, 2, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	rules = []*placement.Rule{
		{
			GroupID:        placement.DefaultGroupID,
			ID:             placement.DefaultRuleID,
			Role:           placement.Voter,
			Count:          3,
			LocationLabels: []string{},
		},
	}
	rulesArgs, err := json.Marshal(rules)
	re.NoError(err)

	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/batch"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/group/pd"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/region/2"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	var fit placement.RegionFit
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/region/2/detail"), &fit,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/key/0000000000000001"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule/pd/2"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule/pd/2"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group/pd"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group/pd"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_groups"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)

	// test redirect is disabled
	err = testutil.CheckGetJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), http.NoBody)
	re.NoError(err)
	req.Header.Set(apiutil.XForbiddenForwardToMicroServiceHeader, "true")
	httpResp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, httpResp.StatusCode)
	defer httpResp.Body.Close()
	re.Empty(httpResp.Header.Get(apiutil.XForwardedToMicroServiceHeader))
}

func (suite *apiTestSuite) TestConfig() {
	suite.env.RunTestInAPIMode(suite.checkConfig)
}

func (suite *apiTestSuite) checkConfig(cluster *tests.TestCluster) {
	re := suite.Require()
	s := cluster.GetSchedulingPrimaryServer()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
	addr := s.GetAddr()
	urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/config", addr)

	var cfg config.Config
	testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &cfg)
	re.Equal(cfg.GetListenAddr(), s.GetConfig().GetListenAddr())
	re.Equal(cfg.Schedule.LeaderScheduleLimit, s.GetConfig().Schedule.LeaderScheduleLimit)
	re.Equal(cfg.Schedule.EnableCrossTableMerge, s.GetConfig().Schedule.EnableCrossTableMerge)
	re.Equal(cfg.Replication.MaxReplicas, s.GetConfig().Replication.MaxReplicas)
	re.Equal(cfg.Replication.LocationLabels, s.GetConfig().Replication.LocationLabels)
	re.Equal(cfg.DataDir, s.GetConfig().DataDir)
}

func (suite *apiTestSuite) TestConfigForward() {
	suite.env.RunTestInAPIMode(suite.checkConfigForward)
}

func (suite *apiTestSuite) checkConfigForward(cluster *tests.TestCluster) {
	re := suite.Require()
	sche := cluster.GetSchedulingPrimaryServer()
	opts := sche.GetPersistConfig()
	var cfg map[string]any
	addr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/config", addr)

	// Test config forward
	// Expect to get same config in scheduling server and api server
	testutil.Eventually(re, func() bool {
		testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &cfg)
		re.Equal(cfg["schedule"].(map[string]any)["leader-schedule-limit"],
			float64(opts.GetLeaderScheduleLimit()))
		return cfg["replication"].(map[string]any)["max-replicas"] == float64(opts.GetReplicationConfig().MaxReplicas)
	})

	// Test to change config in api server
	// Expect to get new config in scheduling server and api server
	reqData, err := json.Marshal(map[string]any{
		"max-replicas": 4,
	})
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, urlPrefix, reqData, testutil.StatusOK(re))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &cfg)
		return cfg["replication"].(map[string]any)["max-replicas"] == 4. &&
			opts.GetReplicationConfig().MaxReplicas == 4.
	})

	// Test to change config only in scheduling server
	// Expect to get new config in scheduling server but not old config in api server
	scheCfg := opts.GetScheduleConfig().Clone()
	scheCfg.LeaderScheduleLimit = 100
	opts.SetScheduleConfig(scheCfg)
	re.Equal(100, int(opts.GetLeaderScheduleLimit()))
	testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &cfg)
	re.Equal(100., cfg["schedule"].(map[string]any)["leader-schedule-limit"])
	repCfg := opts.GetReplicationConfig().Clone()
	repCfg.MaxReplicas = 5
	opts.SetReplicationConfig(repCfg)
	re.Equal(5, int(opts.GetReplicationConfig().MaxReplicas))
	testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &cfg)
	re.Equal(5., cfg["replication"].(map[string]any)["max-replicas"])
}

func (suite *apiTestSuite) TestAdminRegionCache() {
	suite.env.RunTestInAPIMode(suite.checkAdminRegionCache)
}

func (suite *apiTestSuite) checkAdminRegionCache(cluster *tests.TestCluster) {
	re := suite.Require()
	r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
	tests.MustPutRegionInfo(re, cluster, r1)
	r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
	tests.MustPutRegionInfo(re, cluster, r2)
	r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetRegionConfVer(100), core.SetRegionVersion(100))
	tests.MustPutRegionInfo(re, cluster, r3)

	schedulingServer := cluster.GetSchedulingPrimaryServer()
	re.Equal(3, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))

	addr := schedulingServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/admin/cache/regions", addr)
	err := testutil.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "30"), testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(2, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))

	err = testutil.CheckDelete(tests.TestDialClient, urlPrefix, testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(0, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
}

func (suite *apiTestSuite) TestAdminRegionCacheForward() {
	suite.env.RunTestInAPIMode(suite.checkAdminRegionCacheForward)
}

func (suite *apiTestSuite) checkAdminRegionCacheForward(cluster *tests.TestCluster) {
	re := suite.Require()
	r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
	tests.MustPutRegionInfo(re, cluster, r1)
	r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
	tests.MustPutRegionInfo(re, cluster, r2)
	r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetRegionConfVer(100), core.SetRegionVersion(100))
	tests.MustPutRegionInfo(re, cluster, r3)

	apiServer := cluster.GetLeaderServer().GetServer()
	schedulingServer := cluster.GetSchedulingPrimaryServer()
	re.Equal(3, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
	re.Equal(3, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}))

	addr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/admin/cache/region", addr)
	err := testutil.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "30"), testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(2, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
	re.Equal(2, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}))

	err = testutil.CheckDelete(tests.TestDialClient, urlPrefix+"s", testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(0, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
	re.Equal(0, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}))
}

func (suite *apiTestSuite) TestFollowerForward() {
	suite.env.RunTestBasedOnMode(suite.checkFollowerForward)
	suite.TearDownSuite()
	suite.SetupSuite()
}

func (suite *apiTestSuite) checkFollowerForward(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderAddr := cluster.GetLeaderServer().GetAddr()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	follower, err := cluster.JoinAPIServer(ctx)
	re.NoError(err)
	re.NoError(follower.Run())
	re.NotEmpty(cluster.WaitLeader())

	followerAddr := follower.GetAddr()
	if cluster.GetLeaderServer().GetAddr() != leaderAddr {
		followerAddr = leaderAddr
	}

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", followerAddr)
	rules := []*placement.Rule{}
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		// follower will forward to scheduling server directly
		re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
		err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
			testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"),
		)
		re.NoError(err)
	} else {
		// follower will forward to leader server
		re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
		err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
			testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader),
		)
		re.NoError(err)
	}

	// follower will forward to leader server
	re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
	results := make(map[string]any)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config"), &results,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader),
	)
	re.NoError(err)
}

func (suite *apiTestSuite) TestMetrics() {
	suite.env.RunTestInAPIMode(suite.checkMetrics)
}

func (suite *apiTestSuite) checkMetrics(cluster *tests.TestCluster) {
	re := suite.Require()
	s := cluster.GetSchedulingPrimaryServer()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
	resp, err := tests.TestDialClient.Get(s.GetConfig().GetAdvertiseListenAddr() + "/metrics")
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	respBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Contains(string(respBytes), "pd_server_info")
}

func (suite *apiTestSuite) TestStatus() {
	suite.env.RunTestInAPIMode(suite.checkStatus)
}

func (suite *apiTestSuite) checkStatus(cluster *tests.TestCluster) {
	re := suite.Require()
	s := cluster.GetSchedulingPrimaryServer()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
	resp, err := tests.TestDialClient.Get(s.GetConfig().GetAdvertiseListenAddr() + "/status")
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	respBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	var status versioninfo.Status
	re.NoError(json.Unmarshal(respBytes, &status))
	re.Equal(versioninfo.PDBuildTS, status.BuildTS)
	re.Equal(versioninfo.PDGitHash, status.GitHash)
	re.Equal(versioninfo.PDReleaseVersion, status.Version)
}

func (suite *apiTestSuite) TestStores() {
	suite.env.RunTestInAPIMode(suite.checkStores)
}

func (suite *apiTestSuite) checkStores(cluster *tests.TestCluster) {
	re := suite.Require()
	stores := []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			Id:        4,
			Address:   "tikv4",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Offline == 1
			Id:        6,
			Address:   "tikv6",
			State:     metapb.StoreState_Offline,
			NodeState: metapb.NodeState_Removing,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:        7,
			Address:   "tikv7",
			State:     metapb.StoreState_Tombstone,
			NodeState: metapb.NodeState_Removed,
			Version:   "2.0.0",
		},
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/doNotBuryStore", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/doNotBuryStore"))
	}()
	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}
	// Test /stores
	apiServerAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/stores", apiServerAddr)
	var resp map[string]any
	err := testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["stores"].([]any), 3)
	scheServerAddr := cluster.GetSchedulingPrimaryServer().GetAddr()
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores", scheServerAddr)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["stores"].([]any), 3)
	// Test /stores/{id}
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/1", scheServerAddr)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal("tikv1", resp["store"].(map[string]any)["address"])
	re.Equal("Up", resp["store"].(map[string]any)["state_name"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/6", scheServerAddr)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal("tikv6", resp["store"].(map[string]any)["address"])
	re.Equal("Offline", resp["store"].(map[string]any)["state_name"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/7", scheServerAddr)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal("tikv7", resp["store"].(map[string]any)["address"])
	re.Equal("Tombstone", resp["store"].(map[string]any)["state_name"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/233", scheServerAddr)
	testutil.CheckGetJSON(tests.TestDialClient, urlPrefix, nil,
		testutil.Status(re, http.StatusNotFound), testutil.StringContain(re, "not found"))
}

func (suite *apiTestSuite) TestRegions() {
	suite.env.RunTestInAPIMode(suite.checkRegions)
}

func (suite *apiTestSuite) checkRegions(cluster *tests.TestCluster) {
	re := suite.Require()
	tests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"))
	tests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"))
	tests.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"))
	// Test /regions
	apiServerAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/regions", apiServerAddr)
	var resp map[string]any
	err := testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["regions"].([]any), 3)
	scheServerAddr := cluster.GetSchedulingPrimaryServer().GetAddr()
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions", scheServerAddr)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["regions"].([]any), 3)
	// Test /regions/{id} and /regions/count
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions/1", scheServerAddr)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	key := fmt.Sprintf("%x", "a")
	re.Equal(key, resp["start_key"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions/count", scheServerAddr)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3., resp["count"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions/233", scheServerAddr)
	testutil.CheckGetJSON(tests.TestDialClient, urlPrefix, nil,
		testutil.Status(re, http.StatusNotFound), testutil.StringContain(re, "not found"))
}
