package scheduling_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	_ "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

var testDialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

type apiTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestAPI(t *testing.T) {
	suite.Run(t, new(apiTestSuite))
}

func (suite *apiTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/apiutil/serverapi/checkHeader", "return(true)"))
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *apiTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/apiutil/serverapi/checkHeader"))
	suite.env.Cleanup()
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
		resp := make(map[string]interface{})
		err := testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		re.NoError(err)
		re.False(resp["paused"].(bool))
		// paused
		err = co.PauseOrResumeChecker(name, 30)
		re.NoError(err)
		resp = make(map[string]interface{})
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		re.NoError(err)
		re.True(resp["paused"].(bool))
		// resumed
		err = co.PauseOrResumeChecker(name, 1)
		re.NoError(err)
		time.Sleep(time.Second)
		resp = make(map[string]interface{})
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
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
	var slice []string
	var resp map[string]interface{}
	testutil.Eventually(re, func() bool {
		return leader.GetRaftCluster().IsServiceIndependent(utils.SchedulingServiceName)
	})

	// Test operators
	err := testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), &slice,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	re.Empty(slice)

	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), []byte(``),
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/2"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/2"),
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/records"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test checker
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "checker/merge"), &resp,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	re.False(resp["paused"].(bool))

	// Test pause
	postChecker := func(delay int) {
		input := make(map[string]interface{})
		input["delay"] = delay
		pauseArgs, err := json.Marshal(input)
		re.NoError(err)
		err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "checker/merge"), pauseArgs,
			testutil.StatusOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
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
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), &slice,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	re.Contains(slice, "balance-leader-scheduler")

	postScheduler := func(delay int) {
		input := make(map[string]interface{})
		input["delay"] = delay
		pauseArgs, err := json.Marshal(input)
		re.NoError(err)
		err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"), pauseArgs,
			testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
		re.NoError(err)
	}
	postScheduler(30)
	postScheduler(0)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/diagnostic/balance-leader-scheduler"), &resp,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "scheduler-config"), &resp,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	re.Contains(resp, "balance-leader-scheduler")
	re.Contains(resp, "balance-witness-scheduler")
	re.Contains(resp, "balance-hot-region-scheduler")

	schedulers := []string{
		"balance-leader-scheduler",
		"balance-witness-scheduler",
		"balance-hot-region-scheduler",
	}
	for _, schedulerName := range schedulers {
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s/%s/%s", urlPrefix, "scheduler-config", schedulerName, "list"), &resp,
			testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
		re.NoError(err)
	}

	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), nil,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)

	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"),
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)

	input := make(map[string]interface{})
	input["name"] = "balance-leader-scheduler"
	b, err := json.Marshal(input)
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), b,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)

	// Test hotspot
	var hotRegions statistics.StoreHotPeersInfos
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/write"), &hotRegions,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/read"), &hotRegions,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	var stores handler.HotStoreStats
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/stores"), &stores,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	var buckets handler.HotBucketsResponse
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/buckets"), &buckets,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	var history storage.HistoryHotRegions
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/history"), &history,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test region label
	var labelRules []*labeler.LabelRule
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rules"), &labelRules,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSONWithBody(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rules/ids"), []byte(`["rule1", "rule3"]`),
		&labelRules, testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rule/rule1"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1"), nil,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1/label/key"), nil,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1/labels"), nil,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test Region
	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/accelerate-schedule"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`[{"start_key":"%s", "end_key": "%s"}, {"start_key":"%s", "end_key": "%s"}]`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")), hex.EncodeToString([]byte("a4")), hex.EncodeToString([]byte("a6")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/accelerate-schedule/batch"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("b1")), hex.EncodeToString([]byte("b3")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/scatter"), []byte(body),
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`{"retry_limit":%v, "split_keys": ["%s","%s","%s"]}`, 3,
		hex.EncodeToString([]byte("bbb")),
		hex.EncodeToString([]byte("ccc")),
		hex.EncodeToString([]byte("ddd")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/split"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a2"))), nil,
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
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

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/batch"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/group/pd"), &rules,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/region/2"), &rules,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	var fit placement.RegionFit
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/region/2/detail"), &fit,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/key/0000000000000001"), &rules,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule/pd/2"), nil,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule/pd/2"),
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group/pd"), nil,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group/pd"),
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_groups"), nil,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule"), nil,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), nil,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"),
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)
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
	testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
	re.Equal(cfg.GetListenAddr(), s.GetConfig().GetListenAddr())
	re.Equal(cfg.Schedule.LeaderScheduleLimit, s.GetConfig().Schedule.LeaderScheduleLimit)
	re.Equal(cfg.Schedule.EnableCrossTableMerge, s.GetConfig().Schedule.EnableCrossTableMerge)
	re.Equal(cfg.Replication.MaxReplicas, s.GetConfig().Replication.MaxReplicas)
	re.Equal(cfg.Replication.LocationLabels, s.GetConfig().Replication.LocationLabels)
	re.Equal(cfg.DataDir, s.GetConfig().DataDir)
	testutil.Eventually(re, func() bool {
		// wait for all schedulers to be loaded in scheduling server.
		return len(cfg.Schedule.SchedulersPayload) == 6
	})
	re.Contains(cfg.Schedule.SchedulersPayload, "balance-leader-scheduler")
	re.Contains(cfg.Schedule.SchedulersPayload, "balance-region-scheduler")
	re.Contains(cfg.Schedule.SchedulersPayload, "balance-hot-region-scheduler")
	re.Contains(cfg.Schedule.SchedulersPayload, "balance-witness-scheduler")
	re.Contains(cfg.Schedule.SchedulersPayload, "transfer-witness-leader-scheduler")
	re.Contains(cfg.Schedule.SchedulersPayload, "evict-slow-store-scheduler")
}

func (suite *apiTestSuite) TestConfigForward() {
	suite.env.RunTestInAPIMode(suite.checkConfigForward)
}

func (suite *apiTestSuite) checkConfigForward(cluster *tests.TestCluster) {
	re := suite.Require()
	sche := cluster.GetSchedulingPrimaryServer()
	opts := sche.GetPersistConfig()
	var cfg map[string]interface{}
	addr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/config", addr)

	// Test config forward
	// Expect to get same config in scheduling server and api server
	testutil.Eventually(re, func() bool {
		testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
		re.Equal(cfg["schedule"].(map[string]interface{})["leader-schedule-limit"],
			float64(opts.GetLeaderScheduleLimit()))
		re.Equal(cfg["replication"].(map[string]interface{})["max-replicas"],
			float64(opts.GetReplicationConfig().MaxReplicas))
		schedulers := cfg["schedule"].(map[string]interface{})["schedulers-payload"].(map[string]interface{})
		return len(schedulers) == 6
	})

	// Test to change config in api server
	// Expect to get new config in scheduling server and api server
	reqData, err := json.Marshal(map[string]interface{}{
		"max-replicas": 4,
	})
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, urlPrefix, reqData, testutil.StatusOK(re))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
		return cfg["replication"].(map[string]interface{})["max-replicas"] == 4. &&
			opts.GetReplicationConfig().MaxReplicas == 4.
	})

	// Test to change config only in scheduling server
	// Expect to get new config in scheduling server but not old config in api server
	opts.GetScheduleConfig().LeaderScheduleLimit = 100
	re.Equal(100, int(opts.GetLeaderScheduleLimit()))
	testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
	re.Equal(100., cfg["schedule"].(map[string]interface{})["leader-schedule-limit"])
	opts.GetReplicationConfig().MaxReplicas = 5
	re.Equal(5, int(opts.GetReplicationConfig().MaxReplicas))
	testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
	re.Equal(5., cfg["replication"].(map[string]interface{})["max-replicas"])
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
	err := testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "30"), testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(2, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))

	err = testutil.CheckDelete(testDialClient, urlPrefix, testutil.StatusOK(re))
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
	re.Equal(3, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}).Count)

	addr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/admin/cache/region", addr)
	err := testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "30"), testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(2, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
	re.Equal(2, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}).Count)

	err = testutil.CheckDelete(testDialClient, urlPrefix+"s", testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(0, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
	re.Equal(0, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}).Count)
}

func (suite *apiTestSuite) TestFollowerForward() {
	suite.env.RunTestInTwoModes(suite.checkFollowerForward)
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
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
			testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"),
		)
		re.NoError(err)
	} else {
		// follower will forward to leader server
		re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
			testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader),
		)
		re.NoError(err)
	}

	// follower will forward to leader server
	re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
	results := make(map[string]interface{})
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config"), &results,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader),
	)
	re.NoError(err)
}
