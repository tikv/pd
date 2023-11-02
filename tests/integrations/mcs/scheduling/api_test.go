package scheduling_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	_ "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
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
	ctx              context.Context
	cleanupFunc      testutil.CleanupFunc
	cluster          *tests.TestCluster
	server           *tests.TestServer
	backendEndpoints string
	dialClient       *http.Client
}

func TestAPI(t *testing.T) {
	suite.Run(t, &apiTestSuite{})
}

func (suite *apiTestSuite) SetupSuite() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestAPICluster(suite.ctx, 1)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	suite.NoError(suite.server.BootstrapCluster())
	suite.backendEndpoints = suite.server.GetAddr()
	suite.dialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	suite.cleanupFunc = func() {
		cancel()
	}
}

func (suite *apiTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cleanupFunc()
}

func (suite *apiTestSuite) TestGetCheckerByName() {
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

	re := suite.Require()
	s, cleanup := tests.StartSingleSchedulingTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
	addr := s.GetAddr()
	urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/checkers", addr)
	co := s.GetCoordinator()

	for _, testCase := range testCases {
		name := testCase.name
		// normal run
		resp := make(map[string]interface{})
		err := testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		suite.NoError(err)
		suite.False(resp["paused"].(bool))
		// paused
		err = co.PauseOrResumeChecker(name, 30)
		suite.NoError(err)
		resp = make(map[string]interface{})
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		suite.NoError(err)
		suite.True(resp["paused"].(bool))
		// resumed
		err = co.PauseOrResumeChecker(name, 1)
		suite.NoError(err)
		time.Sleep(time.Second)
		resp = make(map[string]interface{})
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		suite.NoError(err)
		suite.False(resp["paused"].(bool))
	}
}

func (suite *apiTestSuite) TestAPIForward() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/apiutil/serverapi/checkHeader", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/apiutil/serverapi/checkHeader"))
	}()

	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", suite.backendEndpoints)
	var slice []string
	var resp map[string]interface{}

	// Test opeartor
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), &slice,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
	re.Len(slice, 0)

	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), []byte(``),
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	suite.NoError(err)

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
	suite.False(resp["paused"].(bool))

	input := make(map[string]interface{})
	input["delay"] = 10
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "checker/merge"), pauseArgs,
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	suite.NoError(err)

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

	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"), pauseArgs,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	suite.NoError(err)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/diagnostic/balance-leader-scheduler"), &resp,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	suite.NoError(err)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "scheduler-config"), &resp,
		testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	suite.NoError(err)
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
		suite.NoError(err)
	}

	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), pauseArgs,
		testutil.WithoutHeader(re, apiutil.ForwardToMicroServiceHeader))
	re.NoError(err)

	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"),
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
}

func (suite *apiTestSuite) TestConfig() {
	checkConfig := func(cluster *tests.TestCluster) {
		re := suite.Require()
		s := cluster.GetSchedulingPrimaryServer()
		testutil.Eventually(re, func() bool {
			return s.IsServing()
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
		addr := s.GetAddr()
		urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/config", addr)

		var cfg config.Config
		testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
		suite.Equal(cfg.GetListenAddr(), s.GetConfig().GetListenAddr())
		suite.Equal(cfg.Schedule.LeaderScheduleLimit, s.GetConfig().Schedule.LeaderScheduleLimit)
		suite.Equal(cfg.Schedule.EnableCrossTableMerge, s.GetConfig().Schedule.EnableCrossTableMerge)
		suite.Equal(cfg.Replication.MaxReplicas, s.GetConfig().Replication.MaxReplicas)
		suite.Equal(cfg.Replication.LocationLabels, s.GetConfig().Replication.LocationLabels)
		suite.Equal(cfg.DataDir, s.GetConfig().DataDir)
		testutil.Eventually(re, func() bool {
			// wait for all schedulers to be loaded in scheduling server.
			return len(cfg.Schedule.SchedulersPayload) == 5
		})
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-leader-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-region-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-hot-region-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-witness-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "transfer-witness-leader-scheduler")
	}
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInAPIMode(checkConfig)
}

func TestConfigForward(t *testing.T) {
	re := require.New(t)
	checkConfigForward := func(cluster *tests.TestCluster) {
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
			return len(schedulers) == 5
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
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkConfigForward)
}

func TestAdminRegionCache(t *testing.T) {
	re := require.New(t)
	checkAdminRegionCache := func(cluster *tests.TestCluster) {
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
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkAdminRegionCache)
}

func TestAdminRegionCacheForward(t *testing.T) {
	re := require.New(t)
	checkAdminRegionCache := func(cluster *tests.TestCluster) {
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
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkAdminRegionCache)
}
