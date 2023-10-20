package scheduling_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	_ "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
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
