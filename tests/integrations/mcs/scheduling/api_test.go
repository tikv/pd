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
	"github.com/tikv/pd/pkg/core"
	_ "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
	"github.com/tikv/pd/pkg/schedule/handler"
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

func (suite *apiTestSuite) SetupTest() {
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
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.backendEndpoints)
	suite.NoError(err)
	suite.cluster.SetSchedulingCluster(tc)
	tc.WaitForPrimaryServing(suite.Require())
}

func (suite *apiTestSuite) TearDownTest() {
	suite.cluster.Destroy()
	suite.cleanupFunc()
}

func (suite *apiTestSuite) TestGetCheckerByName() {
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

	s := suite.cluster.GetSchedulingPrimaryServer()
	urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/checkers", s.GetAddr())
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

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", suite.backendEndpoints)
	var slice []string
	var resp map[string]interface{}

	// Test opeartor
	err := testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), &slice,
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

	// Test rules: only forward `GET` request
	var rules []*placement.Rule
	tests.MustPutRegion(re, suite.cluster, 2, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	rules = []*placement.Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           "voter",
			Count:          3,
			LocationLabels: []string{},
		},
	}
	rulesArgs, err := json.Marshal(rules)
	suite.NoError(err)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "/config/rules"), &rules,
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
