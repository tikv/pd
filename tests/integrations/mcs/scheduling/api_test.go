package scheduling_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	_ "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
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
	suite.server = cluster.GetServer(cluster.GetLeader())
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
