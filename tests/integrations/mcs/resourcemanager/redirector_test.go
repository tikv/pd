// Copyright 2025 TiKV Project Authors.
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

package resourcemanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// resourceManagerRedirectorTestSuite verifies requests are redirected to the microservice when fallback is disabled.
type resourceManagerRedirectorTestSuite struct {
	suite.Suite
	ctx          context.Context
	cancel       context.CancelFunc
	pdCluster    *tests.TestCluster
	rmCluster    *tests.TestResourceManagerCluster
	pdLeader     *tests.TestServer
	rmPrimary    *server.Server
	keyspaceName string
	keyspaceID   uint32
}

func TestResourceManagerRedirector(t *testing.T) {
	suite.Run(t, new(resourceManagerRedirectorTestSuite))
}

func (suite *resourceManagerRedirectorTestSuite) SetupTest() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/fastRefreshResourceGroupSettings", "return(true)"))
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.pdCluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1, func(conf *config.Config, _ string) {
		conf.Microservice.EnableResourceManagerFallback = false
	})
	re.NoError(err)
	re.NoError(suite.pdCluster.RunInitialServers())
	leaderName := suite.pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.pdCluster.GetServer(leaderName)
	re.NoError(suite.pdLeader.BootstrapCluster())
	suite.rmCluster, err = tests.NewTestResourceManagerCluster(suite.ctx, 1, suite.pdLeader.GetAddr())
	re.NoError(err)
	suite.rmPrimary = suite.rmCluster.WaitForPrimaryServing(re)
	re.NotNil(suite.rmPrimary)
	suite.waitForResourceManagerPrimary()
	suite.keyspaceName = "redirector_keyspace"
	meta, err := suite.pdLeader.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{Name: suite.keyspaceName})
	re.NoError(err)
	suite.keyspaceID = meta.GetId()
}

func (suite *resourceManagerRedirectorTestSuite) TearDownTest() {
	suite.Require().NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	suite.Require().NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/fastRefreshResourceGroupSettings"))
	suite.cancel()
	suite.rmCluster.Destroy()
	suite.pdCluster.Destroy()
}

func (suite *resourceManagerRedirectorTestSuite) waitForResourceManagerPrimary() {
	re := suite.Require()
	testutil.Eventually(re, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		addr, ok := suite.pdLeader.GetServer().GetServicePrimaryAddr(ctx, constant.ResourceManagerServiceName)
		return ok && addr != ""
	}, testutil.WithWaitFor(30*time.Second))
}

func (suite *resourceManagerRedirectorTestSuite) TestRedirectsConfigRequests() {
	re := suite.Require()
	groupName := "redirector_group"
	suite.createResourceGroupViaPD(groupName, 200)
	var rmGroup *server.ResourceGroup
	testutil.Eventually(re, func() bool {
		rmGroup = suite.fetchResourceGroup(suite.rmPrimary.GetAddr(), groupName)
		return rmGroup != nil && rmGroup.Name == groupName
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(200*time.Millisecond))
	pdGroup := suite.fetchResourceGroup(
		suite.pdLeader.GetAddr(),
		groupName,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroserviceHeader),
	)
	re.Equal(rmGroup.Name, pdGroup.Name)
	re.Equal(rmGroup.Priority, pdGroup.Priority)
	re.Equal(rmGroup.RUSettings.RU.Settings.FillRate, pdGroup.RUSettings.RU.Settings.FillRate)
	re.Equal(rmGroup.RUSettings.RU.Settings.BurstLimit, pdGroup.RUSettings.RU.Settings.BurstLimit)
}

func (suite *resourceManagerRedirectorTestSuite) TestRedirectsAdminRequests() {
	re := suite.Require()
	pdPutURL := fmt.Sprintf("%s%sadmin/log", suite.pdLeader.GetAddr(), apis.APIPathPrefix)
	re.NoError(testutil.CheckPutJSON(
		tests.TestDialClient,
		pdPutURL,
		[]byte(`"debug"`),
		testutil.StatusOK(re),
		testutil.StringContain(re, "log level"),
		testutil.WithHeader(re, apiutil.XForwardedToMicroserviceHeader, "true"),
	))
}

func (suite *resourceManagerRedirectorTestSuite) TestSyncsKeyspaceServiceLimitFromPD() {
	re := suite.Require()
	serviceLimit := 123.0
	suite.setKeyspaceServiceLimitViaPD(serviceLimit)

	var rmLimiter, pdLimiter *keyspaceServiceLimit
	testutil.Eventually(re, func() bool {
		rmLimiter = suite.fetchKeyspaceServiceLimit(suite.rmPrimary.GetAddr())
		pdLimiter = suite.fetchKeyspaceServiceLimit(
			suite.pdLeader.GetAddr(),
			testutil.WithoutHeader(re, apiutil.XForwardedToMicroserviceHeader),
		)
		return rmLimiter != nil && pdLimiter != nil &&
			rmLimiter.ServiceLimit == serviceLimit && pdLimiter.ServiceLimit == serviceLimit
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(200*time.Millisecond))
}

func (suite *resourceManagerRedirectorTestSuite) TestSyncsControllerConfigFromPD() {
	re := suite.Require()
	readBaseCost := 123.456
	suite.updateControllerConfigViaPD("read-base-cost", readBaseCost)

	var rmCfg, pdCfg *controllerConfig
	testutil.Eventually(re, func() bool {
		rmCfg = suite.fetchControllerConfig(suite.rmPrimary.GetAddr())
		pdCfg = suite.fetchControllerConfig(
			suite.pdLeader.GetAddr(),
			testutil.WithoutHeader(re, apiutil.XForwardedToMicroserviceHeader),
		)
		return rmCfg != nil && pdCfg != nil &&
			rmCfg.RequestUnit.ReadBaseCost == readBaseCost && pdCfg.RequestUnit.ReadBaseCost == readBaseCost
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(200*time.Millisecond))
}

func (suite *resourceManagerRedirectorTestSuite) TestGRPCRedirectsResourceGroupRequests() {
	re := suite.Require()
	groupName := "redirector_grpc_group"
	suite.createResourceGroupViaPD(groupName, 300)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pdConn, err := grpcutil.GetClientConn(ctx, suite.pdLeader.GetAddr(), nil)
	re.NoError(err)
	defer pdConn.Close()
	rmConn, err := grpcutil.GetClientConn(ctx, suite.rmPrimary.GetAddr(), nil)
	re.NoError(err)
	defer rmConn.Close()

	pdClient := rmpb.NewResourceManagerClient(pdConn)
	rmClient := rmpb.NewResourceManagerClient(rmConn)
	req := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: groupName,
		KeyspaceId: &rmpb.KeyspaceIDValue{
			Value: suite.keyspaceID,
		},
	}
	// Resource group writes are handled locally by pd-server now; the resource-manager
	// microservice observes them asynchronously via storage refresh.
	var pdResp, rmResp *rmpb.GetResourceGroupResponse
	testutil.Eventually(re, func() bool {
		cctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var err error
		pdResp, err = pdClient.GetResourceGroup(cctx, req)
		if err != nil || pdResp.GetError() != nil || pdResp.GetGroup() == nil {
			return false
		}
		rmResp, err = rmClient.GetResourceGroup(cctx, req)
		if err != nil || rmResp.GetError() != nil || rmResp.GetGroup() == nil {
			return false
		}
		return true
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(200*time.Millisecond))
	re.Nil(pdResp.GetError())
	re.Nil(rmResp.GetError())
	re.NotNil(pdResp.GetGroup())
	re.NotNil(rmResp.GetGroup())
	re.Equal(rmResp.GetGroup().GetName(), pdResp.GetGroup().GetName())
	re.Equal(rmResp.GetGroup().GetPriority(), pdResp.GetGroup().GetPriority())
	rmSettings := rmResp.GetGroup().GetRUSettings().GetRU().GetSettings()
	pdSettings := pdResp.GetGroup().GetRUSettings().GetRU().GetSettings()
	re.Equal(rmSettings.GetFillRate(), pdSettings.GetFillRate())
	re.Equal(rmSettings.GetBurstLimit(), pdSettings.GetBurstLimit())
}

func (suite *resourceManagerRedirectorTestSuite) createResourceGroupViaPD(name string, fillRate uint64) {
	re := suite.Require()
	group := &rmpb.ResourceGroup{
		Name:     name,
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: fillRate, BurstLimit: 200},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: suite.keyspaceID},
	}
	payload, err := json.Marshal(group)
	re.NoError(err)
	pdPostURL := fmt.Sprintf("%s%sconfig/group", suite.pdLeader.GetAddr(), apis.APIPathPrefix)
	re.NoError(testutil.CheckPostJSON(
		tests.TestDialClient,
		pdPostURL,
		payload,
		testutil.StatusOK(re),
		testutil.StringContain(re, "Success!"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroserviceHeader),
	))
}

func (suite *resourceManagerRedirectorTestSuite) setKeyspaceServiceLimitViaPD(serviceLimit float64) {
	re := suite.Require()
	req := &apis.KeyspaceServiceLimitRequest{ServiceLimit: serviceLimit}
	payload, err := json.Marshal(req)
	re.NoError(err)
	pdPostURL := fmt.Sprintf("%s%sconfig/keyspace/service-limit/%s", suite.pdLeader.GetAddr(), apis.APIPathPrefix, suite.keyspaceName)
	re.NoError(testutil.CheckPostJSON(
		tests.TestDialClient,
		pdPostURL,
		payload,
		testutil.StatusOK(re),
		testutil.StringContain(re, "Success!"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroserviceHeader),
	))
}

type keyspaceServiceLimit struct {
	ServiceLimit float64 `json:"service_limit"`
}

func (suite *resourceManagerRedirectorTestSuite) fetchKeyspaceServiceLimit(addr string, opts ...func([]byte, int, http.Header)) *keyspaceServiceLimit {
	url := fmt.Sprintf("%s%sconfig/keyspace/service-limit/%s", addr, apis.APIPathPrefix, suite.keyspaceName)
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil
	}
	resp, err := tests.TestDialClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, opt := range opts {
		opt(body, resp.StatusCode, resp.Header)
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	limiter := &keyspaceServiceLimit{}
	if err := json.Unmarshal(body, limiter); err != nil {
		return nil
	}
	return limiter
}

func (suite *resourceManagerRedirectorTestSuite) updateControllerConfigViaPD(item string, value any) {
	re := suite.Require()
	payload, err := json.Marshal(map[string]any{item: value})
	re.NoError(err)
	pdPostURL := fmt.Sprintf("%s%sconfig/controller", suite.pdLeader.GetAddr(), apis.APIPathPrefix)
	re.NoError(testutil.CheckPostJSON(
		tests.TestDialClient,
		pdPostURL,
		payload,
		testutil.StatusOK(re),
		testutil.StringContain(re, "Success!"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroserviceHeader),
	))
}

type controllerConfig struct {
	RequestUnit struct {
		ReadBaseCost float64 `json:"read-base-cost"`
	} `json:"request-unit"`
}

func (suite *resourceManagerRedirectorTestSuite) fetchControllerConfig(addr string, opts ...func([]byte, int, http.Header)) *controllerConfig {
	url := fmt.Sprintf("%s%sconfig/controller", addr, apis.APIPathPrefix)
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil
	}
	resp, err := tests.TestDialClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, opt := range opts {
		opt(body, resp.StatusCode, resp.Header)
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	cfg := &controllerConfig{}
	if err := json.Unmarshal(body, cfg); err != nil {
		return nil
	}
	return cfg
}

func (suite *resourceManagerRedirectorTestSuite) fetchResourceGroup(addr, groupName string, opts ...func([]byte, int, http.Header)) *server.ResourceGroup {
	querySuffix := fmt.Sprintf("?keyspace_name=%s", suite.keyspaceName)
	url := fmt.Sprintf("%s%sconfig/group/%s%s", addr, apis.APIPathPrefix, groupName, querySuffix)
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil
	}
	resp, err := tests.TestDialClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, opt := range opts {
		opt(body, resp.StatusCode, resp.Header)
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	group := &server.ResourceGroup{}
	if err := json.Unmarshal(body, group); err != nil {
		return nil
	}
	return group
}
