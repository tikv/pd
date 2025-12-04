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

package resourcemanager

import (
	"context"
	"encoding/json"
	"fmt"
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
	rmGroup := suite.fetchResourceGroup(suite.rmPrimary.GetAddr(), groupName)
	re.NotNil(rmGroup)
	pdGroup := suite.fetchResourceGroup(
		suite.pdLeader.GetAddr(),
		groupName,
		testutil.WithHeader(re, apiutil.XForwardedToMicroserviceHeader, "true"),
	)
	re.Equal(rmGroup.Name, pdGroup.Name)
	re.Equal(rmGroup.Priority, pdGroup.Priority)
	re.Equal(rmGroup.RUSettings.RU.Settings.FillRate, pdGroup.RUSettings.RU.Settings.FillRate)
	re.Equal(rmGroup.RUSettings.RU.Settings.BurstLimit, pdGroup.RUSettings.RU.Settings.BurstLimit)
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
	pdResp, err := pdClient.GetResourceGroup(ctx, req)
	re.NoError(err)
	rmResp, err := rmClient.GetResourceGroup(ctx, req)
	re.NoError(err)
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
		testutil.WithHeader(re, apiutil.XForwardedToMicroserviceHeader, "true"),
	))
}

func (suite *resourceManagerRedirectorTestSuite) fetchResourceGroup(addr, groupName string, opts ...func([]byte, int, http.Header)) *server.ResourceGroup {
	re := suite.Require()
	querySuffix := fmt.Sprintf("?keyspace_name=%s", suite.keyspaceName)
	url := fmt.Sprintf("%s%sconfig/group/%s%s", addr, apis.APIPathPrefix, groupName, querySuffix)
	group := &server.ResourceGroup{}
	re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, url, group, opts...))
	return group
}
