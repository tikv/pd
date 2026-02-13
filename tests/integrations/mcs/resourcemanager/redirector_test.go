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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	keyspaceconstant "github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// resourceManagerRedirectorTestSuite verifies PD redirector behavior when fallback is disabled.
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
	suite.keyspaceName = keyspaceconstant.DefaultKeyspaceName
	suite.keyspaceID = keyspaceconstant.DefaultKeyspaceID
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
	pdGroup := suite.fetchResourceGroup(
		suite.pdLeader.GetAddr(),
		groupName,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroserviceHeader),
	)
	re.Equal(groupName, pdGroup.Name)
	re.Equal(uint32(5), pdGroup.Priority)
	re.Equal(uint64(200), pdGroup.RUSettings.RU.Settings.FillRate)
	re.Equal(int64(200), pdGroup.RUSettings.RU.Settings.BurstLimit)
}

func (suite *resourceManagerRedirectorTestSuite) TestGRPCRedirectsResourceGroupRequests() {
	re := suite.Require()
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

	addGroupName := "redirector_grpc_add_group"
	addGroup := &rmpb.ResourceGroup{
		Name:     addGroupName,
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 7,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   400,
					BurstLimit: 600,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: suite.keyspaceID},
	}
	addResp, err := pdClient.AddResourceGroup(ctx, &rmpb.PutResourceGroupRequest{Group: addGroup})
	re.NoError(err)
	re.Nil(addResp.GetError())
	re.Equal("Success!", addResp.GetBody())

	addGetReq := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: addGroupName,
		KeyspaceId: &rmpb.KeyspaceIDValue{
			Value: suite.keyspaceID,
		},
	}
	pdAddGetResp, err := pdClient.GetResourceGroup(ctx, addGetReq)
	re.NoError(err)
	re.Nil(pdAddGetResp.GetError())
	re.Equal(addGroupName, pdAddGetResp.GetGroup().GetName())

	rmAddGetResp, err := rmClient.GetResourceGroup(ctx, addGetReq)
	re.NoError(err)
	re.Nil(rmAddGetResp.GetError())
	re.Equal(addGroupName, rmAddGetResp.GetGroup().GetName())
	re.Equal(pdAddGetResp.GetGroup().GetPriority(), rmAddGetResp.GetGroup().GetPriority())
	re.Equal(pdAddGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetFillRate(), rmAddGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetFillRate())

	addGroup.Priority = 9
	addGroup.RUSettings.RU.Settings.FillRate = 800
	addGroup.RUSettings.RU.Settings.BurstLimit = 900
	modifyResp, err := pdClient.ModifyResourceGroup(ctx, &rmpb.PutResourceGroupRequest{Group: addGroup})
	re.NoError(err)
	re.Nil(modifyResp.GetError())
	re.Equal("Success!", modifyResp.GetBody())

	pdModifyGetResp, err := pdClient.GetResourceGroup(ctx, addGetReq)
	re.NoError(err)
	re.Nil(pdModifyGetResp.GetError())
	re.Equal(uint32(9), pdModifyGetResp.GetGroup().GetPriority())
	re.Equal(uint64(800), pdModifyGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetFillRate())
	re.Equal(int64(900), pdModifyGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetBurstLimit())

	rmModifyGetResp, err := rmClient.GetResourceGroup(ctx, addGetReq)
	re.NoError(err)
	re.Nil(rmModifyGetResp.GetError())
	re.Equal(pdModifyGetResp.GetGroup().GetPriority(), rmModifyGetResp.GetGroup().GetPriority())
	re.Equal(pdModifyGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetFillRate(), rmModifyGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetFillRate())
	re.Equal(pdModifyGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetBurstLimit(), rmModifyGetResp.GetGroup().GetRUSettings().GetRU().GetSettings().GetBurstLimit())

	deleteResp, err := pdClient.DeleteResourceGroup(ctx, &rmpb.DeleteResourceGroupRequest{
		ResourceGroupName: addGroupName,
		KeyspaceId: &rmpb.KeyspaceIDValue{
			Value: suite.keyspaceID,
		},
	})
	re.NoError(err)
	re.Nil(deleteResp.GetError())
	re.Equal("Success!", deleteResp.GetBody())

	_, err = rmClient.GetResourceGroup(ctx, addGetReq)
	re.ErrorContains(err, "resource group does not exist")
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

func (suite *resourceManagerRedirectorTestSuite) fetchResourceGroup(addr, groupName string, opts ...func([]byte, int, http.Header)) *server.ResourceGroup {
	re := suite.Require()
	querySuffix := fmt.Sprintf("?keyspace_name=%s", suite.keyspaceName)
	url := fmt.Sprintf("%s%sconfig/group/%s%s", addr, apis.APIPathPrefix, groupName, querySuffix)
	group := &server.ResourceGroup{}
	re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, url, group, opts...))
	return group
}
