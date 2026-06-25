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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/resource_group/controller"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/tests"
)

const testKeyspaceName = "test_keyspace"

type serviceLimitTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     func()
	cluster    *tests.TestCluster
	leader     *tests.TestServer
	client     pd.Client
	controller *controller.ResourceGroupsController
	keyspaceID uint32
}

func TestServiceLimitTestSuite(t *testing.T) {
	suite.Run(t, new(serviceLimitTestSuite))
}

func (suite *serviceLimitTestSuite) SetupTest() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)
	suite.cluster = cluster
	re.NoError(cluster.RunInitialServers())
	suite.leader = suite.cluster.GetServer(suite.cluster.WaitLeader())
	re.NotNil(suite.leader)
	// Create a keyspace.
	meta, err := suite.leader.GetKeyspaceManager().CreateKeyspace(
		&keyspace.CreateKeyspaceRequest{
			Name: testKeyspaceName,
		},
	)
	re.NoError(err)
	suite.keyspaceID = meta.GetId()
	// Prepare the client and controller.
	suite.client, err = pd.NewClientWithContext(
		suite.ctx,
		caller.TestComponent,
		suite.cluster.GetConfig().GetClientURLs(),
		pd.SecurityOption{})
	re.NoError(err)
	waitLeaderServingClient(re, suite.client, suite.leader.GetAddr())
	suite.controller, err = controller.NewResourceGroupController(suite.ctx, 1, suite.client, nil, suite.keyspaceID)
	re.NoError(err)
	suite.controller.Start(suite.ctx)
}

func (suite *serviceLimitTestSuite) TearDownTest() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *serviceLimitTestSuite) TestKeyspaceServiceLimit() {
	re := suite.Require()

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	const requestRU = 200.
	tokenBucketRequest := &rmpb.TokenBucketsRequest{
		Requests: []*rmpb.TokenBucketRequest{
			{
				ResourceGroupName: server.DefaultResourceGroupName,
				Request: &rmpb.TokenBucketRequest_RuItems{
					RuItems: &rmpb.TokenBucketRequest_RequestRU{
						RequestRU: []*rmpb.RequestUnitItem{
							{
								Type:  rmpb.RequestUnitType_RU,
								Value: requestRU,
							},
						},
					},
				},
				ConsumptionSinceLastRequest: &rmpb.Consumption{
					RRU: requestRU,
				},
				KeyspaceId: &rmpb.KeyspaceIDValue{
					Value: suite.keyspaceID,
				},
			},
		},
		TargetRequestPeriodMs: 1000,
		ClientUniqueId:        1,
	}
	// Acquire token buckets before setting the service limit.
	token := suite.acquireTokenBuckets(ctx, re, tokenBucketRequest)
	re.Equal(requestRU, token.GetTokens())
	re.Equal(int64(server.UnlimitedBurstLimit), token.GetSettings().GetBurstLimit())
	// Set the service limit to a smaller value.
	serviceLimit := requestRU / 2
	suite.setAndGetServiceLimit(re, serviceLimit)
	// Acquire token buckets after setting the service limit.
	time.Sleep(time.Second)
	token = suite.acquireTokenBuckets(ctx, re, tokenBucketRequest)
	// Due to the service limit has a 5-second burst limit, the returned tokens might be slightly exceed the service limit..
	re.InDelta(serviceLimit, token.GetTokens(), serviceLimit*0.1)
	re.Equal(int64(serviceLimit), token.GetSettings().GetBurstLimit())
	// Set the service limit to a larger value.
	serviceLimit = requestRU * 2
	suite.setAndGetServiceLimit(re, serviceLimit)
	// Acquire token buckets after setting the service limit.
	time.Sleep(time.Second)
	token = suite.acquireTokenBuckets(ctx, re, tokenBucketRequest)
	re.Equal(requestRU, token.GetTokens())
	re.Equal(int64(serviceLimit), token.GetSettings().GetBurstLimit())
}

func (suite *serviceLimitTestSuite) setAndGetServiceLimit(re *require.Assertions, serviceLimit float64) {
	resp, statusCode := tryToSetKeyspaceServiceLimit(re, suite.leader.GetAddr(), testKeyspaceName, serviceLimit)
	re.Equal(http.StatusOK, statusCode)
	re.Equal("Success!", resp)
	// Get the service limit.
	limit, statusCode := tryToGetKeyspaceServiceLimit(re, suite.leader.GetAddr(), testKeyspaceName)
	re.Equal(serviceLimit, limit)
	re.Equal(http.StatusOK, statusCode)
}

func (suite *serviceLimitTestSuite) acquireTokenBuckets(ctx context.Context, re *require.Assertions, tokenBucketRequest *rmpb.TokenBucketsRequest) *rmpb.TokenBucket {
	resps, err := suite.client.AcquireTokenBuckets(ctx, tokenBucketRequest)
	re.NoError(err)
	re.Len(resps, 1)
	resp := resps[0]
	re.Equal(server.DefaultResourceGroupName, resp.ResourceGroupName)
	grantedTokens := resp.GetGrantedRUTokens()
	re.Len(grantedTokens, 1)
	return grantedTokens[0].GetGrantedTokens()
}
