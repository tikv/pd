// Copyright 2024 TiKV Project Authors.
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

package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

type limitTestSuite struct {
	suite.Suite
	cluster   *tests.TestCluster
	client    pd.Client
	rawClient pdpb.PDClient
	ctx       context.Context
	cleanup   context.CancelFunc
}

func TestLimitTestSuite(t *testing.T) {
	suite.Run(t, new(limitTestSuite))
}

func (suite *limitTestSuite) SetupSuite() {
	re := suite.Require()
	suite.ctx, suite.cleanup = context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.cluster = cluster

	leader := suite.cluster.GetLeaderServer()
	cc, err := grpcutil.GetClientConn(suite.ctx, leader.GetAddr(), nil)
	re.NoError(err)
	suite.rawClient = pdpb.NewPDClient(cc)
	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	suite.client = setupCli(re, suite.ctx, leader.GetServer().GetEndpoints())

	suite.bootstrapServer(re, newHeader(leader.GetServer()), grpcPDClient)
}

func (suite *limitTestSuite) bootstrapServer(re *require.Assertions, header *pdpb.RequestHeader, client pdpb.PDClient) {
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers[:1],
	}
	req := &pdpb.BootstrapRequest{
		Header: header,
		Store:  stores[0],
		Region: region,
	}
	resp, err := client.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())

	regionID = regionIDAllocator.alloc()
	region = &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	regionReq := &pdpb.RegionHeartbeatRequest{
		Header: suite.getHeader(),
		Region: region,
		Leader: peers[0],
	}
	regionHeartbeat, err := client.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	err = regionHeartbeat.Send(regionReq)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"))
		re.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader) &&
			r.Buckets == nil
	})
}

func (suite *limitTestSuite) TearDownSuite() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *limitTestSuite) getLeader() *server.Server {
	return suite.cluster.GetLeaderServer().GetServer()
}

func (suite *limitTestSuite) getHeader() *pdpb.RequestHeader {
	return newHeader(suite.getLeader())
}

func (suite *limitTestSuite) TestLimitStoreHeartbeart() {
	re := suite.Require()
	input := map[string]any{
		"enable-grpc-rate-limit": "true",
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	httpReq, _ := http.NewRequest(http.MethodPost, suite.getLeader().GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	resp.Body.Close()
	re.True(suite.getLeader().GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled())
	input = make(map[string]any)
	input["label"] = "StoreHeartbeat"
	input["bbr"] = true
	jsonBody, err := json.Marshal(input)
	re.NoError(err)
	httpReq, _ = http.NewRequest(http.MethodPost, suite.getLeader().GetAddr()+"/pd/api/v1/service-middleware/config/grpc-rate-limit", bytes.NewBuffer(jsonBody))
	resp, err = dialClient.Do(httpReq)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	content, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	re.NoError(err)
	r := &rateLimitResult{}
	err = json.Unmarshal(content, r)
	re.NoError(err)
	re.Equal("BBR option is enabled.", r.BBRUpdateFlag)
	re.True(suite.getLeader().GetServiceMiddlewarePersistOptions().GetGRPCRateLimitConfig().LimiterConfig["StoreHeartbeat"].EnableBBR)

	in := &pdpb.StoreHeartbeatRequest{
		Header: suite.getHeader(),
		Stats: &pdpb.StoreStats{
			StoreId: stores[0].GetId(),
		},
	}
	res, err := suite.rawClient.StoreHeartbeat(suite.ctx, in)
	re.NoError(err)
	re.Nil(res.Header.Error)

	var wg sync.WaitGroup
	success := int32(0)
	fail := int32(0)
	for i := 0; i < 50; i++ {
		time.Sleep(250 * time.Millisecond)
		wg.Add(1)
		go func() {
			res, err := suite.rawClient.StoreHeartbeat(suite.ctx, in)
			if err == nil && res.Header.Error != nil {
				atomic.AddInt32(&fail, 1)
			} else {
				atomic.AddInt32(&success, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	re.Equal(int32(50), success)

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/slowHeartbeat", `return()`))
	var breakFlag atomic.Bool

	for i := 0; i < 50 && !breakFlag.Load(); i++ {
		time.Sleep(250 * time.Millisecond)
		wg.Add(1)
		go func() {
			res, err := suite.rawClient.StoreHeartbeat(suite.ctx, in)
			if err == nil && res.Header.Error != nil {
				breakFlag.Store(true)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	re.True(breakFlag.Load())
	success = int32(0)
	fail = int32(0)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			res, err := suite.rawClient.StoreHeartbeat(suite.ctx, in)
			if err == nil && res.Header.Error != nil {
				atomic.AddInt32(&fail, 1)
			} else {
				atomic.AddInt32(&success, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	re.Less(success, int32(15))
	re.Greater(success, int32(5))
	re.Less(fail, int32(15))
	re.Greater(fail, int32(5))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/slowHeartbeat"))
}

type rateLimitResult struct {
	ConcurrencyUpdatedFlag string                               `json:"concurrency"`
	QPSRateUpdatedFlag     string                               `json:"qps"`
	BBRUpdateFlag          string                               `json:"BBR"`
	LimiterConfig          map[string]ratelimit.DimensionConfig `json:"limiter-config"`
}
