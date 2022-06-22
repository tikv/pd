// Copyright 2022 TiKV Project Authors.
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

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

type configPersistTestSuite struct {
	suite.Suite

	cleanup func()
	cluster *tests.TestCluster
}

func TestConfigPersistTestSuite(t *testing.T) {
	suite.Run(t, new(configPersistTestSuite))
}

func (suite *configPersistTestSuite) SetupSuite() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3)
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.cluster = cluster
}

func (suite *configPersistTestSuite) TearDownSuite() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *configPersistTestSuite) TestRateLimitConfigReload() {
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())

	suite.Len(leader.GetServer().GetServiceMiddlewareConfig().RateLimitConfig.LimiterConfig, 0)
	limitCfg := make(map[string]ratelimit.DimensionConfig)
	limitCfg["GetRegions"] = ratelimit.DimensionConfig{QPS: 1}

	input := map[string]interface{}{
		"enable-rate-limit": "true",
		"limiter-config":    limitCfg,
	}
	data, err := json.Marshal(input)
	suite.NoError(err)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.Equal(true, leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	suite.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)

	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	suite.mustWaitLeader(suite.cluster.GetServers())
	leader = suite.cluster.GetServer(suite.cluster.GetLeader())

	suite.Equal(true, leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	suite.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)
}

func (suite *configPersistTestSuite) mustWaitLeader(svrs map[string]*tests.TestServer) *server.Server {
	var leader *server.Server
	re := suite.Require()
	testutil.Eventually(re, func() bool {
		for _, s := range svrs {
			if !s.GetServer().IsClosed() && s.GetServer().GetMember().IsLeader() {
				leader = s.GetServer()
				return true
			}
		}
		return false
	})
	return leader
}
