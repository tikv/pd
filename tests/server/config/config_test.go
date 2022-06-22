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



type testConfigPresistSuite struct {
	cleanup func()
	cluster *tests.TestCluster
}

func SetUpSuite(t *testing.T) {
    re := require.New(t)
    ctx, cancel := context.WithCancel(context.Background())
	s.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RunInitialServers())
re.Nil(cluster.RunInitialServers())

	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
	s.cluster = cluster
}

func TearDownSuite(t *testing.T) {
    re := require.New(t)
    s.cleanup()
	s.cluster.Destroy()
}

func TestRateLimitConfigReload(t *testing.T) {
    re := require.New(t)
    leader := s.cluster.GetServer(s.cluster.GetLeader())

	re.Len(leader.GetServer().GetServiceMiddlewareConfig().RateLimitConfig.LimiterConfig, 0)
	limitCfg := make(map[string]ratelimit.DimensionConfig)
	limitCfg["GetRegions"] = ratelimit.DimensionConfig{QPS: 1}

	input := map[string]interface{}{
		"enable-rate-limit": "true",
		"limiter-config":    limitCfg,
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.Equal(true, leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)

	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	mustWaitLeader(c, s.cluster.GetServers())
	leader = s.cluster.GetServer(s.cluster.GetLeader())

	re.Equal(true, leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)
}

func mustWaitLeader(c *C, svrs map[string]*tests.TestServer) *server.Server {
	var leader *server.Server
	testutil.WaitUntil(c, func() bool {
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
