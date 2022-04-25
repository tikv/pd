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

	. "github.com/pingcap/check"
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

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testConfigPresistSuite{})

type testConfigPresistSuite struct {
	cleanup func()
	cluster *tests.TestCluster
}

func (s *testConfigPresistSuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3)
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
	s.cluster = cluster
}

func (s *testConfigPresistSuite) TearDownSuite(c *C) {
	s.cleanup()
	s.cluster.Destroy()
}

func (s *testConfigPresistSuite) TestRateLimitConfigReload(c *C) {
	leader := s.cluster.GetServer(s.cluster.GetLeader())

	c.Assert(leader.GetServer().GetConfig().PDServerCfg.RateLimitConfig, HasLen, 0)
	limitCfg := ratelimit.NewLimiterConfig()
	limitCfg["GetRegions"] = ratelimit.DimensionConfig{QPS: 1}

	input := map[string]interface{}{
		"enable-rate-limit": "true",
		"rate-limit-config": limitCfg,
	}
	data, err := json.Marshal(input)
	c.Assert(err, IsNil)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(leader.GetServer().GetPersistOptions().IsRateLimitEnabled(), Equals, true)
	c.Assert(leader.GetServer().GetConfig().PDServerCfg.RateLimitConfig, HasLen, 1)

	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	mustWaitLeader(c, s.cluster.GetServers())
	leader = s.cluster.GetServer(s.cluster.GetLeader())

	c.Assert(leader.GetServer().GetPersistOptions().IsRateLimitEnabled(), Equals, true)
	c.Assert(leader.GetServer().GetConfig().PDServerCfg.RateLimitConfig, HasLen, 1)
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
