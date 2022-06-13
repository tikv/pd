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

package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/ratelimit"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testAuditMiddlewareSuite{})

type testAuditMiddlewareSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testAuditMiddlewareSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c, func(cfg *config.Config) {
		cfg.Replication.EnablePlacementRules = false
	})
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testAuditMiddlewareSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testAuditMiddlewareSuite) TestConfigAuditSwitch(c *C) {
	addr := fmt.Sprintf("%s/service-middleware/config", s.urlPrefix)

	sc := &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableAudit, Equals, false)

	ms := map[string]interface{}{
		"enable-audit": "true",
	}
	postData, err := json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc = &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableAudit, Equals, true)
	ms = map[string]interface{}{
		"audit.enable-audit": "false",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc = &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableAudit, Equals, false)

	// test empty
	ms = map[string]interface{}{}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c), tu.StringContain(c, "The input is empty.")), IsNil)

	ms = map[string]interface{}{
		"audit": "false",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "config item audit not found")), IsNil)

	c.Assert(failpoint.Enable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail", "return(true)"), IsNil)
	ms = map[string]interface{}{
		"audit.enable-audit": "true",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(c, http.StatusBadRequest)), IsNil)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail"), IsNil)

	ms = map[string]interface{}{
		"audit.audit": "false",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "config item audit not found")), IsNil)
}

var _ = Suite(&testRateLimitConfigSuite{})

type testRateLimitConfigSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRateLimitConfigSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})
	mustBootstrapCluster(c, s.svr)
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", s.svr.GetAddr(), apiPrefix)
}

func (s *testRateLimitConfigSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRateLimitConfigSuite) TestConfigRateLimitSwitch(c *C) {
	addr := fmt.Sprintf("%s/service-middleware/config", s.urlPrefix)

	sc := &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableRateLimit, Equals, false)

	ms := map[string]interface{}{
		"enable-rate-limit": "true",
	}
	postData, err := json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc = &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableRateLimit, Equals, true)
	ms = map[string]interface{}{
		"enable-rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc = &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableRateLimit, Equals, false)

	// test empty
	ms = map[string]interface{}{}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c), tu.StringContain(c, "The input is empty.")), IsNil)

	ms = map[string]interface{}{
		"rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "config item rate-limit not found")), IsNil)

	c.Assert(failpoint.Enable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail", "return(true)"), IsNil)
	ms = map[string]interface{}{
		"rate-limit.enable-rate-limit": "true",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(c, http.StatusBadRequest)), IsNil)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail"), IsNil)

	ms = map[string]interface{}{
		"rate-limit.rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "config item rate-limit not found")), IsNil)
}

func (s *testRateLimitConfigSuite) TestConfigLimiterConifgByOriginAPI(c *C) {
	// this test case is used to test updating `limiter-config` by origin API simply
	addr := fmt.Sprintf("%s/service-middleware/config", s.urlPrefix)
	dimensionConfig := ratelimit.DimensionConfig{QPS: 1}
	limiterConfig := map[string]interface{}{
		"CreateOperator": dimensionConfig,
	}
	ms := map[string]interface{}{
		"limiter-config": limiterConfig,
	}
	postData, err := json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc := &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.RateLimitConfig.LimiterConfig["CreateOperator"].QPS, Equals, 1.)
}
