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
		"enable-audit":      "true",
		"enable-rate-limit": "true",
	}
	postData, err := json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc = &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableAudit, Equals, true)
	c.Assert(sc.EnableRateLimit, Equals, true)
	ms = map[string]interface{}{
		"audit.enable-audit": "false",
		"enable-rate-limit":  "false",
	}
	postData, err = json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc = &config.ServiceMiddlewareConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)
	c.Assert(sc.EnableAudit, Equals, false)
	c.Assert(sc.EnableRateLimit, Equals, false)

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

func (s *testRateLimitConfigSuite) TestUpdateRateLimitConfig(c *C) {
	urlPrefix := fmt.Sprintf("%s%s/api/v1/service-middleware/rate-limit/config", s.svr.GetAddr(), apiPrefix)

	// test empty type
	input := make(map[string]interface{})
	input["type"] = 123
	jsonBody, err := json.Marshal(input)
	c.Assert(err, IsNil)

	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "\"The type is empty.\"\n"))
	c.Assert(err, IsNil)
	// test invalid type
	input = make(map[string]interface{})
	input["type"] = "url"
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "\"The type is invalid.\"\n"))
	c.Assert(err, IsNil)

	// test empty label
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = ""
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "\"The label is empty.\"\n"))
	c.Assert(err, IsNil)
	// test no label matched
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = "TestLabel"
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "\"There is no label matched.\"\n"))
	c.Assert(err, IsNil)

	// test empty path
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = ""
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "\"The path is empty.\"\n"))
	c.Assert(err, IsNil)

	// test path but no label matched
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/test"
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(c, http.StatusBadRequest), tu.StringEqual(c, "\"There is no label matched.\"\n"))
	c.Assert(err, IsNil)

	// no change
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = "GetHealthStatus"
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(c), tu.StringEqual(c, "\"No changed.\"\n"))
	c.Assert(err, IsNil)

	// change concurrency
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = "GET"
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(c), tu.StringEqual(c, "Concurrency limiter is changed."))
	c.Assert(err, IsNil)
	input["concurrency"] = 0
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(c), tu.StringEqual(c, "Concurrency limiter is deleted."))
	c.Assert(err, IsNil)

	// change qps
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = "GET"
	input["qps"] = 100
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(c), tu.StringEqual(c, "QPS rate limiter is changed."))
	c.Assert(err, IsNil)

	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = "GET"
	input["qps"] = 0.3
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(c), tu.StringEqual(c, "QPS rate limiter is changed."))
	c.Assert(err, IsNil)
	c.Assert(s.svr.GetRateLimitConfig().LimiterConfig["GetHealthStatus"].QPSBurst, Equals, 1)

	input["qps"] = -1
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(c), tu.StringEqual(c, "QPS rate limiter is deleted."))
	c.Assert(err, IsNil)

	// change both
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/debug/pprof/profile"
	input["qps"] = 100
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(c), tu.StringEqual(c, "\"Concurrency limiter is changed. QPS rate limiter is changed.\"\n"))
	c.Assert(err, IsNil)

	limiter := s.svr.GetServiceRateLimiter()
	limiter.Update("SetRatelimitConfig", ratelimit.AddLabelAllowList())

	// Allow list
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = "SetRatelimitConfig"
	input["qps"] = 100
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusNotOK(c), tu.StringEqual(c, "\"This service is in allow list whose config can not be changed.\"\n"))
	c.Assert(err, IsNil)
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
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(c, http.StatusInternalServerError), tu.StringEqual(c, "config item rate-limit not found")), IsNil)

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
