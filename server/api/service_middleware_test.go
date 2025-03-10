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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/ratelimit"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type auditMiddlewareTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestAuditMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, new(auditMiddlewareTestSuite))
}

func (suite *auditMiddlewareTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) {
		cfg.Replication.EnablePlacementRules = false
	})
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (suite *auditMiddlewareTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *auditMiddlewareTestSuite) TestConfigAuditSwitch() {
	re := suite.Require()
	addr := fmt.Sprintf("%s/service-middleware/config", suite.urlPrefix)
	sc := &config.ServiceMiddlewareConfig{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	re.True(sc.EnableAudit)

	ms := map[string]any{
		"audit.enable-audit": "false",
	}
	postData, err := json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	re.False(sc.EnableAudit)
	ms = map[string]any{
		"enable-audit": "true",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	re.True(sc.EnableAudit)

	// test empty
	ms = map[string]any{}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re), tu.StringContain(re, "The input is empty.")))
	ms = map[string]any{
		"audit": "false",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item audit not found")))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail", "return(true)"))
	ms = map[string]any{
		"audit.enable-audit": "false",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest)))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail"))
	ms = map[string]any{
		"audit.audit": "false",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item audit not found")))
}

type rateLimitConfigTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestRateLimitConfigTestSuite(t *testing.T) {
	suite.Run(t, new(rateLimitConfigTestSuite))
}

func (suite *rateLimitConfigTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})
	mustBootstrapCluster(re, suite.svr)
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", suite.svr.GetAddr(), apiPrefix)
}

func (suite *rateLimitConfigTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *rateLimitConfigTestSuite) TestUpdateRateLimitConfig() {
	re := suite.Require()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/service-middleware/config/rate-limit", suite.svr.GetAddr(), apiPrefix)

	// test empty type
	input := make(map[string]any)
	input["type"] = 123
	jsonBody, err := json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "The type is empty."))
	re.NoError(err)
	// test invalid type
	input = make(map[string]any)
	input["type"] = "url"
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "The type is invalid."))
	re.NoError(err)

	// test empty label
	input = make(map[string]any)
	input["type"] = "label"
	input["label"] = ""
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "The label is empty."))
	re.NoError(err)
	// test no label matched
	input = make(map[string]any)
	input["type"] = "label"
	input["label"] = "TestLabel"
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "There is no label matched."))
	re.NoError(err)

	// test empty path
	input = make(map[string]any)
	input["type"] = "path"
	input["path"] = ""
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "The path is empty."))
	re.NoError(err)

	// test path but no label matched
	input = make(map[string]any)
	input["type"] = "path"
	input["path"] = "/pd/api/v1/test"
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "There is no label matched."))
	re.NoError(err)

	// no change
	input = make(map[string]any)
	input["type"] = "label"
	input["label"] = "GetHealthStatus"
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringEqual(re, "Rate limiter is not changed."))
	re.NoError(err)

	// change concurrency
	input = make(map[string]any)
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = http.MethodGet
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Rate limiter is updated"))
	re.NoError(err)
	input["concurrency"] = 0
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Rate limiter is deleted"))
	re.NoError(err)

	// change qps
	input = make(map[string]any)
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = http.MethodGet
	input["qps"] = 100
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Rate limiter is updated."))
	re.NoError(err)

	input = make(map[string]any)
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = http.MethodGet
	input["qps"] = 0.3
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Rate limiter is updated."))
	re.NoError(err)
	re.Equal(1, suite.svr.GetRateLimitConfig().LimiterConfig["GetHealthStatus"].QPSBurst)

	input["qps"] = -1
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Rate limiter is deleted."))
	re.NoError(err)

	// change both
	input = make(map[string]any)
	input["type"] = "path"
	input["path"] = "/pd/api/v1/debug/pprof/profile"
	input["qps"] = 100
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Rate limiter is updated."))
	re.NoError(err)

	limiter := suite.svr.GetServiceRateLimiter()
	limiter.Update("SetRateLimitConfig", ratelimit.AddLabelAllowList())

	// Allow list
	input = make(map[string]any)
	input["type"] = "label"
	input["label"] = "SetRateLimitConfig"
	input["qps"] = 100
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusNotOK(re), tu.StringEqual(re, "This service is in allow list whose config can not be changed."))
	re.NoError(err)
}

func (suite *rateLimitConfigTestSuite) TestUpdateGRPCRateLimitConfig() {
	urlPrefix := fmt.Sprintf("%s%s/api/v1/service-middleware/config/grpc-rate-limit", suite.svr.GetAddr(), apiPrefix)
	re := suite.Require()

	// test empty label
	input := make(map[string]any)
	input["label"] = ""
	jsonBody, err := json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "The label is empty."))
	re.NoError(err)
	// test no label matched
	input = make(map[string]any)
	input["label"] = "TestLabel"
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "There is no label matched."))
	re.NoError(err)

	// no change
	input = make(map[string]any)
	input["label"] = "StoreHeartbeat"
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringEqual(re, "gRPC limiter is not changed."))
	re.NoError(err)

	// change concurrency
	input = make(map[string]any)
	input["label"] = "StoreHeartbeat"
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "gRPC limiter is updated."))
	re.NoError(err)
	input["concurrency"] = 0
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "gRPC limiter is deleted."))
	re.NoError(err)

	// change qps
	input = make(map[string]any)
	input["label"] = "StoreHeartbeat"
	input["qps"] = 100
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "gRPC limiter is updated."))
	re.NoError(err)

	input = make(map[string]any)
	input["label"] = "StoreHeartbeat"
	input["qps"] = 0.3
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "gRPC limiter is updated."))
	re.NoError(err)
	re.Equal(1, suite.svr.GetGRPCRateLimitConfig().LimiterConfig["StoreHeartbeat"].QPSBurst)

	input["qps"] = -1
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "gRPC limiter is deleted."))
	re.NoError(err)

	// change both
	input = make(map[string]any)
	input["label"] = "GetStore"
	input["qps"] = 100
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re),
	)
	re.NoError(err)
}

func (suite *rateLimitConfigTestSuite) TestConfigRateLimitSwitch() {
	addr := fmt.Sprintf("%s/service-middleware/config", suite.urlPrefix)
	sc := &config.ServiceMiddlewareConfig{}
	re := suite.Require()
	re.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	re.True(sc.RateLimitConfig.EnableRateLimit)
	re.True(sc.GRPCRateLimitConfig.EnableRateLimit)

	ms := map[string]any{
		"enable-rate-limit":      "false",
		"enable-grpc-rate-limit": "false",
	}
	postData, err := json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	re.False(sc.RateLimitConfig.EnableRateLimit)
	re.False(sc.GRPCRateLimitConfig.EnableRateLimit)
	ms = map[string]any{
		"enable-rate-limit":      "true",
		"enable-grpc-rate-limit": "true",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	re.True(sc.RateLimitConfig.EnableRateLimit)
	re.True(sc.GRPCRateLimitConfig.EnableRateLimit)

	// test empty
	ms = map[string]any{}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re), tu.StringContain(re, "The input is empty.")))
	ms = map[string]any{
		"rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item rate-limit not found")))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail", "return(true)"))
	ms = map[string]any{
		"rate-limit.enable-rate-limit":           "false",
		"grpc-rate-limit.enable-grpc-rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest)))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail"))
	ms = map[string]any{
		"rate-limit.rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item rate-limit not found")))
}

func (suite *rateLimitConfigTestSuite) TestConfigLimiterConfigByOriginAPI() {
	re := suite.Require()
	// this test case is used to test updating `limiter-config` by origin API simply
	addr := fmt.Sprintf("%s/service-middleware/config", suite.urlPrefix)
	dimensionConfig := ratelimit.DimensionConfig{QPS: 1}
	limiterConfig := map[string]any{
		"CreateOperator": dimensionConfig,
	}
	ms := map[string]any{
		"limiter-config": limiterConfig,
	}
	postData, err := json.Marshal(ms)
	re.NoError(err)
	re.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc := &config.ServiceMiddlewareConfig{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	re.Equal(1., sc.RateLimitConfig.LimiterConfig["CreateOperator"].QPS)
}
