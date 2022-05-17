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
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testServiceMiddlewareSuite{})

type testServiceMiddlewareSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testServiceMiddlewareSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c, func(cfg *config.Config) {
		cfg.Replication.EnablePlacementRules = false
	})
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testServiceMiddlewareSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testServiceMiddlewareSuite) TestConfigAudit(c *C) {
	addr := fmt.Sprintf("%s/service-middleware/config", s.urlPrefix)
	ms := map[string]interface{}{
		"enable-audit": "true",
	}
	postData, err := json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c)), IsNil)
	sc := &config.ServiceMiddlewareConfig{}
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
