// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"net/http"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRuleSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/config", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testRuleSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRuleSuite) Testrule(c *C) {
	c.Assert(postJSON(testDialClient, s.urlPrefix, []byte(`{"enable-placement-rules":"true"}`)), IsNil)
	rule1 := map[string]interface{}{
		"group_id": "a",
		"id":       "10",
		"role":     "voter",
		"count":    1,
	}
	rule2 := map[string]interface{}{
		"group_id": "a",
		"id":       "20",
		"role":     "voter",
		"count":    2,
	}
	rule3 := map[string]interface{}{
		"group_id": "b",
		"id":       "20",
		"role":     "voter",
		"count":    3,
		"region":   "4",
		"key":      "123abc",
	}
	//Set
	postData, err := json.Marshal(rule1)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", postData)
	c.Assert(err, IsNil)
	postData, err = json.Marshal(rule2)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", postData)
	c.Assert(err, IsNil)
	postData, err = json.Marshal(rule3)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", postData)
	c.Assert(err, IsNil)

	//Get
	var resp placement.Rule
	err = readJSON(testDialClient, s.urlPrefix+"/rule/pd/default", &resp)
	c.Assert(err, IsNil)
	c.Assert(resp.Count, Equals, 3)

	//GetAll
	var resp2 []*placement.Rule
	err = readJSON(testDialClient, s.urlPrefix+"/rules", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), Equals, 4)

	//GetAllByGroup
	err = readJSON(testDialClient, s.urlPrefix+"/rules/group/a", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), Equals, 2)

	//GetAllByRegion
	r := newTestRegionInfo(4, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(c, s.svr, r)
	err = readJSON(testDialClient, s.urlPrefix+"/rules/region/4", &resp2)
	c.Assert(err, IsNil)

	//GetAllByKey
	err = readJSON(testDialClient, s.urlPrefix+"/rules/key/123abc", &resp2)
	c.Assert(err, IsNil)

	//Delete
	resp3, err := doDelete(testDialClient, s.urlPrefix+"/rule/a/10")
	c.Assert(err, IsNil)
	c.Assert(resp3.StatusCode, Equals, http.StatusOK)
}
