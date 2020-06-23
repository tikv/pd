// Copyright 2020 PingCAP, Inc.
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
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/schedule/placement"
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
	PDServerCfg := s.svr.GetConfig().PDServerCfg
	PDServerCfg.KeyType = "raw"
	err := s.svr.SetPDServerConfig(PDServerCfg)
	c.Assert(err, IsNil)
	c.Assert(postJSON(testDialClient, s.urlPrefix, []byte(`{"enable-placement-rules":"true"}`)), IsNil)
	rule1 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	rule2 := placement.Rule{GroupID: "a", ID: "20", StartKeyHex: "3333", EndKeyHex: "5555", Role: "voter", Count: 2}
	rule3 := placement.Rule{GroupID: "b", ID: "20", StartKeyHex: "5555", EndKeyHex: "7777", Role: "voter", Count: 3}

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
	err = readJSON(testDialClient, s.urlPrefix+"/rule/a/10", &resp)
	c.Assert(err, IsNil)
	compareRule(c, &resp, &rule1)

	//GetAll
	var resp2 []*placement.Rule
	err = readJSON(testDialClient, s.urlPrefix+"/rules", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), Equals, 4)

	//GetAllByGroup
	err = readJSON(testDialClient, s.urlPrefix+"/rules/group/b", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), Equals, 1)
	compareRule(c, resp2[0], &rule3)

	//GetAllByRegion
	r := newTestRegionInfo(4, 1, []byte([]byte{0x22, 0x22}), []byte{0x33, 0x33})
	mustRegionHeartbeat(c, s.svr, r)
	err = readJSON(testDialClient, s.urlPrefix+"/rules/region/4", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), Equals, 2)
	if resp2[0].GroupID == "pd" {
		compareRule(c, resp2[1], &rule1)
	} else {
		compareRule(c, resp2[0], &rule1)
	}

	//GetAllByKey
	err = readJSON(testDialClient, s.urlPrefix+"/rules/key/4444", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), Equals, 2)
	if resp2[0].GroupID == "pd" {
		compareRule(c, resp2[1], &rule2)
	} else {
		compareRule(c, resp2[0], &rule2)
	}

	//Delete
	resp3, err := doDelete(testDialClient, s.urlPrefix+"/rule/a/10")
	c.Assert(err, IsNil)
	c.Assert(resp3.StatusCode, Equals, http.StatusOK)
}

func compareRule(c *C, r1 *placement.Rule, r2 *placement.Rule) {
	c.Assert(r1.GroupID, Equals, r2.GroupID)
	c.Assert(r1.ID, Equals, r2.ID)
	c.Assert(r1.StartKeyHex, Equals, r2.StartKeyHex)
	c.Assert(r1.EndKeyHex, Equals, r2.EndKeyHex)
	c.Assert(r1.Role, Equals, r2.Role)
	c.Assert(r1.Count, Equals, r2.Count)
}
