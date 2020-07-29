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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

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
	PDServerCfg := s.svr.GetConfig().PDServerCfg
	PDServerCfg.KeyType = "raw"
	err := s.svr.SetPDServerConfig(PDServerCfg)
	c.Assert(err, IsNil)
	c.Assert(postJSON(testDialClient, s.urlPrefix, []byte(`{"enable-placement-rules":"true"}`)), IsNil)
}

func (s *testRuleSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRuleSuite) TestSet(c *C) {
	rule := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	successData, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	c.Assert(err, IsNil)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	c.Assert(err, IsNil)
	parseErrData := []byte("foo")
	rule1 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: "voter", Count: 1}
	checkErrData, err := json.Marshal(rule1)
	c.Assert(err, IsNil)
	rule2 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: -1}
	setErrData, err := json.Marshal(rule2)
	c.Assert(err, IsNil)
	rule3 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "follower", Count: 3}
	updateData, err := json.Marshal(rule3)
	c.Assert(err, IsNil)
	newStartKey, err := hex.DecodeString(rule.StartKeyHex)
	c.Assert(err, IsNil)
	newEndKey, err := hex.DecodeString(rule.EndKeyHex)
	c.Assert(err, IsNil)

	testcases := []struct {
		name        string
		rawData     []byte
		success     bool
		response    string
		popKeyRange map[string]struct{}
	}{
		{
			name:     "Set a new rule success",
			rawData:  successData,
			success:  true,
			response: "",
			popKeyRange: map[string]struct{}{
				hex.EncodeToString(oldStartKey): {},
				hex.EncodeToString(oldEndKey):   {},
			},
		},
		{
			name:     "Update an existed rule success",
			rawData:  updateData,
			success:  true,
			response: "",
			popKeyRange: map[string]struct{}{
				hex.EncodeToString(oldStartKey): {},
				hex.EncodeToString(oldEndKey):   {},
				hex.EncodeToString(newStartKey): {},
				hex.EncodeToString(newEndKey):   {},
			},
		},
		{
			name:    "Parse Json failed",
			rawData: parseErrData,
			success: false,
			response: `{
  "code": "input",
  "msg": "invalid character 'o' in literal false (expecting 'a')",
  "data": {
    "Offset": 2
  }
}
`,
		},
		{
			name:    "Check rule failed",
			rawData: checkErrData,
			success: false,
			response: `"start key is not in hex format: encoding/hex: invalid byte: U+0058 'X'"
`,
		},
		{
			name:    "Set Rule Failed",
			rawData: setErrData,
			success: false,
			response: `"invalid count -1"
`,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		// clear suspect keyRanges to prevent test case from others
		s.svr.GetRaftCluster().ClearSuspectKeyRanges()
		err = postJSON(testDialClient, s.urlPrefix+"/rule", testcase.rawData)
		if testcase.success {
			c.Assert(err, IsNil)

			popKeyRangeMap := map[string]struct{}{}
			for i := 0; i < len(testcase.popKeyRange)/2; i++ {
				_, v, got := s.svr.GetRaftCluster().PopOneSuspectKeyRange()
				c.Assert(got, Equals, true)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			c.Assert(len(popKeyRangeMap), Equals, len(testcase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testcase.popKeyRange[k]
				c.Assert(ok, Equals, true)
			}

		} else {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, testcase.response)
		}
	}
}

func (s *testRuleSuite) TestGet(c *C) {
	rule := placement.Rule{GroupID: "a", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", data)
	c.Assert(err, IsNil)

	testcases := []struct {
		name  string
		rule  placement.Rule
		found bool
		code  string
	}{
		{
			name:  "found",
			rule:  rule,
			found: true,
			code:  "",
		},
		{
			name:  "not found",
			rule:  placement.Rule{GroupID: "a", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
			found: false,
			code:  "404",
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp placement.Rule
		url := fmt.Sprintf("%s/rule/%s/%s", s.urlPrefix, testcase.rule.GroupID, testcase.rule.ID)
		err = readJSON(testDialClient, url, &resp)
		if testcase.found {
			c.Assert(err, IsNil)
			compareRule(c, &resp, &testcase.rule)
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.HasSuffix(err.Error(), testcase.code), Equals, true)
		}
	}
}

func (s *testRuleSuite) TestGetAll(c *C) {
	rule := placement.Rule{GroupID: "b", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", data)
	c.Assert(err, IsNil)

	var resp2 []*placement.Rule
	err = readJSON(testDialClient, s.urlPrefix+"/rules", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), GreaterEqual, 1)
}

func (s *testRuleSuite) TestGetAllByGroup(c *C) {
	rule := placement.Rule{GroupID: "c", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", data)
	c.Assert(err, IsNil)

	rule1 := placement.Rule{GroupID: "c", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err = json.Marshal(rule1)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", data)
	c.Assert(err, IsNil)

	testcases := []struct {
		name    string
		groupID string
		count   int
	}{
		{
			name:    "found group c",
			groupID: "c",
			count:   2,
		},
		{
			name:    "not found d",
			groupID: "d",
			count:   0,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/group/%s", s.urlPrefix, testcase.groupID)
		err = readJSON(testDialClient, url, &resp)
		c.Assert(err, IsNil)
		c.Assert(len(resp), Equals, testcase.count)
		if testcase.count == 2 {
			compareRule(c, resp[0], &rule)
			compareRule(c, resp[1], &rule1)
		}
	}
}

func (s *testRuleSuite) TestGetAllByRegion(c *C) {
	rule := placement.Rule{GroupID: "e", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", data)
	c.Assert(err, IsNil)

	r := newTestRegionInfo(4, 1, []byte([]byte{0x22, 0x22}), []byte{0x33, 0x33})
	mustRegionHeartbeat(c, s.svr, r)

	testcases := []struct {
		name     string
		regionID string
		success  bool
		code     string
	}{
		{
			name:     "found region",
			regionID: "4",
			success:  true,
		},
		{
			name:     "parse regionId failed",
			regionID: "abc",
			success:  false,
			code:     "400",
		},
		{
			name:     "region not found",
			regionID: "5",
			success:  false,
			code:     "404",
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/region/%s", s.urlPrefix, testcase.regionID)
		err = readJSON(testDialClient, url, &resp)
		if testcase.success {
			c.Assert(err, IsNil)
			for _, r := range resp {
				if r.GroupID == "e" {
					compareRule(c, r, &rule)
				}
			}
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.HasSuffix(err.Error(), testcase.code), Equals, true)
		}
	}
}

func (s *testRuleSuite) TestGetAllByKey(c *C) {
	rule := placement.Rule{GroupID: "f", ID: "40", StartKeyHex: "8888", EndKeyHex: "9111", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", data)
	c.Assert(err, IsNil)

	testcases := []struct {
		name     string
		key      string
		success  bool
		respSize int
		code     string
	}{
		{
			name:     "key in range",
			key:      "8899",
			success:  true,
			respSize: 2,
		},
		{
			name:     "parse key failed",
			key:      "abc",
			success:  false,
			code:     "400",
			respSize: 0,
		},
		{
			name:     "key out of range",
			key:      "9999",
			success:  true,
			respSize: 1,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/key/%s", s.urlPrefix, testcase.key)
		err = readJSON(testDialClient, url, &resp)
		if testcase.success {
			c.Assert(err, IsNil)
			c.Assert(len(resp), Equals, testcase.respSize)
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.HasSuffix(err.Error(), testcase.code), Equals, true)
		}
	}
}

func (s *testRuleSuite) TestDelete(c *C) {
	rule := placement.Rule{GroupID: "g", ID: "10", StartKeyHex: "8888", EndKeyHex: "9111", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/rule", data)
	c.Assert(err, IsNil)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	c.Assert(err, IsNil)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	c.Assert(err, IsNil)

	testcases := []struct {
		name        string
		groupID     string
		id          string
		popKeyRange map[string]struct{}
	}{
		{
			name:    "delete existed rule",
			groupID: "g",
			id:      "10",
			popKeyRange: map[string]struct{}{
				hex.EncodeToString(oldStartKey): {},
				hex.EncodeToString(oldEndKey):   {},
			},
		},
		{
			name:        "delete non-existed rule",
			groupID:     "g",
			id:          "15",
			popKeyRange: map[string]struct{}{},
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		url := fmt.Sprintf("%s/rule/%s/%s", s.urlPrefix, testcase.groupID, testcase.id)
		// clear suspect keyRanges to prevent test case from others
		s.svr.GetRaftCluster().ClearSuspectKeyRanges()
		resp, err := doDelete(testDialClient, url)
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		if len(testcase.popKeyRange) > 0 {
			popKeyRangeMap := map[string]struct{}{}
			for i := 0; i < len(testcase.popKeyRange)/2; i++ {
				_, v, got := s.svr.GetRaftCluster().PopOneSuspectKeyRange()
				c.Assert(got, Equals, true)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			c.Assert(len(popKeyRangeMap), Equals, len(testcase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testcase.popKeyRange[k]
				c.Assert(ok, Equals, true)
			}
		}
	}
}

func compareRule(c *C, r1 *placement.Rule, r2 *placement.Rule) {
	c.Assert(r1.GroupID, Equals, r2.GroupID)
	c.Assert(r1.ID, Equals, r2.ID)
	c.Assert(r1.StartKeyHex, Equals, r2.StartKeyHex)
	c.Assert(r1.EndKeyHex, Equals, r2.EndKeyHex)
	c.Assert(r1.Role, Equals, r2.Role)
	c.Assert(r1.Count, Equals, r2.Count)
}
