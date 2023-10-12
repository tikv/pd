// Copyright 2020 TiKV Project Authors.
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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/placement"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type ruleTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestRuleTestSuite(t *testing.T) {
	suite.Run(t, new(ruleTestSuite))
}

func (suite *ruleTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/config", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	PDServerCfg := suite.svr.GetConfig().PDServerCfg
	PDServerCfg.KeyType = "raw"
	err := suite.svr.SetPDServerConfig(PDServerCfg)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.urlPrefix, []byte(`{"enable-placement-rules":"true"}`), tu.StatusOK(re)))
}

func (suite *ruleTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *ruleTestSuite) TearDownTest() {
	def := placement.GroupBundle{
		ID: "pd",
		Rules: []*placement.Rule{
			{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
		},
	}
	data, err := json.Marshal([]placement.GroupBundle{def})
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/placement-rule", data, tu.StatusOK(suite.Require()))
	suite.NoError(err)
}

func (suite *ruleTestSuite) TestSet() {
	rule := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	successData, err := json.Marshal(rule)
	suite.NoError(err)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	suite.NoError(err)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	suite.NoError(err)
	parseErrData := []byte("foo")
	rule1 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: "voter", Count: 1}
	checkErrData, err := json.Marshal(rule1)
	suite.NoError(err)
	rule2 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: -1}
	setErrData, err := json.Marshal(rule2)
	suite.NoError(err)
	rule3 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "follower", Count: 3}
	updateData, err := json.Marshal(rule3)
	suite.NoError(err)
	newStartKey, err := hex.DecodeString(rule.StartKeyHex)
	suite.NoError(err)
	newEndKey, err := hex.DecodeString(rule.EndKeyHex)
	suite.NoError(err)

	testCases := []struct {
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
			response: `"[PD:hex:ErrHexDecodingString]decode string XXXX error"
`,
		},
		{
			name:    "Set Rule Failed",
			rawData: setErrData,
			success: false,
			response: `"[PD:placement:ErrRuleContent]invalid rule content, invalid count -1"
`,
		},
	}
	re := suite.Require()
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		// clear suspect keyRanges to prevent test case from others
		suite.svr.GetRaftCluster().ClearSuspectKeyRanges()
		if testCase.success {
			err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", testCase.rawData, tu.StatusOK(re))
			popKeyRangeMap := map[string]struct{}{}
			for i := 0; i < len(testCase.popKeyRange)/2; i++ {
				v, got := suite.svr.GetRaftCluster().PopOneSuspectKeyRange()
				suite.True(got)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			suite.Len(popKeyRangeMap, len(testCase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testCase.popKeyRange[k]
				suite.True(ok)
			}
		} else {
			err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", testCase.rawData,
				tu.StatusNotOK(re),
				tu.StringEqual(re, testCase.response))
		}
		suite.NoError(err)
	}
}

func (suite *ruleTestSuite) TestGet() {
	rule := placement.Rule{GroupID: "a", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", data, tu.StatusOK(re))
	suite.NoError(err)

	testCases := []struct {
		name  string
		rule  placement.Rule
		found bool
		code  int
	}{
		{
			name:  "found",
			rule:  rule,
			found: true,
			code:  http.StatusOK,
		},
		{
			name:  "not found",
			rule:  placement.Rule{GroupID: "a", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
			found: false,
			code:  http.StatusNotFound,
		},
	}
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		var resp placement.Rule
		url := fmt.Sprintf("%s/rule/%s/%s", suite.urlPrefix, testCase.rule.GroupID, testCase.rule.ID)
		if testCase.found {
			err = tu.ReadGetJSON(re, testDialClient, url, &resp)
			suite.compareRule(&resp, &testCase.rule)
		} else {
			err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, testCase.code))
		}
		suite.NoError(err)
	}
}

func (suite *ruleTestSuite) TestGetAll() {
	rule := placement.Rule{GroupID: "b", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", data, tu.StatusOK(re))
	suite.NoError(err)

	var resp2 []*placement.Rule
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/rules", &resp2)
	suite.NoError(err)
	suite.GreaterOrEqual(len(resp2), 1)
}

func (suite *ruleTestSuite) TestSetAll() {
	rule1 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	rule2 := placement.Rule{GroupID: "b", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	rule3 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: "voter", Count: 1}
	rule4 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: -1}
	rule5 := placement.Rule{GroupID: "pd", ID: "default", StartKeyHex: "", EndKeyHex: "", Role: "voter", Count: 1,
		LocationLabels: []string{"host"}}
	rule6 := placement.Rule{GroupID: "pd", ID: "default", StartKeyHex: "", EndKeyHex: "", Role: "voter", Count: 3}

	suite.svr.GetPersistOptions().GetReplicationConfig().LocationLabels = []string{"host"}
	defaultRule := suite.svr.GetRaftCluster().GetRuleManager().GetRule("pd", "default")
	defaultRule.LocationLabels = []string{"host"}
	suite.svr.GetRaftCluster().GetRuleManager().SetRule(defaultRule)

	successData, err := json.Marshal([]*placement.Rule{&rule1, &rule2})
	suite.NoError(err)

	checkErrData, err := json.Marshal([]*placement.Rule{&rule1, &rule3})
	suite.NoError(err)

	setErrData, err := json.Marshal([]*placement.Rule{&rule1, &rule4})
	suite.NoError(err)

	defaultData, err := json.Marshal([]*placement.Rule{&rule1, &rule5})
	suite.NoError(err)

	recoverData, err := json.Marshal([]*placement.Rule{&rule1, &rule6})
	suite.NoError(err)

	testCases := []struct {
		name          string
		rawData       []byte
		success       bool
		response      string
		isDefaultRule bool
		count         int
	}{
		{
			name:          "Set rules successfully, with oldRules full of nil",
			rawData:       successData,
			success:       true,
			response:      "",
			isDefaultRule: false,
		},
		{
			name:          "Parse Json failed",
			rawData:       []byte("foo"),
			success:       false,
			isDefaultRule: false,
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
			name:          "Check rule failed",
			rawData:       checkErrData,
			success:       false,
			isDefaultRule: false,
			response: `"[PD:hex:ErrHexDecodingString]decode string XXXX error"
`,
		},
		{
			name:          "Set Rule Failed",
			rawData:       setErrData,
			success:       false,
			isDefaultRule: false,
			response: `"[PD:placement:ErrRuleContent]invalid rule content, invalid count -1"
`,
		},
		{
			name:          "set default rule",
			rawData:       defaultData,
			success:       true,
			response:      "",
			isDefaultRule: true,
			count:         1,
		},
		{
			name:          "recover default rule",
			rawData:       recoverData,
			success:       true,
			response:      "",
			isDefaultRule: true,
			count:         3,
		},
	}
	re := suite.Require()
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		if testCase.success {
			err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rules", testCase.rawData, tu.StatusOK(re))
			suite.NoError(err)
			if testCase.isDefaultRule {
				suite.Equal(int(suite.svr.GetPersistOptions().GetReplicationConfig().MaxReplicas), testCase.count)
			}
		} else {
			err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rules", testCase.rawData,
				tu.StringEqual(re, testCase.response))
			suite.NoError(err)
		}
	}
}

func (suite *ruleTestSuite) TestGetAllByGroup() {
	re := suite.Require()
	rule := placement.Rule{GroupID: "c", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", data, tu.StatusOK(re))
	suite.NoError(err)

	rule1 := placement.Rule{GroupID: "c", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err = json.Marshal(rule1)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", data, tu.StatusOK(re))
	suite.NoError(err)

	testCases := []struct {
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

	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/group/%s", suite.urlPrefix, testCase.groupID)
		err = tu.ReadGetJSON(re, testDialClient, url, &resp)
		suite.NoError(err)
		suite.Len(resp, testCase.count)
		if testCase.count == 2 {
			suite.compareRule(resp[0], &rule)
			suite.compareRule(resp[1], &rule1)
		}
	}
}

func (suite *ruleTestSuite) TestGetAllByRegion() {
	rule := placement.Rule{GroupID: "e", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", data, tu.StatusOK(re))
	suite.NoError(err)

	r := core.NewTestRegionInfo(4, 1, []byte{0x22, 0x22}, []byte{0x33, 0x33})
	mustRegionHeartbeat(re, suite.svr, r)

	testCases := []struct {
		name     string
		regionID string
		success  bool
		code     int
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
			code:     400,
		},
		{
			name:     "region not found",
			regionID: "5",
			success:  false,
			code:     404,
		},
	}
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/region/%s", suite.urlPrefix, testCase.regionID)

		if testCase.success {
			err = tu.ReadGetJSON(re, testDialClient, url, &resp)
			for _, r := range resp {
				if r.GroupID == "e" {
					suite.compareRule(r, &rule)
				}
			}
		} else {
			err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, testCase.code))
		}
		suite.NoError(err)
	}
}

func (suite *ruleTestSuite) TestGetAllByKey() {
	rule := placement.Rule{GroupID: "f", ID: "40", StartKeyHex: "8888", EndKeyHex: "9111", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", data, tu.StatusOK(re))
	suite.NoError(err)

	testCases := []struct {
		name     string
		key      string
		success  bool
		respSize int
		code     int
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
			code:     400,
			respSize: 0,
		},
		{
			name:     "key out of range",
			key:      "9999",
			success:  true,
			respSize: 1,
		},
	}
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/key/%s", suite.urlPrefix, testCase.key)
		if testCase.success {
			err = tu.ReadGetJSON(re, testDialClient, url, &resp)
			suite.Len(resp, testCase.respSize)
		} else {
			err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, testCase.code))
		}
		suite.NoError(err)
	}
}

func (suite *ruleTestSuite) TestDelete() {
	rule := placement.Rule{GroupID: "g", ID: "10", StartKeyHex: "8888", EndKeyHex: "9111", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rule", data, tu.StatusOK(suite.Require()))
	suite.NoError(err)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	suite.NoError(err)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	suite.NoError(err)

	testCases := []struct {
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
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		url := fmt.Sprintf("%s/rule/%s/%s", suite.urlPrefix, testCase.groupID, testCase.id)
		// clear suspect keyRanges to prevent test case from others
		suite.svr.GetRaftCluster().ClearSuspectKeyRanges()
		err = tu.CheckDelete(testDialClient, url, tu.StatusOK(suite.Require()))
		suite.NoError(err)
		if len(testCase.popKeyRange) > 0 {
			popKeyRangeMap := map[string]struct{}{}
			for i := 0; i < len(testCase.popKeyRange)/2; i++ {
				v, got := suite.svr.GetRaftCluster().PopOneSuspectKeyRange()
				suite.True(got)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			suite.Len(popKeyRangeMap, len(testCase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testCase.popKeyRange[k]
				suite.True(ok)
			}
		}
	}
}

func (suite *ruleTestSuite) compareRule(r1 *placement.Rule, r2 *placement.Rule) {
	suite.Equal(r2.GroupID, r1.GroupID)
	suite.Equal(r2.ID, r1.ID)
	suite.Equal(r2.StartKeyHex, r1.StartKeyHex)
	suite.Equal(r2.EndKeyHex, r1.EndKeyHex)
	suite.Equal(r2.Role, r1.Role)
	suite.Equal(r2.Count, r1.Count)
}

func (suite *ruleTestSuite) TestBatch() {
	opt1 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "13", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt2 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "b", ID: "13", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt3 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "14", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt4 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "15", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt5 := placement.RuleOp{
		Action: placement.RuleOpDel,
		Rule:   &placement.Rule{GroupID: "a", ID: "14"},
	}
	opt6 := placement.RuleOp{
		Action:           placement.RuleOpDel,
		Rule:             &placement.Rule{GroupID: "b", ID: "1"},
		DeleteByIDPrefix: true,
	}
	opt7 := placement.RuleOp{
		Action: placement.RuleOpDel,
		Rule:   &placement.Rule{GroupID: "a", ID: "1"},
	}
	opt8 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "16", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt9 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "17", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: -1},
	}

	successData1, err := json.Marshal([]placement.RuleOp{opt1, opt2, opt3})
	suite.NoError(err)

	successData2, err := json.Marshal([]placement.RuleOp{opt5, opt7})
	suite.NoError(err)

	successData3, err := json.Marshal([]placement.RuleOp{opt4, opt6})
	suite.NoError(err)

	checkErrData, err := json.Marshal([]placement.RuleOp{opt8})
	suite.NoError(err)

	setErrData, err := json.Marshal([]placement.RuleOp{opt9})
	suite.NoError(err)

	testCases := []struct {
		name     string
		rawData  []byte
		success  bool
		response string
	}{
		{
			name:     "Batch adds successfully",
			rawData:  successData1,
			success:  true,
			response: "",
		},
		{
			name:     "Batch removes successfully",
			rawData:  successData2,
			success:  true,
			response: "",
		},
		{
			name:     "Batch add and remove successfully",
			rawData:  successData3,
			success:  true,
			response: "",
		},
		{
			name:    "Parse Json failed",
			rawData: []byte("foo"),
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
			response: `"[PD:hex:ErrHexDecodingString]decode string XXXX error"
`,
		},
		{
			name:    "Set Rule Failed",
			rawData: setErrData,
			success: false,
			response: `"[PD:placement:ErrRuleContent]invalid rule content, invalid count -1"
`,
		},
	}
	re := suite.Require()
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		if testCase.success {
			err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rules/batch", testCase.rawData, tu.StatusOK(re))
			suite.NoError(err)
		} else {
			err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/rules/batch", testCase.rawData,
				tu.StatusNotOK(re),
				tu.StringEqual(re, testCase.response))
			suite.NoError(err)
		}
	}
}

func (suite *ruleTestSuite) TestBundle() {
	re := suite.Require()
	// GetAll
	b1 := placement.GroupBundle{
		ID: "pd",
		Rules: []*placement.Rule{
			{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
		},
	}
	var bundles []placement.GroupBundle
	err := tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule", &bundles)
	suite.NoError(err)
	suite.Len(bundles, 1)
	suite.compareBundle(bundles[0], b1)

	// Set
	b2 := placement.GroupBundle{
		ID:       "foo",
		Index:    42,
		Override: true,
		Rules: []*placement.Rule{
			{GroupID: "foo", ID: "bar", Index: 1, Override: true, Role: "voter", Count: 1},
		},
	}
	data, err := json.Marshal(b2)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/placement-rule/foo", data, tu.StatusOK(re))
	suite.NoError(err)

	// Get
	var bundle placement.GroupBundle
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule/foo", &bundle)
	suite.NoError(err)
	suite.compareBundle(bundle, b2)

	// GetAll again
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule", &bundles)
	suite.NoError(err)
	suite.Len(bundles, 2)
	suite.compareBundle(bundles[0], b1)
	suite.compareBundle(bundles[1], b2)

	// Delete
	err = tu.CheckDelete(testDialClient, suite.urlPrefix+"/placement-rule/pd", tu.StatusOK(suite.Require()))
	suite.NoError(err)

	// GetAll again
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule", &bundles)
	suite.NoError(err)
	suite.Len(bundles, 1)
	suite.compareBundle(bundles[0], b2)

	// SetAll
	b2.Rules = append(b2.Rules, &placement.Rule{GroupID: "foo", ID: "baz", Index: 2, Role: "follower", Count: 1})
	b2.Index, b2.Override = 0, false
	b3 := placement.GroupBundle{ID: "foobar", Index: 100}
	data, err = json.Marshal([]placement.GroupBundle{b1, b2, b3})
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/placement-rule", data, tu.StatusOK(re))
	suite.NoError(err)

	// GetAll again
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule", &bundles)
	suite.NoError(err)
	suite.Len(bundles, 3)
	suite.compareBundle(bundles[0], b2)
	suite.compareBundle(bundles[1], b1)
	suite.compareBundle(bundles[2], b3)

	// Delete using regexp
	err = tu.CheckDelete(testDialClient, suite.urlPrefix+"/placement-rule/"+url.PathEscape("foo.*")+"?regexp", tu.StatusOK(suite.Require()))
	suite.NoError(err)

	// GetAll again
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule", &bundles)
	suite.NoError(err)
	suite.Len(bundles, 1)
	suite.compareBundle(bundles[0], b1)

	// Set
	id := "rule-without-group-id"
	b4 := placement.GroupBundle{
		Index: 4,
		Rules: []*placement.Rule{
			{ID: "bar", Index: 1, Override: true, Role: "voter", Count: 1},
		},
	}
	data, err = json.Marshal(b4)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/placement-rule/"+id, data, tu.StatusOK(re))
	suite.NoError(err)

	b4.ID = id
	b4.Rules[0].GroupID = b4.ID

	// Get
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule/"+id, &bundle)
	suite.NoError(err)
	suite.compareBundle(bundle, b4)

	// GetAll again
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule", &bundles)
	suite.NoError(err)
	suite.Len(bundles, 2)
	suite.compareBundle(bundles[0], b1)
	suite.compareBundle(bundles[1], b4)

	// SetAll
	b5 := placement.GroupBundle{
		ID:    "rule-without-group-id-2",
		Index: 5,
		Rules: []*placement.Rule{
			{ID: "bar", Index: 1, Override: true, Role: "voter", Count: 1},
		},
	}
	data, err = json.Marshal([]placement.GroupBundle{b1, b4, b5})
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/placement-rule", data, tu.StatusOK(re))
	suite.NoError(err)

	b5.Rules[0].GroupID = b5.ID

	// GetAll again
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/placement-rule", &bundles)
	suite.NoError(err)
	suite.Len(bundles, 3)
	suite.compareBundle(bundles[0], b1)
	suite.compareBundle(bundles[1], b4)
	suite.compareBundle(bundles[2], b5)
}

func (suite *ruleTestSuite) TestBundleBadRequest() {
	testCases := []struct {
		uri  string
		data string
		ok   bool
	}{
		{"/placement-rule/foo", `{"group_id":"foo"}`, true},
		{"/placement-rule/foo", `{"group_id":"bar"}`, false},
		{"/placement-rule/foo", `{"group_id":"foo", "rules": [{"group_id":"foo", "id":"baz", "role":"voter", "count":1}]}`, true},
		{"/placement-rule/foo", `{"group_id":"foo", "rules": [{"group_id":"bar", "id":"baz", "role":"voter", "count":1}]}`, false},
		{"/placement-rule", `[{"group_id":"foo", "rules": [{"group_id":"foo", "id":"baz", "role":"voter", "count":1}]}]`, true},
		{"/placement-rule", `[{"group_id":"foo", "rules": [{"group_id":"bar", "id":"baz", "role":"voter", "count":1}]}]`, false},
	}
	for _, testCase := range testCases {
		err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+testCase.uri, []byte(testCase.data),
			func(_ []byte, code int, _ http.Header) {
				suite.Equal(testCase.ok, code == http.StatusOK)
			})
		suite.NoError(err)
	}
}

func (suite *ruleTestSuite) compareBundle(b1, b2 placement.GroupBundle) {
	suite.Equal(b2.ID, b1.ID)
	suite.Equal(b2.Index, b1.Index)
	suite.Equal(b2.Override, b1.Override)
	suite.Len(b2.Rules, len(b1.Rules))
	for i := range b1.Rules {
		suite.compareRule(b1.Rules[i], b2.Rules[i])
	}
}

type regionRuleTestSuite struct {
	suite.Suite
	svr       *server.Server
	grpcSvr   *server.GrpcServer
	cleanup   tu.CleanupFunc
	urlPrefix string
	stores    []*metapb.Store
	regions   []*core.RegionInfo
}

func TestRegionRuleTestSuite(t *testing.T) {
	suite.Run(t, new(regionRuleTestSuite))
}

func (suite *regionRuleTestSuite) SetupSuite() {
	suite.stores = []*metapb.Store{
		{
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			Id:        2,
			Address:   "tikv2",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
	}
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) {
		cfg.Replication.EnablePlacementRules = true
		cfg.Replication.MaxReplicas = 1
	})
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.grpcSvr = &server.GrpcServer{Server: suite.svr}
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)

	for _, store := range suite.stores {
		mustPutStore(re, suite.svr, store.Id, store.State, store.NodeState, nil)
	}
	suite.regions = make([]*core.RegionInfo, 0)
	peers1 := []*metapb.Peer{
		{Id: 102, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 103, StoreId: 2, Role: metapb.PeerRole_Voter}}
	suite.regions = append(suite.regions, core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers1, RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}}, peers1[0],
		core.WithStartKey([]byte("abc")), core.WithEndKey([]byte("def"))))
	peers2 := []*metapb.Peer{
		{Id: 104, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 105, StoreId: 2, Role: metapb.PeerRole_Learner}}
	suite.regions = append(suite.regions, core.NewRegionInfo(&metapb.Region{Id: 2, Peers: peers2, RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2}}, peers2[0],
		core.WithStartKey([]byte("ghi")), core.WithEndKey([]byte("jkl"))))
	peers3 := []*metapb.Peer{
		{Id: 106, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 107, StoreId: 2, Role: metapb.PeerRole_Learner}}
	suite.regions = append(suite.regions, core.NewRegionInfo(&metapb.Region{Id: 3, Peers: peers3, RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 3}}, peers3[0],
		core.WithStartKey([]byte("mno")), core.WithEndKey([]byte("pqr"))))
	for _, rg := range suite.regions {
		suite.svr.GetBasicCluster().PutRegion(rg)
	}
}

func (suite *regionRuleTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *regionRuleTestSuite) TestRegionPlacementRule() {
	ruleManager := suite.svr.GetRaftCluster().GetRuleManager()
	ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test2",
		StartKeyHex: hex.EncodeToString([]byte("ghi")),
		EndKeyHex:   hex.EncodeToString([]byte("jkl")),
		Role:        placement.Learner,
		Count:       1,
	})
	ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test3",
		StartKeyHex: hex.EncodeToString([]byte("ooo")),
		EndKeyHex:   hex.EncodeToString([]byte("ppp")),
		Role:        placement.Learner,
		Count:       1,
	})
	re := suite.Require()
	url := fmt.Sprintf("%s/config/rules/region/%d/detail", suite.urlPrefix, 1)
	fit := &placement.RegionFit{}
	err := tu.ReadGetJSON(re, testDialClient, url, fit)
	suite.Equal(len(fit.RuleFits), 1)
	suite.Equal(len(fit.OrphanPeers), 1)
	suite.NoError(err)
	url = fmt.Sprintf("%s/config/rules/region/%d/detail", suite.urlPrefix, 2)
	fit = &placement.RegionFit{}
	err = tu.ReadGetJSON(re, testDialClient, url, fit)
	suite.Equal(len(fit.RuleFits), 2)
	suite.Equal(len(fit.OrphanPeers), 0)
	suite.NoError(err)
	url = fmt.Sprintf("%s/config/rules/region/%d/detail", suite.urlPrefix, 3)
	fit = &placement.RegionFit{}
	err = tu.ReadGetJSON(re, testDialClient, url, fit)
	suite.Equal(len(fit.RuleFits), 0)
	suite.Equal(len(fit.OrphanPeers), 2)
	suite.NoError(err)

	url = fmt.Sprintf("%s/config/rules/region/%d/detail", suite.urlPrefix, 4)
	err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusNotFound), tu.StringContain(
		re, "region 4 not found"))
	suite.NoError(err)

	url = fmt.Sprintf("%s/config/rules/region/%s/detail", suite.urlPrefix, "id")
	err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusBadRequest), tu.StringContain(
		re, errs.ErrRegionInvalidID.Error()))
	suite.NoError(err)

	suite.svr.GetRaftCluster().GetReplicationConfig().EnablePlacementRules = false
	url = fmt.Sprintf("%s/config/rules/region/%d/detail", suite.urlPrefix, 1)
	err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusPreconditionFailed), tu.StringContain(
		re, "placement rules feature is disabled"))
	suite.NoError(err)
}
