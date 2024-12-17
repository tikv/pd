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
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type ruleTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestRuleTestSuite(t *testing.T) {
	suite.Run(t, new(ruleTestSuite))
}

func (suite *ruleTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T(), func(conf *config.Config, _ string) {
		conf.PDServerCfg.KeyType = "raw"
		conf.Replication.EnablePlacementRules = true
	})
}

func (suite *ruleTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *ruleTestSuite) TearDownTest() {
	re := suite.Require()
	cleanFunc := func(cluster *tests.TestCluster) {
		def := placement.GroupBundle{
			ID: "pd",
			Rules: []*placement.Rule{
				{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
			},
		}
		data, err := json.Marshal([]placement.GroupBundle{def})
		re.NoError(err)
		urlPrefix := cluster.GetLeaderServer().GetAddr()
		err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/pd/api/v1/config/placement-rule", data, tu.StatusOK(re))
		re.NoError(err)
	}
	suite.env.RunTestBasedOnMode(cleanFunc)
}

func (suite *ruleTestSuite) TestSet() {
	suite.env.RunTestBasedOnMode(suite.checkSet)
}

func (suite *ruleTestSuite) checkSet(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	successData, err := json.Marshal(rule)
	re.NoError(err)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	re.NoError(err)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	re.NoError(err)
	parseErrData := []byte("foo")
	rule1 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	checkErrData, err := json.Marshal(rule1)
	re.NoError(err)
	rule2 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: -1}
	setErrData, err := json.Marshal(rule2)
	re.NoError(err)
	rule3 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Follower, Count: 3}
	updateData, err := json.Marshal(rule3)
	re.NoError(err)
	newStartKey, err := hex.DecodeString(rule.StartKeyHex)
	re.NoError(err)
	newEndKey, err := hex.DecodeString(rule.EndKeyHex)
	re.NoError(err)

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
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		// clear suspect keyRanges to prevent test case from others
		leaderServer.GetRaftCluster().ClearSuspectKeyRanges()
		if testCase.success {
			err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", testCase.rawData, tu.StatusOK(re))
			popKeyRangeMap := map[string]struct{}{}
			for range len(testCase.popKeyRange) / 2 {
				v, got := leaderServer.GetRaftCluster().PopOneSuspectKeyRange()
				re.True(got)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			re.Len(popKeyRangeMap, len(testCase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testCase.popKeyRange[k]
				re.True(ok)
			}
		} else {
			err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", testCase.rawData,
				tu.StatusNotOK(re),
				tu.StringEqual(re, testCase.response))
		}
		re.NoError(err)
	}
}

func (suite *ruleTestSuite) TestGet() {
	suite.env.RunTestBasedOnMode(suite.checkGet)
}

func (suite *ruleTestSuite) checkGet(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule := placement.Rule{GroupID: "a", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	data, err := json.Marshal(rule)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
	re.NoError(err)

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
			rule:  placement.Rule{GroupID: "a", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1},
			found: false,
			code:  http.StatusNotFound,
		},
	}
	for i, testCase := range testCases {
		suite.T().Log(testCase.name)
		var resp placement.Rule
		url := fmt.Sprintf("%s/rule/%s/%s", urlPrefix, testCase.rule.GroupID, testCase.rule.ID)
		if testCase.found {
			tu.Eventually(re, func() bool {
				err = tu.ReadGetJSON(re, tests.TestDialClient, url, &resp)
				return compareRule(&resp, &testCases[i].rule)
			})
		} else {
			err = tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.Status(re, testCase.code))
		}
		re.NoError(err)
	}
}

func (suite *ruleTestSuite) TestGetAll() {
	suite.env.RunTestBasedOnMode(suite.checkGetAll)
}

func (suite *ruleTestSuite) checkGetAll(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule := placement.Rule{GroupID: "b", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	data, err := json.Marshal(rule)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
	re.NoError(err)

	var resp2 []*placement.Rule
	err = tu.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rules", &resp2)
	re.NoError(err)
	re.NotEmpty(resp2)
}

func (suite *ruleTestSuite) TestSetAll() {
	suite.env.RunTestBasedOnMode(suite.checkSetAll)
}

func (suite *ruleTestSuite) checkSetAll(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule1 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	rule2 := placement.Rule{GroupID: "b", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	rule3 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	rule4 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: -1}
	rule5 := placement.Rule{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, StartKeyHex: "", EndKeyHex: "", Role: placement.Voter, Count: 1,
		LocationLabels: []string{"host"}}
	rule6 := placement.Rule{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, StartKeyHex: "", EndKeyHex: "", Role: placement.Voter, Count: 3}

	leaderServer.GetPersistOptions().GetReplicationConfig().LocationLabels = []string{"host"}
	defaultRule := leaderServer.GetRaftCluster().GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	defaultRule.LocationLabels = []string{"host"}
	leaderServer.GetRaftCluster().GetRuleManager().SetRule(defaultRule)

	successData, err := json.Marshal([]*placement.Rule{&rule1, &rule2})
	re.NoError(err)

	checkErrData, err := json.Marshal([]*placement.Rule{&rule1, &rule3})
	re.NoError(err)

	setErrData, err := json.Marshal([]*placement.Rule{&rule1, &rule4})
	re.NoError(err)

	defaultData, err := json.Marshal([]*placement.Rule{&rule1, &rule5})
	re.NoError(err)

	recoverData, err := json.Marshal([]*placement.Rule{&rule1, &rule6})
	re.NoError(err)

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
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		if testCase.success {
			err := tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rules", testCase.rawData, tu.StatusOK(re))
			re.NoError(err)
			if testCase.isDefaultRule {
				re.Equal(int(leaderServer.GetPersistOptions().GetReplicationConfig().MaxReplicas), testCase.count)
			}
		} else {
			err := tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rules", testCase.rawData,
				tu.StringEqual(re, testCase.response))
			re.NoError(err)
		}
	}
}

func (suite *ruleTestSuite) TestGetAllByGroup() {
	suite.env.RunTestBasedOnMode(suite.checkGetAllByGroup)
}

func (suite *ruleTestSuite) checkGetAllByGroup(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule := placement.Rule{GroupID: "c", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	data, err := json.Marshal(rule)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
	re.NoError(err)

	rule1 := placement.Rule{GroupID: "c", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	data, err = json.Marshal(rule1)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
	re.NoError(err)

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
		url := fmt.Sprintf("%s/rules/group/%s", urlPrefix, testCase.groupID)
		tu.Eventually(re, func() bool {
			err = tu.ReadGetJSON(re, tests.TestDialClient, url, &resp)
			re.NoError(err)
			if len(resp) != testCase.count {
				return false
			}
			if testCase.count == 2 {
				return compareRule(resp[0], &rule) && compareRule(resp[1], &rule1)
			}
			return true
		})
	}
}

func (suite *ruleTestSuite) TestGetAllByRegion() {
	suite.env.RunTestBasedOnMode(suite.checkGetAllByRegion)
}

func (suite *ruleTestSuite) checkGetAllByRegion(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule := placement.Rule{GroupID: "e", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1}
	data, err := json.Marshal(rule)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
	re.NoError(err)

	r := core.NewTestRegionInfo(4, 1, []byte{0x22, 0x22}, []byte{0x33, 0x33})
	tests.MustPutRegionInfo(re, cluster, r)

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
		url := fmt.Sprintf("%s/rules/region/%s", urlPrefix, testCase.regionID)

		if testCase.success {
			tu.Eventually(re, func() bool {
				err = tu.ReadGetJSON(re, tests.TestDialClient, url, &resp)
				for _, r := range resp {
					if r.GroupID == "e" {
						return compareRule(r, &rule)
					}
				}
				return true
			})
		} else {
			err = tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.Status(re, testCase.code))
		}
		re.NoError(err)
	}
}

func (suite *ruleTestSuite) TestGetAllByKey() {
	suite.env.RunTestBasedOnMode(suite.checkGetAllByKey)
}

func (suite *ruleTestSuite) checkGetAllByKey(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule := placement.Rule{GroupID: "f", ID: "40", StartKeyHex: "8888", EndKeyHex: "9111", Role: placement.Voter, Count: 1}
	data, err := json.Marshal(rule)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
	re.NoError(err)

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
		url := fmt.Sprintf("%s/rules/key/%s", urlPrefix, testCase.key)
		if testCase.success {
			tu.Eventually(re, func() bool {
				err = tu.ReadGetJSON(re, tests.TestDialClient, url, &resp)
				return len(resp) == testCase.respSize
			})
		} else {
			err = tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.Status(re, testCase.code))
		}
		re.NoError(err)
	}
}

func (suite *ruleTestSuite) TestDelete() {
	suite.env.RunTestBasedOnMode(suite.checkDelete)
}

func (suite *ruleTestSuite) checkDelete(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	rule := placement.Rule{GroupID: "g", ID: "10", StartKeyHex: "8888", EndKeyHex: "9111", Role: placement.Voter, Count: 1}
	data, err := json.Marshal(rule)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
	re.NoError(err)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	re.NoError(err)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	re.NoError(err)

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
		url := fmt.Sprintf("%s/rule/%s/%s", urlPrefix, testCase.groupID, testCase.id)
		// clear suspect keyRanges to prevent test case from others
		leaderServer.GetRaftCluster().ClearSuspectKeyRanges()
		err = tu.CheckDelete(tests.TestDialClient, url, tu.StatusOK(re))
		re.NoError(err)
		if len(testCase.popKeyRange) > 0 {
			popKeyRangeMap := map[string]struct{}{}
			for range len(testCase.popKeyRange) / 2 {
				v, got := leaderServer.GetRaftCluster().PopOneSuspectKeyRange()
				re.True(got)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			re.Len(popKeyRangeMap, len(testCase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testCase.popKeyRange[k]
				re.True(ok)
			}
		}
	}
}

func (suite *ruleTestSuite) TestBatch() {
	suite.env.RunTestBasedOnMode(suite.checkBatch)
}

func (suite *ruleTestSuite) checkBatch(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	opt1 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "13", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1},
	}
	opt2 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "b", ID: "13", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1},
	}
	opt3 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "14", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1},
	}
	opt4 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "15", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: 1},
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
		Rule:   &placement.Rule{GroupID: "a", ID: "16", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: placement.Voter, Count: 1},
	}
	opt9 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "17", StartKeyHex: "1111", EndKeyHex: "3333", Role: placement.Voter, Count: -1},
	}

	successData1, err := json.Marshal([]placement.RuleOp{opt1, opt2, opt3})
	re.NoError(err)

	successData2, err := json.Marshal([]placement.RuleOp{opt5, opt7})
	re.NoError(err)

	successData3, err := json.Marshal([]placement.RuleOp{opt4, opt6})
	re.NoError(err)

	checkErrData, err := json.Marshal([]placement.RuleOp{opt8})
	re.NoError(err)

	setErrData, err := json.Marshal([]placement.RuleOp{opt9})
	re.NoError(err)

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
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		if testCase.success {
			err := tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rules/batch", testCase.rawData, tu.StatusOK(re))
			re.NoError(err)
		} else {
			err := tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rules/batch", testCase.rawData,
				tu.StatusNotOK(re),
				tu.StringEqual(re, testCase.response))
			re.NoError(err)
		}
	}
}

func (suite *ruleTestSuite) TestBundle() {
	suite.env.RunTestBasedOnMode(suite.checkBundle)
}

func (suite *ruleTestSuite) checkBundle(cluster *tests.TestCluster) {
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

	re := suite.Require()
	// GetAll
	b1 := placement.GroupBundle{
		ID: placement.DefaultGroupID,
		Rules: []*placement.Rule{
			{
				GroupID: placement.DefaultGroupID,
				ID:      placement.DefaultRuleID,
				Role:    placement.Voter,
				Count:   3,
			},
		},
	}
	assertBundlesEqual(re, urlPrefix+"/placement-rule", []placement.GroupBundle{b1}, 1)

	// Set
	b2 := placement.GroupBundle{
		ID:       "foo",
		Index:    42,
		Override: true,
		Rules: []*placement.Rule{
			{GroupID: "foo", ID: "bar", Index: 1, Override: true, Role: placement.Voter, Count: 1},
		},
	}
	data, err := json.Marshal(b2)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/placement-rule/foo", data, tu.StatusOK(re))
	re.NoError(err)

	// Get
	assertBundleEqual(re, urlPrefix+"/placement-rule/foo", b2)

	// GetAll again
	assertBundlesEqual(re, urlPrefix+"/placement-rule", []placement.GroupBundle{b1, b2}, 2)

	// Delete
	err = tu.CheckDelete(tests.TestDialClient, urlPrefix+"/placement-rule/pd", tu.StatusOK(re))
	re.NoError(err)

	// GetAll again
	assertBundlesEqual(re, urlPrefix+"/placement-rule", []placement.GroupBundle{b2}, 1)

	// SetAll
	b2.Rules = append(b2.Rules, &placement.Rule{GroupID: "foo", ID: "baz", Index: 2, Role: placement.Follower, Count: 1})
	b2.Index, b2.Override = 0, false
	b3 := placement.GroupBundle{ID: "foobar", Index: 100}
	data, err = json.Marshal([]placement.GroupBundle{b1, b2, b3})
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/placement-rule", data, tu.StatusOK(re))
	re.NoError(err)

	// GetAll again
	assertBundlesEqual(re, urlPrefix+"/placement-rule", []placement.GroupBundle{b1, b2, b3}, 3)

	// Delete using regexp
	err = tu.CheckDelete(tests.TestDialClient, urlPrefix+"/placement-rule/"+url.PathEscape("foo.*")+"?regexp", tu.StatusOK(re))
	re.NoError(err)

	// GetAll again
	assertBundlesEqual(re, urlPrefix+"/placement-rule", []placement.GroupBundle{b1}, 1)

	// Set
	id := "rule-without-group-id"
	b4 := placement.GroupBundle{
		Index: 4,
		Rules: []*placement.Rule{
			{ID: "bar", Index: 1, Override: true, Role: placement.Voter, Count: 1},
		},
	}
	data, err = json.Marshal(b4)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/placement-rule/"+id, data, tu.StatusOK(re))
	re.NoError(err)

	b4.ID = id
	b4.Rules[0].GroupID = b4.ID
	// Get
	assertBundleEqual(re, urlPrefix+"/placement-rule/"+id, b4)

	// GetAll again
	assertBundlesEqual(re, urlPrefix+"/placement-rule", []placement.GroupBundle{b1, b4}, 2)

	// SetAll
	b5 := placement.GroupBundle{
		ID:    "rule-without-group-id-2",
		Index: 5,
		Rules: []*placement.Rule{
			{ID: "bar", Index: 1, Override: true, Role: placement.Voter, Count: 1},
		},
	}
	data, err = json.Marshal([]placement.GroupBundle{b1, b4, b5})
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/placement-rule", data, tu.StatusOK(re))
	re.NoError(err)

	b5.Rules[0].GroupID = b5.ID

	// GetAll again
	assertBundlesEqual(re, urlPrefix+"/placement-rule", []placement.GroupBundle{b1, b4, b5}, 3)
}

func (suite *ruleTestSuite) TestBundleBadRequest() {
	suite.env.RunTestBasedOnMode(suite.checkBundleBadRequest)
}

func (suite *ruleTestSuite) checkBundleBadRequest(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1/config", pdAddr, apiPrefix)

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
		err := tu.CheckPostJSON(tests.TestDialClient, urlPrefix+testCase.uri, []byte(testCase.data),
			func(_ []byte, code int, _ http.Header) {
				re.Equal(testCase.ok, code == http.StatusOK)
			})
		re.NoError(err)
	}
}

func (suite *ruleTestSuite) TestLeaderAndVoter() {
	suite.env.RunTestBasedOnMode(suite.checkLeaderAndVoter)
}

func (suite *ruleTestSuite) checkLeaderAndVoter(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1", pdAddr, apiPrefix)

	stores := []*metapb.Store{
		{
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "7.5.0",
			Labels:    []*metapb.StoreLabel{{Key: "zone", Value: "z1"}},
		},
		{
			Id:        2,
			Address:   "tikv2",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "7.5.0",
			Labels:    []*metapb.StoreLabel{{Key: "zone", Value: "z2"}},
		},
	}

	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}

	bundles := [][]placement.GroupBundle{
		{
			{
				ID:    "1",
				Index: 1,
				Rules: []*placement.Rule{
					{
						ID: "rule_1", Index: 1, Role: placement.Voter, Count: 1, GroupID: "1",
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"z1"}},
						},
					},
					{
						ID: "rule_2", Index: 2, Role: placement.Leader, Count: 1, GroupID: "1",
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"z2"}},
						},
					},
				},
			},
		},
		{
			{
				ID:    "1",
				Index: 1,
				Rules: []*placement.Rule{
					{
						ID: "rule_1", Index: 1, Role: placement.Leader, Count: 1, GroupID: "1",
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"z2"}},
						},
					},
					{
						ID: "rule_2", Index: 2, Role: placement.Voter, Count: 1, GroupID: "1",
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"z1"}},
						},
					},
				},
			},
		}}
	for _, bundle := range bundles {
		data, err := json.Marshal(bundle)
		re.NoError(err)
		err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
		re.NoError(err)

		tu.Eventually(re, func() bool {
			respBundle := make([]placement.GroupBundle, 0)
			err := tu.CheckGetJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", nil,
				tu.StatusOK(re), tu.ExtractJSON(re, &respBundle))
			re.NoError(err)
			re.Len(respBundle, 1)
			return compareBundle(respBundle[0], bundle[0])
		})
	}
}

func (suite *ruleTestSuite) TestDeleteAndUpdate() {
	suite.env.RunTestBasedOnMode(suite.checkDeleteAndUpdate)
}

func (suite *ruleTestSuite) checkDeleteAndUpdate(cluster *tests.TestCluster) {
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1", pdAddr, apiPrefix)

	bundles := [][]placement.GroupBundle{
		// 1 rule group with 1 rule
		{{
			ID:    "1",
			Index: 1,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 1, Role: placement.Voter, Count: 1, GroupID: "1",
				},
			},
		}},
		// 2 rule groups with different range rules
		{{
			ID:    "1",
			Index: 1,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 1, Role: placement.Voter, Count: 1, GroupID: "1",
					StartKey: []byte("a"), EndKey: []byte("b"),
				},
			},
		}, {
			ID:    "2",
			Index: 2,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 2, Role: placement.Voter, Count: 1, GroupID: "2",
					StartKey: []byte("b"), EndKey: []byte("c"),
				},
			},
		}},
		// 2 rule groups with 1 rule and 2 rules
		{{
			ID:    "3",
			Index: 3,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 3, Role: placement.Voter, Count: 1, GroupID: "3",
				},
			},
		}, {
			ID:    "4",
			Index: 4,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 4, Role: placement.Voter, Count: 1, GroupID: "4",
				},
				{
					ID: "bar", Index: 6, Role: placement.Voter, Count: 1, GroupID: "4",
				},
			},
		}},
		// 1 rule group with 2 rules
		{{
			ID:    "5",
			Index: 5,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 5, Role: placement.Voter, Count: 1, GroupID: "5",
				},
				{
					ID: "bar", Index: 6, Role: placement.Voter, Count: 1, GroupID: "5",
				},
			},
		}},
	}

	for _, bundle := range bundles {
		suite.postAndCheckRuleBundle(urlPrefix, bundle)
	}
}

func (suite *ruleTestSuite) TestConcurrency() {
	suite.env.RunTestBasedOnMode(suite.checkConcurrency)
}

func (suite *ruleTestSuite) checkConcurrency(cluster *tests.TestCluster) {
	// test concurrency of set rule group with different group id
	suite.checkConcurrencyWith(cluster,
		func(i int) []placement.GroupBundle {
			return []placement.GroupBundle{
				{
					ID:    strconv.Itoa(i),
					Index: i,
					Rules: []*placement.Rule{
						{
							ID: "foo", Index: i, Role: placement.Voter, Count: 1, GroupID: strconv.Itoa(i),
						},
					},
				},
			}
		},
		func(resp []placement.GroupBundle, i int) bool {
			return len(resp) == 1 && resp[0].ID == strconv.Itoa(i)
		},
	)
	// test concurrency of set rule with different id
	suite.checkConcurrencyWith(cluster,
		func(i int) []placement.GroupBundle {
			return []placement.GroupBundle{
				{
					ID:    "pd",
					Index: 1,
					Rules: []*placement.Rule{
						{
							ID: strconv.Itoa(i), Index: i, Role: placement.Voter, Count: 1, GroupID: "pd",
						},
					},
				},
			}
		},
		func(resp []placement.GroupBundle, i int) bool {
			return len(resp) == 1 && resp[0].ID == "pd" && resp[0].Rules[0].ID == strconv.Itoa(i)
		},
	)
}

func (suite *ruleTestSuite) checkConcurrencyWith(cluster *tests.TestCluster,
	genBundle func(int) []placement.GroupBundle,
	checkBundle func([]placement.GroupBundle, int) bool) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1", pdAddr, apiPrefix)
	expectResult := struct {
		syncutil.RWMutex
		val int
	}{}
	wg := sync.WaitGroup{}

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bundle := genBundle(i)
			data, err := json.Marshal(bundle)
			re.NoError(err)
			for range 10 {
				expectResult.Lock()
				err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
				re.NoError(err)
				expectResult.val = i
				expectResult.Unlock()
			}
		}(i)
	}

	wg.Wait()
	expectResult.RLock()
	defer expectResult.RUnlock()
	re.NotZero(expectResult.val)
	tu.Eventually(re, func() bool {
		respBundle := make([]placement.GroupBundle, 0)
		err := tu.CheckGetJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", nil,
			tu.StatusOK(re), tu.ExtractJSON(re, &respBundle))
		re.NoError(err)
		re.Len(respBundle, 1)
		return checkBundle(respBundle, expectResult.val)
	})
}

func (suite *ruleTestSuite) TestLargeRules() {
	suite.env.RunTestBasedOnMode(suite.checkLargeRules)
}

func (suite *ruleTestSuite) checkLargeRules(cluster *tests.TestCluster) {
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1", pdAddr, apiPrefix)
	genBundlesWithRulesNum := func(num int) []placement.GroupBundle {
		bundle := []placement.GroupBundle{
			{
				ID:    "1",
				Index: 1,
				Rules: make([]*placement.Rule, 0),
			},
		}
		for i := range num {
			bundle[0].Rules = append(bundle[0].Rules, &placement.Rule{
				ID: strconv.Itoa(i), Index: i, Role: placement.Voter, Count: 1, GroupID: "1",
				StartKey: []byte(strconv.Itoa(i)), EndKey: []byte(strconv.Itoa(i + 1)),
			})
		}
		return bundle
	}
	suite.postAndCheckRuleBundle(urlPrefix, genBundlesWithRulesNum(etcdutil.MaxEtcdTxnOps/2))
	suite.postAndCheckRuleBundle(urlPrefix, genBundlesWithRulesNum(etcdutil.MaxEtcdTxnOps*2))
}

func assertBundleEqual(re *require.Assertions, url string, expectedBundle placement.GroupBundle) {
	var bundle placement.GroupBundle
	tu.Eventually(re, func() bool {
		err := tu.ReadGetJSON(re, tests.TestDialClient, url, &bundle)
		if err != nil {
			return false
		}
		return compareBundle(bundle, expectedBundle)
	})
}

func assertBundlesEqual(re *require.Assertions, url string, expectedBundles []placement.GroupBundle, expectedLen int) {
	var bundles []placement.GroupBundle
	tu.Eventually(re, func() bool {
		err := tu.ReadGetJSON(re, tests.TestDialClient, url, &bundles)
		if err != nil {
			return false
		}
		if len(bundles) != expectedLen {
			return false
		}
		sort.Slice(bundles, func(i, j int) bool { return bundles[i].ID < bundles[j].ID })
		sort.Slice(expectedBundles, func(i, j int) bool { return expectedBundles[i].ID < expectedBundles[j].ID })
		for i := range bundles {
			if !compareBundle(bundles[i], expectedBundles[i]) {
				return false
			}
		}
		return true
	})
}

func compareBundle(b1, b2 placement.GroupBundle) bool {
	if b2.ID != b1.ID || b2.Index != b1.Index || b2.Override != b1.Override || len(b2.Rules) != len(b1.Rules) {
		return false
	}
	sort.Slice(b1.Rules, func(i, j int) bool { return b1.Rules[i].ID < b1.Rules[j].ID })
	sort.Slice(b2.Rules, func(i, j int) bool { return b2.Rules[i].ID < b2.Rules[j].ID })
	for i := range b1.Rules {
		if !compareRule(b1.Rules[i], b2.Rules[i]) {
			return false
		}
	}
	return true
}

func compareRule(r1 *placement.Rule, r2 *placement.Rule) bool {
	return r2.GroupID == r1.GroupID &&
		r2.ID == r1.ID &&
		r2.StartKeyHex == r1.StartKeyHex &&
		r2.EndKeyHex == r1.EndKeyHex &&
		r2.Role == r1.Role &&
		r2.Count == r1.Count
}

func (suite *ruleTestSuite) postAndCheckRuleBundle(urlPrefix string, bundle []placement.GroupBundle) {
	re := suite.Require()
	data, err := json.Marshal(bundle)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	re.NoError(err)

	tu.Eventually(re, func() bool {
		respBundle := make([]placement.GroupBundle, 0)
		err = tu.CheckGetJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", nil,
			tu.StatusOK(re), tu.ExtractJSON(re, &respBundle))
		re.NoError(err)
		if len(respBundle) != len(bundle) {
			return false
		}
		sort.Slice(respBundle, func(i, j int) bool { return respBundle[i].ID < respBundle[j].ID })
		sort.Slice(bundle, func(i, j int) bool { return bundle[i].ID < bundle[j].ID })
		for i := range respBundle {
			if !compareBundle(respBundle[i], bundle[i]) {
				return false
			}
		}
		return true
	})
}

type regionRuleTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestRegionRuleTestSuite(t *testing.T) {
	suite.Run(t, new(regionRuleTestSuite))
}

func (suite *regionRuleTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T(), func(conf *config.Config, _ string) {
		conf.Replication.EnablePlacementRules = true
		conf.Replication.MaxReplicas = 1
	})
}

func (suite *regionRuleTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *regionRuleTestSuite) TestRegionPlacementRule() {
	suite.env.RunTestBasedOnMode(suite.checkRegionPlacementRule)
}

func (suite *regionRuleTestSuite) checkRegionPlacementRule(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	urlPrefix := fmt.Sprintf("%s%s/api/v1", pdAddr, apiPrefix)

	stores := []*metapb.Store{
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
	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}
	regions := make([]*core.RegionInfo, 0)
	peers1 := []*metapb.Peer{
		{Id: 102, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 103, StoreId: 2, Role: metapb.PeerRole_Voter}}
	regions = append(regions, core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers1, RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}}, peers1[0],
		core.WithStartKey([]byte("abc")), core.WithEndKey([]byte("def"))))
	peers2 := []*metapb.Peer{
		{Id: 104, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 105, StoreId: 2, Role: metapb.PeerRole_Learner}}
	regions = append(regions, core.NewRegionInfo(&metapb.Region{Id: 2, Peers: peers2, RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2}}, peers2[0],
		core.WithStartKey([]byte("ghi")), core.WithEndKey([]byte("jkl"))))
	peers3 := []*metapb.Peer{
		{Id: 106, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 107, StoreId: 2, Role: metapb.PeerRole_Learner}}
	regions = append(regions, core.NewRegionInfo(&metapb.Region{Id: 3, Peers: peers3, RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 3}}, peers3[0],
		core.WithStartKey([]byte("mno")), core.WithEndKey([]byte("pqr"))))
	for _, rg := range regions {
		tests.MustPutRegionInfo(re, cluster, rg)
	}

	ruleManager := leaderServer.GetRaftCluster().GetRuleManager()
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
	fit := &placement.RegionFit{}

	u := fmt.Sprintf("%s/config/rules/region/%d/detail", urlPrefix, 1)
	err := tu.ReadGetJSON(re, tests.TestDialClient, u, fit)
	re.NoError(err)
	re.Len(fit.RuleFits, 1)
	re.Len(fit.OrphanPeers, 1)
	u = fmt.Sprintf("%s/config/rules/region/%d/detail", urlPrefix, 2)
	fit = &placement.RegionFit{}
	err = tu.ReadGetJSON(re, tests.TestDialClient, u, fit)
	re.NoError(err)
	re.Len(fit.RuleFits, 2)
	re.Empty(fit.OrphanPeers)
	u = fmt.Sprintf("%s/config/rules/region/%d/detail", urlPrefix, 3)
	fit = &placement.RegionFit{}
	err = tu.ReadGetJSON(re, tests.TestDialClient, u, fit)
	re.NoError(err)
	re.Empty(fit.RuleFits)
	re.Len(fit.OrphanPeers, 2)

	var label labeler.LabelRule
	escapedID := url.PathEscape("keyspaces/0")
	u = fmt.Sprintf("%s/config/region-label/rule/%s", urlPrefix, escapedID)
	err = tu.ReadGetJSON(re, tests.TestDialClient, u, &label)
	re.NoError(err)
	re.Equal("keyspaces/0", label.ID)

	var labels []labeler.LabelRule
	u = fmt.Sprintf("%s/config/region-label/rules", urlPrefix)
	err = tu.ReadGetJSON(re, tests.TestDialClient, u, &labels)
	re.NoError(err)
	re.Len(labels, 1)
	re.Equal("keyspaces/0", labels[0].ID)

	u = fmt.Sprintf("%s/config/region-label/rules/ids", urlPrefix)
	err = tu.CheckGetJSON(tests.TestDialClient, u, []byte(`["rule1", "rule3"]`), func(resp []byte, _ int, _ http.Header) {
		err := json.Unmarshal(resp, &labels)
		re.NoError(err)
		re.Empty(labels)
	})
	re.NoError(err)

	err = tu.CheckGetJSON(tests.TestDialClient, u, []byte(`["keyspaces/0"]`), func(resp []byte, _ int, _ http.Header) {
		err := json.Unmarshal(resp, &labels)
		re.NoError(err)
		re.Len(labels, 1)
		re.Equal("keyspaces/0", labels[0].ID)
	})
	re.NoError(err)

	u = fmt.Sprintf("%s/config/rules/region/%d/detail", urlPrefix, 4)
	err = tu.CheckGetJSON(tests.TestDialClient, u, nil, tu.Status(re, http.StatusNotFound), tu.StringContain(
		re, "region 4 not found"))
	re.NoError(err)

	u = fmt.Sprintf("%s/config/rules/region/%s/detail", urlPrefix, "id")
	err = tu.CheckGetJSON(tests.TestDialClient, u, nil, tu.Status(re, http.StatusBadRequest), tu.StringContain(
		re, errs.ErrRegionInvalidID.Error()))
	re.NoError(err)

	data := make(map[string]any)
	data["enable-placement-rules"] = "false"
	reqData, e := json.Marshal(data)
	re.NoError(e)
	u = fmt.Sprintf("%s/config", urlPrefix)
	err = tu.CheckPostJSON(tests.TestDialClient, u, reqData, tu.StatusOK(re))
	re.NoError(err)
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		// wait for the scheduling server to update the config
		tu.Eventually(re, func() bool {
			return !sche.GetCluster().GetCheckerConfig().IsPlacementRulesEnabled()
		})
	}
	u = fmt.Sprintf("%s/config/rules/region/%d/detail", urlPrefix, 1)
	err = tu.CheckGetJSON(tests.TestDialClient, u, nil, tu.Status(re, http.StatusPreconditionFailed), tu.StringContain(
		re, "placement rules feature is disabled"))
	re.NoError(err)
}
