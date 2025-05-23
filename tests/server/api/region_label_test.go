// Copyright 2021 TiKV Project Authors.
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
	"net/url"
	"sort"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/schedule/labeler"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type regionLabelTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestRegionLabelTestSuite(t *testing.T) {
	suite.Run(t, new(regionLabelTestSuite))
}

func (suite *regionLabelTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *regionLabelTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.env.Cleanup()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func (suite *regionLabelTestSuite) TestGetSet() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetSet)
}

func (suite *regionLabelTestSuite) checkGetSet(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1/config/region-label"

	var resp []*labeler.LabelRule
	err := tu.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rules", &resp)
	re.NoError(err)
	re.Empty(resp)

	rules := []*labeler.LabelRule{
		{ID: "rule1", Labels: []labeler.RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: makeKeyRanges("1234", "5678")},
		{ID: "rule2/a/b", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []labeler.RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: makeKeyRanges("abcd", "efef")},
	}
	ruleIDs := []string{"rule1", "rule2/a/b", "rule3"}
	for _, rule := range rules {
		data, _ := json.Marshal(rule)
		err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, tu.StatusOK(re))
		re.NoError(err)
	}
	for i, id := range ruleIDs {
		var rule labeler.LabelRule
		err = tu.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rule/"+url.QueryEscape(id), &rule)
		re.NoError(err)
		re.Equal(rules[i], &rule)
	}

	err = tu.ReadGetJSONWithBody(re, tests.TestDialClient, urlPrefix+"/rules/ids", []byte(`["rule1", "rule3"]`), &resp)
	re.NoError(err)
	expects := []*labeler.LabelRule{rules[0], rules[2]}
	re.Equal(expects, resp)

	err = tu.CheckDelete(tests.TestDialClient, urlPrefix+"/rule/"+url.QueryEscape("rule2/a/b"), tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rules", &resp)
	re.NoError(err)
	sort.Slice(resp, func(i, j int) bool { return resp[i].ID < resp[j].ID })
	re.Equal([]*labeler.LabelRule{rules[0], rules[2]}, resp)

	patch := labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{
			{ID: "rule2/a/b", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		},
		DeleteRules: []string{"rule1"},
	}
	data, _ := json.Marshal(patch)
	err = tu.CheckPatchJSON(tests.TestDialClient, urlPrefix+"/rules", data, tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rules", &resp)
	re.NoError(err)
	sort.Slice(resp, func(i, j int) bool { return resp[i].ID < resp[j].ID })
	re.Equal([]*labeler.LabelRule{rules[1], rules[2]}, resp)
}

func makeKeyRanges(keys ...string) []any {
	var res []any
	for i := 0; i < len(keys); i += 2 {
		res = append(res, map[string]any{"start_key": keys[i], "end_key": keys[i+1]})
	}
	return res
}
