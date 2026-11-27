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
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
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
	err := testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rules", &resp)
	re.NoError(err)
	re.Empty(resp)

	rules := []*labeler.LabelRule{
		{ID: "rule1", Labels: []labeler.RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: keyutil.BuildKeyRangeMaps("1234", "5678")},
		{ID: "rule2/a/b", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: keyutil.BuildKeyRangeMaps("ab12", "cd12")},
		{ID: "rule3", Labels: []labeler.RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: keyutil.BuildKeyRangeMaps("abcd", "efef")},
	}
	ruleIDs := []string{"rule1", "rule2/a/b", "rule3"}
	for _, rule := range rules {
		data, err := json.Marshal(rule)
		re.NoError(err)
		err = testutil.CheckPostJSON(tests.TestDialClient, urlPrefix+"/rule", data, testutil.StatusOK(re))
		re.NoError(err)
	}
	for i, id := range ruleIDs {
		var rule labeler.LabelRule
		err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rule/"+url.QueryEscape(id), &rule)
		re.NoError(err)
		re.Equal(rules[i], &rule)
	}

	err = testutil.ReadGetJSONWithBody(re, tests.TestDialClient, urlPrefix+"/rules/ids", []byte(`["rule1", "rule3"]`), &resp)
	re.NoError(err)
	expects := []*labeler.LabelRule{rules[0], rules[2]}
	re.Equal(expects, resp)

	err = testutil.CheckDelete(tests.TestDialClient, urlPrefix+"/rule/"+url.QueryEscape("rule2/a/b"), testutil.StatusOK(re))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rules", &resp)
	re.NoError(err)
	sort.Slice(resp, func(i, j int) bool { return resp[i].ID < resp[j].ID })
	re.Equal([]*labeler.LabelRule{rules[0], rules[2]}, resp)

	patch := labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{
			{ID: "rule2/a/b", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: keyutil.BuildKeyRangeMaps("ab12", "cd12")},
		},
		DeleteRules: []string{"rule1"},
	}
	data, err := json.Marshal(patch)
	re.NoError(err)
	err = testutil.CheckPatchJSON(tests.TestDialClient, urlPrefix+"/rules", data, testutil.StatusOK(re))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/rules", &resp)
	re.NoError(err)
	sort.Slice(resp, func(i, j int) bool { return resp[i].ID < resp[j].ID })
	re.Equal([]*labeler.LabelRule{rules[1], rules[2]}, resp)

	err = testutil.CheckDelete(tests.TestDialClient, urlPrefix+"/rule/no-exist", testutil.Status(re, http.StatusNotFound))
	re.NoError(err)
}

func (suite *regionLabelTestSuite) TestAffinityRuleWriteProtection() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		prefix := leader.GetAddr() + "/pd/api/v1/config/region-label"
		waitLabelRuleSync := func(id string) {
			if scheduling := cluster.GetSchedulingPrimaryServer(); scheduling != nil {
				testutil.Eventually(re, func() bool {
					return reflect.DeepEqual(
						leader.GetRaftCluster().GetRegionLabeler().GetLabelRule(id),
						scheduling.GetCluster().GetRegionLabeler().GetLabelRule(id),
					)
				})
			}
		}
		affinityURL := leader.GetAddr() + "/pd/api/v2/affinity-groups"
		const groupID = "protected-group"
		ruleID := affinity.GetLabelRuleID(groupID)
		ruleURL := prefix + "/rule/" + url.QueryEscape(ruleID)
		// In MCS mode, GET reads the scheduling service's cache, which is updated by an etcd watcher.
		waitForRule := func(ruleURL string, matches func(*assert.Assertions, *labeler.LabelRule) bool) labeler.LabelRule {
			var observed labeler.LabelRule
			testutil.Eventually(re, func() bool {
				var rule labeler.LabelRule
				var decoded bool
				err := testutil.CheckGetJSON(tests.TestDialClient, ruleURL, nil, func(body []byte, status int, _ http.Header) {
					decoded = status == http.StatusOK && json.Unmarshal(body, &rule) == nil
				})
				if err != nil || !decoded || !matches(assert.New(&assert.CollectT{}), &rule) {
					return false
				}
				observed = rule
				return true
			})
			return observed
		}

		createRange := testutil.TxnKeyspaceRange([]byte{0x10}, []byte{0x20})
		addRange := testutil.TxnKeyspaceRange([]byte{0x20}, []byte{0x30})
		data, err := json.Marshal(handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				groupID: {Ranges: []handlers.AffinityKeyRange{{StartKey: createRange.StartKey, EndKey: createRange.EndKey}}},
			},
		})
		re.NoError(err)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, affinityURL, data, testutil.StatusOK(re)))
		waitLabelRuleSync(ruleID)
		createRangeData := keyutil.BuildKeyRangeMaps(
			hex.EncodeToString(createRange.StartKey),
			hex.EncodeToString(createRange.EndKey),
		)
		original := waitForRule(ruleURL, func(check *assert.Assertions, rule *labeler.LabelRule) bool {
			return check.ElementsMatch(createRangeData, rule.Data)
		})

		ordinary := &labeler.LabelRule{
			ID: "ordinary-protected-batch", Labels: []labeler.RegionLabel{{Key: "k", Value: "v"}},
			RuleType: labeler.KeyRange, Data: keyutil.BuildKeyRangeMaps("30", "40"),
		}
		data, err = json.Marshal(ordinary)
		re.NoError(err)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, prefix+"/rule", data, testutil.StatusOK(re)))
		waitLabelRuleSync(ordinary.ID)
		unwritten := *ordinary
		unwritten.ID = "ordinary-rejected-batch"
		expectedData, err := json.Marshal(errs.ErrAffinityLabelProtected.FastGenByArgs())
		re.NoError(err)
		protectedErrorChecks := []func([]byte, int, http.Header){
			testutil.Status(re, http.StatusBadRequest),
			testutil.WithHeader(re, "TiDB-Error-Code", "input"),
			func(body []byte, _ int, _ http.Header) {
				var response struct {
					Code string          `json:"code"`
					Msg  string          `json:"msg"`
					Data json.RawMessage `json:"data"`
				}
				re.NoError(json.Unmarshal(body, &response))
				re.Equal("input", response.Code)
				re.Contains(response.Msg, "PD:affinity:ErrAffinityLabelProtected")
				re.Contains(response.Msg, "affinity API")
				re.JSONEq(string(expectedData), string(response.Data))
			},
		}

		for _, rule := range []*labeler.LabelRule{
			{ID: affinity.GetLabelRuleID("missing"), Labels: ordinary.Labels, RuleType: labeler.KeyRange, Data: ordinary.Data},
			{ID: "affinity-alias", Labels: []labeler.RegionLabel{{Key: affinity.AffinityLabelKey, Value: groupID}}, RuleType: labeler.KeyRange, Data: ordinary.Data},
			{ID: ruleID, Labels: ordinary.Labels, RuleType: labeler.KeyRange, Data: ordinary.Data},
		} {
			data, err = json.Marshal(rule)
			re.NoError(err)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, prefix+"/rule", data, protectedErrorChecks...))
			// Reject ordinary writes and deletions in the same batch too.
			data, err = json.Marshal(labeler.LabelRulePatch{SetRules: []*labeler.LabelRule{&unwritten, rule}, DeleteRules: []string{ordinary.ID}})
			re.NoError(err)
			re.NoError(testutil.CheckPatchJSON(tests.TestDialClient, prefix+"/rules", data, protectedErrorChecks...))
		}
		re.NoError(testutil.CheckDelete(tests.TestDialClient, ruleURL, protectedErrorChecks...))
		data, err = json.Marshal(labeler.LabelRulePatch{SetRules: []*labeler.LabelRule{&unwritten}, DeleteRules: []string{ordinary.ID, ruleID}})
		re.NoError(err)
		re.NoError(testutil.CheckPatchJSON(tests.TestDialClient, prefix+"/rules", data, protectedErrorChecks...))
		re.NoError(testutil.CheckGetJSON(tests.TestDialClient, prefix+"/rule/ordinary-rejected-batch", nil, testutil.Status(re, http.StatusNotFound)))
		waitForRule(prefix+"/rule/"+ordinary.ID, func(check *assert.Assertions, rule *labeler.LabelRule) bool {
			return check.Equal(ordinary, rule)
		})

		// Custom IDs are not owned by the affinity manager. Legacy rules may
		// be replaced with ordinary labels or deleted through either endpoint.
		for _, batch := range []bool{false, true} {
			leftover := &labeler.LabelRule{
				ID: "affinity-leftover", Labels: []labeler.RegionLabel{{Key: affinity.AffinityLabelKey, Value: "leftover"}},
				RuleType: labeler.KeyRange, Data: keyutil.BuildKeyRangeMaps("50", "60"),
			}
			re.NoError(leader.GetRaftCluster().GetRegionLabeler().SetLabelRule(leftover))
			replacement := &labeler.LabelRule{ID: leftover.ID, Labels: ordinary.Labels, RuleType: labeler.KeyRange, Data: ordinary.Data}
			data, err = json.Marshal(replacement)
			re.NoError(err)
			if batch {
				data, err = json.Marshal(labeler.LabelRulePatch{SetRules: []*labeler.LabelRule{replacement}})
				re.NoError(err)
				re.NoError(testutil.CheckPatchJSON(tests.TestDialClient, prefix+"/rules", data, testutil.StatusOK(re)))
			} else {
				re.NoError(testutil.CheckPostJSON(tests.TestDialClient, prefix+"/rule", data, testutil.StatusOK(re)))
			}
			leftoverURL := prefix + "/rule/" + leftover.ID
			waitLabelRuleSync(leftover.ID)
			waitForRule(leftoverURL, func(check *assert.Assertions, rule *labeler.LabelRule) bool {
				return check.Equal(ordinary.Labels, rule.Labels)
			})
			leftover.Data = keyutil.BuildKeyRangeMaps("50", "60")
			re.NoError(leader.GetRaftCluster().GetRegionLabeler().SetLabelRule(leftover))
			if batch {
				data, err = json.Marshal(labeler.LabelRulePatch{DeleteRules: []string{leftover.ID}})
				re.NoError(err)
				re.NoError(testutil.CheckPatchJSON(tests.TestDialClient, prefix+"/rules", data, testutil.StatusOK(re)))
			} else {
				re.NoError(testutil.CheckDelete(tests.TestDialClient, leftoverURL, testutil.StatusOK(re)))
			}
			waitLabelRuleSync(leftover.ID)
			re.NoError(testutil.CheckGetUntilStatusCode(re, tests.TestDialClient, leftoverURL, http.StatusNotFound))
		}
		var kept labeler.LabelRule
		re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, ruleURL, &kept))
		re.Equal(original, kept)

		// The owning API can still update its protected label rule's ranges.
		data, err = json.Marshal(handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: groupID, Ranges: []handlers.AffinityKeyRange{{StartKey: addRange.StartKey, EndKey: addRange.EndKey}}},
			},
		})
		re.NoError(err)
		re.NoError(testutil.CheckPatchJSON(tests.TestDialClient, affinityURL, data, testutil.StatusOK(re)))
		waitLabelRuleSync(ruleID)
		updatedRanges := keyutil.BuildKeyRangeMaps(
			hex.EncodeToString(createRange.StartKey),
			hex.EncodeToString(createRange.EndKey),
			hex.EncodeToString(addRange.StartKey),
			hex.EncodeToString(addRange.EndKey),
		)
		waitForRule(ruleURL, func(check *assert.Assertions, rule *labeler.LabelRule) bool {
			return check.Equal(original.Labels, rule.Labels) && check.ElementsMatch(updatedRanges, rule.Data)
		})

		re.NoError(testutil.CheckDelete(tests.TestDialClient, affinityURL+"/"+groupID+"?force=true", testutil.StatusOK(re)))
		waitLabelRuleSync(ruleID)
		re.NoError(testutil.CheckGetUntilStatusCode(re, tests.TestDialClient, ruleURL, http.StatusNotFound))
		re.NoError(testutil.CheckDelete(tests.TestDialClient, prefix+"/rule/"+ordinary.ID, testutil.StatusOK(re)))
		waitLabelRuleSync(ordinary.ID)
	})
}
