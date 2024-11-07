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

package labeler

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestAdjustRule(t *testing.T) {
	re := require.New(t)
	rule := LabelRule{
		ID: "foo",
		Labels: []RegionLabel{
			{Key: "k1", Value: "v1"},
		},
		RuleType: "key-range",
		Data:     MakeKeyRanges("12abcd", "34cdef", "56abcd", "78cdef"),
	}
	err := rule.checkAndAdjust()
	re.NoError(err)
	re.Len(rule.Data.([]*KeyRangeRule), 2)
	re.Equal([]byte{0x12, 0xab, 0xcd}, rule.Data.([]*KeyRangeRule)[0].StartKey)
	re.Equal([]byte{0x34, 0xcd, 0xef}, rule.Data.([]*KeyRangeRule)[0].EndKey)
	re.Equal([]byte{0x56, 0xab, 0xcd}, rule.Data.([]*KeyRangeRule)[1].StartKey)
	re.Equal([]byte{0x78, 0xcd, 0xef}, rule.Data.([]*KeyRangeRule)[1].EndKey)
}

func TestAdjustRule2(t *testing.T) {
	re := require.New(t)
	ruleData := `{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`
	var rule LabelRule
	err := json.Unmarshal([]byte(ruleData), &rule)
	re.NoError(err)
	err = rule.checkAndAdjust()
	re.NoError(err)

	badRuleData := []string{
		// no id
		`{"id":"", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// no labels
		`{"id":"id", "labels": [], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// empty label key
		`{"id":"id", "labels": [{"key": "", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// empty label value
		`{"id":"id", "labels": [{"key": "k1", "value": ""}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// unknown rule type
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"unknown", "data": [{"start_key":"", "end_key":""}]}`,
		// wrong rule content
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": 123}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": {"start_key":"", "end_key":""}}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": []}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":123, "end_key":""}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":123}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"123", "end_key":"abcd"}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"abcd", "end_key":"123"}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"abcd", "end_key":"1234"}]}`,
	}
	for _, str := range badRuleData {
		var rule LabelRule
		err := json.Unmarshal([]byte(str), &rule)
		re.NoError(err)
		err = rule.checkAndAdjust()
		re.Error(err)
	}
}

func TestGetSetRule(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labeler, err := NewRegionLabeler(context.Background(), store, time.Millisecond*10)
	re.NoError(err)
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: MakeKeyRanges("1234", "5678")},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: MakeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: MakeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := labeler.SetLabelRule(r)
		re.NoError(err)
	}

	allRules := labeler.GetAllLabelRules()
	sort.Slice(allRules, func(i, j int) bool { return allRules[i].ID < allRules[j].ID })
	re.Equal(rules, allRules)

	byIDs, err := labeler.GetLabelRules([]string{"rule3", "rule1"})
	re.NoError(err)
	re.Equal([]*LabelRule{rules[2], rules[0]}, byIDs)

	err = labeler.DeleteLabelRule("rule2")
	re.NoError(err)
	re.Nil(labeler.GetLabelRule("rule2"))
	byIDs, err = labeler.GetLabelRules([]string{"rule1", "rule2"})
	re.NoError(err)
	re.Equal([]*LabelRule{rules[0]}, byIDs)

	// patch
	patch := LabelRulePatch{
		SetRules: []*LabelRule{
			{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: MakeKeyRanges("ab12", "cd12")},
		},
		DeleteRules: []string{"rule1"},
	}
	err = labeler.Patch(patch)
	re.NoError(err)
	allRules = labeler.GetAllLabelRules()
	sort.Slice(allRules, func(i, j int) bool { return allRules[i].ID < allRules[j].ID })
	for id, rule := range allRules {
		expectSameRules(re, rule, rules[id+1])
	}

	for _, r := range rules {
		labeler.DeleteLabelRule(r.ID)
	}
	re.Empty(labeler.GetAllLabelRules())
}

func TestTxnWithEtcd(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client, "")
	labeler, err := NewRegionLabeler(context.Background(), store, time.Millisecond*10)
	re.NoError(err)
	// test patch rules in batch
	rulesNum := 200
	patch := LabelRulePatch{}
	for i := 1; i <= rulesNum; i++ {
		patch.SetRules = append(patch.SetRules, &LabelRule{
			ID: fmt.Sprintf("rule_%d", i),
			Labels: []RegionLabel{
				{Key: fmt.Sprintf("k_%d", i), Value: fmt.Sprintf("v_%d", i)},
			},
			RuleType: "key-range",
			Data:     MakeKeyRanges("", ""),
		})
	}
	err = labeler.Patch(patch)
	re.NoError(err)
	allRules := labeler.GetAllLabelRules()
	re.Len(allRules, rulesNum)
	sort.Slice(allRules, func(i, j int) bool {
		i1, err := strconv.Atoi(allRules[i].ID[5:])
		re.NoError(err)
		j1, err := strconv.Atoi(allRules[j].ID[5:])
		re.NoError(err)
		return i1 < j1
	})
	for id, rule := range allRules {
		expectSameRules(re, rule, patch.SetRules[id])
	}
	patch.SetRules = patch.SetRules[:0]
	patch.DeleteRules = patch.DeleteRules[:0]
	for i := 1; i <= rulesNum; i++ {
		patch.DeleteRules = append(patch.DeleteRules, fmt.Sprintf("rule_%d", i))
	}
	err = labeler.Patch(patch)
	re.NoError(err)
	allRules = labeler.GetAllLabelRules()
	re.Empty(allRules)

	// test patch rules in batch with duplicated rule id
	patch.SetRules = patch.SetRules[:0]
	patch.DeleteRules = patch.DeleteRules[:0]
	for i := 0; i <= 3; i++ {
		patch.SetRules = append(patch.SetRules, &LabelRule{
			ID: "rule_1",
			Labels: []RegionLabel{
				{Key: fmt.Sprintf("k_%d", i), Value: fmt.Sprintf("v_%d", i)},
			},
			RuleType: "key-range",
			Data:     MakeKeyRanges("", ""),
		})
	}
	patch.DeleteRules = append(patch.DeleteRules, "rule_1")
	err = labeler.Patch(patch)
	re.NoError(err)
	allRules = labeler.GetAllLabelRules()
	re.Len(allRules, 1)
	re.Equal("rule_1", allRules[0].ID)
	re.Len(allRules[0].Labels, 1)
	re.Equal("k_3", allRules[0].Labels[0].Key)
}

func TestIndex(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labeler, err := NewRegionLabeler(context.Background(), store, time.Millisecond*10)
	re.NoError(err)
	rules := []*LabelRule{
		{ID: "rule0", Labels: []RegionLabel{{Key: "k1", Value: "v0"}}, RuleType: "key-range", Data: MakeKeyRanges("", "")},
		{ID: "rule1", Index: 1, Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: MakeKeyRanges("1234", "5678")},
		{ID: "rule2", Index: 2, Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: MakeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Index: 1, Labels: []RegionLabel{{Key: "k2", Value: "v3"}}, RuleType: "key-range", Data: MakeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := labeler.SetLabelRule(r)
		re.NoError(err)
	}

	type testCase struct {
		start, end string
		labels     map[string]string
	}
	testCases := []testCase{
		{"", "1234", map[string]string{"k1": "v0"}},
		{"1234", "5678", map[string]string{"k1": "v1"}},
		{"ab12", "abcd", map[string]string{"k1": "v0", "k2": "v2"}},
		{"abcd", "cd12", map[string]string{"k1": "v0", "k2": "v2"}},
		{"cdef", "efef", map[string]string{"k1": "v0", "k2": "v3"}},
	}
	for _, testCase := range testCases {
		start, _ := hex.DecodeString(testCase.start)
		end, _ := hex.DecodeString(testCase.end)
		region := core.NewTestRegionInfo(1, 1, start, end)
		labels := labeler.GetRegionLabels(region)
		re.Len(labels, len(testCase.labels))
		for _, l := range labels {
			re.Equal(testCase.labels[l.Key], l.Value)
		}
		for _, k := range []string{"k1", "k2"} {
			re.Equal(testCase.labels[k], labeler.GetRegionLabel(region, k))
		}
	}
}

func TestSaveLoadRule(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labeler, err := NewRegionLabeler(context.Background(), store, time.Millisecond*10)
	re.NoError(err)
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: MakeKeyRanges("1234", "5678")},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: MakeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: MakeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := labeler.SetLabelRule(r)
		re.NoError(err)
	}

	labeler, err = NewRegionLabeler(context.Background(), store, time.Millisecond*100)
	re.NoError(err)
	for _, r := range rules {
		r2 := labeler.GetLabelRule(r.ID)
		expectSameRules(re, r2, r)
	}
}

func expectSameRegionLabels(re *require.Assertions, r1, r2 *RegionLabel) {
	r1.checkAndAdjustExpire()
	r2.checkAndAdjustExpire()
	if len(r1.TTL) == 0 {
		re.Equal(r1, r2)
	}

	r2.StartAt = r1.StartAt
	r2.checkAndAdjustExpire()

	re.Equal(r1, r2)
}

func expectSameRules(re *require.Assertions, r1, r2 *LabelRule) {
	re.Len(r1.Labels, len(r2.Labels))
	for id := range r1.Labels {
		expectSameRegionLabels(re, &r1.Labels[id], &r2.Labels[id])
	}

	re.Equal(r1, r2)
}

func TestKeyRange(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labeler, err := NewRegionLabeler(context.Background(), store, time.Millisecond*10)
	re.NoError(err)
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: MakeKeyRanges("1234", "5678")},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: MakeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: MakeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := labeler.SetLabelRule(r)
		re.NoError(err)
	}

	type testCase struct {
		start, end string
		labels     map[string]string
	}
	testCases := []testCase{
		{"1234", "5678", map[string]string{"k1": "v1"}},
		{"1234", "aaaa", map[string]string{}},
		{"abcd", "abff", map[string]string{"k2": "v2", "k3": "v3"}},
		{"cd12", "dddd", map[string]string{"k3": "v3"}},
		{"ffee", "ffff", map[string]string{}},
	}
	for _, testCase := range testCases {
		start, _ := hex.DecodeString(testCase.start)
		end, _ := hex.DecodeString(testCase.end)
		region := core.NewTestRegionInfo(1, 1, start, end)
		labels := labeler.GetRegionLabels(region)
		re.Len(labels, len(testCase.labels))
		for _, l := range labels {
			re.Equal(l.Value, testCase.labels[l.Key])
		}
		for _, k := range []string{"k1", "k2", "k3"} {
			re.Equal(testCase.labels[k], labeler.GetRegionLabel(region, k))
		}
	}
}

func TestLabelerRuleTTL(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labeler, err := NewRegionLabeler(context.Background(), store, time.Minute)
	re.NoError(err)
	rules := []*LabelRule{
		{
			ID: "rule1",
			Labels: []RegionLabel{
				{Key: "k1", Value: "v1"},
			},
			RuleType: "key-range",
			Data:     MakeKeyRanges("1234", "5678")},
		{
			ID: "rule2",
			Labels: []RegionLabel{
				{Key: "k2", Value: "v2", TTL: "1s"}, // would expire first.
			},
			RuleType: "key-range",

			Data: MakeKeyRanges("1234", "5678")},

		{
			ID:       "rule3",
			Labels:   []RegionLabel{{Key: "k3", Value: "v3", TTL: "1h"}},
			RuleType: "key-range",
			Data:     MakeKeyRanges("1234", "5678")},
	}

	start, _ := hex.DecodeString("1234")
	end, _ := hex.DecodeString("5678")
	region := core.NewTestRegionInfo(1, 1, start, end)
	// the region has no label rule at the beginning.
	re.Empty(labeler.GetRegionLabels(region))

	// set rules for the region.
	for _, r := range rules {
		err := labeler.SetLabelRule(r)
		re.NoError(err)
	}
	// get rule with "rule2".
	re.NotNil(labeler.GetLabelRule("rule2"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/labeler/regionLabelExpireSub1Minute", "return(true)"))

	// rule2 should expire and only 2 labels left.
	testutil.Eventually(re, func() bool {
		labels := labeler.GetRegionLabels(region)
		return len(labels) == 2
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/labeler/regionLabelExpireSub1Minute"))
	// rule2 should be existed since `GetRegionLabels` won't clear it physically.
	checkRuleInMemoryAndStorage(re, labeler, "rule2", true)
	re.Nil(labeler.GetLabelRule("rule2"))
	// rule2 should be physically clear.
	checkRuleInMemoryAndStorage(re, labeler, "rule2", false)

	re.Equal("", labeler.GetRegionLabel(region, "k2"))

	re.NotNil(labeler.GetLabelRule("rule3"))
	re.NotNil(labeler.GetLabelRule("rule1"))
}

func checkRuleInMemoryAndStorage(re *require.Assertions, labeler *RegionLabeler, ruleID string, exist bool) {
	re.Equal(exist, labeler.labelRules[ruleID] != nil)
	existInStorage := false
	labeler.storage.LoadRegionRules(func(k, _ string) {
		if k == ruleID {
			existInStorage = true
		}
	})
	re.Equal(exist, existInStorage)
}

func TestGC(t *testing.T) {
	re := require.New(t)
	// set gcInterval to 1 hour.
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labeler, err := NewRegionLabeler(context.Background(), store, time.Hour)
	re.NoError(err)
	ttls := []string{"1ms", "1ms", "1ms", "5ms", "5ms", "10ms", "1h", "24h"}
	start, _ := hex.DecodeString("1234")
	end, _ := hex.DecodeString("5678")
	region := core.NewTestRegionInfo(1, 1, start, end)
	// the region has no label rule at the beginning.
	re.Empty(labeler.GetRegionLabels(region))

	labels := make([]RegionLabel, 0, len(ttls))
	for id, ttl := range ttls {
		labels = append(labels, RegionLabel{Key: fmt.Sprintf("k%d", id), Value: fmt.Sprintf("v%d", id), TTL: ttl})
		rule := &LabelRule{
			ID:       fmt.Sprintf("rule%d", id),
			Labels:   labels,
			RuleType: "key-range",
			Data:     MakeKeyRanges("1234", "5678")}
		err := labeler.SetLabelRule(rule)
		re.NoError(err)
	}

	re.Len(labeler.labelRules, len(ttls))

	// check all rules until some rule expired.
	for {
		time.Sleep(time.Millisecond * 5)
		labels := labeler.GetRegionLabels(region)
		if len(labels) != len(ttls) {
			break
		}
	}

	// no rule was cleared because the gc interval is big.
	re.Len(labeler.labelRules, len(ttls))

	labeler.checkAndClearExpiredLabels()

	labeler.RLock()
	currentRuleLen := len(labeler.labelRules)
	labeler.RUnlock()
	re.LessOrEqual(currentRuleLen, 5)
}
