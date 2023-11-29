// Copyright 2023 TiKV Project Authors.
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

package http

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeRegionsInfo(t *testing.T) {
	re := require.New(t)
	regionsInfo1 := &RegionsInfo{
		Count: 1,
		Regions: []RegionInfo{
			{
				ID:       1,
				StartKey: "",
				EndKey:   "a",
			},
		},
	}
	regionsInfo2 := &RegionsInfo{
		Count: 1,
		Regions: []RegionInfo{
			{
				ID:       2,
				StartKey: "a",
				EndKey:   "",
			},
		},
	}
	regionsInfo := regionsInfo1.Merge(regionsInfo2)
	re.Equal(int64(2), regionsInfo.Count)
	re.Equal(2, len(regionsInfo.Regions))
	re.Equal(append(regionsInfo1.Regions, regionsInfo2.Regions...), regionsInfo.Regions)
}

func TestRuleStartEndKey(t *testing.T) {
	re := require.New(t)
	// Empty start/end key and key hex.
	ruleToMarshal := &Rule{}
	rule := mustMarshalAndUnmarshal(re, ruleToMarshal)
	re.Equal("", rule.StartKeyHex)
	re.Equal("", rule.EndKeyHex)
	re.Equal([]byte(""), rule.StartKey)
	re.Equal([]byte(""), rule.EndKey)
	// Empty start/end key and non-empty key hex.
	ruleToMarshal = &Rule{
		StartKeyHex: rawKeyToKeyHexStr([]byte("a")),
		EndKeyHex:   rawKeyToKeyHexStr([]byte("b")),
	}
	rule = mustMarshalAndUnmarshal(re, ruleToMarshal)
	re.Equal([]byte("a"), rule.StartKey)
	re.Equal([]byte("b"), rule.EndKey)
	re.Equal(ruleToMarshal.StartKeyHex, rule.StartKeyHex)
	re.Equal(ruleToMarshal.EndKeyHex, rule.EndKeyHex)
	// Non-empty start/end key and empty key hex.
	ruleToMarshal = &Rule{
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}
	rule = mustMarshalAndUnmarshal(re, ruleToMarshal)
	re.Equal(ruleToMarshal.StartKey, rule.StartKey)
	re.Equal(ruleToMarshal.EndKey, rule.EndKey)
	re.Equal(rawKeyToKeyHexStr(ruleToMarshal.StartKey), rule.StartKeyHex)
	re.Equal(rawKeyToKeyHexStr(ruleToMarshal.EndKey), rule.EndKeyHex)
	// Non-empty start/end key and non-empty key hex.
	ruleToMarshal = &Rule{
		StartKey:    []byte("a"),
		EndKey:      []byte("b"),
		StartKeyHex: rawKeyToKeyHexStr([]byte("c")),
		EndKeyHex:   rawKeyToKeyHexStr([]byte("d")),
	}
	rule = mustMarshalAndUnmarshal(re, ruleToMarshal)
	re.Equal([]byte("c"), rule.StartKey)
	re.Equal([]byte("d"), rule.EndKey)
	re.Equal(ruleToMarshal.StartKeyHex, rule.StartKeyHex)
	re.Equal(ruleToMarshal.EndKeyHex, rule.EndKeyHex)
	// Half of each pair of keys is empty.
	ruleToMarshal = &Rule{
		StartKey:  []byte("a"),
		EndKeyHex: rawKeyToKeyHexStr([]byte("d")),
	}
	rule = mustMarshalAndUnmarshal(re, ruleToMarshal)
	re.Equal(ruleToMarshal.StartKey, rule.StartKey)
	re.Equal([]byte("d"), rule.EndKey)
	re.Equal(rawKeyToKeyHexStr(ruleToMarshal.StartKey), rule.StartKeyHex)
	re.Equal(ruleToMarshal.EndKeyHex, rule.EndKeyHex)
}

func mustMarshalAndUnmarshal(re *require.Assertions, rule *Rule) *Rule {
	ruleJSON, err := json.Marshal(rule)
	re.NoError(err)
	var newRule *Rule
	err = json.Unmarshal(ruleJSON, &newRule)
	re.NoError(err)
	return newRule
}

func TestRuleOpStartEndKey(t *testing.T) {
	re := require.New(t)
	// Empty start/end key and key hex.
	ruleOpToMarshal := &RuleOp{
		Rule: &Rule{},
	}
	ruleOp := mustMarshalAndUnmarshalRuleOp(re, ruleOpToMarshal)
	re.Equal("", ruleOp.StartKeyHex)
	re.Equal("", ruleOp.EndKeyHex)
	re.Equal([]byte(""), ruleOp.StartKey)
	re.Equal([]byte(""), ruleOp.EndKey)
	// Empty start/end key and non-empty key hex.
	ruleOpToMarshal = &RuleOp{
		Rule: &Rule{
			StartKeyHex: rawKeyToKeyHexStr([]byte("a")),
			EndKeyHex:   rawKeyToKeyHexStr([]byte("b")),
		},
		Action:           RuleOpAdd,
		DeleteByIDPrefix: true,
	}
	ruleOp = mustMarshalAndUnmarshalRuleOp(re, ruleOpToMarshal)
	re.Equal([]byte("a"), ruleOp.StartKey)
	re.Equal([]byte("b"), ruleOp.EndKey)
	re.Equal(ruleOpToMarshal.StartKeyHex, ruleOp.StartKeyHex)
	re.Equal(ruleOpToMarshal.EndKeyHex, ruleOp.EndKeyHex)
	re.Equal(ruleOpToMarshal.Action, ruleOp.Action)
	re.Equal(ruleOpToMarshal.DeleteByIDPrefix, ruleOp.DeleteByIDPrefix)
	// Non-empty start/end key and empty key hex.
	ruleOpToMarshal = &RuleOp{
		Rule: &Rule{
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
		Action:           RuleOpAdd,
		DeleteByIDPrefix: true,
	}
	ruleOp = mustMarshalAndUnmarshalRuleOp(re, ruleOpToMarshal)
	re.Equal(ruleOpToMarshal.StartKey, ruleOp.StartKey)
	re.Equal(ruleOpToMarshal.EndKey, ruleOp.EndKey)
	re.Equal(rawKeyToKeyHexStr(ruleOpToMarshal.StartKey), ruleOp.StartKeyHex)
	re.Equal(rawKeyToKeyHexStr(ruleOpToMarshal.EndKey), ruleOp.EndKeyHex)
	re.Equal(ruleOpToMarshal.Action, ruleOp.Action)
	re.Equal(ruleOpToMarshal.DeleteByIDPrefix, ruleOp.DeleteByIDPrefix)
	// Non-empty start/end key and non-empty key hex.
	ruleOpToMarshal = &RuleOp{
		Rule: &Rule{
			StartKey:    []byte("a"),
			EndKey:      []byte("b"),
			StartKeyHex: rawKeyToKeyHexStr([]byte("c")),
			EndKeyHex:   rawKeyToKeyHexStr([]byte("d")),
		},
		Action:           RuleOpAdd,
		DeleteByIDPrefix: true,
	}
	ruleOp = mustMarshalAndUnmarshalRuleOp(re, ruleOpToMarshal)
	re.Equal([]byte("c"), ruleOp.StartKey)
	re.Equal([]byte("d"), ruleOp.EndKey)
	re.Equal(ruleOpToMarshal.StartKeyHex, ruleOp.StartKeyHex)
	re.Equal(ruleOpToMarshal.EndKeyHex, ruleOp.EndKeyHex)
	re.Equal(ruleOpToMarshal.Action, ruleOp.Action)
	re.Equal(ruleOpToMarshal.DeleteByIDPrefix, ruleOp.DeleteByIDPrefix)
	// Half of each pair of keys is empty.
	ruleOpToMarshal = &RuleOp{
		Rule: &Rule{
			StartKey:  []byte("a"),
			EndKeyHex: rawKeyToKeyHexStr([]byte("d")),
		},
		Action:           RuleOpDel,
		DeleteByIDPrefix: false,
	}
	ruleOp = mustMarshalAndUnmarshalRuleOp(re, ruleOpToMarshal)
	re.Equal(ruleOpToMarshal.StartKey, ruleOp.StartKey)
	re.Equal([]byte("d"), ruleOp.EndKey)
	re.Equal(rawKeyToKeyHexStr(ruleOpToMarshal.StartKey), ruleOp.StartKeyHex)
	re.Equal(ruleOpToMarshal.EndKeyHex, ruleOp.EndKeyHex)
	re.Equal(ruleOpToMarshal.Action, ruleOp.Action)
	re.Equal(ruleOpToMarshal.DeleteByIDPrefix, ruleOp.DeleteByIDPrefix)
}

func mustMarshalAndUnmarshalRuleOp(re *require.Assertions, ruleOp *RuleOp) *RuleOp {
	ruleOpJSON, err := json.Marshal(ruleOp)
	re.NoError(err)
	var newRuleOp *RuleOp
	err = json.Unmarshal(ruleOpJSON, &newRuleOp)
	re.NoError(err)
	return newRuleOp
}
