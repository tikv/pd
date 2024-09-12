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
	testCases := []struct {
		source *RegionsInfo
		target *RegionsInfo
	}{
		// Different regions.
		{
			source: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       1,
						StartKey: "",
						EndKey:   "a",
					},
				},
			},
			target: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       2,
						StartKey: "a",
						EndKey:   "",
					},
				},
			},
		},
		// Same region.
		{
			source: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       1,
						StartKey: "",
						EndKey:   "a",
					},
				},
			},
			target: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       1,
						StartKey: "",
						EndKey:   "a",
					},
				},
			},
		},
		{
			source: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       1,
						StartKey: "",
						EndKey:   "a",
					},
				},
			},
			target: nil,
		},
		{
			source: nil,
			target: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       2,
						StartKey: "a",
						EndKey:   "",
					},
				},
			},
		},
		{
			source: nil,
			target: nil,
		},
		{
			source: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       1,
						StartKey: "",
						EndKey:   "a",
					},
				},
			},
			target: newRegionsInfo(0),
		},
		{
			source: newRegionsInfo(0),
			target: &RegionsInfo{
				Count: 1,
				Regions: []RegionInfo{
					{
						ID:       2,
						StartKey: "a",
						EndKey:   "",
					},
				},
			},
		},
		{
			source: newRegionsInfo(0),
			target: newRegionsInfo(0),
		},
	}
	for idx, tc := range testCases {
		regionsInfo := tc.source.Merge(tc.target)
		if tc.source == nil {
			tc.source = newRegionsInfo(0)
		}
		if tc.target == nil {
			tc.target = newRegionsInfo(0)
		}
		m := make(map[int64]RegionInfo, tc.source.Count+tc.target.Count)
		for _, region := range tc.source.Regions {
			m[region.ID] = region
		}
		for _, region := range tc.target.Regions {
			m[region.ID] = region
		}
		mergedCount := len(m)
		re.Equal(int64(mergedCount), regionsInfo.Count, "case %d", idx)
		re.Len(regionsInfo.Regions, mergedCount, "case %d", idx)
		// All regions in source and target should be in the merged result.
		for _, region := range append(tc.source.Regions, tc.target.Regions...) {
			re.Contains(regionsInfo.Regions, region, "case %d", idx)
		}
	}
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

// startKey and endKey are json:"-" which means cannot be Unmarshal from json
// We need to take care of `Clone` method.
func TestRuleKeyClone(t *testing.T) {
	re := require.New(t)
	r := &Rule{
		StartKey: []byte{1, 2, 3},
		EndKey:   []byte{4, 5, 6},
	}

	clone := r.Clone()
	// Modify the original rule
	r.StartKey[0] = 9
	r.EndKey[0] = 9

	// The clone should not be affected
	re.Equal([]byte{1, 2, 3}, clone.StartKey)
	re.Equal([]byte{4, 5, 6}, clone.EndKey)

	// Modify the clone
	clone.StartKey[0] = 8
	clone.EndKey[0] = 8

	// The original rule should not be affected
	re.Equal([]byte{9, 2, 3}, r.StartKey)
	re.Equal([]byte{9, 5, 6}, r.EndKey)
}
