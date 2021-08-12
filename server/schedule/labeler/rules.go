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
// See the License for the specific language governing permissions and
// limitations under the License.

package labeler

import (
	"bytes"

	"github.com/tikv/pd/server/core"
)

// RegionLabel is the label of a region.
type RegionLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// LabelRule is the rule to assign labels to a region.
type LabelRule struct {
	ID       string        `json:"id"`
	Labels   []RegionLabel `json:"labels"`
	RuleType string        `json:"rule_type"`
	Rule     interface{}   `json:"rule"`
}

// IsMatch returns if the region matches the rule.
func (r *LabelRule) IsMatch(region *core.RegionInfo) bool {
	switch r.RuleType {
	case KeyRange:
		return r.Rule.(*KeyRangeRule).IsMatch(region)
	}
	return false
}

const (
	// KeyRange is the rule type that labels regions by key range.
	KeyRange = "key-range"
)

// KeyRangeRule contains the start key and end key of the LabelRule.
type KeyRangeRule struct {
	StartKey    []byte `json:"-"`         // range start key
	StartKeyHex string `json:"start_key"` // hex format start key, for marshal/unmarshal
	EndKey      []byte `json:"-"`         // range end key
	EndKeyHex   string `json:"end_key"`   // hex format end key, for marshal/unmarshal
}

// IsMatch returns if the region matches the rule.
func (r *KeyRangeRule) IsMatch(region *core.RegionInfo) bool {
	return bytes.Compare(region.GetStartKey(), r.StartKey) >= 0 && bytes.Compare(region.GetEndKey(), r.EndKey) <= 0
}

// LabelRulePatch is the patch to update the label rules.
type LabelRulePatch struct {
	SetRules    []*LabelRule `json:"sets"`
	DeleteRules []string     `json:"deletes"`
}
