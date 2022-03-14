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
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"go.uber.org/zap"
)

// RegionLabel is the label of a region.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type RegionLabel struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	TTL     string `json:"ttl,omitempty"`
	StartAt string `json:"start_at,omitempty"`
	start   time.Time
	expire  time.Time
}

// LabelRule is the rule to assign labels to a region.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type LabelRule struct {
	ID        string        `json:"id"`
	Index     int           `json:"index"`
	Labels    []RegionLabel `json:"labels"`
	RuleType  string        `json:"rule_type"`
	Data      interface{}   `json:"data"`
	minExpire time.Time
}

const (
	// KeyRange is the rule type that specifies a list of key ranges.
	KeyRange = "key-range"
)

const (
	scheduleOptionLabel      = "schedule"
	scheduleOptioonValueDeny = "deny"
)

// KeyRangeRule contains the start key and end key of the LabelRule.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type KeyRangeRule struct {
	StartKey    []byte `json:"-"`         // range start key
	StartKeyHex string `json:"start_key"` // hex format start key, for marshal/unmarshal
	EndKey      []byte `json:"-"`         // range end key
	EndKeyHex   string `json:"end_key"`   // hex format end key, for marshal/unmarshal
}

// LabelRulePatch is the patch to update the label rules.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type LabelRulePatch struct {
	SetRules    []*LabelRule `json:"sets"`
	DeleteRules []string     `json:"deletes"`
}

func (l *RegionLabel) expireBefore(t time.Time) bool {
	return l.getExpire().Before(t)
}

var unlimittedExpire time.Time

func init() {
	unlimittedExpire, _ = time.Parse(time.UnixDate, "Mon Jan 12 22:22:22 MST 2222")
}

func (l *RegionLabel) checkAndAdjustExpire() (err error) {
	if len(l.TTL) == 0 {
		l.expire = unlimittedExpire
		return nil
	}
	ttl, err := time.ParseDuration(l.TTL)
	if err != nil {
		return err
	}
	if len(l.StartAt) == 0 {
		l.start = time.Now()
		l.StartAt = l.start.Format(time.UnixDate)
	} else {
		l.start, err = time.Parse(time.UnixDate, l.StartAt)
		if err != nil {
			return err
		}
	}
	l.expire = l.start.Add(ttl)
	return nil
}

// GetExpire returns time the rule expired.
func (l *RegionLabel) getExpire() time.Time {
	if !l.expire.IsZero() {
		return l.expire
	}
	l.checkAndAdjustExpire()
	return l.expire
}

func (rule *LabelRule) checkAndRemoveExpireLabels(now time.Time) bool {
	minExpire := unlimittedExpire
	labels := make([]RegionLabel, 0)
	for _, l := range rule.Labels {
		if l.expireBefore(now) {
			continue
		}
		labels = append(labels, l)
		if l.expireBefore(minExpire) {
			minExpire = l.expire
		}
	}

	if len(labels) == len(rule.Labels) && rule.minExpire.Equal(minExpire) {
		return false
	}
	rule.minExpire = minExpire
	rule.Labels = labels
	return true
}

func (rule *LabelRule) checkAndAdjust() error {
	if rule.ID == "" {
		return errs.ErrRegionRuleContent.FastGenByArgs("empty rule id")
	}
	if len(rule.Labels) == 0 {
		return errs.ErrRegionRuleContent.FastGenByArgs("no region labels")
	}
	for _, l := range rule.Labels {
		if l.Key == "" {
			return errs.ErrRegionRuleContent.FastGenByArgs("empty region label key")
		}
		if l.Value == "" {
			return errs.ErrRegionRuleContent.FastGenByArgs("empty region label value")
		}
		if err := l.checkAndAdjustExpire(); err != nil {
			err := fmt.Sprintf("region label with invalid ttl info %v", err)
			return errs.ErrRegionRuleContent.FastGenByArgs(err)
		}
	}
	rule.checkAndRemoveExpireLabels(time.Now())
	if len(rule.Labels) == 0 {
		err := fmt.Errorf("region label with expired ttl")
		return errs.ErrRegionRuleContent.FastGenByArgs(err)
	}

	// TODO: change it to switch statement once we support more types.
	if rule.RuleType == KeyRange {
		var err error
		rule.Data, err = initKeyRangeRulesFromLabelRuleData(rule.Data)
		return err
	}
	log.Error("invalid rule type", zap.String("rule-type", rule.RuleType))
	return errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid rule type: %s", rule.RuleType))
}

// initKeyRangeRulesFromLabelRuleData init and adjust []KeyRangeRule from `LabelRule.Data``
func initKeyRangeRulesFromLabelRuleData(data interface{}) ([]*KeyRangeRule, error) {
	rules, ok := data.([]interface{})
	if !ok {
		return nil, errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid rule type: %T", data))
	}
	if len(rules) == 0 {
		return nil, errs.ErrRegionRuleContent.FastGenByArgs("no key ranges")
	}
	rs := make([]*KeyRangeRule, 0, len(rules))
	for _, r := range rules {
		rr, err := initAndAdjustKeyRangeRule(r)
		if err != nil {
			return nil, err
		}
		rs = append(rs, rr)
	}
	return rs, nil
}

// initAndAdjustKeyRangeRule inits and adjusts the KeyRangeRule from one item in `LabelRule.Data`
func initAndAdjustKeyRangeRule(rule interface{}) (*KeyRangeRule, error) {
	data, ok := rule.(map[string]interface{})
	if !ok {
		return nil, errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid rule type: %T", reflect.TypeOf(rule)))
	}
	startKey, ok := data["start_key"].(string)
	if !ok {
		return nil, errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid startKey type: %T", reflect.TypeOf(data["start_key"])))
	}
	endKey, ok := data["end_key"].(string)
	if !ok {
		return nil, errs.ErrRegionRuleContent.FastGenByArgs(fmt.Sprintf("invalid endKey type: %T", reflect.TypeOf(data["end_key"])))
	}
	var r KeyRangeRule
	r.StartKeyHex, r.EndKeyHex = startKey, endKey
	var err error
	r.StartKey, err = hex.DecodeString(r.StartKeyHex)
	if err != nil {
		return nil, errs.ErrHexDecodingString.FastGenByArgs(r.StartKeyHex)
	}
	r.EndKey, err = hex.DecodeString(r.EndKeyHex)
	if err != nil {
		return nil, errs.ErrHexDecodingString.FastGenByArgs(r.EndKeyHex)
	}
	if len(r.EndKey) > 0 && bytes.Compare(r.EndKey, r.StartKey) <= 0 {
		return nil, errs.ErrRegionRuleContent.FastGenByArgs("endKey should be greater than startKey")
	}
	return &r, nil
}
