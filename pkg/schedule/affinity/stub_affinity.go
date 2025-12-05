// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"strings"

	"github.com/tikv/pd/pkg/schedule/labeler"
)

// Temporary stubs to make early commits build; replaced by real implementations later.

// LabelRuleIDPrefix is a stubbed label rule prefix.
const LabelRuleIDPrefix = "affinity_group/"

// GetLabelRuleID is a stubbed helper to align with later commits.
func GetLabelRuleID(groupID string) string {
	return LabelRuleIDPrefix + groupID
}

func parseAffinityGroupIDFromLabelRule(rule *labeler.LabelRule) (string, bool) {
	if rule == nil || !strings.HasPrefix(rule.ID, LabelRuleIDPrefix) {
		return "", false
	}
	groupID := strings.TrimPrefix(rule.ID, LabelRuleIDPrefix)
	for _, label := range rule.Labels {
		if label.Key == labelKey && label.Value == groupID {
			return groupID, true
		}
	}
	return "", false
}

func extractKeyRangesFromLabelRule(_ *labeler.LabelRule) (GroupKeyRanges, error) {
	return GroupKeyRanges{}, nil
}

//nolint:revive
func (m *Manager) loadRegionLabel() error {
	_ = m
	return nil
}

//nolint:revive
func (m *Manager) CreateAffinityGroups([]GroupKeyRanges) error {
	_ = m
	return nil
}

//nolint:revive
func (m *Manager) DeleteAffinityGroups([]string, bool) error {
	_ = m
	return nil
}

//nolint:revive
func (m *Manager) UpdateAffinityGroupPeers(string, uint64, []uint64) (*GroupState, error) {
	_ = m
	return nil, nil
}

//nolint:revive
func (m *Manager) UpdateAffinityGroupKeyRanges([]GroupKeyRanges, []GroupKeyRanges) error {
	_ = m
	return nil
}

//nolint:revive,unused
func (m *Manager) hasUnavailableStore(uint64, []uint64) error {
	_ = m
	return nil
}

//nolint:revive
func (m *Manager) startAvailabilityCheckLoop() {
	_ = m
}
