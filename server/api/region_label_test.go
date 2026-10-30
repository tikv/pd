// Copyright 2026 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
)

func TestValidateRegionLabelPatchRejectsAffinityIDsBeforeWrites(t *testing.T) {
	require.NoError(t, rejectAffinityLabelID("ordinary", nil))
	require.NoError(t, rejectAffinityLabelRule(nil))
	require.ErrorIs(t, rejectAffinityLabelID("affinity_group/g", nil), errs.ErrAffinityLabelProtected)
	require.ErrorIs(t, rejectAffinityLabelRule(&labeler.LabelRule{ID: "custom", Labels: []labeler.RegionLabel{{Key: affinity.AffinityLabelKey}}}), errs.ErrAffinityLabelProtected)
	require.ErrorIs(t, validateRegionLabelPatch(labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{{ID: "ordinary"}, {ID: "affinity_group/g"}},
	}, nil), errs.ErrAffinityLabelProtected)
	require.NoError(t, validateRegionLabelPatch(labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{{ID: "ordinary"}}, DeleteRules: []string{"other"},
	}, nil))
}

func TestRegionLabelPatchProtectsExistingAffinityRule(t *testing.T) {
	existing := &labeler.LabelRule{ID: "custom", Labels: []labeler.RegionLabel{{Key: affinity.AffinityLabelKey, Value: "g"}}}
	lookup := func(id string) *labeler.LabelRule {
		if id == existing.ID {
			return existing
		}
		return nil
	}
	require.ErrorIs(t, rejectAffinityLabelID(existing.ID, lookup), errs.ErrAffinityLabelProtected)
	require.ErrorIs(t, validateRegionLabelPatch(labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{{ID: "ordinary"}, {ID: existing.ID}},
	}, lookup), errs.ErrAffinityLabelProtected)
	// The affinity API never creates a rule with another ID and the affinity
	// label, so such a rule can be deleted, unlike one with the affinity prefix.
	require.NoError(t, validateRegionLabelPatch(labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{{ID: "ordinary"}}, DeleteRules: []string{existing.ID},
	}, lookup))
	require.NoError(t, rejectAffinityRuleDeletion(existing.ID))
	require.ErrorIs(t, rejectAffinityRuleDeletion("affinity_group/g"), errs.ErrAffinityLabelProtected)
	require.ErrorIs(t, validateRegionLabelPatch(labeler.LabelRulePatch{
		DeleteRules: []string{existing.ID, "affinity_group/g"},
	}, lookup), errs.ErrAffinityLabelProtected)
}
