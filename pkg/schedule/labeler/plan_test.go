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

package labeler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestPlanSetLabelRule(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	labeler, err := NewRegionLabeler(ctx, store, time.Millisecond*10)
	re.NoError(err)

	plan := labeler.NewPlan()

	// Test valid rule
	rule := &LabelRule{
		ID:       "rule1",
		Labels:   []RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("1234", "5678"),
	}

	err = plan.SetLabelRule(rule)
	re.NoError(err)
	re.Len(plan.commitOps, 1)
	re.Len(plan.applyLockedOps, 1)

	// Test invalid rule (empty ID)
	invalidRule := &LabelRule{
		ID:       "",
		Labels:   []RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("1234", "5678"),
	}

	err = plan.SetLabelRule(invalidRule)
	re.Error(err)

	// Test that subsequent SetLabelRule calls return the error
	anotherRule := &LabelRule{
		ID:       "rule2",
		Labels:   []RegionLabel{{Key: "k2", Value: "v2"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("abcd", "efef"),
	}
	err = plan.SetLabelRule(anotherRule)
	re.Error(err)
	re.Equal(plan.err, err)
}

func TestPlanDeleteLabelRule(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	labeler, err := NewRegionLabeler(ctx, store, time.Millisecond*10)
	re.NoError(err)

	plan := labeler.NewPlan()

	// Delete a rule
	err = plan.DeleteLabelRule("rule1")
	re.NoError(err)
	re.Len(plan.commitOps, 1)
	re.Len(plan.applyLockedOps, 1)

	// Set error and test DeleteLabelRule returns error
	plan.err = errs.ErrRegionRuleContent.FastGenByArgs("test error")
	err = plan.DeleteLabelRule("rule2")
	re.Error(err)
	re.Equal(plan.err, err)
}

func TestPlanCommitOps(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	labeler, err := NewRegionLabeler(ctx, store, time.Millisecond*10)
	re.NoError(err)

	plan := labeler.NewPlan()

	// No ops initially
	ops := plan.CommitOps()
	re.Empty(ops)

	// Add a rule
	rule := &LabelRule{
		ID:       "rule1",
		Labels:   []RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("1234", "5678"),
	}
	err = plan.SetLabelRule(rule)
	re.NoError(err)

	// Should have one op
	ops = plan.CommitOps()
	re.Len(ops, 1)

	// Add delete
	err = plan.DeleteLabelRule("rule2")
	re.NoError(err)

	// Should have two ops
	ops = plan.CommitOps()
	re.Len(ops, 2)

	// When there's an error, CommitOps should return nil
	plan.err = errs.ErrRegionRuleContent.FastGenByArgs("test error")
	ops = plan.CommitOps()
	re.Nil(ops)
}

func TestPlanApply(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	labeler, err := NewRegionLabeler(ctx, store, time.Millisecond*10)
	re.NoError(err)

	// First, add a rule to the labeler
	existingRule := &LabelRule{
		ID:       "existing",
		Labels:   []RegionLabel{{Key: "k0", Value: "v0"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("0000", "1111"),
	}
	err = labeler.SetLabelRule(existingRule)
	re.NoError(err)

	plan := labeler.NewPlan()

	// Add operations to the plan
	rule1 := &LabelRule{
		ID:       "rule1",
		Labels:   []RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("1234", "5678"),
	}
	err = plan.SetLabelRule(rule1)
	re.NoError(err)

	rule2 := &LabelRule{
		ID:       "rule2",
		Labels:   []RegionLabel{{Key: "k2", Value: "v2"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("abcd", "efef"),
	}
	err = plan.SetLabelRule(rule2)
	re.NoError(err)

	err = plan.DeleteLabelRule("existing")
	re.NoError(err)

	// Execute commit operations first (simulate transaction commit)
	commitOps := plan.CommitOps()
	re.Len(commitOps, 3) // 2 sets + 1 delete

	// Run commit ops in a transaction
	err = store.RunInTxn(ctx, func(txn kv.Txn) error {
		for _, op := range commitOps {
			if err := op(txn); err != nil {
				return err
			}
		}
		return nil
	})
	re.NoError(err)

	// Now apply the changes to memory
	plan.Apply()

	// Verify the rules are in memory
	re.NotNil(labeler.GetLabelRule("rule1"))
	re.NotNil(labeler.GetLabelRule("rule2"))
	re.Nil(labeler.GetLabelRule("existing"))
}

func TestPlanApplyWithError(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	labeler, err := NewRegionLabeler(ctx, store, time.Millisecond*10)
	re.NoError(err)

	plan := labeler.NewPlan()

	// Add a valid rule
	rule := &LabelRule{
		ID:       "rule1",
		Labels:   []RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: "key-range",
		Data:     MakeKeyRanges("1234", "5678"),
	}
	err = plan.SetLabelRule(rule)
	re.NoError(err)

	// Set an error
	plan.err = errs.ErrRegionRuleContent.FastGenByArgs("test error")

	// Apply should do nothing when there's an error
	plan.Apply()

	// The rule should not be in memory (Apply didn't execute)
	re.Nil(labeler.GetLabelRule("rule1"))
}

func TestPlanMultipleOperations(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	labeler, err := NewRegionLabeler(ctx, store, time.Millisecond*10)
	re.NoError(err)

	plan := labeler.NewPlan()

	// Add multiple rules
	for i := range 5 {
		rule := &LabelRule{
			ID:       "rule" + string(rune('0'+i)),
			Labels:   []RegionLabel{{Key: "k" + string(rune('0'+i)), Value: "v" + string(rune('0'+i))}},
			RuleType: "key-range",
			Data:     MakeKeyRanges("1234", "5678"),
		}
		err = plan.SetLabelRule(rule)
		re.NoError(err)
	}

	// Delete some rules
	err = plan.DeleteLabelRule("rule0")
	re.NoError(err)
	err = plan.DeleteLabelRule("rule2")
	re.NoError(err)

	// Verify commit ops count
	ops := plan.CommitOps()
	re.Len(ops, 7) // 5 sets + 2 deletes

	// Execute the plan
	err = store.RunInTxn(ctx, func(txn kv.Txn) error {
		for _, op := range ops {
			if err := op(txn); err != nil {
				return err
			}
		}
		return nil
	})
	re.NoError(err)

	plan.Apply()

	// Verify rules in memory
	re.NotNil(labeler.GetLabelRule("rule1"))
	re.NotNil(labeler.GetLabelRule("rule3"))
	re.NotNil(labeler.GetLabelRule("rule4"))
	re.Nil(labeler.GetLabelRule("rule0"))
	re.Nil(labeler.GetLabelRule("rule2"))
}

func TestPlanDeleteNonExistentRule(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	labeler, err := NewRegionLabeler(ctx, store, time.Millisecond*10)
	re.NoError(err)

	plan := labeler.NewPlan()

	// Delete a rule that doesn't exist should not cause an error
	err = plan.DeleteLabelRule("nonexistent")
	re.NoError(err)

	// Execute the plan
	ops := plan.CommitOps()
	re.Len(ops, 1)

	err = store.RunInTxn(ctx, func(txn kv.Txn) error {
		for _, op := range ops {
			if err := op(txn); err != nil {
				return err
			}
		}
		return nil
	})
	re.NoError(err)

	plan.Apply()

	// Verify the rule still doesn't exist
	re.Nil(labeler.GetLabelRule("nonexistent"))
}
