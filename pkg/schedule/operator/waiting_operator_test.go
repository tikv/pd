// Copyright 2019 TiKV Project Authors.
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

package operator

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core/constant"
)

func TestRandBuckets(t *testing.T) {
	re := require.New(t)
	rb := newRandBuckets()
	addOperators(rb)
	for range priorityWeight {
		op := rb.GetOperator()
		re.NotNil(op)
	}
	re.Nil(rb.GetOperator())
}

func addOperators(wop WaitingOperator) {
	op := NewTestOperator(uint64(1), &metapb.RegionEpoch{}, OpRegion, []OpStep{
		RemovePeer{FromStore: uint64(1)},
	}...)
	op.SetPriorityLevel(constant.Medium)
	wop.PutOperator(op)
	op = NewTestOperator(uint64(2), &metapb.RegionEpoch{}, OpRegion, []OpStep{
		RemovePeer{FromStore: uint64(2)},
	}...)
	op.SetPriorityLevel(constant.High)
	wop.PutOperator(op)
	op = NewTestOperator(uint64(3), &metapb.RegionEpoch{}, OpRegion, []OpStep{
		RemovePeer{FromStore: uint64(3)},
	}...)
	op.SetPriorityLevel(constant.Low)
	wop.PutOperator(op)
	op = NewTestOperator(uint64(4), &metapb.RegionEpoch{}, OpRegion, []OpStep{
		RemovePeer{FromStore: uint64(4)},
	}...)
	op.SetPriorityLevel(constant.Urgent)
	wop.PutOperator(op)
}

func TestListOperator(t *testing.T) {
	re := require.New(t)
	rb := newRandBuckets()
	addOperators(rb)
	re.Len(rb.ListOperator(), len(priorityWeight))
}

func TestRandomBucketsWithMergeRegion(t *testing.T) {
	re := require.New(t)
	rb := newRandBuckets()
	descs := []string{"merge-region", "admin-merge-region", "random-merge"}
	for j := range 100 {
		// adds operators
		desc := descs[j%3]
		op1 := NewTestOperator(uint64(1), &metapb.RegionEpoch{}, OpRegion|OpMerge, []OpStep{
			MergeRegion{
				FromRegion: &metapb.Region{
					Id:          1,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				ToRegion: &metapb.Region{Id: 2,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				IsPassive: false,
			},
		}...)
		op1.SetDesc(desc)
		op2 := NewTestOperator(uint64(2), &metapb.RegionEpoch{}, OpRegion|OpMerge, []OpStep{
			MergeRegion{
				FromRegion: &metapb.Region{
					Id:          1,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				ToRegion: &metapb.Region{Id: 2,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				IsPassive: true,
			},
		}...)
		op2.SetDesc(desc)
		// Set RelatedMergeRegion to make them a pair
		op1.Sync(op2)
		rb.PutMergeOperators([]*Operator{op1, op2})

		op3 := NewTestOperator(uint64(3), &metapb.RegionEpoch{}, OpRegion, []OpStep{
			RemovePeer{FromStore: uint64(3)},
		}...)
		op3.SetDesc("testOperatorHigh")
		op3.SetPriorityLevel(constant.High)
		rb.PutOperator(op3)

		for range 2 {
			op := rb.GetOperator()
			re.NotNil(op)
		}
		re.Nil(rb.GetOperator())
	}
}

func TestPutMergeOperators(t *testing.T) {
	re := require.New(t)
	rb := newRandBuckets()

	// Helper function to create merge operators
	createMergeOps := func(sourceID, targetID uint64, kind OpKind) []*Operator {
		op1 := NewTestOperator(sourceID, &metapb.RegionEpoch{}, kind, []OpStep{
			MergeRegion{
				FromRegion: &metapb.Region{
					Id:          sourceID,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{},
				},
				ToRegion: &metapb.Region{
					Id:          targetID,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{},
				},
				IsPassive: false,
			},
		}...)
		op2 := NewTestOperator(targetID, &metapb.RegionEpoch{}, kind, []OpStep{
			MergeRegion{
				FromRegion: &metapb.Region{
					Id:          sourceID,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{},
				},
				ToRegion: &metapb.Region{
					Id:          targetID,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{},
				},
				IsPassive: true,
			},
		}...)
		// Set RelatedMergeRegion to make them a pair
		op1.Sync(op2)
		return []*Operator{op1, op2}
	}

	// Test 1: PutMergeOperators with OpMerge
	opMergeOps := createMergeOps(1, 2, OpMerge)
	rb.PutMergeOperators(opMergeOps)

	// Test 2: PutMergeOperators with OpAffinity
	opAffinityOps := createMergeOps(3, 4, OpAffinity)
	rb.PutMergeOperators(opAffinityOps)

	// Verify both have RelatedMergeRegion set
	re.True(opMergeOps[0].HasRelatedMergeRegion())
	re.True(opMergeOps[1].HasRelatedMergeRegion())
	re.True(opAffinityOps[0].HasRelatedMergeRegion())
	re.True(opAffinityOps[1].HasRelatedMergeRegion())

	// Test 3: GetOperator should return both operators for OpMerge
	ops := rb.GetOperator()
	re.NotNil(ops)
	re.Len(ops, 2, "OpMerge should return 2 operators")
	re.True(ops[0].HasRelatedMergeRegion())
	re.True(ops[1].HasRelatedMergeRegion())

	// Test 4: GetOperator should return both operators for OpAffinity
	ops = rb.GetOperator()
	re.NotNil(ops)
	re.Len(ops, 2, "OpAffinity merge should return 2 operators")
	re.True(ops[0].HasRelatedMergeRegion())
	re.True(ops[1].HasRelatedMergeRegion())

	// Test 5: Queue should be empty now
	ops = rb.GetOperator()
	re.Nil(ops)

	// Test 6: PutMergeOperators should reject operators without RelatedMergeRegion
	invalidOps := []*Operator{
		NewTestOperator(5, &metapb.RegionEpoch{}, OpRegion, []OpStep{RemovePeer{FromStore: 1}}...),
		NewTestOperator(6, &metapb.RegionEpoch{}, OpRegion, []OpStep{RemovePeer{FromStore: 2}}...),
	}
	rb.PutMergeOperators(invalidOps)
	ops = rb.GetOperator()
	re.Nil(ops, "Invalid operators should not be added")

	// Test 7: Mixed scenario - add regular operator and merge operators
	regularOp := NewTestOperator(7, &metapb.RegionEpoch{}, OpRegion, []OpStep{RemovePeer{FromStore: 3}}...)
	rb.PutOperator(regularOp)

	mixedMergeOps := createMergeOps(8, 9, OpAffinity)
	rb.PutMergeOperators(mixedMergeOps)

	// Should be able to get both types
	for range 2 {
		ops = rb.GetOperator()
		re.NotNil(ops)
		if len(ops) == 1 {
			// Regular operator
			re.False(ops[0].HasRelatedMergeRegion())
		} else {
			// Merge operators
			re.Len(ops, 2)
			re.True(ops[0].HasRelatedMergeRegion())
			re.True(ops[1].HasRelatedMergeRegion())
		}
	}

	// Queue should be empty
	ops = rb.GetOperator()
	re.Nil(ops)
}
