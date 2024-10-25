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
		op := NewTestOperator(uint64(1), &metapb.RegionEpoch{}, OpRegion|OpMerge, []OpStep{
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
		op.SetDesc(desc)
		rb.PutOperator(op)
		op = NewTestOperator(uint64(2), &metapb.RegionEpoch{}, OpRegion|OpMerge, []OpStep{
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
		op.SetDesc(desc)
		rb.PutOperator(op)
		op = NewTestOperator(uint64(3), &metapb.RegionEpoch{}, OpRegion, []OpStep{
			RemovePeer{FromStore: uint64(3)},
		}...)
		op.SetDesc("testOperatorHigh")
		op.SetPriorityLevel(constant.High)
		rb.PutOperator(op)

		for range 2 {
			op := rb.GetOperator()
			re.NotNil(op)
		}
		re.Nil(rb.GetOperator())
	}
}
