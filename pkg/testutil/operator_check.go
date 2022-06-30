// Copyright 2017 TiKV Project Authors.
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

package testutil

import (
	"github.com/pingcap/check"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/schedule/operator"
)

// CheckTransferLeader checks if the operator is to transfer leader between the specified source and target stores.
func CheckTransferLeader(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	c.Assert(op, check.NotNil)
	c.Assert(op.Len(), check.Equals, 1)
	c.Assert(op.Step(0), check.DeepEquals, operator.TransferLeader{FromStore: sourceID, ToStore: targetID})
	kind |= operator.OpLeader
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

// CheckTransferLeaderFrom checks if the operator is to transfer leader out of the specified store.
func CheckTransferLeaderFrom(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID uint64) {
	c.Assert(op, check.NotNil)
	c.Assert(op.Len(), check.Equals, 1)
	c.Assert(op.Step(0).(operator.TransferLeader).FromStore, check.Equals, sourceID)
	kind |= operator.OpLeader
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

// CheckMultiTargetTransferLeader checks if the operator is to transfer leader from the specified source to one of the target stores.
func CheckMultiTargetTransferLeader(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID uint64, targetIDs []uint64) {
	c.Assert(op, check.NotNil)
	c.Assert(op.Len(), check.Equals, 1)
	expectedOps := make([]interface{}, 0, len(targetIDs))
	for _, targetID := range targetIDs {
		expectedOps = append(expectedOps, operator.TransferLeader{FromStore: sourceID, ToStore: targetID, ToStores: targetIDs})
	}
	c.Assert(op.Step(0), check.DeepEqualsIn, expectedOps)
	kind |= operator.OpLeader
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

func trimTransferLeaders(op *operator.Operator) (steps []operator.OpStep, lastLeader uint64) {
	for i := 0; i < op.Len(); i++ {
		step := op.Step(i)
		if s, ok := step.(operator.TransferLeader); ok {
			lastLeader = s.ToStore
		} else {
			steps = append(steps, step)
		}
	}
	return
}

// CheckTransferPeer checks if the operator is to transfer peer between the specified source and target stores.
func CheckTransferPeer(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	c.Assert(op, check.NotNil)

	steps, _ := trimTransferLeaders(op)
	c.Assert(steps, check.HasLen, 3)
	c.Assert(steps[0].(operator.AddLearner).ToStore, check.Equals, targetID)
	c.Assert(steps[1], check.FitsTypeOf, operator.PromoteLearner{})
	c.Assert(steps[2].(operator.RemovePeer).FromStore, check.Equals, sourceID)
	kind |= operator.OpRegion
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

// CheckTransferLearner checks if the operator is to transfer learner between the specified source and target stores.
func CheckTransferLearner(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	c.Assert(op, check.NotNil)

	steps, _ := trimTransferLeaders(op)
	c.Assert(steps, check.HasLen, 2)
	c.Assert(steps[0].(operator.AddLearner).ToStore, check.Equals, targetID)
	c.Assert(steps[1].(operator.RemovePeer).FromStore, check.Equals, sourceID)
	kind |= operator.OpRegion
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

// CheckTransferPeerWithLeaderTransfer checks if the operator is to transfer
// peer between the specified source and target stores and it meanwhile
// transfers the leader out of source store.
func CheckTransferPeerWithLeaderTransfer(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	c.Assert(op, check.NotNil)

	steps, lastLeader := trimTransferLeaders(op)
	c.Assert(steps, check.HasLen, 3)
	c.Assert(steps[0].(operator.AddLearner).ToStore, check.Equals, targetID)
	c.Assert(steps[1], check.FitsTypeOf, operator.PromoteLearner{})
	c.Assert(steps[2].(operator.RemovePeer).FromStore, check.Equals, sourceID)
	c.Assert(lastLeader, check.Not(check.Equals), uint64(0))
	c.Assert(lastLeader, check.Not(check.Equals), sourceID)
	kind |= operator.OpRegion
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

// CheckTransferPeerWithLeaderTransferFrom checks if the operator is to transfer
// peer out of the specified store and it meanwhile transfers the leader out of
// the store.
func CheckTransferPeerWithLeaderTransferFrom(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID uint64) {
	c.Assert(op, check.NotNil)

	steps, lastLeader := trimTransferLeaders(op)
	c.Assert(steps[0], check.FitsTypeOf, operator.AddLearner{})
	c.Assert(steps[1], check.FitsTypeOf, operator.PromoteLearner{})
	c.Assert(steps[2].(operator.RemovePeer).FromStore, check.Equals, sourceID)
	c.Assert(lastLeader, check.Not(check.Equals), uint64(0))
	c.Assert(lastLeader, check.Not(check.Equals), sourceID)
	kind |= operator.OpRegion | operator.OpLeader
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

// CheckAddPeer checks if the operator is to add peer on specified store.
func CheckAddPeer(re *require.Assertions, op *operator.Operator, kind operator.OpKind, storeID uint64) {
	re.NotNil(op)
	re.Equal(2, op.Len())
	re.Equal(storeID, op.Step(0).(operator.AddLearner).ToStore)
	re.IsType(operator.PromoteLearner{}, op.Step(1))
	kind |= operator.OpRegion
	re.Equal(kind, op.Kind()&kind)
}

// CheckRemovePeer checks if the operator is to remove peer on specified store.
func CheckRemovePeer(re *require.Assertions, op *operator.Operator, storeID uint64) {
	re.NotNil(op)
	if op.Len() == 1 {
		re.Equal(storeID, op.Step(0).(operator.RemovePeer).FromStore)
	} else {
		re.Equal(2, op.Len())
		re.Equal(storeID, op.Step(0).(operator.TransferLeader).FromStore)
		re.Equal(storeID, op.Step(1).(operator.RemovePeer).FromStore)
	}
}

// CheckTransferLeaderWithTestify checks if the operator is to transfer leader between the specified source and target stores.
func CheckTransferLeaderWithTestify(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	re.NotNil(op)
	re.Equal(1, op.Len())
	re.Equal(operator.TransferLeader{FromStore: sourceID, ToStore: targetID}, op.Step(0))
	kind |= operator.OpLeader
	re.Equal(kind, op.Kind()&kind)
}

// CheckTransferPeerWithTestify checks if the operator is to transfer peer between the specified source and target stores.
func CheckTransferPeerWithTestify(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	re.NotNil(op)

	steps, _ := trimTransferLeaders(op)
	re.Len(steps, 3)
	re.Equal(targetID, steps[0].(operator.AddLearner).ToStore)
	re.IsType(operator.PromoteLearner{}, steps[1])
	re.Equal(sourceID, steps[2].(operator.RemovePeer).FromStore)
	kind |= operator.OpRegion
	re.Equal(kind, op.Kind()&kind)
}

// CheckSteps checks if the operator matches the given steps.
func CheckSteps(re *require.Assertions, op *operator.Operator, steps []operator.OpStep) {
	re.NotEqual(0, op.Kind()&operator.OpMerge)
	re.NotNil(steps)
	re.Len(steps, op.Len())
	for i := range steps {
		switch op.Step(i).(type) {
		case operator.AddLearner:
			re.Equal(steps[i].(operator.AddLearner).ToStore, op.Step(i).(operator.AddLearner).ToStore)
		case operator.PromoteLearner:
			re.Equal(steps[i].(operator.PromoteLearner).ToStore, op.Step(i).(operator.PromoteLearner).ToStore)
		case operator.TransferLeader:
			re.Equal(steps[i].(operator.TransferLeader).FromStore, op.Step(i).(operator.TransferLeader).FromStore)
			re.Equal(steps[i].(operator.TransferLeader).ToStore, op.Step(i).(operator.TransferLeader).ToStore)
		case operator.RemovePeer:
			re.Equal(steps[i].(operator.RemovePeer).FromStore, op.Step(i).(operator.RemovePeer).FromStore)
		case operator.MergeRegion:
			re.Equal(steps[i].(operator.MergeRegion).IsPassive, op.Step(i).(operator.MergeRegion).IsPassive)
		default:
			re.FailNow("unknown operator step type")
		}
	}
}
