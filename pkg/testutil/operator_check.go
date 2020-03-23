// Copyright 2017 PingCAP, Inc.
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

package testutil

import (
	check "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server/schedule/operator"
)

// CheckAddPeer checks if the operator is to add peer on specified store.
func CheckAddPeer(c *check.C, op *operator.Operator, kind operator.OpKind, storeID uint64) {
	c.Assert(op, check.NotNil)
	c.Assert(op.Len(), check.Equals, 2)
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, check.Equals, storeID)
	c.Assert(op.Step(1), check.FitsTypeOf, operator.PromoteLearner{})
	kind |= operator.OpRegion
	c.Assert(op.Kind()&kind, check.Equals, kind)
}

// CheckRemovePeer checks if the operator is to remove peer on specified store.
func CheckRemovePeer(c *check.C, op *operator.Operator, storeID uint64) {
	if op.Len() == 1 {
		c.Assert(op.Step(0).(operator.RemovePeer).FromStore, check.Equals, storeID)
	} else {
		c.Assert(op.Len(), check.Equals, 2)
		c.Assert(op.Step(0).(operator.TransferLeader).FromStore, check.Equals, storeID)
		c.Assert(op.Step(1).(operator.RemovePeer).FromStore, check.Equals, storeID)
	}
}

// CheckTransferLeader checks if the operator is to transfer leader between the specified source and target stores.
func CheckTransferLeader(c *check.C, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	c.Assert(op, check.NotNil)
	c.Assert(op.Len(), check.Equals, 1)
	c.Assert(op.Step(0), check.Equals, operator.TransferLeader{FromStore: sourceID, ToStore: targetID})
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
	kind |= (operator.OpRegion | operator.OpLeader)
	c.Assert(op.Kind()&kind, check.Equals, kind)
}
