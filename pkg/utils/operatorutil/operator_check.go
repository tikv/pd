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

package operatorutil

import (
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/schedule/operator"
)

// CheckTransferLeader checks if the operator is to transfer leader between the specified source and target stores.
func CheckTransferLeader(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	re.NotNil(op)
	re.Equal(1, op.Len())
	re.Equal(operator.TransferLeader{FromStore: sourceID, ToStore: targetID}, op.Step(0))
	kind |= operator.OpLeader
	re.Equal(kind, op.Kind()&kind)
}

// CheckTransferLeaderFrom checks if the operator is to transfer leader out of the specified store.
func CheckTransferLeaderFrom(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID uint64) {
	re.NotNil(op)
	re.Equal(1, op.Len())
	re.Equal(sourceID, op.Step(0).(operator.TransferLeader).FromStore)
	kind |= operator.OpLeader
	re.Equal(kind, op.Kind()&kind)
}

// CheckMultiTargetTransferLeader checks if the operator is to transfer leader from the specified source to one of the target stores.
func CheckMultiTargetTransferLeader(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID uint64, targetIDs []uint64) {
	re.NotNil(op)
	re.Equal(1, op.Len())
	expectedOps := make([]any, 0, len(targetIDs))
	for _, targetID := range targetIDs {
		expectedOps = append(expectedOps, operator.TransferLeader{FromStore: sourceID, ToStore: targetID, ToStores: targetIDs})
	}
	re.Contains(expectedOps, op.Step(0))
	kind |= operator.OpLeader
	re.Equal(kind, op.Kind()&kind)
}

func trimTransferLeaders(op *operator.Operator) (steps []operator.OpStep, lastLeader uint64) {
	for i := range op.Len() {
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
func CheckTransferPeer(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	re.NotNil(op)
	var addLearnerTo, removePeerFrom uint64
	steps, _ := trimTransferLeaders(op)
	switch len(steps) {
	case 3: // without joint consensus
		re.IsType(operator.AddLearner{}, steps[0])
		re.IsType(operator.PromoteLearner{}, steps[1])
		re.IsType(operator.RemovePeer{}, steps[2])
		addLearnerTo = steps[0].(operator.AddLearner).ToStore
		removePeerFrom = steps[2].(operator.RemovePeer).FromStore
	case 4: // with joint consensus
		re.IsType(operator.AddLearner{}, steps[0])
		re.IsType(operator.ChangePeerV2Enter{}, steps[1])
		re.IsType(operator.ChangePeerV2Leave{}, steps[2])
		re.IsType(operator.RemovePeer{}, steps[3])
		addLearnerTo = steps[0].(operator.AddLearner).ToStore
		removePeerFrom = steps[3].(operator.RemovePeer).FromStore
	default:
		re.FailNow("unexpected operator steps: " + op.String())
	}
	re.Equal(sourceID, removePeerFrom)
	re.Equal(targetID, addLearnerTo)
	kind |= operator.OpRegion
	re.Equal(kind, op.Kind()&kind)
}

// CheckTransferLearner checks if the operator is to transfer learner between the specified source and target stores.
func CheckTransferLearner(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	re.NotNil(op)
	steps, _ := trimTransferLeaders(op)
	re.Len(steps, 2)
	re.Equal(targetID, steps[0].(operator.AddLearner).ToStore)
	re.Equal(sourceID, steps[1].(operator.RemovePeer).FromStore)
	kind |= operator.OpRegion
	re.Equal(kind, op.Kind()&kind)
}

// CheckTransferPeerWithLeaderTransfer checks if the operator is to transfer
// peer between the specified source and target stores and it meanwhile
// transfers the leader out of source store.
// If targetID is 0, it means the operator is to transfer peer to any store.
func CheckTransferPeerWithLeaderTransfer(re *require.Assertions, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	re.NotNil(op)
	var addLearnerTo, removePeerFrom uint64
	steps, lastLeader := trimTransferLeaders(op)
	switch len(steps) {
	case 3: // without joint consensus
		re.IsType(operator.AddLearner{}, steps[0])
		re.IsType(operator.PromoteLearner{}, steps[1])
		re.IsType(operator.RemovePeer{}, steps[2])
		addLearnerTo = steps[0].(operator.AddLearner).ToStore
		removePeerFrom = steps[2].(operator.RemovePeer).FromStore
	case 4: // with joint consensus
		re.IsType(operator.AddLearner{}, steps[0])
		re.IsType(operator.ChangePeerV2Enter{}, steps[1])
		re.IsType(operator.ChangePeerV2Leave{}, steps[2])
		re.IsType(operator.RemovePeer{}, steps[3])
		addLearnerTo = steps[0].(operator.AddLearner).ToStore
		removePeerFrom = steps[3].(operator.RemovePeer).FromStore
	default:
		re.FailNow("unexpected operator steps: " + op.String())
	}
	re.NotZero(lastLeader)
	re.NotEqual(sourceID, lastLeader)
	kind |= operator.OpRegion
	re.Equal(kind, op.Kind()&kind)
	re.Equal(sourceID, removePeerFrom)
	if targetID != 0 {
		re.Equal(targetID, addLearnerTo)
	}
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

// CheckSteps checks if the operator matches the given steps.
func CheckSteps(re *require.Assertions, op *operator.Operator, steps []operator.OpStep) {
	re.NotZero(op.Kind() & operator.OpMerge)
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
			re.FailNow("unexpected operator steps: " + op.String())
		}
	}
}
