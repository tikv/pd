// Copyright 2020 TiKV Project Authors.
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

package checker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/operator"
)

func TestLeaveJointState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	jsc := NewJointStateChecker(cluster)
	for id := uint64(1); id <= 10; id++ {
		cluster.PutStoreWithLabels(id)
	}
	type testCase struct {
		Peers   []*metapb.Peer // first is leader
		OpSteps []operator.OpStep
	}
	testCases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []operator.DemoteVoter{{ToStore: 2}},
				},
			},
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []operator.DemoteVoter{},
				},
			},
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{},
					DemoteVoters:    []operator.DemoteVoter{{ToStore: 2}},
				},
			},
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			[]operator.OpStep{
				operator.TransferLeader{FromStore: 1, ToStore: 2},
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{},
					DemoteVoters:    []operator.DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			nil,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			nil,
		},
	}

	for _, testCase := range testCases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: testCase.Peers}, testCase.Peers[0])
		op := jsc.Check(region)
		checkSteps(re, op, testCase.OpSteps)
	}
}

func checkSteps(re *require.Assertions, op *operator.Operator, steps []operator.OpStep) {
	if len(steps) == 0 {
		re.Nil(op)
		return
	}

	re.NotNil(op)
	re.Equal("leave-joint-state", op.Desc())

	re.Len(steps, op.Len())
	for i := range steps {
		switch obtain := op.Step(i).(type) {
		case operator.ChangePeerV2Leave:
			expect := steps[i].(operator.ChangePeerV2Leave)
			re.Len(obtain.PromoteLearners, len(expect.PromoteLearners))
			re.Len(obtain.DemoteVoters, len(expect.DemoteVoters))
			for j, p := range expect.PromoteLearners {
				re.Equal(p.ToStore, obtain.PromoteLearners[j].ToStore)
			}
			for j, d := range expect.DemoteVoters {
				re.Equal(d.ToStore, obtain.DemoteVoters[j].ToStore)
			}
		case operator.TransferLeader:
			expect := steps[i].(operator.TransferLeader)
			re.Equal(expect.FromStore, obtain.FromStore)
			re.Equal(expect.ToStore, obtain.ToStore)
		default:
			re.FailNow("unknown operator step type")
		}
	}
}
