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

package operator

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

type createOperatorTestSuite struct {
	suite.Suite

	cluster *mockcluster.Cluster
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestCreateOperatorTestSuite(t *testing.T) {
	suite.Run(t, new(createOperatorTestSuite))
}

func (suite *createOperatorTestSuite) SetupTest() {
	opts := config.NewTestOptions()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster = mockcluster.NewCluster(suite.ctx, opts)
	suite.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		config.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	suite.cluster.SetLocationLabels([]string{"zone", "host"})
	suite.cluster.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(3, 0, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(4, 0, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(5, 0, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(6, 0, map[string]string{"zone": "z1", "host": "h2"})
	suite.cluster.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "host": "h2"})
	suite.cluster.AddLabelsStore(8, 0, map[string]string{"zone": "z2", "host": "h1"})
	suite.cluster.AddLabelsStore(9, 0, map[string]string{"zone": "z2", "host": "h2"})
	suite.cluster.AddLabelsStore(10, 0, map[string]string{"zone": "z3", "host": "h1", "noleader": "true"})
}

func (suite *createOperatorTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *createOperatorTestSuite) TestCreateSplitRegionOperator() {
	type testCase struct {
		startKey      []byte
		endKey        []byte
		originPeers   []*metapb.Peer // first is leader
		policy        pdpb.CheckPolicy
		keys          [][]byte
		expectedError bool
	}
	testCases := []testCase{
		{
			startKey: []byte("a"),
			endKey:   []byte("b"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        pdpb.CheckPolicy_APPROXIMATE,
			expectedError: false,
		},
		{
			startKey: []byte("c"),
			endKey:   []byte("d"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        pdpb.CheckPolicy_SCAN,
			expectedError: false,
		},
		{
			startKey: []byte("e"),
			endKey:   []byte("h"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        pdpb.CheckPolicy_USEKEY,
			keys:          [][]byte{[]byte("f"), []byte("g")},
			expectedError: false,
		},
		{
			startKey: []byte("i"),
			endKey:   []byte("j"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			policy:        pdpb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
		{
			startKey: []byte("k"),
			endKey:   []byte("l"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
			},
			policy:        pdpb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
	}

	for _, testCase := range testCases {
		region := core.NewRegionInfo(&metapb.Region{
			Id:       1,
			StartKey: testCase.startKey,
			EndKey:   testCase.endKey,
			Peers:    testCase.originPeers,
		}, testCase.originPeers[0])
		op, err := CreateSplitRegionOperator("test", region, 0, testCase.policy, testCase.keys)
		if testCase.expectedError {
			suite.Error(err)
			continue
		}
		suite.NoError(err)
		suite.Equal(OpSplit, op.Kind())
		suite.Len(op.steps, 1)
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case SplitRegion:
				suite.Equal(testCase.startKey, step.StartKey)
				suite.Equal(testCase.endKey, step.EndKey)
				suite.Equal(testCase.policy, step.Policy)
				suite.Equal(testCase.keys, step.SplitKeys)
			default:
				suite.T().Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

func (suite *createOperatorTestSuite) TestCreateMergeRegionOperator() {
	type testCase struct {
		sourcePeers   []*metapb.Peer // first is leader
		targetPeers   []*metapb.Peer // first is leader
		kind          OpKind
		expectedError bool
		prepareSteps  []OpStep
	}
	testCases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			[]*metapb.Peer{
				{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			OpMerge,
			false,
			[]OpStep{},
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			[]*metapb.Peer{
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			OpMerge | OpLeader | OpRegion,
			false,
			[]OpStep{
				AddLearner{ToStore: 3},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				RemovePeer{FromStore: 1},
			},
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
			},
			[]*metapb.Peer{
				{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			0,
			true,
			nil,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			[]*metapb.Peer{
				{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			true,
			nil,
		},
	}

	for _, testCase := range testCases {
		source := core.NewRegionInfo(&metapb.Region{Id: 68, Peers: testCase.sourcePeers}, testCase.sourcePeers[0])
		target := core.NewRegionInfo(&metapb.Region{Id: 86, Peers: testCase.targetPeers}, testCase.targetPeers[0])
		ops, err := CreateMergeRegionOperator("test", suite.cluster, source, target, 0)
		if testCase.expectedError {
			suite.Error(err)
			continue
		}
		suite.NoError(err)
		suite.Len(ops, 2)
		suite.Equal(testCase.kind, ops[0].kind)
		suite.Equal(len(testCase.prepareSteps)+1, ops[0].Len())
		suite.Equal(testCase.kind, ops[1].kind)
		suite.Equal(1, ops[1].Len())
		suite.Equal(MergeRegion{source.GetMeta(), target.GetMeta(), true}, ops[1].Step(0).(MergeRegion))

		expectedSteps := append(testCase.prepareSteps, MergeRegion{source.GetMeta(), target.GetMeta(), false})
		for i := 0; i < ops[0].Len(); i++ {
			switch step := ops[0].Step(i).(type) {
			case TransferLeader:
				suite.Equal(expectedSteps[i].(TransferLeader).FromStore, step.FromStore)
				suite.Equal(expectedSteps[i].(TransferLeader).ToStore, step.ToStore)
			case AddLearner:
				suite.Equal(expectedSteps[i].(AddLearner).ToStore, step.ToStore)
			case RemovePeer:
				suite.Equal(expectedSteps[i].(RemovePeer).FromStore, step.FromStore)
			case ChangePeerV2Enter:
				suite.Len(step.PromoteLearners, len(expectedSteps[i].(ChangePeerV2Enter).PromoteLearners))
				suite.Len(step.DemoteVoters, len(expectedSteps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Enter).PromoteLearners {
					suite.Equal(p.ToStore, step.PromoteLearners[j].ToStore)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Enter).DemoteVoters {
					suite.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			case ChangePeerV2Leave:
				suite.Len(step.PromoteLearners, len(expectedSteps[i].(ChangePeerV2Leave).PromoteLearners))
				suite.Len(step.DemoteVoters, len(expectedSteps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Leave).PromoteLearners {
					suite.Equal(p.ToStore, step.PromoteLearners[j].ToStore)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Leave).DemoteVoters {
					suite.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			case MergeRegion:
				suite.Equal(expectedSteps[i].(MergeRegion), step)
			}
		}
	}
}

func (suite *createOperatorTestSuite) TestCreateTransferLeaderOperator() {
	type testCase struct {
		originPeers         []*metapb.Peer // first is leader
		targetLeaderStoreID uint64
		isErr               bool
	}
	testCases := []testCase{
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetLeaderStoreID: 3,
			isErr:               false,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetLeaderStoreID: 1,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetLeaderStoreID: 4,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			targetLeaderStoreID: 3,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			targetLeaderStoreID: 3,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			targetLeaderStoreID: 4,
			isErr:               false,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			targetLeaderStoreID: 3,
			isErr:               false,
		},
	}
	for _, testCase := range testCases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: testCase.originPeers}, testCase.originPeers[0])
		op, err := CreateTransferLeaderOperator("test", suite.cluster, region, testCase.originPeers[0].StoreId, testCase.targetLeaderStoreID, []uint64{}, 0)

		if testCase.isErr {
			suite.Error(err)
			continue
		}

		suite.NoError(err)
		suite.Equal(OpLeader, op.Kind())
		suite.Len(op.steps, 1)
		switch step := op.Step(0).(type) {
		case TransferLeader:
			suite.Equal(testCase.originPeers[0].StoreId, step.FromStore)
			suite.Equal(testCase.targetLeaderStoreID, step.ToStore)
		default:
			suite.T().Errorf("unexpected type: %s", step.String())
		}
	}
}

func (suite *createOperatorTestSuite) TestCreateLeaveJointStateOperator() {
	type testCase struct {
		originPeers   []*metapb.Peer // first is leader
		offlineStores []uint64
		kind          OpKind
		steps         []OpStep // empty means error
	}
	testCases := []testCase{
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 3}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			offlineStores: []uint64{2},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 3},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			offlineStores: []uint64{2, 3},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			offlineStores: []uint64{1, 2, 3, 4},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_IncomingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 1}, {ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 3}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 3}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 10, StoreId: 10, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 3}},
				},
			},
		},
	}

	for _, testCase := range testCases {
		for _, storeID := range testCase.offlineStores {
			suite.cluster.SetStoreOffline(storeID)
		}

		revertOffline := func() {
			for _, storeID := range testCase.offlineStores {
				suite.cluster.SetStoreUp(storeID)
			}
		}

		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: testCase.originPeers}, testCase.originPeers[0])
		op, err := CreateLeaveJointStateOperator("test", suite.cluster, region)
		if len(testCase.steps) == 0 {
			suite.Error(err)
			revertOffline()
			continue
		}
		suite.NoError(err)
		suite.Equal(testCase.kind, op.Kind())
		suite.Len(op.steps, len(testCase.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				suite.Equal(testCase.steps[i].(TransferLeader).FromStore, step.FromStore)
				suite.Equal(testCase.steps[i].(TransferLeader).ToStore, step.ToStore)
			case ChangePeerV2Leave:
				suite.Len(step.PromoteLearners, len(testCase.steps[i].(ChangePeerV2Leave).PromoteLearners))
				suite.Len(step.DemoteVoters, len(testCase.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range testCase.steps[i].(ChangePeerV2Leave).PromoteLearners {
					suite.Equal(p.ToStore, step.PromoteLearners[j].ToStore)
				}
				for j, d := range testCase.steps[i].(ChangePeerV2Leave).DemoteVoters {
					suite.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			default:
				suite.T().Errorf("unexpected type: %s", step.String())
			}
		}

		revertOffline()
	}
}

func (suite *createOperatorTestSuite) TestCreateMoveRegionOperator() {
	type testCase struct {
		name            string
		originPeers     []*metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	testCases := []testCase{
		{
			name: "move region partially with incoming voter, demote existed voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 3, PeerID: 3},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 3, PeerID: 3},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming leader",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming learner, demote leader",
			originPeers: []*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 2, ToStore: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move entirely with incoming voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				4: placement.Leader,
				5: placement.Voter,
				6: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				AddLearner{ToStore: 5, PeerID: 5},
				AddLearner{ToStore: 6, PeerID: 6},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 4, PeerID: 4},
						{ToStore: 5, PeerID: 5},
						{ToStore: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
						{ToStore: 3, PeerID: 3},
					},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 4, PeerID: 4},
						{ToStore: 5, PeerID: 5},
						{ToStore: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
						{ToStore: 3, PeerID: 3},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
				RemovePeer{FromStore: 2, PeerID: 2},
				RemovePeer{FromStore: 3, PeerID: 3},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming and old voter, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 2, PeerID: 8},
				TransferLeader{FromStore: 4, ToStore: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 5, PeerID: 5},
					},
				},
				RemovePeer{FromStore: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming voter and follower, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 1, PeerID: 9},
				AddLearner{ToStore: 2, PeerID: 10},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 1, PeerID: 9},
						{ToStore: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 3, PeerID: 3},
						{ToStore: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 1, PeerID: 9},
						{ToStore: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 3, PeerID: 3},
						{ToStore: 5, PeerID: 5},
					},
				},
				TransferLeader{FromStore: 4, ToStore: 2},
				RemovePeer{FromStore: 3, PeerID: 3},
				RemovePeer{FromStore: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with all incoming follower, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("region need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 6},
				PromoteLearner{ToStore: 6},
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
	}
	for _, testCase := range testCases {
		suite.T().Log((testCase.name))
		region := core.NewRegionInfo(&metapb.Region{Id: 10, Peers: testCase.originPeers}, testCase.originPeers[0])
		op, err := CreateMoveRegionOperator("test", suite.cluster, region, OpAdmin, testCase.targetPeerRoles)

		if testCase.expectedError == nil {
			suite.NoError(err)
		} else {
			suite.Error(err)
			suite.Contains(err.Error(), testCase.expectedError.Error())
			continue
		}
		suite.NotNil(op)

		suite.Len(testCase.steps, op.Len())
		// Since the peer id may be generated by allocator in runtime, we only check store id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				suite.Equal(testCase.steps[i].(TransferLeader).FromStore, step.FromStore)
				suite.Equal(testCase.steps[i].(TransferLeader).ToStore, step.ToStore)
			case ChangePeerV2Leave:
				suite.Len(step.PromoteLearners, len(testCase.steps[i].(ChangePeerV2Leave).PromoteLearners))
				suite.Len(step.DemoteVoters, len(testCase.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range testCase.steps[i].(ChangePeerV2Leave).PromoteLearners {
					suite.Equal(p.ToStore, step.PromoteLearners[j].ToStore)
				}
				for j, d := range testCase.steps[i].(ChangePeerV2Leave).DemoteVoters {
					suite.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			case ChangePeerV2Enter:
				suite.Len(step.PromoteLearners, len(testCase.steps[i].(ChangePeerV2Enter).PromoteLearners))
				suite.Len(step.DemoteVoters, len(testCase.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range testCase.steps[i].(ChangePeerV2Enter).PromoteLearners {
					suite.Equal(p.ToStore, step.PromoteLearners[j].ToStore)
				}
				for j, d := range testCase.steps[i].(ChangePeerV2Enter).DemoteVoters {
					suite.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			case AddLearner:
				suite.Equal(testCase.steps[i].(AddLearner).ToStore, step.ToStore)
			case PromoteLearner:
				suite.Equal(testCase.steps[i].(PromoteLearner).ToStore, step.ToStore)
			case RemovePeer:
				suite.Equal(testCase.steps[i].(RemovePeer).FromStore, step.FromStore)
			default:
				suite.T().Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

func (suite *createOperatorTestSuite) TestMoveRegionWithoutJointConsensus() {
	type testCase struct {
		name            string
		originPeers     []*metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	testCases := []testCase{
		{
			name: "move region partially with incoming voter, demote existed voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
				RemovePeer{FromStore: 3},
				AddLearner{ToStore: 3},
			},
		},
		{
			name: "move region partially with incoming leader",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
				TransferLeader{FromStore: 2, ToStore: 4},
			},
		},
		{
			name: "move region partially with incoming learner, demote leader",
			originPeers: []*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				RemovePeer{FromStore: 1},
				TransferLeader{FromStore: 2, ToStore: 3},
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 2},
				AddLearner{ToStore: 4},
			},
		},
		{
			name: "move region partially with all incoming follower, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("region need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 6},
				PromoteLearner{ToStore: 6},
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
	}

	suite.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		region := core.NewRegionInfo(&metapb.Region{Id: 10, Peers: testCase.originPeers}, testCase.originPeers[0])
		op, err := CreateMoveRegionOperator("test", suite.cluster, region, OpAdmin, testCase.targetPeerRoles)

		if testCase.expectedError == nil {
			suite.NoError(err)
		} else {
			suite.Error(err)
			suite.Contains(err.Error(), testCase.expectedError.Error())
			continue
		}
		suite.NotNil(op)

		suite.Len(testCase.steps, op.Len())
		// Since the peer id may be generated by allocator in runtime, we only check store id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				suite.Equal(testCase.steps[i].(TransferLeader).FromStore, step.FromStore)
				suite.Equal(testCase.steps[i].(TransferLeader).ToStore, step.ToStore)
			case AddLearner:
				suite.Equal(testCase.steps[i].(AddLearner).ToStore, step.ToStore)
			case PromoteLearner:
				suite.Equal(testCase.steps[i].(PromoteLearner).ToStore, step.ToStore)
			case RemovePeer:
				suite.Equal(testCase.steps[i].(RemovePeer).FromStore, step.FromStore)
			default:
				suite.T().Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

// Ref https://github.com/tikv/pd/issues/5401
func TestCreateLeaveJointStateOperatorWithoutFitRules(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := config.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	re.NoError(cluster.SetRules([]*placement.Rule{
		{
			GroupID:     "pd",
			ID:          "default",
			StartKeyHex: hex.EncodeToString([]byte("")),
			EndKeyHex:   hex.EncodeToString([]byte("")),
			Role:        placement.Voter,
			Count:       1,
		},
		{
			GroupID:     "t1",
			ID:          "t1",
			StartKeyHex: hex.EncodeToString([]byte("a")),
			EndKeyHex:   hex.EncodeToString([]byte("b")),
			Role:        placement.Voter,
			Count:       1,
		},
		{
			GroupID:     "t2",
			ID:          "t2",
			StartKeyHex: hex.EncodeToString([]byte("b")),
			EndKeyHex:   hex.EncodeToString([]byte("c")),
			Role:        placement.Voter,
			Count:       1,
		},
	}))
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(2, 1)
	cluster.AddRegionStore(3, 1)
	cluster.AddRegionStore(4, 1)
	originPeers := []*metapb.Peer{
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
		{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
	}

	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: originPeers, StartKey: []byte("a"), EndKey: []byte("c")}, originPeers[0])
	op, err := CreateLeaveJointStateOperator("test", cluster, region)
	re.NoError(err)
	re.Equal(OpLeader, op.Kind())
	re.Len(op.steps, 2)
	step0 := op.steps[0].(TransferLeader)
	re.Equal(uint64(3), step0.FromStore)
	re.Equal(uint64(4), step0.ToStore)
	step1 := op.steps[1].(ChangePeerV2Leave)
	re.Len(step1.PromoteLearners, 1)
	re.Len(step1.DemoteVoters, 1)
	re.Equal(uint64(4), step1.PromoteLearners[0].ToStore)
	re.Equal(uint64(3), step1.DemoteVoters[0].ToStore)
}

func (suite *createOperatorTestSuite) TestCreateNonWitnessPeerOperator() {
	type testCase struct {
		originPeers   []*metapb.Peer // first is leader
		kind          OpKind
		expectedError bool
		prepareSteps  []OpStep
	}
	testCases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner, IsWitness: true},
			},
			0,
			false,
			[]OpStep{
				BecomeNonWitness{StoreID: 2, PeerID: 2},
			},
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter, IsWitness: true},
			},
			0,
			false,
			[]OpStep{
				ChangePeerV2Enter{
					DemoteVoters: []DemoteVoter{{ToStore: 2, PeerID: 2, IsWitness: true}},
				},
				BecomeNonWitness{StoreID: 2, PeerID: 2},
			},
		},
	}

	for _, testCase := range testCases {
		region := core.NewRegionInfo(&metapb.Region{Id: 68, Peers: testCase.originPeers}, testCase.originPeers[0])
		op, err := CreateNonWitnessPeerOperator("test", suite.cluster, region, testCase.originPeers[1])
		suite.NoError(err)
		suite.NotNil(op)
		suite.Equal(testCase.kind, op.kind)

		expectedSteps := testCase.prepareSteps
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case ChangePeerV2Enter:
				suite.Len(step.DemoteVoters, len(expectedSteps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, d := range expectedSteps[i].(ChangePeerV2Enter).DemoteVoters {
					suite.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			case BecomeNonWitness:
				suite.Equal(step.StoreID, expectedSteps[i].(BecomeNonWitness).StoreID)
				suite.Equal(step.PeerID, expectedSteps[i].(BecomeNonWitness).PeerID)
			}
		}
	}
}
