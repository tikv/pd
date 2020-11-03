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
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testCreateOperatorSuite{})

type testCreateOperatorSuite struct {
	cluster *mockcluster.Cluster
}

func (s *testCreateOperatorSuite) SetUpTest(c *C) {
	opts := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(opts)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	s.cluster.SetLocationLabels([]string{"zone", "host"})
	s.cluster.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(3, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(4, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(5, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(6, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsStore(8, 0, map[string]string{"zone": "z2", "host": "h1"})
	s.cluster.AddLabelsStore(9, 0, map[string]string{"zone": "z2", "host": "h2"})
	s.cluster.AddLabelsStore(10, 0, map[string]string{"zone": "z3", "host": "h1", "noleader": "true"})
}

func (s *testCreateOperatorSuite) TestCreateLeaveJointStateOperator(c *C) {
	type testCase struct {
		originPeers []*metapb.Peer // first is leader
		kind        OpKind
		steps       []OpStep // empty means error
	}
	cases := []testCase{
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

	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateLeaveJointStateOperator("test", s.cluster, region)
		if len(tc.steps) == 0 {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(op.Kind(), Equals, tc.kind)
		c.Assert(len(op.steps), Equals, len(tc.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, tc.steps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, tc.steps[i].(TransferLeader).ToStore)
			case ChangePeerV2Leave:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			default:
				c.Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

func (s *testCreateOperatorSuite) TestCreateMoveRegionOperator(c *C) {
	type testCase struct {
		name            string
		originPeers     []*metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
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
	for _, tc := range tt {
		c.Log(tc.name)
		region := core.NewRegionInfo(&metapb.Region{Id: 10, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateMoveRegionOperator("test", s.cluster, region, OpAdmin, tc.targetPeerRoles)

		if tc.expectedError == nil {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), tc.expectedError.Error()), IsTrue)
			continue
		}
		c.Assert(op, NotNil)
		c.Assert(op.Len(), Equals, len(tc.steps))
		// Since the peer id may be generated by allocator in runtime, we only check store id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, tc.steps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, tc.steps[i].(TransferLeader).ToStore)
			case ChangePeerV2Leave:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			case ChangePeerV2Enter:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Enter).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Enter).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Enter).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			case AddLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddLearner).ToStore)
			case PromoteLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(PromoteLearner).ToStore)
			case RemovePeer:
				c.Assert(step.FromStore, Equals, tc.steps[i].(RemovePeer).FromStore)
			default:
				c.Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

func (s *testCreateOperatorSuite) TestMoveRegionWithoutJointConsensus(c *C) {
	type testCase struct {
		name            string
		originPeers     []*metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
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

	s.cluster.DisableFeature(versioninfo.JointConsensus)
	for _, tc := range tt {
		c.Log(tc.name)
		region := core.NewRegionInfo(&metapb.Region{Id: 10, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateMoveRegionOperator("test", s.cluster, region, OpAdmin, tc.targetPeerRoles)

		if tc.expectedError == nil {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), tc.expectedError.Error()), IsTrue)
			continue
		}
		c.Log(op)
		c.Assert(op, NotNil)
		c.Assert(op.Len(), Equals, len(tc.steps))
		// Since the peer id may be generated by allocator in runtime, we only check store id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, tc.steps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, tc.steps[i].(TransferLeader).ToStore)
			case AddLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddLearner).ToStore)
			case PromoteLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(PromoteLearner).ToStore)
			case RemovePeer:
				c.Assert(step.FromStore, Equals, tc.steps[i].(RemovePeer).FromStore)
			default:
				c.Errorf("unexpected type: %s", step.String())
			}
		}
	}
}
