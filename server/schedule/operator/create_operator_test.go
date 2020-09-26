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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
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
		steps       []OpStep
	}
	cases := []testCase{
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
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
	// cfg := s.cluster.GetOpts().GetScheduleConfig().Clone()
	// cfg.EnableJointConsensus = false
	// s.cluster.GetOpts().SetScheduleConfig(cfg)
	type testCase struct {
		name            string
		originPeers     []*metapb.Peer // first is leader
		targetPeers     map[uint64]*metapb.Peer
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
			targetPeers: map[uint64]*metapb.Peer{
				2: {Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				3: {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				4: {Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
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
			targetPeers: map[uint64]*metapb.Peer{
				2: {Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				3: {Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				4: {Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
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
			targetPeers: map[uint64]*metapb.Peer{
				2: {Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				3: {Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				4: {Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
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
			targetPeers: map[uint64]*metapb.Peer{
				2: {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				3: {Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				4: {Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
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
			targetPeers: map[uint64]*metapb.Peer{
				4: {Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				5: {Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
				6: {Id: 6, StoreId: 6, Role: metapb.PeerRole_Voter},
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
	}
	for _, tc := range tt {
		c.Log(tc.name)
		region := core.NewRegionInfo(&metapb.Region{Id: 10, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateMoveRegionOperator("test", s.cluster, region, OpAdmin, tc.targetPeers, tc.targetPeerRoles)
		c.Assert(err, Equals, tc.expectedError)
		c.Assert(len(op.steps), Equals, len(tc.steps))
		for i, step := range op.steps {
			c.Assert(step.String(), Equals, tc.steps[i].String())
		}
	}
}
