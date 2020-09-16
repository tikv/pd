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

package checker

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
)

var _ = Suite(&testJointStateCheckerSuite{})

type testJointStateCheckerSuite struct {
	cluster *mockcluster.Cluster
	jsc     *JointStateChecker
}

func (s *testJointStateCheckerSuite) SetUpTest(c *C) {
	s.cluster = mockcluster.NewCluster(config.NewTestOptions())
	s.jsc = NewJointStateChecker(s.cluster)
	for id := uint64(1); id <= 10; id++ {
		s.cluster.PutStoreWithLabels(id)
	}
}

func (s *testJointStateCheckerSuite) TestLeaveJointState(c *C) {
	jsc := s.jsc
	type testCase struct {
		Peers   []*metapb.Peer // first is leader
		OpSteps []operator.OpStep
	}
	cases := []testCase{
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

	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.Peers}, tc.Peers[0])
		op := jsc.Check(region)
		s.checkSteps(c, op, tc.OpSteps)
	}
}

func (s *testJointStateCheckerSuite) checkSteps(c *C, op *operator.Operator, steps []operator.OpStep) {
	if len(steps) == 0 {
		c.Assert(op, IsNil)
		return
	}

	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "leave-joint-state")

	c.Assert(op.Len(), Equals, len(steps))
	for i := range steps {
		switch obtain := op.Step(i).(type) {
		case operator.ChangePeerV2Leave:
			expect := steps[i].(operator.ChangePeerV2Leave)
			c.Assert(len(obtain.PromoteLearners), Equals, len(expect.PromoteLearners))
			c.Assert(len(obtain.DemoteVoters), Equals, len(expect.DemoteVoters))
			for j, p := range expect.PromoteLearners {
				c.Assert(expect.PromoteLearners[j].ToStore, Equals, p.ToStore)
			}
			for j, d := range expect.DemoteVoters {
				c.Assert(obtain.DemoteVoters[j].ToStore, Equals, d.ToStore)
			}
		case operator.TransferLeader:
			expect := steps[i].(operator.TransferLeader)
			c.Assert(obtain.FromStore, Equals, expect.FromStore)
			c.Assert(obtain.ToStore, Equals, expect.ToStore)
		default:
			c.Fatal("unknown operator step type")
		}
	}
}
