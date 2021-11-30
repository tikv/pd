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
)

type testStepSuite struct {
	cluster *mockcluster.Cluster
}

var _ = Suite(&testStepSuite{})

type testCase struct {
	Peers          []*metapb.Peer // first is leader
	ConfVerChanged uint64
	IsFinish       bool
	CheckInProgres Checker
}

func (s *testStepSuite) SetUpTest(c *C) {
	s.cluster = mockcluster.NewCluster(config.NewTestOptions())
	for i := 1; i <= 10; i++ {
		s.cluster.PutStoreWithLabels(uint64(i))
	}
	s.cluster.SetStoreDown(8)
	s.cluster.SetStoreDown(9)
	s.cluster.SetStoreDown(10)
}

func (s *testStepSuite) TestTransferLeader(c *C) {
	step := TransferLeader{FromStore: 1, ToStore: 2}
	cases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			IsNil,
		},
		{
			[]*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			true,
			IsNil,
		},
		{
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			IsNil,
		},
	}
	s.check(c, step, "transfer leader from store 1 to store 2", cases)

	step = TransferLeader{FromStore: 1, ToStore: 9} // 9 is down
	cases = []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 9, StoreId: 9, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
	}
	s.check(c, step, "transfer leader from store 1 to store 9", cases)
}

func (s *testStepSuite) TestAddPeer(c *C) {
	step := AddPeer{ToStore: 2, PeerID: 2}
	cases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			IsNil,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			1,
			true,
			IsNil,
		},
	}
	s.check(c, step, "add peer 2 on store 2", cases)

	step = AddPeer{ToStore: 9, PeerID: 9}
	cases = []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
	}
	s.check(c, step, "add peer 9 on store 9", cases)
}

func (s *testStepSuite) TestAddLearner(c *C) {
	step := AddLearner{ToStore: 2, PeerID: 2}
	cases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			IsNil,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
			},
			1,
			true,
			IsNil,
		},
	}
	s.check(c, step, "add learner peer 2 on store 2", cases)

	step = AddLearner{ToStore: 9, PeerID: 9}
	cases = []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
	}
	s.check(c, step, "add learner peer 9 on store 9", cases)
}

func (s *testStepSuite) TestDemoteFollower(c *C) {
	df := DemoteFollower{ToStore: 2, PeerID: 2}
	cases := []testCase{
		{ // before step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			IsNil,
		},
		{ // after step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			1,
			true,
			IsNil,
		},
		{ // miss peer id
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
		{ // miss store id
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
		{ // miss peer id
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
		{ // demote leader
			[]*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
	}
	s.check(c, df, "demote follower peer 2 on store 2 to learner", cases)
}

func (s *testStepSuite) TestChangePeerV2Enter(c *C) {
	cpe := ChangePeerV2Enter{
		PromoteLearners: []PromoteLearner{{PeerID: 3, ToStore: 3}, {PeerID: 4, ToStore: 4}},
		DemoteVoters:    []DemoteVoter{{PeerID: 1, ToStore: 1}, {PeerID: 2, ToStore: 2}},
	}
	cases := []testCase{
		{ // before step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			0,
			false,
			IsNil,
		},
		{ // after step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			4,
			true,
			IsNil,
		},
		{ // miss peer id
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			0,
			false,
			NotNil,
		},
		{ // miss store id
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 5, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			0,
			false,
			NotNil,
		},
		{ // miss peer id
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 5, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			NotNil,
		},
		{ // change is not atomic
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			NotNil,
		},
		{ // change is not atomic
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			0,
			false,
			NotNil,
		},
		{ // there are other peers in the joint state
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
			},
			4,
			true,
			NotNil,
		},
		{ // there are other peers in the joint state
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
				{Id: 6, StoreId: 6, Role: metapb.PeerRole_DemotingVoter},
			},
			0,
			false,
			NotNil,
		},
	}
	desc := "use joint consensus, " +
		"promote learner peer 3 on store 3 to voter, promote learner peer 4 on store 4 to voter, " +
		"demote voter peer 1 on store 1 to learner, demote voter peer 2 on store 2 to learner"
	s.check(c, cpe, desc, cases)
}

func (s *testStepSuite) TestChangePeerV2Leave(c *C) {
	cpl := ChangePeerV2Leave{
		PromoteLearners: []PromoteLearner{{PeerID: 3, ToStore: 3}, {PeerID: 4, ToStore: 4}},
		DemoteVoters:    []DemoteVoter{{PeerID: 1, ToStore: 1}, {PeerID: 2, ToStore: 2}},
	}
	cases := []testCase{
		{ // before step
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			IsNil,
		},
		{ // after step
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
			},
			4,
			true,
			IsNil,
		},
		{ // miss peer id
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 5, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			NotNil,
		},
		{ // miss store id
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 5, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			NotNil,
		},
		{ // miss peer id
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
		{ // change is not atomic
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			NotNil,
		},
		{ // change is not atomic
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			NotNil,
		},
		{ // there are other peers in the joint state
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			NotNil,
		},
		{ // there are other peers in the joint state
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
				{Id: 6, StoreId: 6, Role: metapb.PeerRole_DemotingVoter},
			},
			4,
			false,
			NotNil,
		},
		{ // demote leader
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			NotNil,
		},
	}
	desc := "leave joint state, " +
		"promote learner peer 3 on store 3 to voter, promote learner peer 4 on store 4 to voter, " +
		"demote voter peer 1 on store 1 to learner, demote voter peer 2 on store 2 to learner"
	s.check(c, cpl, desc, cases)
}

func (s *testStepSuite) check(c *C, step OpStep, desc string, cases []testCase) {
	c.Assert(step.String(), Equals, desc)
	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.Peers}, tc.Peers[0])
		c.Assert(step.ConfVerChanged(region), Equals, tc.ConfVerChanged)
		c.Assert(step.IsFinish(region), Equals, tc.IsFinish)
		c.Assert(step.CheckInProgress(s.cluster, region), tc.CheckInProgres)
	}
}
