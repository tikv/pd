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
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

type operatorStepTestSuite struct {
	suite.Suite

	cluster *mockcluster.Cluster
}

func TestOperatorStepTestSuite(t *testing.T) {
	suite.Run(t, new(operatorStepTestSuite))
}

type testCase struct {
	Peers           []*metapb.Peer // first is leader
	ConfVerChanged  uint64
	IsFinish        bool
	CheckInProgress func(err error, msgAndArgs ...any)
}

func (suite *operatorStepTestSuite) SetupTest() {
	suite.cluster = mockcluster.NewCluster(context.Background(), mockconfig.NewTestOptions())
	for i := 1; i <= 10; i++ {
		suite.cluster.PutStoreWithLabels(uint64(i))
	}
	suite.cluster.SetStoreDown(8)
	suite.cluster.SetStoreDown(9)
	suite.cluster.SetStoreDown(10)
}

func (suite *operatorStepTestSuite) TestTransferLeader() {
	re := suite.Require()
	step := TransferLeader{FromStore: 1, ToStore: 2}
	testCases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.NoError,
		},
		{
			[]*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			true,
			re.NoError,
		},
		{
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.NoError,
		},
	}
	suite.check(re, step, "transfer leader from store 1 to store 2", testCases)

	step = TransferLeader{FromStore: 1, ToStore: 9} // 9 is down
	testCases = []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 9, StoreId: 9, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.Error,
		},
	}
	suite.check(re, step, "transfer leader from store 1 to store 9", testCases)
}

func (suite *operatorStepTestSuite) TestAddPeer() {
	re := suite.Require()
	step := AddPeer{ToStore: 2, PeerID: 2}
	testCases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.NoError,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			1,
			true,
			re.NoError,
		},
	}
	suite.check(re, step, "add peer 2 on store 2", testCases)

	step = AddPeer{ToStore: 9, PeerID: 9}
	testCases = []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.Error,
		},
	}
	suite.check(re, step, "add peer 9 on store 9", testCases)
}

func (suite *operatorStepTestSuite) TestAddLearner() {
	re := suite.Require()
	step := AddLearner{ToStore: 2, PeerID: 2}
	testCases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.NoError,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
			},
			1,
			true,
			re.NoError,
		},
	}
	suite.check(re, step, "add learner peer 2 on store 2", testCases)

	step = AddLearner{ToStore: 9, PeerID: 9}
	testCases = []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.Error,
		},
	}
	suite.check(re, step, "add learner peer 9 on store 9", testCases)
}

func (suite *operatorStepTestSuite) TestChangePeerV2Enter() {
	re := suite.Require()
	cpe := ChangePeerV2Enter{
		PromoteLearners: []PromoteLearner{{PeerID: 3, ToStore: 3}, {PeerID: 4, ToStore: 4}},
		DemoteVoters:    []DemoteVoter{{PeerID: 1, ToStore: 1}, {PeerID: 2, ToStore: 2}},
	}
	testCases := []testCase{
		{ // before step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			0,
			false,
			re.NoError,
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
			re.NoError,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
		},
	}
	desc := "use joint consensus, " +
		"promote learner peer 3 on store 3 to voter, promote learner peer 4 on store 4 to voter, " +
		"demote voter peer 1 on store 1 to learner, demote voter peer 2 on store 2 to learner"
	suite.check(re, cpe, desc, testCases)
}

func (suite *operatorStepTestSuite) TestChangePeerV2EnterWithSingleChange() {
	re := suite.Require()
	cpe := ChangePeerV2Enter{
		PromoteLearners: []PromoteLearner{{PeerID: 3, ToStore: 3}},
	}
	testCases := []testCase{
		{ // before step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			0,
			false,
			re.NoError,
		},
		{ // after step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			1,
			true,
			re.NoError,
		},
		{ // after step (direct)
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			1,
			true,
			re.NoError,
		},
		{ // error role
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
			},
			0,
			false,
			re.Error,
		},
	}
	desc := "use joint consensus, promote learner peer 3 on store 3 to voter"
	suite.check(re, cpe, desc, testCases)

	cpe = ChangePeerV2Enter{
		DemoteVoters: []DemoteVoter{{PeerID: 3, ToStore: 3}},
	}
	testCases = []testCase{
		{ // before step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.NoError,
		},
		{ // after step
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
			},
			1,
			true,
			re.NoError,
		},
		{ // after step (direct)
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			1,
			true,
			re.NoError,
		},
		{ // demote and remove peer
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			1, // correct calculation is required
			false,
			re.Error,
		},
		{ // error role
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			re.Error,
		},
	}
	desc = "use joint consensus, demote voter peer 3 on store 3 to learner"
	suite.check(re, cpe, desc, testCases)
}

func (suite *operatorStepTestSuite) TestChangePeerV2Leave() {
	re := suite.Require()
	cpl := ChangePeerV2Leave{
		PromoteLearners: []PromoteLearner{{PeerID: 3, ToStore: 3}, {PeerID: 4, ToStore: 4}},
		DemoteVoters:    []DemoteVoter{{PeerID: 1, ToStore: 1}, {PeerID: 2, ToStore: 2}},
	}
	testCases := []testCase{
		{ // before step
			[]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			false,
			re.NoError,
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
			re.NoError,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
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
			re.Error,
		},
	}
	desc := "leave joint state, " +
		"promote learner peer 3 on store 3 to voter, promote learner peer 4 on store 4 to voter, " +
		"demote voter peer 1 on store 1 to learner, demote voter peer 2 on store 2 to learner"
	suite.check(re, cpl, desc, testCases)
}

func (suite *operatorStepTestSuite) TestSwitchToWitness() {
	re := suite.Require()
	step := BecomeWitness{StoreID: 2, PeerID: 2}
	testCases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
			},
			0,
			false,
			re.NoError,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			0,
			false,
			re.NoError,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter, IsWitness: true},
			},
			1,
			true,
			re.NoError,
		},
	}
	suite.check(re, step, "switch peer 2 on store 2 to witness", testCases)
}

func (suite *operatorStepTestSuite) check(re *require.Assertions, step OpStep, desc string, testCases []testCase) {
	re.Equal(desc, step.String())
	for _, testCase := range testCases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: testCase.Peers}, testCase.Peers[0])
		re.Equal(testCase.ConfVerChanged, step.ConfVerChanged(region))
		re.Equal(testCase.IsFinish, step.IsFinish(region))
		err := step.CheckInProgress(suite.cluster.GetBasicCluster(), suite.cluster.GetSharedConfig(), region)
		testCase.CheckInProgress(err)
		_ = step.GetCmd(region, true)

		if _, ok := step.(ChangePeerV2Leave); ok {
			// Ref https://github.com/tikv/pd/issues/5788
			pendingPeers := region.GetLearners()
			region = region.Clone(core.WithPendingPeers(pendingPeers))
			re.Equal(testCase.IsFinish, step.IsFinish(region))
		}
	}
}
