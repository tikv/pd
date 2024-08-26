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
	CheckInProgress func(err error, msgAndArgs ...any)
	Peers           []*metapb.Peer
	ConfVerChanged  uint64
	IsFinish        bool
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
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{
			Peers: []*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
		{
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
	}
	suite.check(re, step, "transfer leader from store 1 to store 2", testCases)

	step = TransferLeader{FromStore: 1, ToStore: 9} // 9 is down
	testCases = []testCase{
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 9, StoreId: 9, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
	}
	suite.check(re, step, "transfer leader from store 1 to store 9", testCases)
}

func (suite *operatorStepTestSuite) TestAddPeer() {
	re := suite.Require()
	step := AddPeer{ToStore: 2, PeerID: 2}
	testCases := []testCase{
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  1,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
	}
	suite.check(re, step, "add peer 2 on store 2", testCases)

	step = AddPeer{ToStore: 9, PeerID: 9}
	testCases = []testCase{
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
	}
	suite.check(re, step, "add peer 9 on store 9", testCases)
}

func (suite *operatorStepTestSuite) TestAddLearner() {
	re := suite.Require()
	step := AddLearner{ToStore: 2, PeerID: 2}
	testCases := []testCase{
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  1,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
	}
	suite.check(re, step, "add learner peer 2 on store 2", testCases)

	step = AddLearner{ToStore: 9, PeerID: 9}
	testCases = []testCase{
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
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
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{ // after step
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  4,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
		{ // miss peer id
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // miss store id
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 5, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // miss peer id
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 5, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // change is not atomic
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // change is not atomic
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // there are other peers in the joint state
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  4,
			IsFinish:        true,
			CheckInProgress: re.Error,
		},
		{ // there are other peers in the joint state
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
				{Id: 6, StoreId: 6, Role: metapb.PeerRole_DemotingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
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
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{ // after step
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  1,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
		{ // after step (direct)
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  1,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
		{ // error role
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
	}
	desc := "use joint consensus, promote learner peer 3 on store 3 to voter"
	suite.check(re, cpe, desc, testCases)

	cpe = ChangePeerV2Enter{
		DemoteVoters: []DemoteVoter{{PeerID: 3, ToStore: 3}},
	}
	testCases = []testCase{
		{ // before step
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{ // after step
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
			},
			ConfVerChanged:  1,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
		{ // after step (direct)
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  1,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
		{ // demote and remove peer
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  1, // correct calculation is required
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // error role
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
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
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{ // after step
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  4,
			IsFinish:        true,
			CheckInProgress: re.NoError,
		},
		{ // miss peer id
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 5, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // miss store id
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 5, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // miss peer id
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // change is not atomic
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // change is not atomic
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // there are other peers in the joint state
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // there are other peers in the joint state
			Peers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_IncomingVoter},
				{Id: 6, StoreId: 6, Role: metapb.PeerRole_DemotingVoter},
			},
			ConfVerChanged:  4,
			IsFinish:        false,
			CheckInProgress: re.Error,
		},
		{ // demote leader
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.Error,
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
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			ConfVerChanged:  0,
			IsFinish:        false,
			CheckInProgress: re.NoError,
		},
		{
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter, IsWitness: true},
			},
			ConfVerChanged:  1,
			IsFinish:        true,
			CheckInProgress: re.NoError,
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
