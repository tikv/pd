// Copyright 2019 TiKV Project Authors.
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

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
)

type operatorBuilderTestSuite struct {
	suite.Suite

	cluster *mockcluster.Cluster
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestOperatorBuilderTestSuite(t *testing.T) {
	suite.Run(t, new(operatorBuilderTestSuite))
}

func (suite *operatorBuilderTestSuite) SetupTest() {
	opts := mockconfig.NewTestOptions()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster = mockcluster.NewCluster(suite.ctx, opts)
	suite.cluster.SetLabelProperty(config.RejectLeader, "noleader", "true")
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

func (suite *operatorBuilderTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *operatorBuilderTestSuite) TestNewBuilder() {
	re := suite.Require()
	peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2, Role: metapb.PeerRole_Learner}}
	region := core.NewRegionInfo(&metapb.Region{Id: 42, Peers: peers}, peers[0])
	builder := NewBuilder("test", suite.cluster, region)
	re.NoError(builder.err)
	re.Len(builder.originPeers, 2)
	re.Equal(peers[0], builder.originPeers[1])
	re.Equal(peers[1], builder.originPeers[2])
	re.Equal(uint64(1), builder.originLeaderStoreID)
	re.Len(builder.targetPeers, 2)
	re.Equal(peers[0], builder.targetPeers[1])
	re.Equal(peers[1], builder.targetPeers[2])

	region = region.Clone(core.WithLeader(nil))
	builder = NewBuilder("test", suite.cluster, region)
	re.Error(builder.err)
}

func (suite *operatorBuilderTestSuite) newBuilder() *Builder {
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 12, StoreId: 2},
		{Id: 13, StoreId: 3, Role: metapb.PeerRole_Learner},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	return NewBuilder("test", suite.cluster, region)
}

func (suite *operatorBuilderTestSuite) TestRecord() {
	re := suite.Require()
	re.Error(suite.newBuilder().AddPeer(&metapb.Peer{StoreId: 1}).err)
	re.NoError(suite.newBuilder().AddPeer(&metapb.Peer{StoreId: 4}).err)
	re.Error(suite.newBuilder().PromoteLearner(1).err)
	re.NoError(suite.newBuilder().PromoteLearner(3).err)
	re.NoError(suite.newBuilder().SetLeader(1).SetLeader(2).err)
	re.Error(suite.newBuilder().SetLeader(3).err)
	re.Error(suite.newBuilder().RemovePeer(4).err)
	re.NoError(suite.newBuilder().AddPeer(&metapb.Peer{StoreId: 4, Role: metapb.PeerRole_Learner}).RemovePeer(4).err)
	re.Error(suite.newBuilder().SetLeader(2).RemovePeer(2).err)
	re.Error(suite.newBuilder().PromoteLearner(4).err)
	re.Error(suite.newBuilder().SetLeader(4).err)
	re.Error(suite.newBuilder().SetPeers(map[uint64]*metapb.Peer{2: {Id: 2}}).err)

	m := map[uint64]*metapb.Peer{
		2: {StoreId: 2},
		3: {StoreId: 3, Role: metapb.PeerRole_Learner},
		4: {StoreId: 4},
	}
	builder := suite.newBuilder().SetPeers(m).SetAddLightPeer()
	re.Len(builder.targetPeers, 3)
	re.Equal(m[2], builder.targetPeers[2])
	re.Equal(m[3], builder.targetPeers[3])
	re.Equal(m[4], builder.targetPeers[4])
	re.Equal(uint64(0), builder.targetLeaderStoreID)
	re.True(builder.addLightPeer)
}

func (suite *operatorBuilderTestSuite) TestPrepareBuild() {
	re := suite.Require()
	// no voter.
	_, err := suite.newBuilder().SetPeers(map[uint64]*metapb.Peer{4: {StoreId: 4, Role: metapb.PeerRole_Learner}}).prepareBuild()
	re.Error(err)

	// use joint consensus
	builder := suite.newBuilder().SetPeers(map[uint64]*metapb.Peer{
		1: {StoreId: 1, Role: metapb.PeerRole_Learner},
		3: {StoreId: 3},
		4: {StoreId: 4, Id: 14},
		5: {StoreId: 5, Role: metapb.PeerRole_Learner},
	})
	_, err = builder.prepareBuild()
	re.NoError(err)
	re.Len(builder.toAdd, 2)
	re.NotEqual(metapb.PeerRole_Learner, builder.toAdd[4].GetRole())
	re.Equal(uint64(14), builder.toAdd[4].GetId())
	re.Equal(metapb.PeerRole_Learner, builder.toAdd[5].GetRole())
	re.NotEqual(uint64(0), builder.toAdd[5].GetId())
	re.Len(builder.toRemove, 1)
	re.NotNil(builder.toRemove[2])
	re.Len(builder.toPromote, 1)
	re.NotNil(builder.toPromote[3])
	re.Len(builder.toDemote, 1)
	re.NotNil(builder.toDemote[1])
	re.Equal(uint64(1), builder.currentLeaderStoreID)

	// do not use joint consensus
	builder = suite.newBuilder().SetPeers(map[uint64]*metapb.Peer{
		1: {StoreId: 1, Role: metapb.PeerRole_Learner},
		2: {StoreId: 2},
		3: {StoreId: 3},
		4: {StoreId: 4, Id: 14},
		5: {StoreId: 5, Role: metapb.PeerRole_Learner},
	})
	builder.useJointConsensus = false
	_, err = builder.prepareBuild()
	re.NoError(err)
	re.Len(builder.toAdd, 3)
	re.Equal(metapb.PeerRole_Learner, builder.toAdd[1].GetRole())
	re.NotEqual(uint64(0), builder.toAdd[1].GetId())
	re.NotEqual(metapb.PeerRole_Learner, builder.toAdd[4].GetRole())
	re.Equal(uint64(14), builder.toAdd[4].GetId())
	re.Equal(metapb.PeerRole_Learner, builder.toAdd[5].GetRole())
	re.NotEqual(uint64(0), builder.toAdd[5].GetId())
	re.Len(builder.toRemove, 1)
	re.NotNil(builder.toRemove[1])
	re.Len(builder.toPromote, 1)
	re.NotNil(builder.toPromote[3])
	re.Equal(uint64(1), builder.currentLeaderStoreID)
}

func (suite *operatorBuilderTestSuite) TestBuild() {
	re := suite.Require()
	type testCase struct {
		name              string
		useJointConsensus bool
		originPeers       []*metapb.Peer // first is leader
		targetPeers       []*metapb.Peer // first is leader
		kind              OpKind
		steps             []OpStep // empty means error
	}
	testCases := []testCase{
		{
			"(disable JointConsensus) empty step",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			0,
			[]OpStep{},
		},
		{
			"(enable JointConsensus) empty step",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			0,
			[]OpStep{},
		},
		{
			"(disable JointConsensus) no valid leader",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			[]*metapb.Peer{{Id: 10, StoreId: 10}},
			0,
			[]OpStep{},
		},
		{
			"(enable JointConsensus) no valid leader",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			[]*metapb.Peer{{Id: 10, StoreId: 10}},
			0,
			[]OpStep{},
		},
		{
			"(disable JointConsensus) promote 1 learner and transfer leader",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2}, {Id: 1, StoreId: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
			},
		},
		{
			"(enable JointConsensus) promote 1 learner and transfer leader",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2}, {Id: 1, StoreId: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
			},
		},
		{
			"(disable JointConsensus) prefer replace",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{StoreId: 4}, {StoreId: 5, Role: metapb.PeerRole_Learner}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 5},
				RemovePeer{FromStore: 3},
				TransferLeader{FromStore: 1, ToStore: 4},
				RemovePeer{FromStore: 1},
			},
		},
		{
			"(enable JointConsensus) transfer leader in joint state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{StoreId: 4}, {StoreId: 5, Role: metapb.PeerRole_Learner}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 4},
				AddLearner{ToStore: 5},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 2}},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 2}},
				},
				RemovePeer{FromStore: 1},
				RemovePeer{FromStore: 2},
				RemovePeer{FromStore: 3},
			},
		},
		{
			"(disable JointConsensus) transfer leader before remove leader",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			[]*metapb.Peer{{StoreId: 2}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 2},
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
			},
		},
		{
			"(enable JointConsensus) transfer leader in joint state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			[]*metapb.Peer{{StoreId: 2}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 2}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 2}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				RemovePeer{FromStore: 1},
			},
		},
		{
			"(disable JointConsensus) replace voter with learner",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			OpRegion,
			[]OpStep{
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 2},
			},
		},
		{
			"(enable JointConsensus) demote 1 peer directly",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{StoreId: 1}, {StoreId: 2, Role: metapb.PeerRole_Learner}},
			0, // Note that there is no OpRegion here
			[]OpStep{
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters:    []DemoteVoter{{ToStore: 2}},
				},
			},
		},
		{
			"(disable JointConsensus) prefer replace with nearest peer",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 6, StoreId: 6}, {Id: 8, StoreId: 8}},
			//             z1,h1                z1,h2                 z2,h1
			[]*metapb.Peer{{StoreId: 9}, {StoreId: 7}, {StoreId: 10}},
			//             z2,h2         z1,h1         z3,h1
			OpLeader | OpRegion,
			[]OpStep{
				// 6->7
				AddLearner{ToStore: 7},
				PromoteLearner{ToStore: 7},
				RemovePeer{FromStore: 6},
				// 8->9
				AddLearner{ToStore: 9},
				PromoteLearner{ToStore: 9},
				RemovePeer{FromStore: 8},
				// 1->10
				AddLearner{ToStore: 10},
				PromoteLearner{ToStore: 10},
				TransferLeader{FromStore: 1, ToStore: 7}, // transfer oldest voter
				RemovePeer{FromStore: 1},
				// transfer leader
				TransferLeader{FromStore: 7, ToStore: 9},
			},
		},
		{
			"(disable JointConsensus) promote learner + demote voter + add learner",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter}, {Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 3},
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
				AddLearner{ToStore: 1},
			},
		},
		{
			"(disable JointConsensus) add learner + promote learner + remove voter",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 3},
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
			},
		},
		{
			"(disable JointConsensus) add voter + demote voter + remove learner",
			false,
			[]*metapb.Peer{{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter}, {Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 3},
				PromoteLearner{ToStore: 3},
				TransferLeader{FromStore: 1, ToStore: 3},
				RemovePeer{FromStore: 1},
				AddLearner{ToStore: 1},
				RemovePeer{FromStore: 2},
			},
		},
		{
			"(enable JointConsensus) transfer leader before entering joint state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}},
			OpLeader | OpRegion,
			[]OpStep{
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
			"(enable JointConsensus) transfer leader after leaving joint state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 3, StoreId: 3}, {Id: 1, StoreId: 1}},
			OpLeader | OpRegion,
			[]OpStep{
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 2}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 2}},
				},
				TransferLeader{FromStore: 1, ToStore: 3},
				RemovePeer{FromStore: 2},
			},
		},
		{
			"(enable JointConsensus) add 1 peer(learner) should always build steps without joint consensus state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			OpRegion,
			[]OpStep{
				AddLearner{ToStore: 3},
			},
		},
		{
			"(enable JointConsensus) remove 1 peer(learner) should always build steps without joint consensus state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 3, StoreId: 3}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 3, StoreId: 3}},
			OpRegion,
			[]OpStep{
				RemovePeer{FromStore: 2},
			},
		},
		{
			"(enable JointConsensus) add 1+ learners should not enter joint consensus state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			OpRegion,
			[]OpStep{
				AddLearner{ToStore: 2},
				AddLearner{ToStore: 3},
			},
		},
		{
			"(enable JointConsensus) remove 1+ learners should not enter joint consensus state",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			OpRegion,
			[]OpStep{
				RemovePeer{FromStore: 2},
				RemovePeer{FromStore: 3},
			},
		},
		{
			"(enable JointConsensus) demote 1 voter should enter JointConsensus, and TiKV will handle the leave step",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			0,
			[]OpStep{
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters:    []DemoteVoter{{ToStore: 3}},
				},
			},
		},
		{
			"(enable JointConsensus) add 1 learner should goto to buildStepsWithoutJointConsensus",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			OpRegion,
			[]OpStep{
				AddLearner{ToStore: 3},
			},
		},
		{
			// issue: https://github.com/tikv/pd/issues/4411
			"(enable JointConsensus) remove 1 voter from 2 voter replicas raft group",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			OpRegion,
			[]OpStep{
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters:    []DemoteVoter{{ToStore: 2}},
				},
				RemovePeer{FromStore: 2},
			},
		},
		{
			// issue: https://github.com/tikv/pd/issues/4411
			"(enable JointConsensus) remove 1 voter from 2 voter replicas raft group, with transfer leader",
			true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 2, StoreId: 2}},
			OpLeader | OpRegion,
			[]OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				RemovePeer{FromStore: 1},
			},
		},
	}

	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: testCase.originPeers}, testCase.originPeers[0])
		builder := NewBuilder("test", suite.cluster, region)
		builder.useJointConsensus = testCase.useJointConsensus
		m := make(map[uint64]*metapb.Peer)
		for _, p := range testCase.targetPeers {
			m[p.GetStoreId()] = p
		}
		builder.SetPeers(m).SetLeader(testCase.targetPeers[0].GetStoreId())
		op, err := builder.Build(0)
		if len(testCase.steps) == 0 {
			re.Error(err)
			continue
		}
		re.NoError(err)
		re.Equal(testCase.kind, op.Kind())
		re.Len(testCase.steps, op.Len())
		for i := range op.Len() {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				re.Equal(testCase.steps[i].(TransferLeader).FromStore, step.FromStore)
				re.Equal(testCase.steps[i].(TransferLeader).ToStore, step.ToStore)
			case AddPeer:
				re.Equal(testCase.steps[i].(AddPeer).ToStore, step.ToStore)
			case RemovePeer:
				re.Equal(testCase.steps[i].(RemovePeer).FromStore, step.FromStore)
			case AddLearner:
				re.Equal(testCase.steps[i].(AddLearner).ToStore, step.ToStore)
			case PromoteLearner:
				re.Equal(testCase.steps[i].(PromoteLearner).ToStore, step.ToStore)
			case ChangePeerV2Enter:
				re.Len(step.PromoteLearners, len(testCase.steps[i].(ChangePeerV2Enter).PromoteLearners))
				re.Len(step.DemoteVoters, len(testCase.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range testCase.steps[i].(ChangePeerV2Enter).PromoteLearners {
					re.Equal(p.ToStore, step.PromoteLearners[j].ToStore)
				}
				for j, d := range testCase.steps[i].(ChangePeerV2Enter).DemoteVoters {
					re.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			case ChangePeerV2Leave:
				re.Len(step.PromoteLearners, len(testCase.steps[i].(ChangePeerV2Leave).PromoteLearners))
				re.Len(step.DemoteVoters, len(testCase.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range testCase.steps[i].(ChangePeerV2Leave).PromoteLearners {
					re.Equal(p.ToStore, step.PromoteLearners[j].ToStore)
				}
				for j, d := range testCase.steps[i].(ChangePeerV2Leave).DemoteVoters {
					re.Equal(d.ToStore, step.DemoteVoters[j].ToStore)
				}
			}
		}
	}
}

func (suite *operatorBuilderTestSuite) TestTargetUnhealthyPeer() {
	re := suite.Require()
	p := &metapb.Peer{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithPendingPeers([]*metapb.Peer{p}))
	builder := NewBuilder("test", suite.cluster, region)
	builder.PromoteLearner(2)
	re.Error(builder.err)
	region = core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithDownPeers([]*pdpb.PeerStats{{Peer: p}}))
	builder = NewBuilder("test", suite.cluster, region)
	builder.PromoteLearner(2)
	re.Error(builder.err)
	p = &metapb.Peer{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter}
	region = core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithPendingPeers([]*metapb.Peer{p}))
	builder = NewBuilder("test", suite.cluster, region)
	builder.SetLeader(2)
	re.Error(builder.err)
	region = core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithDownPeers([]*pdpb.PeerStats{{Peer: p}}))
	builder = NewBuilder("test", suite.cluster, region)
	builder.SetLeader(2)
	re.Error(builder.err)
}
