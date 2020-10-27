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
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
)

var _ = Suite(&testBuilderSuite{})

type testBuilderSuite struct {
	cluster *mockcluster.Cluster
}

func (s *testBuilderSuite) SetUpTest(c *C) {
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

func (s *testBuilderSuite) TestNewBuilder(c *C) {
	peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2, Role: metapb.PeerRole_Learner}}
	region := core.NewRegionInfo(&metapb.Region{Id: 42, Peers: peers}, peers[0])
	builder := NewBuilder("test", s.cluster, region)
	c.Assert(builder.err, IsNil)
	c.Assert(len(builder.originPeers), Equals, 2)
	c.Assert(builder.originPeers[1], DeepEquals, peers[0])
	c.Assert(builder.originPeers[2], DeepEquals, peers[1])
	c.Assert(builder.originLeaderStoreID, Equals, uint64(1))
	c.Assert(len(builder.targetPeers), Equals, 2)
	c.Assert(builder.targetPeers[1], DeepEquals, peers[0])
	c.Assert(builder.targetPeers[2], DeepEquals, peers[1])

	region = region.Clone(core.WithLeader(nil))
	builder = NewBuilder("test", s.cluster, region)
	c.Assert(builder.err, NotNil)
}

func (s *testBuilderSuite) newBuilder() *Builder {
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 12, StoreId: 2},
		{Id: 13, StoreId: 3, Role: metapb.PeerRole_Learner},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	return NewBuilder("test", s.cluster, region)
}

func (s *testBuilderSuite) TestRecord(c *C) {
	c.Assert(s.newBuilder().AddPeer(&metapb.Peer{StoreId: 1}).err, NotNil)
	c.Assert(s.newBuilder().AddPeer(&metapb.Peer{StoreId: 4}).err, IsNil)
	c.Assert(s.newBuilder().PromoteLearner(1).err, NotNil)
	c.Assert(s.newBuilder().PromoteLearner(3).err, IsNil)
	c.Assert(s.newBuilder().SetLeader(1).SetLeader(2).err, IsNil)
	c.Assert(s.newBuilder().SetLeader(3).err, NotNil)
	c.Assert(s.newBuilder().RemovePeer(4).err, NotNil)
	c.Assert(s.newBuilder().AddPeer(&metapb.Peer{StoreId: 4, Role: metapb.PeerRole_Learner}).RemovePeer(4).err, IsNil)
	c.Assert(s.newBuilder().SetLeader(2).RemovePeer(2).err, NotNil)
	c.Assert(s.newBuilder().PromoteLearner(4).err, NotNil)
	c.Assert(s.newBuilder().SetLeader(4).err, NotNil)
	c.Assert(s.newBuilder().SetPeers(map[uint64]*metapb.Peer{2: {Id: 2}}).err, NotNil)

	m := map[uint64]*metapb.Peer{
		2: {StoreId: 2},
		3: {StoreId: 3, Role: metapb.PeerRole_Learner},
		4: {StoreId: 4},
	}
	builder := s.newBuilder().SetPeers(m).EnableLightWeight()
	c.Assert(len(builder.targetPeers), Equals, 3)
	c.Assert(builder.targetPeers[2], DeepEquals, m[2])
	c.Assert(builder.targetPeers[3], DeepEquals, m[3])
	c.Assert(builder.targetPeers[4], DeepEquals, m[4])
	c.Assert(builder.targetLeaderStoreID, Equals, uint64(0))
	c.Assert(builder.lightWeight, IsTrue)
}

func (s *testBuilderSuite) TestPrepareBuild(c *C) {
	// no voter.
	_, err := s.newBuilder().SetPeers(map[uint64]*metapb.Peer{4: {StoreId: 4, Role: metapb.PeerRole_Learner}}).prepareBuild()
	c.Assert(err, NotNil)

	// use joint consensus
	builder := s.newBuilder().SetPeers(map[uint64]*metapb.Peer{
		1: {StoreId: 1, Role: metapb.PeerRole_Learner},
		3: {StoreId: 3},
		4: {StoreId: 4, Id: 14},
		5: {StoreId: 5, Role: metapb.PeerRole_Learner},
	})
	_, err = builder.prepareBuild()
	c.Assert(err, IsNil)
	c.Assert(len(builder.toAdd), Equals, 2)
	c.Assert(builder.toAdd[4].GetRole(), Not(Equals), metapb.PeerRole_Learner)
	c.Assert(builder.toAdd[4].GetId(), Equals, uint64(14))
	c.Assert(builder.toAdd[5].GetRole(), Equals, metapb.PeerRole_Learner)
	c.Assert(builder.toAdd[5].GetId(), Not(Equals), uint64(0))
	c.Assert(len(builder.toRemove), Equals, 1)
	c.Assert(builder.toRemove[2], NotNil)
	c.Assert(len(builder.toPromote), Equals, 1)
	c.Assert(builder.toPromote[3], NotNil)
	c.Assert(len(builder.toDemote), Equals, 1)
	c.Assert(builder.toDemote[1], NotNil)
	c.Assert(builder.currentLeaderStoreID, Equals, uint64(1))

	// do not use joint consensus
	builder = s.newBuilder().SetPeers(map[uint64]*metapb.Peer{
		1: {StoreId: 1, Role: metapb.PeerRole_Learner},
		2: {StoreId: 2},
		3: {StoreId: 3},
		4: {StoreId: 4, Id: 14},
		5: {StoreId: 5, Role: metapb.PeerRole_Learner},
	})
	builder.allowDemote = false
	builder.useJointConsensus = false
	_, err = builder.prepareBuild()
	c.Assert(err, IsNil)
	c.Assert(len(builder.toAdd), Equals, 3)
	c.Assert(builder.toAdd[1].GetRole(), Equals, metapb.PeerRole_Learner)
	c.Assert(builder.toAdd[1].GetId(), Not(Equals), uint64(0))
	c.Assert(builder.toAdd[4].GetRole(), Not(Equals), metapb.PeerRole_Learner)
	c.Assert(builder.toAdd[4].GetId(), Equals, uint64(14))
	c.Assert(builder.toAdd[5].GetRole(), Equals, metapb.PeerRole_Learner)
	c.Assert(builder.toAdd[5].GetId(), Not(Equals), uint64(0))
	c.Assert(len(builder.toRemove), Equals, 1)
	c.Assert(builder.toRemove[1], NotNil)
	c.Assert(len(builder.toPromote), Equals, 1)
	c.Assert(builder.toPromote[3], NotNil)
	c.Assert(builder.currentLeaderStoreID, Equals, uint64(1))
}

func (s *testBuilderSuite) TestBuild(c *C) {
	type testCase struct {
		allowDemote       bool
		useJointConsensus bool
		originPeers       []*metapb.Peer // first is leader
		targetPeers       []*metapb.Peer // first is leader
		kind              OpKind
		steps             []OpStep // empty means error
	}
	cases := []testCase{
		{ // empty step
			false, false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			0,
			[]OpStep{},
		},
		{ // empty step
			true, true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			0,
			[]OpStep{},
		},
		{ // no valid leader
			false, false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			[]*metapb.Peer{{Id: 10, StoreId: 10}},
			0,
			[]OpStep{},
		},
		{ // no valid leader
			true, true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}},
			[]*metapb.Peer{{Id: 10, StoreId: 10}},
			0,
			[]OpStep{},
		},
		{ // promote learner
			false, false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2}, {Id: 1, StoreId: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
			},
		},
		{ // promote learner
			true, true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2}, {Id: 1, StoreId: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
			},
		},
		{ // not use joint consensus: prefer replace
			false, false,
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
		{ // use joint consensus: transfer leader in joint state
			true, true,
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
		{ // not use joint consensus: transfer leader before remove leader
			false, false,
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
		{ // use joint consensus: transfer leader in joint state
			true, true,
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
		{ // not use joint consensus: replace voter with learner
			false, false,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			OpRegion,
			[]OpStep{
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 2},
			},
		},
		{ // use joint consensus: demote directly
			true, true,
			[]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
			[]*metapb.Peer{{StoreId: 1}, {StoreId: 2, Role: metapb.PeerRole_Learner}},
			0, // Note that there is no OpRegion here
			[]OpStep{
				DemoteFollower{ToStore: 2},
			},
		},
		// not use joint consensus
		{ // prefer replace with nearest peer
			false, false,
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
		{ // promote learner + demote voter
			true, false,
			[]*metapb.Peer{{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter}, {Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner}, {Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}},
			OpLeader | OpRegion,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
				DemoteFollower{ToStore: 1},
				AddLearner{ToStore: 3},
			},
		},
		{ // add learner + promote learner + remove voter
			false, false,
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
		{ // add voter + demote voter + remove learner.
			true, false,
			[]*metapb.Peer{{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter}, {Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}},
			[]*metapb.Peer{{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter}, {Id: 1, StoreId: 1, Role: metapb.PeerRole_Learner}},
			OpLeader | OpRegion,
			[]OpStep{
				AddLearner{ToStore: 3},
				PromoteLearner{ToStore: 3},
				TransferLeader{FromStore: 1, ToStore: 3},
				DemoteFollower{ToStore: 1},
				RemovePeer{FromStore: 2},
			},
		},
		// use joint consensus
		{ // transfer leader before entering joint state
			true, true,
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
		{ // transfer leader after leaving joint state
			true, true,
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
	}

	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.originPeers}, tc.originPeers[0])
		builder := NewBuilder("test", s.cluster, region)
		builder.allowDemote = tc.allowDemote
		builder.useJointConsensus = tc.useJointConsensus
		m := make(map[uint64]*metapb.Peer)
		for _, p := range tc.targetPeers {
			m[p.GetStoreId()] = p
		}
		builder.SetPeers(m).SetLeader(tc.targetPeers[0].GetStoreId())
		op, err := builder.Build(0)
		if len(tc.steps) == 0 {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(op.Kind(), Equals, tc.kind)
		c.Assert(op.Len(), Equals, len(tc.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, tc.steps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, tc.steps[i].(TransferLeader).ToStore)
			case AddPeer:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddPeer).ToStore)
			case AddLightPeer:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddLightPeer).ToStore)
			case RemovePeer:
				c.Assert(step.FromStore, Equals, tc.steps[i].(RemovePeer).FromStore)
			case AddLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddLearner).ToStore)
			case AddLightLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddLightLearner).ToStore)
			case PromoteLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(PromoteLearner).ToStore)
			case DemoteFollower:
				c.Assert(step.ToStore, Equals, tc.steps[i].(DemoteFollower).ToStore)
			case ChangePeerV2Enter:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Enter).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Enter).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Enter).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			case ChangePeerV2Leave:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			}
		}
	}
}

// Test for not set unhealthy peer as target for promote learner and transfer leader
func (s *testBuilderSuite) TestTargetUnhealthyPeer(c *C) {
	p := &metapb.Peer{Id: 2, StoreId: 2, Role: metapb.PeerRole_Learner}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithPendingPeers([]*metapb.Peer{p}))
	builder := NewBuilder("test", s.cluster, region)
	builder.PromoteLearner(2)
	c.Assert(builder.err, NotNil)
	region = core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithDownPeers([]*pdpb.PeerStats{{Peer: p}}))
	builder = NewBuilder("test", s.cluster, region)
	builder.PromoteLearner(2)
	c.Assert(builder.err, NotNil)

	p = &metapb.Peer{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter}
	region = core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithPendingPeers([]*metapb.Peer{p}))
	builder = NewBuilder("test", s.cluster, region)
	builder.SetLeader(2)
	c.Assert(builder.err, NotNil)
	region = core.NewRegionInfo(&metapb.Region{Id: 1, Peers: []*metapb.Peer{{Id: 1, StoreId: 1},
		p}}, &metapb.Peer{Id: 1, StoreId: 1}, core.WithDownPeers([]*pdpb.PeerStats{{Peer: p}}))
	builder = NewBuilder("test", s.cluster, region)
	builder.SetLeader(2)
	c.Assert(builder.err, NotNil)
}
