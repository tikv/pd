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
