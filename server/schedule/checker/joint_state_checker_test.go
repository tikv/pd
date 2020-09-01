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
	"github.com/tikv/pd/pkg/mock/mockoption"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testJointStateCheckerSuite{})

type testJointStateCheckerSuite struct {
	cluster *mockcluster.Cluster
	jsc     *JointStateChecker
}

func (s *testJointStateCheckerSuite) SetUpTest(c *C) {
	s.cluster = mockcluster.NewCluster(mockoption.NewScheduleOptions())
	s.jsc = NewJointStateChecker(s.cluster)
	for id := uint64(1); id <= 10; id++ {
		s.cluster.PutStoreWithLabels(id)
	}
}

func (s *testJointStateCheckerSuite) TestLeaveJointState(c *C) {
	jsc := s.jsc
	type testCase struct {
		Peers []*metapb.Peer // first is leader
		OpLen int
	}
	cases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			1,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			1,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			1,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			2,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			0,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			0,
		},
	}

	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.Peers}, tc.Peers[0])
		op := jsc.Check(region)
		if tc.OpLen == 0 {
			c.Assert(op, IsNil)
		} else {
			c.Assert(op.Desc(), Equals, "leave-joint-state")
			c.Assert(op.Len(), Equals, tc.OpLen)
		}
	}
}
