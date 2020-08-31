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

type testJointStateCheckerSuite struct{}

var _ = Suite(&testJointStateCheckerSuite{})

func (s *testJointStateCheckerSuite) TestLeaveJointState(c *C) {
	cluster := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	jsc := NewJointStateChecker(cluster)
	type testCase struct {
		Peers   []*metapb.Peer // first is leader
		Checker Checker
	}
	cases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			NotNil,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			NotNil,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			NotNil,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			IsNil,
		},
		{
			[]*metapb.Peer{
				{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			IsNil,
		},
	}

	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.Peers}, tc.Peers[0])
		op := jsc.Check(region)
		c.Assert(op, tc.Checker)
		if op != nil {
			c.Assert(op.Desc(), Equals, "leave-joint-state")
		}
	}
}
