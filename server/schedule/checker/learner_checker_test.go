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
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testLearnerCheckerSuite{})

type testLearnerCheckerSuite struct {
	cluster *mockcluster.Cluster
	lc      *LearnerChecker
}

func (s *testLearnerCheckerSuite) SetUpTest(c *C) {
	s.cluster = mockcluster.NewCluster(config.NewTestOptions())
	s.cluster.DisableFeature(versioninfo.JointConsensus)
	s.lc = NewLearnerChecker(s.cluster)
	for id := uint64(1); id <= 10; id++ {
		s.cluster.PutStoreWithLabels(id)
	}
}

func (s *testLearnerCheckerSuite) TestPromoteLearner(c *C) {
	lc := s.lc

	region := core.NewRegionInfo(
		&metapb.Region{
			Id: 1,
			Peers: []*metapb.Peer{
				{Id: 101, StoreId: 1},
				{Id: 102, StoreId: 2},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
		}, &metapb.Peer{Id: 101, StoreId: 1})
	op := lc.Check(region)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "promote-learner")
	c.Assert(op.Step(0), FitsTypeOf, operator.PromoteLearner{})
	c.Assert(op.Step(0).(operator.PromoteLearner).ToStore, Equals, uint64(3))

	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeer(103)}))
	op = lc.Check(region)
	c.Assert(op, IsNil)
}
