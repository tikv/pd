// Copyright 2019 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
)

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct {
	cluster *mockcluster.Cluster
	rc      *ReplicaChecker
}

func (s *testReplicaCheckerSuite) SetUpTest(c *C) {
	cfg := mockoption.NewScheduleOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.rc = NewReplicaChecker(s.cluster)
	stats := &pdpb.StoreStats{
		Capacity:  100,
		Available: 100,
	}
	stores := []*core.StoreInfo{
		core.NewStoreInfo(
			&metapb.Store{
				Id:    1,
				State: metapb.StoreState_Offline,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id:    3,
				State: metapb.StoreState_Up,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id:    4,
				State: metapb.StoreState_Up,
			}, core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
	}
	for _, store := range stores {
		s.cluster.PutStore(store)
	}
	s.cluster.AddLabelsStore(2, 1, map[string]string{"noleader": "true"})
}

func (s *testReplicaCheckerSuite) TestReplacePendingPeer(c *C) {
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      4,
			StoreId: 3,
		},
	}
	r := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[1], core.WithPendingPeers(peers[0:1]))
	s.cluster.PutRegion(r)
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(1).(operator.PromoteLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(2).(operator.RemovePeer).FromStore, Equals, uint64(1))
}

func (s *testReplicaCheckerSuite) TestReplaceOfflinePeer(c *C) {
	s.cluster.LabelProperties = map[string][]*metapb.StoreLabel{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	}
	peers := []*metapb.Peer{
		{
			Id:      4,
			StoreId: 1,
		},
		{
			Id:      5,
			StoreId: 2,
		},
		{
			Id:      6,
			StoreId: 3,
		},
	}
	r := core.NewRegionInfo(&metapb.Region{Id: 2, Peers: peers}, peers[0])
	s.cluster.PutRegion(r)
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Step(0).(operator.TransferLeader).ToStore, Equals, uint64(3))
	c.Assert(op.Step(1).(operator.AddLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(2).(operator.PromoteLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(3).(operator.RemovePeer).FromStore, Equals, uint64(1))
}

func (s *testReplicaCheckerSuite) TestOfflineWithOneReplica(c *C) {
	s.cluster.MaxReplicas = 1
	peers := []*metapb.Peer{
		{
			Id:      4,
			StoreId: 1,
		},
	}
	r := core.NewRegionInfo(&metapb.Region{Id: 2, Peers: peers}, peers[0])
	s.cluster.PutRegion(r)
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "replace-offline-replica")
}
