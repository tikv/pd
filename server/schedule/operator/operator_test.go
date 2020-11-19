// Copyright 2016 TiKV Project Authors.
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
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/schedule/opt"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct {
	cluster *mockcluster.Cluster
}

func (s *testOperatorSuite) SetUpTest(c *C) {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.cluster.SetMaxMergeRegionSize(2)
	s.cluster.SetMaxMergeRegionKeys(2)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "reject", Value: "leader"}},
	})
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for storeID, labels := range stores {
		s.cluster.PutStoreWithLabels(storeID, labels...)
	}
}

func (s *testOperatorSuite) newTestRegion(regionID uint64, leaderPeer uint64, peers ...[2]uint64) *core.RegionInfo {
	var (
		region metapb.Region
		leader *metapb.Peer
	)
	region.Id = regionID
	for i := range peers {
		peer := &metapb.Peer{
			Id:      peers[i][1],
			StoreId: peers[i][0],
		}
		region.Peers = append(region.Peers, peer)
		if peer.GetId() == leaderPeer {
			leader = peer
		}
	}
	regionInfo := core.NewRegionInfo(&region, leader, core.SetApproximateSize(50), core.SetApproximateKeys(50))
	return regionInfo
}

func (s *testOperatorSuite) TestOperatorStep(c *C) {
	region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	c.Assert(TransferLeader{FromStore: 1, ToStore: 2}.IsFinish(region), IsFalse)
	c.Assert(TransferLeader{FromStore: 2, ToStore: 1}.IsFinish(region), IsTrue)
	c.Assert(AddPeer{ToStore: 3, PeerID: 3}.IsFinish(region), IsFalse)
	c.Assert(AddPeer{ToStore: 1, PeerID: 1}.IsFinish(region), IsTrue)
	c.Assert(RemovePeer{FromStore: 1}.IsFinish(region), IsFalse)
	c.Assert(RemovePeer{FromStore: 3}.IsFinish(region), IsTrue)
}

func (s *testOperatorSuite) newTestOperator(regionID uint64, kind OpKind, steps ...OpStep) *Operator {
	return NewOperator("test", "test", regionID, &metapb.RegionEpoch{}, OpAdmin|kind, steps...)
}

func (s *testOperatorSuite) checkSteps(c *C, op *Operator, steps []OpStep) {
	c.Assert(op.Len(), Equals, len(steps))
	for i := range steps {
		c.Assert(op.Step(i), Equals, steps[i])
	}
}

func (s *testOperatorSuite) TestOperator(c *C) {
	region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	// addPeer1, transferLeader1, removePeer3
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 3, ToStore: 1},
		RemovePeer{FromStore: 3},
	}
	op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
	c.Assert(op.GetPriorityLevel(), Equals, core.HighPriority)
	s.checkSteps(c, op, steps)
	op.Start()
	c.Assert(op.Check(region), IsNil)
	c.Assert(op.Status(), Equals, SUCCESS)
	SetOperatorStatusReachTime(op, STARTED, time.Now().Add(-SlowOperatorWaitTime-time.Second))
	c.Assert(op.CheckTimeout(), IsFalse)

	// addPeer1, transferLeader1, removePeer2
	steps = []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op = s.newTestOperator(1, OpLeader|OpRegion, steps...)
	s.checkSteps(c, op, steps)
	op.Start()
	c.Assert(op.Check(region), Equals, RemovePeer{FromStore: 2})
	c.Assert(atomic.LoadInt32(&op.currentStep), Equals, int32(2))
	c.Assert(op.CheckTimeout(), IsFalse)
	SetOperatorStatusReachTime(op, STARTED, op.GetStartTime().Add(-FastOperatorWaitTime-time.Second))
	c.Assert(op.CheckTimeout(), IsFalse)
	SetOperatorStatusReachTime(op, STARTED, op.GetStartTime().Add(-SlowOperatorWaitTime-time.Second))
	c.Assert(op.CheckTimeout(), IsTrue)
	res, err := json.Marshal(op)
	c.Assert(err, IsNil)
	c.Assert(len(res), Equals, len(op.String())+2)

	// check short timeout for transfer leader only operators.
	steps = []OpStep{TransferLeader{FromStore: 2, ToStore: 1}}
	op = s.newTestOperator(1, OpLeader, steps...)
	op.Start()
	c.Assert(op.CheckTimeout(), IsFalse)
	SetOperatorStatusReachTime(op, STARTED, op.GetStartTime().Add(-FastOperatorWaitTime-time.Second))
	c.Assert(op.CheckTimeout(), IsTrue)
}

func (s *testOperatorSuite) TestInfluence(c *C) {
	region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	opInfluence := OpInfluence{StoresInfluence: make(map[uint64]*StoreInfluence)}
	storeOpInfluence := opInfluence.StoresInfluence
	storeOpInfluence[1] = &StoreInfluence{}
	storeOpInfluence[2] = &StoreInfluence{}

	AddPeer{ToStore: 2, PeerID: 2}.Influence(opInfluence, region)
	c.Assert(*storeOpInfluence[2], DeepEquals, StoreInfluence{
		LeaderSize:  0,
		LeaderCount: 0,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	})

	TransferLeader{FromStore: 1, ToStore: 2}.Influence(opInfluence, region)
	c.Assert(*storeOpInfluence[1], DeepEquals, StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -1,
		RegionSize:  0,
		RegionCount: 0,
		StepCost:    nil,
	})
	c.Assert(*storeOpInfluence[2], DeepEquals, StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	})

	RemovePeer{FromStore: 1}.Influence(opInfluence, region)
	c.Assert(*storeOpInfluence[1], DeepEquals, StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -1,
		RegionSize:  -50,
		RegionCount: -1,
		StepCost:    map[storelimit.Type]int64{storelimit.RemovePeer: 1000},
	})
	c.Assert(*storeOpInfluence[2], DeepEquals, StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	})

	MergeRegion{IsPassive: false}.Influence(opInfluence, region)
	c.Assert(*storeOpInfluence[1], DeepEquals, StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -1,
		RegionSize:  -50,
		RegionCount: -1,
		StepCost:    map[storelimit.Type]int64{storelimit.RemovePeer: 1000},
	})
	c.Assert(*storeOpInfluence[2], DeepEquals, StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	})

	MergeRegion{IsPassive: true}.Influence(opInfluence, region)
	c.Assert(*storeOpInfluence[1], DeepEquals, StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -2,
		RegionSize:  -50,
		RegionCount: -2,
		StepCost:    map[storelimit.Type]int64{storelimit.RemovePeer: 1000},
	})
	c.Assert(*storeOpInfluence[2], DeepEquals, StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 0,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	})
}

func (s *testOperatorSuite) TestOperatorKind(c *C) {
	c.Assert((OpLeader | OpReplica).String(), Equals, "leader,replica")
	c.Assert(OpKind(0).String(), Equals, "unknown")
	k, err := ParseOperatorKind("region,leader")
	c.Assert(err, IsNil)
	c.Assert(k, Equals, OpRegion|OpLeader)
	_, err = ParseOperatorKind("leader,region")
	c.Assert(err, IsNil)
	_, err = ParseOperatorKind("foobar")
	c.Assert(err, NotNil)
}

func (s *testOperatorSuite) TestCheckSuccess(c *C) {
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
		c.Assert(op.Status(), Equals, CREATED)
		c.Assert(op.CheckSuccess(), IsFalse)
		c.Assert(op.Start(), IsTrue)
		c.Assert(op.CheckSuccess(), IsFalse)
		op.currentStep = int32(len(op.steps))
		c.Assert(op.CheckSuccess(), IsTrue)
		c.Assert(op.CheckSuccess(), IsTrue)
	}
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
		op.currentStep = int32(len(op.steps))
		c.Assert(op.Status(), Equals, CREATED)
		c.Assert(op.CheckSuccess(), IsFalse)
		c.Assert(op.Start(), IsTrue)
		c.Assert(op.CheckSuccess(), IsTrue)
		c.Assert(op.CheckSuccess(), IsTrue)
	}
}

func (s *testOperatorSuite) TestCheckTimeout(c *C) {
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
		c.Assert(op.Status(), Equals, CREATED)
		c.Assert(op.Start(), IsTrue)
		op.currentStep = int32(len(op.steps))
		c.Assert(op.CheckTimeout(), IsFalse)
		c.Assert(op.Status(), Equals, SUCCESS)
	}
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
		c.Assert(op.Status(), Equals, CREATED)
		c.Assert(op.Start(), IsTrue)
		op.currentStep = int32(len(op.steps))
		SetOperatorStatusReachTime(op, STARTED, time.Now().Add(-SlowOperatorWaitTime))
		c.Assert(op.CheckTimeout(), IsFalse)
		c.Assert(op.Status(), Equals, SUCCESS)
	}
}

func (s *testOperatorSuite) TestStart(c *C) {
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
	c.Assert(op.GetStartTime().Nanosecond(), Equals, 0)
	c.Assert(op.Status(), Equals, CREATED)
	c.Assert(op.Start(), IsTrue)
	c.Assert(op.GetStartTime().Nanosecond(), Not(Equals), 0)
	c.Assert(op.Status(), Equals, STARTED)
}

func (s *testOperatorSuite) TestCheckExpired(c *C) {
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
	c.Assert(op.CheckExpired(), IsFalse)
	c.Assert(op.Status(), Equals, CREATED)
	SetOperatorStatusReachTime(op, CREATED, time.Now().Add(-OperatorExpireTime))
	c.Assert(op.CheckExpired(), IsTrue)
	c.Assert(op.Status(), Equals, EXPIRED)
}

func (s *testOperatorSuite) TestCheck(c *C) {
	{
		region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
		c.Assert(op.Start(), IsTrue)
		c.Assert(op.Check(region), NotNil)
		c.Assert(op.Status(), Equals, STARTED)
		region = s.newTestRegion(1, 1, [2]uint64{1, 1})
		c.Assert(op.Check(region), IsNil)
		c.Assert(op.Status(), Equals, SUCCESS)
	}
	{
		region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
		c.Assert(op.Start(), IsTrue)
		c.Assert(op.Check(region), NotNil)
		c.Assert(op.Status(), Equals, STARTED)
		op.status.setTime(STARTED, time.Now().Add(-SlowOperatorWaitTime))
		c.Assert(op.Check(region), NotNil)
		c.Assert(op.Status(), Equals, TIMEOUT)
	}
	{
		region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
		c.Assert(op.Start(), IsTrue)
		c.Assert(op.Check(region), NotNil)
		c.Assert(op.Status(), Equals, STARTED)
		op.status.setTime(STARTED, time.Now().Add(-SlowOperatorWaitTime))
		region = s.newTestRegion(1, 1, [2]uint64{1, 1})
		c.Assert(op.Check(region), IsNil)
		c.Assert(op.Status(), Equals, SUCCESS)
	}
}
