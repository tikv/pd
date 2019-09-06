// Copyright 2018 PingCAP, Inc.
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

package schedule

import (
	"container/heap"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockhbstream"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testOperatorControllerSuite{})

type testOperatorControllerSuite struct{}

// issue #1338
func (t *testOperatorControllerSuite) TestGetOpInfluence(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(tc, nil)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []OperatorStep{
		RemovePeer{FromStore: 2},
	}
	op1 := NewOperator("test", 1, &metapb.RegionEpoch{}, OpRegion, steps...)
	op2 := NewOperator("test", 2, &metapb.RegionEpoch{}, OpRegion, steps...)
	oc.SetOperator(op1)
	oc.SetOperator(op2)
	go func() {
		c.Assert(oc.RemoveOperator(op1), IsTrue)
		for {
			c.Assert(oc.RemoveOperator(op1), IsFalse)
		}
	}()
	go func() {
		for {
			oc.GetOpInfluence(tc)
		}
	}()
	time.Sleep(1 * time.Second)
	c.Assert(oc.GetOperator(2), NotNil)
}

func (t *testOperatorControllerSuite) TestOperatorStatus(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(tc, mockhbstream.NewHeartbeatStream())
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []OperatorStep{
		RemovePeer{FromStore: 2},
		AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := NewOperator("test", 1, &metapb.RegionEpoch{}, OpRegion, steps...)
	op2 := NewOperator("test", 2, &metapb.RegionEpoch{}, OpRegion, steps...)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	op1.startTime = time.Now()
	oc.SetOperator(op1)
	op2.startTime = time.Now()
	oc.SetOperator(op2)
	c.Assert(oc.GetOperatorStatus(1).Status, Equals, pdpb.OperatorStatus_RUNNING)
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_RUNNING)
	op1.startTime = time.Now().Add(-10 * time.Minute)
	region2 = ApplyOperatorStep(region2, op2)
	tc.PutRegion(region2)
	oc.Dispatch(region1, "test")
	oc.Dispatch(region2, "test")
	c.Assert(oc.GetOperatorStatus(1).Status, Equals, pdpb.OperatorStatus_TIMEOUT)
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_RUNNING)
	ApplyOperator(tc, op2)
	oc.Dispatch(region2, "test")
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_SUCCESS)
}

// issue #1716
func (t *testOperatorControllerSuite) TestConcurrentRemoveOperator(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(tc, mockhbstream.NewHeartbeatStream())
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 2, 1)
	region1 := tc.GetRegion(1)
	steps := []OperatorStep{
		RemovePeer{FromStore: 1},
		AddPeer{ToStore: 1, PeerID: 4},
	}
	// finished op with normal priority
	op1 := NewOperator("test", 1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
	// unfinished op with high priority
	op2 := NewOperator("test", 1, &metapb.RegionEpoch{}, OpRegion|OpAdmin, steps...)
	op2.SetPriorityLevel(core.HighPriority)

	oc.SetOperator(op1)

	c.Assert(failpoint.Enable("github.com/pingcap/pd/server/schedule/concurrentRemoveOperator", "return(true)"), IsNil)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		oc.Dispatch(region1, "test")
		wg.Done()
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		success := oc.AddOperator(op2)
		// If the assert failed before wg.Done, the test will be blocked.
		defer c.Assert(success, IsTrue)
		wg.Done()
	}()
	wg.Wait()

	c.Assert(oc.GetOperator(1), Equals, op2)
}

func (t *testOperatorControllerSuite) TestPollDispatchRegion(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(tc, mockhbstream.NewHeartbeatStream())
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	tc.AddLeaderRegion(4, 2, 1)
	steps := []OperatorStep{
		RemovePeer{FromStore: 2},
		AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := NewOperator("test", 1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
	op2 := NewOperator("test", 2, &metapb.RegionEpoch{}, OpRegion, steps...)
	op3 := NewOperator("test", 3, &metapb.RegionEpoch{}, OpRegion, steps...)
	op4 := NewOperator("test", 4, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	region4 := tc.GetRegion(4)
	// Adds operator and pushes to the notifier queue.
	{
		oc.SetOperator(op1)
		oc.SetOperator(op3)
		oc.SetOperator(op4)
		oc.SetOperator(op2)
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op1, time: time.Now().Add(100 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op3, time: time.Now().Add(300 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op4, time: time.Now().Add(499 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op2, time: time.Now().Add(500 * time.Millisecond)})
	}
	// fisrt poll got nil
	r, next := oc.pollNeedDispatchRegion()
	c.Assert(r, IsNil)
	c.Assert(next, IsFalse)

	// after wait 100 millisecond, the region1 need to dispatch, but not region2.
	time.Sleep(100 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, NotNil)
	c.Assert(next, IsTrue)
	c.Assert(r.GetID(), Equals, region1.GetID())

	// find op3 with nil region, remove it
	c.Assert(oc.GetOperator(3), NotNil)
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, IsNil)
	c.Assert(next, IsTrue)
	c.Assert(oc.GetOperator(3), IsNil)

	// find op4 finished
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, NotNil)
	c.Assert(next, IsTrue)
	c.Assert(r.GetID(), Equals, region4.GetID())

	// after waiting 500 millseconds, the region2 need to dispatch
	time.Sleep(400 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, NotNil)
	c.Assert(next, IsTrue)
	c.Assert(r.GetID(), Equals, region2.GetID())
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, IsNil)
	c.Assert(next, IsFalse)
}
