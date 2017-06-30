// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"encoding/json"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testResouceKindSuite{})

type testResouceKindSuite struct{}

func (s *testResouceKindSuite) TestString(c *C) {
	tbl := []struct {
		value ResourceKind
		name  string
	}{
		{UnKnownKind, "unknown"},
		{AdminKind, "admin"},
		{LeaderKind, "leader"},
		{RegionKind, "region"},
		{PriorityKind, "priority"},
		{OtherKind, "other"},
		{ResourceKind(404), "unknown"},
	}
	for _, t := range tbl {
		c.Assert(t.value.String(), Equals, t.name)
	}
}

func (s *testResouceKindSuite) TestParseResouceKind(c *C) {
	tbl := []struct {
		name  string
		value ResourceKind
	}{
		{"unknown", UnKnownKind},
		{"admin", AdminKind},
		{"leader", LeaderKind},
		{"region", RegionKind},
		{"priority", PriorityKind},
		{"other", OtherKind},
		{"test", UnKnownKind},
	}
	for _, t := range tbl {
		c.Assert(ParseResourceKind(t.name), Equals, t.value)
	}
}

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct{}

func (o *testOperatorSuite) TestOperatorStateString(c *C) {
	tbl := []struct {
		value OperatorState
		name  string
	}{
		{OperatorUnKnownState, "unknown"},
		{OperatorWaiting, "waiting"},
		{OperatorRunning, "running"},
		{OperatorFinished, "finished"},
		{OperatorTimeOut, "timeout"},
		{OperatorReplaced, "replaced"},
		{OperatorState(404), "unknown"},
	}
	for _, t := range tbl {
		c.Assert(t.value.String(), Equals, t.name)
	}
}

func (o *testOperatorSuite) TestOperatorStateMarshal(c *C) {
	tbl := []struct {
		state  OperatorState
		except OperatorState
	}{
		{OperatorUnKnownState, OperatorUnKnownState},
		{OperatorWaiting, OperatorWaiting},
		{OperatorRunning, OperatorRunning},
		{OperatorFinished, OperatorFinished},
		{OperatorTimeOut, OperatorTimeOut},
		{OperatorReplaced, OperatorReplaced},
		{OperatorState(404), OperatorUnKnownState},
	}
	for _, t := range tbl {
		data, err := json.Marshal(t.state)
		c.Assert(err, IsNil)
		var newState OperatorState
		err = json.Unmarshal(data, &newState)
		c.Assert(err, IsNil)
		c.Assert(newState, Equals, t.except)
	}
}

func doRegionHeartbeatResponse(region *RegionInfo, resp *pdpb.RegionHeartbeatResponse) {
	if resp == nil {
		return
	}

	if resp.GetTransferLeader() != nil {
		region.Leader = resp.GetTransferLeader().GetPeer()
		return
	}

	switch resp.GetChangePeer().GetChangeType() {
	case pdpb.ConfChangeType_AddNode:
		region.Peers = append(region.Peers, resp.GetChangePeer().GetPeer())
	case pdpb.ConfChangeType_RemoveNode:
		var index int
		for i, p := range region.GetPeers() {
			if p.GetId() == resp.GetChangePeer().GetPeer().GetId() {
				index = i
				break
			}
		}
		region.Peers = append(region.Peers[:index], region.Peers[index+1:]...)
	}
}

func (o *testOperatorSuite) TestOperatorState(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	_, opt := newTestScheduleConfig()

	co := newCoordinator(cluster, opt)
	co.run()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	tc.addRegionStore(4, 4)
	tc.addRegionStore(3, 3)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(1, 1)
	tc.addLeaderRegion(1, 4, 2, 3)

	// Get the operator tansfer peer from store 4 to store 1
	waitOperator(c, co, 1)
	op := co.getOperator(1)
	c.Assert(op.GetState(), Equals, OperatorRunning)
	regionInfo := tc.getRegion(1)

	// Do Operator, Operator start running. doRegionHeartbeatRequest will add one peer in store 1
	c.Assert(regionInfo, NotNil)
	res, finished := op.Do(regionInfo)
	c.Assert(res, NotNil)
	c.Assert(finished, IsFalse)
	c.Assert(op.GetState(), Equals, OperatorRunning)
	doRegionHeartbeatResponse(regionInfo, res)

	// Do Operator, Operator still running. doRegionHeartbeatRequest will tranfer leader from 4
	res, finished = op.Do(regionInfo)
	c.Assert(res, NotNil)
	c.Assert(finished, IsFalse)
	c.Assert(op.GetState(), Equals, OperatorRunning)
	doRegionHeartbeatResponse(regionInfo, res)

	// Do Operator, Operator still running. doRegionHeartbeatRequest will remove one peer in store 4
	res, finished = op.Do(regionInfo)
	c.Assert(res, NotNil)
	c.Assert(finished, IsFalse)
	c.Assert(op.GetState(), Equals, OperatorRunning)
	doRegionHeartbeatResponse(regionInfo, res)

	// Do Operator, Operator finished
	res, finished = op.Do(regionInfo)
	c.Assert(res, IsNil)
	c.Assert(finished, IsTrue)
	c.Assert(op.GetState(), Equals, OperatorFinished)

	regionOp, ok := op.(*regionOperator)
	c.Assert(ok, IsTrue)

	// if operator finished, SetState still finished
	op.SetState(OperatorRunning)
	c.Assert(op.GetState(), Equals, OperatorFinished)

	// SetState success
	regionOp.State = OperatorWaiting
	op.SetState(OperatorRunning)
	c.Assert(op.GetState(), Equals, OperatorRunning)

	regionOp.Start = regionOp.Start.Add(-maxOperatorWaitTime).Add(-time.Minute)
	op.Do(regionInfo)
	c.Assert(op.GetState(), Equals, OperatorTimeOut)

}
