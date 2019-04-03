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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testOperatorControllerSuite{})

type testOperatorControllerSuite struct{}

// issue #1338
func (t *testOperatorControllerSuite) TestGetOpInfluence(c *C) {
	opt := NewMockSchedulerOptions()
	tc := NewMockCluster(opt)
	oc := NewOperatorController(tc, nil)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []OperatorStep{
		RemovePeer{FromStore: 2},
	}
	op1 := NewOperator("testOperator", 1, &metapb.RegionEpoch{}, OpRegion, steps...)
	op2 := NewOperator("testOperator", 2, &metapb.RegionEpoch{}, OpRegion, steps...)
	oc.SetOperator(op1)
	oc.SetOperator(op2)
	go func() {
		for {
			oc.RemoveOperator(op1)
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

type mockHeadbeatStream struct{}

func (m mockHeadbeatStream) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	return
}

func (t *testOperatorControllerSuite) TestOperatorStatus(c *C) {
	opt := NewMockSchedulerOptions()
	tc := NewMockCluster(opt)
	oc := NewOperatorController(tc, nil)
	oc.hbStreams = mockHeadbeatStream{}
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []OperatorStep{
		RemovePeer{FromStore: 2},
		AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := NewOperator("testOperator", 1, &metapb.RegionEpoch{}, OpRegion, steps...)
	op2 := NewOperator("testOperator", 2, &metapb.RegionEpoch{}, OpRegion, steps...)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	oc.SetOperator(op1)
	oc.SetOperator(op2)
	c.Assert(oc.GetOperatorStatus(1).Status, Equals, pdpb.OperatorStatus_RUNNING)
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_RUNNING)
	op1.createTime = time.Now().Add(-10 * time.Minute)
	region2 = tc.ApplyOperatorStep(region2, op2)
	tc.PutRegion(region2)
	oc.Dispatch(region1)
	oc.Dispatch(region2)
	c.Assert(oc.GetOperatorStatus(1).Status, Equals, pdpb.OperatorStatus_TIMEOUT)
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_RUNNING)
	tc.ApplyOperator(op2)
	oc.Dispatch(region2)
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_SUCCESS)
}
