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
