// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
)

type operatorControllerTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc
}

func TestOperatorControllerTestSuite(t *testing.T) {
	suite.Run(t, new(operatorControllerTestSuite))
}

func (suite *operatorControllerTestSuite) SetupSuite() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/operator/unexpectedOperator", "return(true)"))
}

func (suite *operatorControllerTestSuite) TearDownSuite() {
	suite.cancel()
}

func (suite *operatorControllerTestSuite) TestCacheInfluence() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	bc := tc.GetBasicCluster()
	oc := NewController(suite.ctx, bc, tc.GetSharedConfig(), nil)
	tc.AddLeaderStore(2, 1)
	region := tc.AddLeaderRegion(1, 1, 2)

	steps := []OpStep{
		RemovePeer{FromStore: 2},
	}
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, steps...)
	oc.SetOperator(op)
	re.True(op.Start())
	influence := NewOpInfluence()
	AddOpInfluence(op, *influence, bc)
	re.Equal(int64(-96), influence.GetStoreInfluence(2).RegionSize)

	// case: influence is same even if the region size changed.
	region = region.Clone(core.SetApproximateSize(100))
	tc.PutRegion(region)
	influence1 := NewOpInfluence()
	AddOpInfluence(op, *influence1, bc)
	re.Equal(int64(-96), influence1.GetStoreInfluence(2).RegionSize)

	// case: influence is valid even if the region is removed.
	tc.RemoveRegion(region)
	influence2 := NewOpInfluence()
	AddOpInfluence(op, *influence2, bc)
	re.Equal(int64(-96), influence2.GetStoreInfluence(2).RegionSize)
}

// issue #1338
func (suite *operatorControllerTestSuite) TestGetOpInfluence() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), nil)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []OpStep{
		RemovePeer{FromStore: 2},
	}
	op1 := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, steps...)
	op2 := NewTestOperator(2, &metapb.RegionEpoch{}, OpRegion, steps...)
	re.True(op1.Start())
	oc.SetOperator(op1)
	re.True(op2.Start())
	oc.SetOperator(op2)
	go func(ctx context.Context) {
		checkRemoveOperatorSuccess(re, oc, op1)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				re.False(oc.RemoveOperator(op1))
			}
		}
	}(suite.ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				oc.GetOpInfluence(tc.GetBasicCluster())
			}
		}
	}(suite.ctx)
	time.Sleep(time.Second)
	re.NotNil(oc.GetOperator(2))
}

func (suite *operatorControllerTestSuite) TestOperatorStatus() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []OpStep{
		RemovePeer{FromStore: 2},
		AddPeer{ToStore: 2, PeerID: 4},
	}
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	op1 := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, steps...)
	op2 := NewTestOperator(2, &metapb.RegionEpoch{}, OpRegion, steps...)
	re.True(op1.Start())
	oc.SetOperator(op1)
	re.True(op2.Start())
	oc.SetOperator(op2)
	re.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(1).Status)
	re.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(2).Status)
	op1.SetStatusReachTime(STARTED, time.Now().Add(-SlowStepWaitTime-FastStepWaitTime))
	region2 = ApplyOperatorStep(region2, op2)
	tc.PutRegion(region2)
	oc.Dispatch(region1, "test", nil)
	oc.Dispatch(region2, "test", nil)
	re.Equal(pdpb.OperatorStatus_TIMEOUT, oc.GetOperatorStatus(1).Status)
	re.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(2).Status)
	ApplyOperator(tc, op2)
	oc.Dispatch(region2, "test", nil)
	re.Equal(pdpb.OperatorStatus_SUCCESS, oc.GetOperatorStatus(2).Status)
}

func (suite *operatorControllerTestSuite) TestFastFailOperator() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	tc.AddLeaderRegion(1, 1, 2)
	steps := []OpStep{
		RemovePeer{FromStore: 2},
		AddPeer{ToStore: 3, PeerID: 4},
	}
	region := tc.GetRegion(1)
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, steps...)
	re.True(op.Start())
	oc.SetOperator(op)
	oc.Dispatch(region, "test", nil)
	re.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(1).Status)
	// change the leader
	region = region.Clone(core.WithLeader(region.GetPeer(2)))
	oc.Dispatch(region, DispatchFromHeartBeat, nil)
	re.Equal(CANCELED, op.Status())
	re.Nil(oc.GetOperator(region.GetID()))

	// transfer leader to an illegal store.
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 5})
	oc.SetOperator(op)
	oc.Dispatch(region, DispatchFromHeartBeat, nil)
	re.Equal(CANCELED, op.Status())
	re.Nil(oc.GetOperator(region.GetID()))
}

// Issue 3353
func (suite *operatorControllerTestSuite) TestFastFailWithUnhealthyStore() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	tc.AddLeaderRegion(1, 1, 2)
	region := tc.GetRegion(1)
	steps := []OpStep{TransferLeader{ToStore: 2}}
	op := NewTestOperator(1, region.GetRegionEpoch(), OpLeader, steps...)
	oc.SetOperator(op)
	re.False(oc.checkStaleOperator(op, steps[0], region))
	tc.SetStoreDown(2)
	re.True(oc.checkStaleOperator(op, steps[0], region))
}

func (suite *operatorControllerTestSuite) TestCheckAddUnexpectedStatus() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/operator/unexpectedOperator"))

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 2, 1)
	tc.AddLeaderRegion(2, 2, 1)
	region1 := tc.GetRegion(1)
	steps := []OpStep{
		RemovePeer{FromStore: 1},
		AddPeer{ToStore: 1, PeerID: 4},
	}
	{
		// finished op
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
		re.True(oc.checkAddOperator(false, op))
		op.Start()
		re.False(oc.checkAddOperator(false, op)) // started
		re.Nil(op.Check(region1))

		re.Equal(SUCCESS, op.Status())
		re.False(oc.checkAddOperator(false, op)) // success
	}
	{
		// finished op canceled
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
		re.True(oc.checkAddOperator(false, op))
		re.True(op.Cancel(AdminStop))
		re.False(oc.checkAddOperator(false, op))
	}
	{
		// finished op replaced
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
		re.True(oc.checkAddOperator(false, op))
		re.True(op.Start())
		re.True(op.Replace())
		re.False(oc.checkAddOperator(false, op))
	}
	{
		// finished op expired
		op1 := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
		op2 := NewTestOperator(2, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 1})
		re.True(oc.checkAddOperator(false, op1, op2))
		op1.SetStatusReachTime(CREATED, time.Now().Add(-OperatorExpireTime))
		op2.SetStatusReachTime(CREATED, time.Now().Add(-OperatorExpireTime))
		re.False(oc.checkAddOperator(false, op1, op2))
		re.Equal(EXPIRED, op1.Status())
		re.Equal(EXPIRED, op2.Status())
	}
	// finished op never timeout

	{
		// unfinished op timeout
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, steps...)
		re.True(oc.checkAddOperator(false, op))
		op.Start()
		op.SetStatusReachTime(STARTED, time.Now().Add(-SlowStepWaitTime-FastStepWaitTime))
		re.True(op.CheckTimeout())
		re.False(oc.checkAddOperator(false, op))
	}
}

// issue #1716
func (suite *operatorControllerTestSuite) TestConcurrentRemoveOperator() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 2, 1)
	region1 := tc.GetRegion(1)
	steps := []OpStep{
		RemovePeer{FromStore: 1},
		AddPeer{ToStore: 1, PeerID: 4},
	}
	// finished op with normal priority
	op1 := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
	// unfinished op with high priority
	op2 := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion|OpAdmin, steps...)

	re.True(op1.Start())
	oc.SetOperator(op1)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/operator/concurrentRemoveOperator", "return(true)"))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		oc.Dispatch(region1, "test", nil)
		wg.Done()
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		success := oc.AddOperator(op2)
		// If the assert failed before wg.Done, the test will be blocked.
		defer re.True(success)
		wg.Done()
	}()
	wg.Wait()

	re.Equal(op2, oc.GetOperator(1))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/operator/concurrentRemoveOperator"))
}

func (suite *operatorControllerTestSuite) TestPollDispatchRegion() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	tc.AddLeaderRegion(4, 2, 1)
	steps := []OpStep{
		RemovePeer{FromStore: 2},
		AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
	op2 := NewTestOperator(2, &metapb.RegionEpoch{}, OpRegion, steps...)
	op3 := NewTestOperator(3, &metapb.RegionEpoch{}, OpRegion, steps...)
	op4 := NewTestOperator(4, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 2})
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	region4 := tc.GetRegion(4)
	// Adds operator and pushes to the notifier queue.
	{
		re.True(op1.Start())
		oc.SetOperator(op1)
		re.True(op3.Start())
		oc.SetOperator(op3)
		re.True(op4.Start())
		oc.SetOperator(op4)
		re.True(op2.Start())
		oc.SetOperator(op2)
		oc.opNotifierQueue.push(&operatorWithTime{op: op1, time: time.Now().Add(100 * time.Millisecond)})
		oc.opNotifierQueue.push(&operatorWithTime{op: op3, time: time.Now().Add(300 * time.Millisecond)})
		oc.opNotifierQueue.push(&operatorWithTime{op: op4, time: time.Now().Add(499 * time.Millisecond)})
		oc.opNotifierQueue.push(&operatorWithTime{op: op2, time: time.Now().Add(500 * time.Millisecond)})
	}
	// first poll got nil
	r, next := oc.pollNeedDispatchRegion()
	re.Nil(r)
	re.False(next)

	// after wait 100 millisecond, the region1 need to dispatch, but not region2.
	time.Sleep(100 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	re.NotNil(r)
	re.True(next)
	re.Equal(region1.GetID(), r.GetID())

	// find op3 with nil region, remove it
	re.NotNil(oc.GetOperator(3))

	r, next = oc.pollNeedDispatchRegion()
	re.Nil(r)
	re.True(next)
	re.Nil(oc.GetOperator(3))

	// find op4 finished
	r, next = oc.pollNeedDispatchRegion()
	re.NotNil(r)
	re.True(next)
	re.Equal(region4.GetID(), r.GetID())

	// after waiting 500 milliseconds, the region2 need to dispatch
	time.Sleep(400 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	re.NotNil(r)
	re.True(next)
	re.Equal(region2.GetID(), r.GetID())
	r, next = oc.pollNeedDispatchRegion()
	re.Nil(r)
	re.False(next)
}

// issue #7992
func (suite *operatorControllerTestSuite) TestPollDispatchRegionForMergeRegion() {
	re := suite.Require()
	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(suite.ctx, opts)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster, false /* no need to run */)
	controller := NewController(suite.ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)
	cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})

	source := newRegionInfo(101, "1a", "1b", 10, 10, []uint64{101, 1}, []uint64{101, 1})
	source.GetMeta().RegionEpoch = &metapb.RegionEpoch{}
	cluster.PutRegion(source)
	target := newRegionInfo(102, "1b", "1c", 10, 10, []uint64{101, 1}, []uint64{101, 1})
	target.GetMeta().RegionEpoch = &metapb.RegionEpoch{}
	cluster.PutRegion(target)

	ops, err := CreateMergeRegionOperator("merge-region", cluster, source, target, OpMerge)
	re.NoError(err)
	re.Len(ops, 2)
	re.Equal(2, controller.AddWaitingOperator(ops...))
	// Change next push time to now, it's used to make test case faster.
	controller.opNotifierQueue.heap[0].time = time.Now()

	// first poll gets source region op.
	r, next := controller.pollNeedDispatchRegion()
	re.True(next)
	re.Equal(r, source)

	// second poll gets target region op.
	controller.opNotifierQueue.heap[0].time = time.Now()
	r, next = controller.pollNeedDispatchRegion()
	re.True(next)
	re.Equal(r, target)

	// third poll removes the two merge-region ops.
	source.GetMeta().RegionEpoch = &metapb.RegionEpoch{ConfVer: 0, Version: 1}
	r, next = controller.pollNeedDispatchRegion()
	re.True(next)
	re.Nil(r)
	re.Equal(1, controller.opNotifierQueue.len())
	re.Empty(controller.GetOperators())
	re.Empty(controller.wop.ListOperator())
	re.NotNil(controller.records.Get(101))
	re.NotNil(controller.records.Get(102))

	// fourth poll removes target region op from opNotifierQueue
	controller.opNotifierQueue.heap[0].time = time.Now()
	r, next = controller.pollNeedDispatchRegion()
	re.True(next)
	re.Nil(r)
	re.Equal(0, controller.opNotifierQueue.len())

	// Add the two ops to waiting operators again.
	source.GetMeta().RegionEpoch = &metapb.RegionEpoch{ConfVer: 0, Version: 0}
	controller.records.ttl.Remove(101)
	controller.records.ttl.Remove(102)
	ops, err = CreateMergeRegionOperator("merge-region", cluster, source, target, OpMerge)
	re.NoError(err)
	re.Equal(2, controller.AddWaitingOperator(ops...))
	// change the target RegionEpoch
	// first poll gets source region from opNotifierQueue
	target.GetMeta().RegionEpoch = &metapb.RegionEpoch{ConfVer: 0, Version: 1}
	controller.opNotifierQueue.heap[0].time = time.Now()
	r, next = controller.pollNeedDispatchRegion()
	re.True(next)
	re.Equal(r, source)

	r, next = controller.pollNeedDispatchRegion()
	re.True(next)
	re.Nil(r)
	re.Equal(1, controller.opNotifierQueue.len())
	re.Empty(controller.GetOperators())
	re.Empty(controller.wop.ListOperator())
	re.NotNil(controller.records.Get(101))
	re.NotNil(controller.records.Get(102))

	controller.opNotifierQueue.heap[0].time = time.Now()
	r, next = controller.pollNeedDispatchRegion()
	re.True(next)
	re.Nil(r)
	re.Equal(0, controller.opNotifierQueue.len())
}

func (suite *operatorControllerTestSuite) TestCheckOperatorLightly() {
	re := suite.Require()
	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(suite.ctx, opts)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster, false /* no need to run */)
	controller := NewController(suite.ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)
	cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})

	source := newRegionInfo(101, "1a", "1b", 10, 10, []uint64{101, 1}, []uint64{101, 1})
	source.GetMeta().RegionEpoch = &metapb.RegionEpoch{}
	cluster.PutRegion(source)
	target := newRegionInfo(102, "1b", "1c", 10, 10, []uint64{101, 1}, []uint64{101, 1})
	target.GetMeta().RegionEpoch = &metapb.RegionEpoch{}
	cluster.PutRegion(target)

	ops, err := CreateMergeRegionOperator("merge-region", cluster, source, target, OpMerge)
	re.NoError(err)
	re.Len(ops, 2)

	// check successfully
	r, reason := controller.checkOperatorLightly(ops[0])
	re.Empty(reason)
	re.Equal(r, source)

	// check failed because of region disappeared
	cluster.RemoveRegion(target)
	r, reason = controller.checkOperatorLightly(ops[1])
	re.Nil(r)
	re.Equal(reason, RegionNotFound)

	// check failed because of versions of region epoch changed
	cluster.PutRegion(target)
	source.GetMeta().RegionEpoch = &metapb.RegionEpoch{ConfVer: 0, Version: 1}
	r, reason = controller.checkOperatorLightly(ops[0])
	re.Nil(r)
	re.Equal(reason, EpochNotMatch)
}

func (suite *operatorControllerTestSuite) TestStoreLimit() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	tc.AddLeaderStore(1, 0)
	tc.UpdateLeaderCount(1, 1000)
	tc.AddLeaderStore(2, 0)
	for i := uint64(1); i <= 1000; i++ {
		tc.AddLeaderRegion(i, i)
		// make it small region
		tc.PutRegion(tc.GetRegion(i).Clone(core.SetApproximateSize(10)))
	}

	tc.SetStoreLimit(2, storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, AddPeer{ToStore: 2, PeerID: i})
		re.True(oc.AddOperator(op))
		checkRemoveOperatorSuccess(re, oc, op)
	}
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, AddPeer{ToStore: 2, PeerID: 1})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))

	tc.SetStoreLimit(2, storelimit.AddPeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = NewTestOperator(i, &metapb.RegionEpoch{}, OpRegion, AddPeer{ToStore: 2, PeerID: i})
		re.True(oc.AddOperator(op))
		checkRemoveOperatorSuccess(re, oc, op)
	}
	tc.SetAllStoresLimit(storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = NewTestOperator(i, &metapb.RegionEpoch{}, OpRegion, AddPeer{ToStore: 2, PeerID: i})
		re.True(oc.AddOperator(op))
		checkRemoveOperatorSuccess(re, oc, op)
	}
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, AddPeer{ToStore: 2, PeerID: 1})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))

	tc.SetStoreLimit(2, storelimit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, RemovePeer{FromStore: 2})
		re.True(oc.AddOperator(op))
		checkRemoveOperatorSuccess(re, oc, op)
	}
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, RemovePeer{FromStore: 2})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))

	tc.SetStoreLimit(2, storelimit.RemovePeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = NewTestOperator(i, &metapb.RegionEpoch{}, OpRegion, RemovePeer{FromStore: 2})
		re.True(oc.AddOperator(op))
		checkRemoveOperatorSuccess(re, oc, op)
	}
	tc.SetAllStoresLimit(storelimit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = NewTestOperator(i, &metapb.RegionEpoch{}, OpRegion, RemovePeer{FromStore: 2})
		re.True(oc.AddOperator(op))
		checkRemoveOperatorSuccess(re, oc, op)
	}
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, RemovePeer{FromStore: 2})
	re.False(oc.AddOperator(op))
	re.False(oc.RemoveOperator(op))
}

// #1652
func (suite *operatorControllerTestSuite) TestDispatchOutdatedRegion() {
	re := suite.Require()
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster, false /* no need to run */)
	controller := NewController(suite.ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)

	cluster.AddLeaderStore(1, 2)
	cluster.AddLeaderStore(2, 0)
	cluster.SetAllStoresLimit(storelimit.RemovePeer, 600)
	cluster.AddLeaderRegion(1, 1, 2)
	steps := []OpStep{
		TransferLeader{FromStore: 1, ToStore: 2},
		RemovePeer{FromStore: 1},
	}

	op := NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 0, Version: 0}, OpRegion, steps...)
	re.True(controller.AddOperator(op))
	re.Equal(1, stream.MsgLength())

	// report the result of transferring leader
	region := cluster.MockRegionInfo(1, 2, []uint64{1, 2}, []uint64{},
		&metapb.RegionEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(region, DispatchFromHeartBeat, nil)
	re.Equal(uint64(0), op.ConfVerChanged(region))
	re.Equal(2, stream.MsgLength())

	// report the result of removing peer
	region = cluster.MockRegionInfo(1, 2, []uint64{2}, []uint64{},
		&metapb.RegionEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(region, DispatchFromHeartBeat, nil)
	re.Equal(uint64(1), op.ConfVerChanged(region))
	re.Equal(2, stream.MsgLength())

	// add and dispatch op again, the op should be stale
	op = NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 0, Version: 0},
		OpRegion, steps...)
	re.True(controller.AddOperator(op))
	re.Equal(uint64(0), op.ConfVerChanged(region))
	re.Equal(3, stream.MsgLength())

	// report region with an abnormal confver
	region = cluster.MockRegionInfo(1, 1, []uint64{1, 2}, []uint64{},
		&metapb.RegionEpoch{ConfVer: 1, Version: 0})
	controller.Dispatch(region, DispatchFromHeartBeat, nil)
	re.Equal(uint64(0), op.ConfVerChanged(region))
	// no new step
	re.Equal(3, stream.MsgLength())
}

func (suite *operatorControllerTestSuite) TestCalcInfluence() {
	re := suite.Require()
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster, false /* no need to run */)
	controller := NewController(suite.ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)

	epoch := &metapb.RegionEpoch{ConfVer: 0, Version: 0}
	region := cluster.MockRegionInfo(1, 1, []uint64{2}, []uint64{}, epoch)
	region = region.Clone(core.SetApproximateSize(20))
	cluster.PutRegion(region)
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(3, 1)

	steps := []OpStep{
		AddLearner{ToStore: 3, PeerID: 3},
		PromoteLearner{ToStore: 3, PeerID: 3},
		TransferLeader{FromStore: 1, ToStore: 3},
		RemovePeer{FromStore: 1},
	}
	op := NewTestOperator(1, epoch, OpRegion, steps...)
	re.True(controller.AddOperator(op))

	check := func(influence OpInfluence, id uint64, expect *StoreInfluence) {
		si := influence.GetStoreInfluence(id)
		re.Equal(si.LeaderCount, expect.LeaderCount)
		re.Equal(si.LeaderSize, expect.LeaderSize)
		re.Equal(si.RegionCount, expect.RegionCount)
		re.Equal(si.RegionSize, expect.RegionSize)
		re.Equal(si.StepCost[storelimit.AddPeer], expect.StepCost[storelimit.AddPeer])
		re.Equal(si.StepCost[storelimit.RemovePeer], expect.StepCost[storelimit.RemovePeer])
	}

	influence := controller.GetOpInfluence(cluster.GetBasicCluster())
	check(influence, 1, &StoreInfluence{
		LeaderSize:  -20,
		LeaderCount: -1,
		RegionSize:  -20,
		RegionCount: -1,
		StepCost: map[storelimit.Type]int64{
			storelimit.RemovePeer: 200,
		},
	})
	check(influence, 3, &StoreInfluence{
		LeaderSize:  20,
		LeaderCount: 1,
		RegionSize:  20,
		RegionCount: 1,
		StepCost: map[storelimit.Type]int64{
			storelimit.AddPeer: 200,
		},
	})

	region2 := region.Clone(
		core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}),
		core.WithIncConfVer(),
	)
	re.True(steps[0].IsFinish(region2))
	op.Check(region2)

	influence = controller.GetOpInfluence(cluster.GetBasicCluster())
	check(influence, 1, &StoreInfluence{
		LeaderSize:  -20,
		LeaderCount: -1,
		RegionSize:  -20,
		RegionCount: -1,
		StepCost: map[storelimit.Type]int64{
			storelimit.RemovePeer: 200,
		},
	})
	check(influence, 3, &StoreInfluence{
		LeaderSize:  20,
		LeaderCount: 1,
		RegionSize:  0,
		RegionCount: 0,
		StepCost:    make(map[storelimit.Type]int64),
	})
}

func (suite *operatorControllerTestSuite) TestDispatchUnfinishedStep() {
	re := suite.Require()
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster, false /* no need to run */)
	controller := NewController(suite.ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)

	// Create a new region with epoch(0, 0)
	// the region has two peers with its peer id allocated incrementally.
	// so the two peers are {peerid: 1, storeid: 1}, {peerid: 2, storeid: 2}
	// The peer on store 1 is the leader
	epoch := &metapb.RegionEpoch{ConfVer: 0, Version: 0}
	region := cluster.MockRegionInfo(1, 1, []uint64{2}, []uint64{}, epoch)
	// Put region into cluster, otherwise, AddOperator will fail because of
	// missing region
	cluster.PutRegion(region)
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(3, 1)
	// The next allocated peer should have peerid 3, so we add this peer
	// to store 3
	testSteps := [][]OpStep{
		{
			AddLearner{ToStore: 3, PeerID: 3},
			PromoteLearner{ToStore: 3, PeerID: 3},
			TransferLeader{FromStore: 1, ToStore: 3},
			RemovePeer{FromStore: 1},
		},
		{
			AddLearner{ToStore: 3, PeerID: 3, IsLightWeight: true},
			PromoteLearner{ToStore: 3, PeerID: 3},
			TransferLeader{FromStore: 1, ToStore: 3},
			RemovePeer{FromStore: 1},
		},
	}

	for _, steps := range testSteps {
		// Create an operator
		op := NewTestOperator(1, epoch, OpRegion, steps...)
		re.True(controller.AddOperator(op))
		re.Equal(1, stream.MsgLength())

		// Create region2 which is cloned from the original region.
		// region2 has peer 2 in pending state, so the AddPeer step
		// is left unfinished
		region2 := region.Clone(
			core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}),
			core.WithPendingPeers([]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			}),
			core.WithIncConfVer(),
		)
		re.NotNil(region2.GetPendingPeers())

		re.False(steps[0].IsFinish(region2))
		controller.Dispatch(region2, DispatchFromHeartBeat, nil)

		// In this case, the conf version has been changed, but the
		// peer added is in pending state, the operator should not be
		// removed by the stale checker
		re.Equal(uint64(1), op.ConfVerChanged(region2))
		re.NotNil(controller.GetOperator(1))

		// The operator is valid yet, but the step should not be sent
		// again, because it is in pending state, so the message channel
		// should not be increased
		re.Equal(1, stream.MsgLength())

		// Finish the step by clearing the pending state
		region3 := region.Clone(
			core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}),
			core.WithIncConfVer(),
		)
		re.True(steps[0].IsFinish(region3))
		controller.Dispatch(region3, DispatchFromHeartBeat, nil)
		re.Equal(uint64(1), op.ConfVerChanged(region3))
		re.Equal(2, stream.MsgLength())

		region4 := region3.Clone(
			core.WithRole(3, metapb.PeerRole_Voter),
			core.WithIncConfVer(),
		)
		re.True(steps[1].IsFinish(region4))
		controller.Dispatch(region4, DispatchFromHeartBeat, nil)
		re.Equal(uint64(2), op.ConfVerChanged(region4))
		re.Equal(3, stream.MsgLength())

		// Transfer leader
		region5 := region4.Clone(
			core.WithLeader(region4.GetStorePeer(3)),
		)
		re.True(steps[2].IsFinish(region5))
		controller.Dispatch(region5, DispatchFromHeartBeat, nil)
		re.Equal(uint64(2), op.ConfVerChanged(region5))
		re.Equal(4, stream.MsgLength())

		// Remove peer
		region6 := region5.Clone(
			core.WithRemoveStorePeer(1),
			core.WithIncConfVer(),
		)
		re.True(steps[3].IsFinish(region6))
		controller.Dispatch(region6, DispatchFromHeartBeat, nil)
		re.Equal(uint64(3), op.ConfVerChanged(region6))

		// The Operator has finished, so no message should be sent
		re.Equal(4, stream.MsgLength())
		re.Nil(controller.GetOperator(1))
		e := stream.Drain(4)
		re.NoError(e)
	}
}

func newRegionInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.RegionInfo {
	prs := make([]*metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		prs = append(prs, &metapb.Peer{Id: peer[0], StoreId: peer[1]})
	}
	start, _ := hex.DecodeString(startKey)
	end, _ := hex.DecodeString(endKey)
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       id,
			StartKey: start,
			EndKey:   end,
			Peers:    prs,
		},
		&metapb.Peer{Id: leader[0], StoreId: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}

func checkRemoveOperatorSuccess(re *require.Assertions, oc *Controller, op *Operator) {
	re.True(oc.RemoveOperator(op))
	re.True(op.IsEnd())
	re.Equal(op, oc.GetOperatorStatus(op.RegionID()).Operator)
}

func (suite *operatorControllerTestSuite) TestAddWaitingOperator() {
	re := suite.Require()
	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(suite.ctx, opts)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster, false /* no need to run */)
	controller := NewController(suite.ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)
	cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
	addPeerOp := func(i uint64) *Operator {
		start := fmt.Sprintf("%da", i)
		end := fmt.Sprintf("%db", i)
		region := newRegionInfo(i, start, end, 1, 1, []uint64{101, 1}, []uint64{101, 1})
		cluster.PutRegion(region)
		peer := &metapb.Peer{
			StoreId: 2,
		}
		op, err := CreateAddPeerOperator("add-peer", cluster, region, peer, OpKind(0))
		re.NoError(err)
		re.NotNil(op)

		return op
	}

	// a batch of operators should be added atomically
	var batch []*Operator
	for i := range cluster.GetSchedulerMaxWaitingOperator() {
		batch = append(batch, addPeerOp(i))
	}
	added := controller.AddWaitingOperator(batch...)
	re.Equal(int(cluster.GetSchedulerMaxWaitingOperator()), added)

	// test adding a batch of operators when some operators will get false in check
	// and remain operators can be added normally
	batch = append(batch, addPeerOp(cluster.GetSchedulerMaxWaitingOperator()))
	added = controller.AddWaitingOperator(batch...)
	re.Equal(1, added)

	scheduleCfg := opts.GetScheduleConfig().Clone()
	scheduleCfg.SchedulerMaxWaitingOperator = 1
	opts.SetScheduleConfig(scheduleCfg)
	batch = append(batch, addPeerOp(100))
	added = controller.AddWaitingOperator(batch...)
	re.Equal(1, added)
	re.NotNil(controller.GetOperator(uint64(100)))

	source := newRegionInfo(101, "1a", "1b", 1, 1, []uint64{101, 1}, []uint64{101, 1})
	cluster.PutRegion(source)
	target := newRegionInfo(102, "0a", "0b", 1, 1, []uint64{101, 1}, []uint64{101, 1})
	cluster.PutRegion(target)

	ops, err := CreateMergeRegionOperator("merge-region", cluster, source, target, OpMerge)
	re.NoError(err)
	re.Len(ops, 2)

	// test with label schedule=deny
	labelerManager := cluster.GetRegionLabeler()
	labelerManager.SetLabelRule(&labeler.LabelRule{
		ID:       "schedulelabel",
		Labels:   []labeler.RegionLabel{{Key: "schedule", Value: "deny"}},
		RuleType: labeler.KeyRange,
		Data:     []any{map[string]any{"start_key": "1a", "end_key": "1b"}},
	})

	re.True(labelerManager.ScheduleDisabled(source))
	// add operator should be success since it is not check in addWaitingOperator
	re.Equal(2, controller.AddWaitingOperator(ops...))
}

// issue #5279
func (suite *operatorControllerTestSuite) TestInvalidStoreId() {
	re := suite.Require()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	// If PD and store 3 are gone, PD will not have info of store 3 after recreating it.
	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 1)
	tc.AddRegionStore(4, 1)
	tc.AddLeaderRegionWithRange(1, "", "", 1, 2, 3, 4)
	steps := []OpStep{
		RemovePeer{FromStore: 3, PeerID: 3, IsDownStore: false},
	}
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, steps...)
	re.True(oc.AddOperator(op))
	// Although store 3 does not exist in PD, PD can also send op to TiKV.
	re.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(1).Status)
}

func TestConcurrentAddOperatorAndSetStoreLimit(t *testing.T) {
	re := require.New(t)
	opt := mockconfig.NewTestOptions()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false /* no need to run */)
	oc := NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)

	regionNum := 1000
	limit := 1600.0
	storeID := uint64(2)
	for i := 1; i < 4; i++ {
		tc.AddRegionStore(uint64(i), regionNum)
		tc.SetStoreLimit(uint64(i), storelimit.AddPeer, limit)
	}
	for i := 1; i <= regionNum; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 3, 4)
	}

	// Add operator and set store limit concurrently
	var wg sync.WaitGroup
	for i := 1; i < 10; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			for j := 1; j < 10; j++ {
				regionID := uint64(j) + i*100
				op := NewTestOperator(regionID, tc.GetRegion(regionID).GetRegionEpoch(), OpRegion, AddPeer{ToStore: storeID, PeerID: regionID})
				re.True(oc.AddOperator(op))
				tc.SetStoreLimit(storeID, storelimit.AddPeer, limit-float64(j)) // every goroutine set a different limit
			}
		}(uint64(i))
	}
	wg.Wait()
}
