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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
)

type operatorTestSuite struct {
	suite.Suite

	cluster *mockcluster.Cluster
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestOperatorTestSuite(t *testing.T) {
	suite.Run(t, new(operatorTestSuite))
}

func (suite *operatorTestSuite) SetupTest() {
	cfg := mockconfig.NewTestOptions()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster = mockcluster.NewCluster(suite.ctx, cfg)
	suite.cluster.SetMaxMergeRegionSize(2)
	suite.cluster.SetMaxMergeRegionKeys(2)
	suite.cluster.SetLabelProperty(config.RejectLeader, "reject", "leader")
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for storeID, labels := range stores {
		suite.cluster.PutStoreWithLabels(storeID, labels...)
	}
}

func (suite *operatorTestSuite) TearDownTest() {
	suite.cancel()
}

func newTestRegion(regionID uint64, leaderPeer uint64, peers ...[2]uint64) *core.RegionInfo {
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

func (suite *operatorTestSuite) TestOperatorStep() {
	re := suite.Require()
	region := newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	re.False(TransferLeader{FromStore: 1, ToStore: 2}.IsFinish(region))
	re.True(TransferLeader{FromStore: 2, ToStore: 1}.IsFinish(region))
	re.False(AddPeer{ToStore: 3, PeerID: 3}.IsFinish(region))
	re.True(AddPeer{ToStore: 1, PeerID: 1}.IsFinish(region))
	re.False(RemovePeer{FromStore: 1}.IsFinish(region))
	re.True(RemovePeer{FromStore: 3}.IsFinish(region))
}

func checkSteps(re *require.Assertions, op *Operator, steps []OpStep) {
	re.Len(steps, op.Len())
	for i := range steps {
		re.Equal(steps[i], op.Step(i))
	}
}

func (suite *operatorTestSuite) TestOperator() {
	re := suite.Require()
	region := newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	// addPeer1, transferLeader1, removePeer3
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 3, ToStore: 1},
		RemovePeer{FromStore: 3},
	}
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpAdmin|OpLeader|OpRegion, steps...)
	re.Equal(constant.Urgent, op.GetPriorityLevel())
	checkSteps(re, op, steps)
	op.Start()
	re.Nil(op.Check(region))

	re.Equal(SUCCESS, op.Status())
	op.SetStatusReachTime(STARTED, time.Now().Add(-SlowStepWaitTime-time.Second))
	re.False(op.CheckTimeout())

	// addPeer1, transferLeader1, removePeer2
	steps = []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
	re.Equal(constant.Medium, op.GetPriorityLevel())
	checkSteps(re, op, steps)
	op.Start()
	re.Equal(RemovePeer{FromStore: 2}, op.Check(region))
	re.Equal(int32(2), atomic.LoadInt32(&op.currentStep))
	re.False(op.CheckTimeout())
	op.SetStatusReachTime(STARTED, op.GetStartTime().Add(-FastStepWaitTime-2*FastStepWaitTime+time.Second))
	re.False(op.CheckTimeout())
	op.SetStatusReachTime(STARTED, op.GetStartTime().Add(-SlowStepWaitTime-2*FastStepWaitTime-time.Second))
	re.True(op.CheckTimeout())
	res, err := json.Marshal(op)
	re.NoError(err)
	re.Len(res, len(op.String())+2)

	// check short timeout for transfer leader only operators.
	steps = []OpStep{TransferLeader{FromStore: 2, ToStore: 1}}
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader, steps...)
	op.Start()
	re.False(op.CheckTimeout())
	op.SetStatusReachTime(STARTED, op.GetStartTime().Add(-FastStepWaitTime-time.Second))
	re.True(op.CheckTimeout())

	// case2: check timeout operator will return false not panic.
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion, TransferLeader{ToStore: 1, FromStore: 4})
	op.currentStep = 1
	re.True(op.status.To(STARTED))
	re.True(op.status.To(TIMEOUT))
	re.False(op.CheckSuccess())
	re.True(op.CheckTimeout())
}

func (suite *operatorTestSuite) TestInfluence() {
	re := suite.Require()
	region := newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	opInfluence := OpInfluence{StoresInfluence: make(map[uint64]*StoreInfluence)}
	storeOpInfluence := opInfluence.StoresInfluence
	storeOpInfluence[1] = &StoreInfluence{}
	storeOpInfluence[2] = &StoreInfluence{}

	resetInfluence := func() {
		storeOpInfluence[1] = &StoreInfluence{}
		storeOpInfluence[2] = &StoreInfluence{}
	}

	AddLearner{ToStore: 2, PeerID: 2, SendStore: 1}.Influence(opInfluence, region)
	re.Equal(StoreInfluence{
		LeaderSize:  0,
		LeaderCount: 0,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	}, *storeOpInfluence[2])

	re.Equal(StoreInfluence{
		LeaderSize:  0,
		LeaderCount: 0,
		RegionSize:  0,
		RegionCount: 0,
		StepCost:    map[storelimit.Type]int64{storelimit.SendSnapshot: 50},
	}, *storeOpInfluence[1])
	resetInfluence()

	BecomeNonWitness{SendStore: 2, PeerID: 2, StoreID: 1}.Influence(opInfluence, region)
	re.Equal(StoreInfluence{
		LeaderSize:   0,
		LeaderCount:  0,
		RegionSize:   50,
		RegionCount:  0,
		WitnessCount: -1,
		StepCost:     map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	}, *storeOpInfluence[1])

	re.Equal(StoreInfluence{
		LeaderSize:  0,
		LeaderCount: 0,
		RegionSize:  0,
		RegionCount: 0,
		StepCost:    map[storelimit.Type]int64{storelimit.SendSnapshot: 50},
	}, *storeOpInfluence[2])
	resetInfluence()

	AddPeer{ToStore: 2, PeerID: 2}.Influence(opInfluence, region)
	re.Equal(StoreInfluence{
		LeaderSize:  0,
		LeaderCount: 0,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	}, *storeOpInfluence[2])

	TransferLeader{FromStore: 1, ToStore: 2}.Influence(opInfluence, region)
	re.Equal(StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -1,
		RegionSize:  0,
		RegionCount: 0,
		StepCost:    nil,
	}, *storeOpInfluence[1])
	re.Equal(StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	}, *storeOpInfluence[2])

	RemovePeer{FromStore: 1}.Influence(opInfluence, region)
	re.Equal(StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -1,
		RegionSize:  -50,
		RegionCount: -1,
		StepCost:    map[storelimit.Type]int64{storelimit.RemovePeer: 1000},
	}, *storeOpInfluence[1])
	re.Equal(StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	}, *storeOpInfluence[2])

	MergeRegion{IsPassive: false}.Influence(opInfluence, region)
	re.Equal(StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -1,
		RegionSize:  -50,
		RegionCount: -1,
		StepCost:    map[storelimit.Type]int64{storelimit.RemovePeer: 1000},
	}, *storeOpInfluence[1])
	re.Equal(StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 1,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	}, *storeOpInfluence[2])

	MergeRegion{IsPassive: true}.Influence(opInfluence, region)
	re.Equal(StoreInfluence{
		LeaderSize:  -50,
		LeaderCount: -2,
		RegionSize:  -50,
		RegionCount: -2,
		StepCost:    map[storelimit.Type]int64{storelimit.RemovePeer: 1000},
	}, *storeOpInfluence[1])
	re.Equal(StoreInfluence{
		LeaderSize:  50,
		LeaderCount: 1,
		RegionSize:  50,
		RegionCount: 0,
		StepCost:    map[storelimit.Type]int64{storelimit.AddPeer: 1000},
	}, *storeOpInfluence[2])
}

func (suite *operatorTestSuite) TestOperatorKind() {
	re := suite.Require()
	re.Equal("replica,leader", (OpLeader | OpReplica).String())
	re.Equal("unknown", OpKind(0).String())
	k, err := ParseOperatorKind("region,leader")
	re.NoError(err)
	re.Equal(OpRegion|OpLeader, k)
	_, err = ParseOperatorKind("leader,region")
	re.NoError(err)
	_, err = ParseOperatorKind("foobar")
	re.Error(err)
}

func (suite *operatorTestSuite) TestCheckSuccess() {
	re := suite.Require()
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
		re.Equal(CREATED, op.Status())
		re.False(op.CheckSuccess())
		re.True(op.Start())
		re.False(op.CheckSuccess())
		op.currentStep = int32(len(op.steps))
		re.True(op.CheckSuccess())
		re.True(op.CheckSuccess())
	}
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
		op.currentStep = int32(len(op.steps))
		re.Equal(CREATED, op.Status())
		re.False(op.CheckSuccess())
		re.True(op.Start())
		re.True(op.CheckSuccess())
		re.True(op.CheckSuccess())
	}
}

func (suite *operatorTestSuite) TestCheckTimeout() {
	re := suite.Require()
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
		re.Equal(CREATED, op.Status())
		re.True(op.Start())
		op.currentStep = int32(len(op.steps))
		re.False(op.CheckTimeout())
		re.Equal(SUCCESS, op.Status())
	}
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
		re.Equal(CREATED, op.Status())
		re.True(op.Start())
		op.currentStep = int32(len(op.steps))
		op.SetStatusReachTime(STARTED, time.Now().Add(-SlowStepWaitTime))
		re.False(op.CheckTimeout())
		re.Equal(SUCCESS, op.Status())
	}
}

func (suite *operatorTestSuite) TestStart() {
	re := suite.Require()
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
	re.Equal(0, op.GetStartTime().Nanosecond())
	re.Equal(CREATED, op.Status())
	re.True(op.Start())
	re.NotEqual(0, op.GetStartTime().Nanosecond())
	re.Equal(STARTED, op.Status())
}

func (suite *operatorTestSuite) TestCheckExpired() {
	re := suite.Require()
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
	re.False(op.CheckExpired())
	re.Equal(CREATED, op.Status())
	op.SetStatusReachTime(CREATED, time.Now().Add(-OperatorExpireTime))
	re.True(op.CheckExpired())
	re.Equal(EXPIRED, op.Status())
}

func (suite *operatorTestSuite) TestCheck() {
	re := suite.Require()
	{
		region := newTestRegion(2, 2, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := NewTestOperator(2, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
		re.True(op.Start())
		re.NotNil(op.Check(region))

		re.Equal(STARTED, op.Status())
		region = newTestRegion(1, 1, [2]uint64{1, 1})
		re.Nil(op.Check(region))

		re.Equal(SUCCESS, op.Status())
	}
	{
		region := newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
		re.True(op.Start())
		re.NotNil(op.Check(region))
		re.Equal(STARTED, op.Status())
		op.SetStatusReachTime(STARTED, time.Now().Add(-SlowStepWaitTime-2*FastStepWaitTime))
		re.NotNil(op.Check(region))
		re.Equal(TIMEOUT, op.Status())
	}
	{
		region := newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
		re.True(op.Start())
		re.NotNil(op.Check(region))
		re.Equal(STARTED, op.Status())
		op.status.setTime(STARTED, time.Now().Add(-SlowStepWaitTime))
		region = newTestRegion(1, 1, [2]uint64{1, 1})
		re.Nil(op.Check(region))
		re.Equal(SUCCESS, op.Status())
	}
}

func (suite *operatorTestSuite) TestSchedulerKind() {
	re := suite.Require()
	testData := []struct {
		op     *Operator
		expect OpKind
	}{
		{
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpAdmin|OpMerge|OpRegion),
			expect: OpAdmin,
		}, {
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpMerge|OpLeader|OpRegion),
			expect: OpMerge,
		}, {
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpReplica|OpRegion),
			expect: OpReplica,
		}, {
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpSplit|OpRegion),
			expect: OpSplit,
		}, {
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpRange|OpRegion),
			expect: OpRange,
		}, {
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpHotRegion|OpLeader|OpRegion),
			expect: OpHotRegion,
		}, {
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpRegion|OpLeader),
			expect: OpRegion,
		}, {
			op:     NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader),
			expect: OpLeader,
		},
	}
	for _, v := range testData {
		re.Equal(v.expect, v.op.SchedulerKind())
	}
}

func (suite *operatorTestSuite) TestOpStepTimeout() {
	re := suite.Require()
	testData := []struct {
		step       []OpStep
		regionSize int64
		expect     time.Duration
	}{
		{
			// case1: 10GB region will have 60,000s to executor.
			step:       []OpStep{AddLearner{}, AddPeer{}},
			regionSize: 10 * 1000,
			expect:     time.Second * (6 * 10 * 1000),
		}, {
			// case2: 10MB region will have at least SlowStepWaitTime(10min) to executor.
			step:       []OpStep{AddLearner{}, AddPeer{}},
			regionSize: 10,
			expect:     SlowStepWaitTime,
		}, {
			// case3:  10GB region will have 1000s to executor for RemovePeer, TransferLeader, SplitRegion, PromoteLearner.
			step:       []OpStep{RemovePeer{}, TransferLeader{}, SplitRegion{}, PromoteLearner{}},
			regionSize: 10 * 1000,
			expect:     time.Second * (10 * 1000 * 0.6),
		}, {
			// case4: 10MB will have at lease FastStepWaitTime(10s) to executor for RemovePeer, TransferLeader, SplitRegion, PromoteLearner.
			step:       []OpStep{RemovePeer{}, TransferLeader{}, SplitRegion{}, PromoteLearner{}},
			regionSize: 10,
			expect:     FastStepWaitTime,
		}, {
			// case5: 10GB region will have 1000*3 for ChangePeerV2Enter, ChangePeerV2Leave.
			step: []OpStep{ChangePeerV2Enter{PromoteLearners: []PromoteLearner{{}, {}}},
				ChangePeerV2Leave{PromoteLearners: []PromoteLearner{{}, {}}}},
			regionSize: 10 * 1000,
			expect:     time.Second * (10 * 1000 * 0.6 * 3),
		}, {
			//case6: 10GB region will have 1000*10s for ChangePeerV2Enter, ChangePeerV2Leave.
			step:       []OpStep{MergeRegion{}},
			regionSize: 10 * 1000,
			expect:     time.Second * (10 * 1000 * 0.6 * 10),
		},
	}
	for i, v := range testData {
		suite.T().Logf("case: %d", i)
		for _, step := range v.step {
			re.Equal(v.expect, step.Timeout(v.regionSize))
		}
	}
}

func (suite *operatorTestSuite) TestRecord() {
	re := suite.Require()
	operator := NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader, AddLearner{ToStore: 1, PeerID: 1}, RemovePeer{FromStore: 1, PeerID: 1})
	now := time.Now()
	time.Sleep(time.Second)
	ob := operator.Record(now)
	re.Equal(now, ob.FinishTime)
	re.Greater(ob.duration.Seconds(), time.Second.Seconds())
}

func (suite *operatorTestSuite) TestToJSONObject() {
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 3, ToStore: 1},
		RemovePeer{FromStore: 3},
	}
	op := NewTestOperator(101, &metapb.RegionEpoch{}, OpLeader|OpRegion, steps...)
	op.Start()
	obj := op.ToJSONObject()
	suite.Equal("test", obj.Desc)
	suite.Equal("test", obj.Brief)
	suite.Equal(uint64(101), obj.RegionID)
	suite.Equal(OpLeader|OpRegion, obj.Kind)
	suite.Equal("12m0s", obj.Timeout)
	suite.Equal(STARTED, obj.Status)

	// Test SUCCESS status.
	region := newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	suite.Nil(op.Check(region))
	suite.Equal(SUCCESS, op.Status())
	obj = op.ToJSONObject()
	suite.Equal(SUCCESS, obj.Status)

	// Test TIMEOUT status.
	steps = []OpStep{TransferLeader{FromStore: 2, ToStore: 1}}
	op = NewTestOperator(1, &metapb.RegionEpoch{}, OpLeader, steps...)
	op.Start()
	op.SetStatusReachTime(STARTED, op.GetStartTime().Add(-FastStepWaitTime-time.Second))
	suite.True(op.CheckTimeout())
	obj = op.ToJSONObject()
	suite.Equal(TIMEOUT, obj.Status)
}

func TestOperatorCheckConcurrently(t *testing.T) {
	re := require.New(t)
	region := newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	// addPeer1, transferLeader1, removePeer3
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 3, ToStore: 1},
		RemovePeer{FromStore: 3},
	}
	op := NewTestOperator(1, &metapb.RegionEpoch{}, OpAdmin|OpLeader|OpRegion, steps...)
	re.Equal(constant.Urgent, op.GetPriorityLevel())
	checkSteps(re, op, steps)
	op.Start()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			re.Nil(op.Check(region))
		}()
	}
	wg.Wait()
}
