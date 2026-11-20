// Copyright 2022 TiKV Project Authors.
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

package schedulers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/sasha-s/go-deadlock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestMain(m *testing.M) {
	// Pooled timers cannot cross synctest bubble boundaries.
	deadlock.Opts.TimerPool = deadlock.TimerPoolDisabled
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

// Control the native sample only. Region eligibility, both planning passes,
// queue admission and transfer completion use the real scheduler/controller.
type fixedBalanceSample struct {
	sche.SchedulerCluster
	out, in *core.RegionInfo
}

func (c fixedBalanceSample) RandLeaderRegions(id uint64, _ []keyutil.KeyRange) []*core.RegionInfo {
	if id == c.out.GetLeader().GetStoreId() {
		return []*core.RegionInfo{c.out}
	}
	return nil
}

func (c fixedBalanceSample) RandFollowerRegions(id uint64, _ []keyutil.KeyRange) []*core.RegionInfo {
	if c.in.GetLeader().GetStoreId() != id && c.in.GetStorePeer(id) != nil {
		return []*core.RegionInfo{c.in}
	}
	return nil
}

func TestBalanceLeaderReturnsPlannedEvacuation(t *testing.T) {
	for _, tt := range []struct {
		name           string
		source, home   uint64
		native, former [2]uint64 // reads/s
		nativeLast     uint64
		largeSize      int64 // MiB, nonzero enables size balance
		nativeSizes    [2]int64
		want           [2]uint64
	}{
		{"both fit", 10000, 1000, [2]uint64{}, [2]uint64{2000, 1900}, 102, 0, [2]int64{}, [2]uint64{11, 12}},
		{"native slot already returns the busiest", 10000, 1000, [2]uint64{}, [2]uint64{2000, 1900}, 11, 0, [2]int64{}, [2]uint64{12, 11}},
		{"two native moves balance exactly", 120, 40, [2]uint64{20, 20}, [2]uint64{30, 20}, 102, 0, [2]int64{}, [2]uint64{12, 102}},
		{"second substitution would cancel first", 106, 45, [2]uint64{15, 8}, [2]uint64{17, 15}, 102, 0, [2]int64{}, [2]uint64{11, 102}},
		{"size would overshoot", 10000, 1000, [2]uint64{}, [2]uint64{2000, 1900}, 102, 250, [2]int64{}, [2]uint64{12, 13}},
		{"different size fits", 10000, 1000, [2]uint64{}, [2]uint64{2000, 1900}, 102, 25, [2]int64{}, [2]uint64{11, 12}},
		{"first size move can fail", 10000, 1000, [2]uint64{}, [2]uint64{2000, 1900}, 102, 30, [2]int64{60, 90}, [2]uint64{101, 102}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, oc := prepareSchedulersTest(false)
			defer cancel()
			now := time.Now().Unix()
			store := func(id uint64, start int64, rate uint64) {
				tc.PutStore(tc.GetStore(id), core.SetStoreStartTime(start), core.SetLastHeartbeatTS(time.Now()), core.SetStoreStats(&pdpb.StoreStats{KeysRead: rate * 10, Interval: &pdpb.TimeInterval{StartTimestamp: uint64(now - 10), EndTimestamp: uint64(now)}}))
			}
			tc.AddLeaderStore(1, 100, 300)
			tc.AddLeaderStore(2, 1, 100)
			if tt.largeSize != 0 {
				tc.SetLeaderSchedulePolicy(constant.BySize.String())
				tc.SetTolerantSizeRatio(1)
			}
			store(1, now-120, tt.source)
			store(2, now-120, tt.home)
			report := func(r *core.RegionInfo, leader, term, rate uint64, end int64) *core.RegionInfo {
				size := uint64(10 << 20)
				if r.GetID() == 11 && tt.largeSize > 0 {
					size = uint64(tt.largeSize) << 20
				}
				if id := r.GetID(); (id == 101 || id == 102) && tt.nativeSizes[id-101] != 0 {
					size = uint64(tt.nativeSizes[id-101]) << 20
				}
				r = core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{Region: r.GetMeta(), Leader: r.GetStorePeer(leader), Term: term, ApproximateSize: size, KeysRead: rate * 10, Interval: &pdpb.TimeInterval{StartTimestamp: uint64(end - 10), EndTimestamp: uint64(end)}}, 0)
				tc.PutRegion(r)
				return r
			}
			round := oc.CurrentRecoveryRound()
			round.ID++
			re.NoError(oc.UpdateRecoveryRound(round, 2, now-120))
			for id := uint64(11); id < 27; id++ {
				rate := uint64(4)
				if id < 13 {
					rate = tt.former[id-11]
				}
				r := report(tc.AddLeaderRegion(id, 2, 1), 2, 7, rate, now-40)
				ev, err := operator.CreateTransferLeaderOperator(types.EvictLeaderScheduler.String(), tc, r, 1, nil, operator.OpLeader)
				re.NoError(err)
				oc.PlanEvictions(tc, []*operator.Operator{ev})
				re.True(oc.AddOperator(ev))
				oc.Dispatch(report(r, 1, 8, rate, now-30), operator.DispatchFromHeartBeat, func(uint64) {})
			}
			now += 20
			store(1, now-140, tt.source)
			store(2, now-40, tt.home)
			a := report(tc.AddLeaderRegion(101, 1, 2), 1, 1, tt.native[0], now)
			report(tc.AddLeaderRegion(102, 1, 2), 1, 1, tt.native[1], now)
			b := tc.GetRegion(tt.nativeLast)
			lb, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, nil))
			re.NoError(err)
			lb.(*balanceLeaderScheduler).conf.Batch = 2
			ops, _ := lb.Schedule(fixedBalanceSample{tc, a, b}, false)
			re.Len(ops, 2)
			re.Equal(2, oc.AddWaitingOperator(ops...))
			for i, op := range ops {
				re.Equal(tt.want[i], op.RegionID())
				re.Same(op, oc.GetOperator(op.RegionID()))
				operatorutil.CheckTransferLeader(re, op, operator.OpLeader, 1, 2)
				r := tc.GetRegion(op.RegionID())
				rate, ok := r.ReadKeysPerSecond()
				re.True(ok)
				oc.Dispatch(report(r, 2, r.GetTerm()+1, uint64(rate), now+1), operator.DispatchFromHeartBeat, func(uint64) {})
				re.Equal(operator.SUCCESS, op.Status())
			}
		})
	}
}

func TestBalanceLeaderSchedulerConfigClone(t *testing.T) {
	re := require.New(t)
	keyRanges1, err := getKeyRanges([]string{"a", "b", "c", "d"})
	re.NoError(err)
	conf := &balanceLeaderSchedulerConfig{
		Ranges: keyRanges1,
		Batch:  10,
	}
	conf2 := conf.clone()
	re.Equal(conf.Batch, conf2.Batch)
	re.Equal(conf.Ranges, conf2.Ranges)

	keyRanges2, err := getKeyRanges([]string{"e", "f", "g", "h"})
	re.NoError(err)
	// update conf2
	conf2.Ranges[1] = keyRanges2[1]
	re.NotEqual(conf.Ranges, conf2.Ranges)
}

func TestBalanceLeaderBatchLimit(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	lb, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)

	body, err := json.Marshal(map[string]any{"batch": MaxBalanceLeaderBatchSize})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	lb.ServeHTTP(resp, req)
	re.Equal(http.StatusOK, resp.Code)
	re.Equal(MaxBalanceLeaderBatchSize, lb.(*balanceLeaderScheduler).conf.getBatch())

	body, err = json.Marshal(map[string]any{"batch": MaxBalanceLeaderBatchSize + 1})
	re.NoError(err)
	req = httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp = httptest.NewRecorder()
	lb.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Equal(MaxBalanceLeaderBatchSize, lb.(*balanceLeaderScheduler).conf.getBatch())
}

type balanceLeaderSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     Scheduler
	oc     *operator.Controller
	conf   config.SchedulerConfigProvider
}

func TestBalanceLeaderSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceLeaderSchedulerTestSuite))
}

func (suite *balanceLeaderSchedulerTestSuite) SetupTest() {
	re := suite.Require()
	suite.cancel, suite.conf, suite.tc, suite.oc = prepareSchedulersTest()
	lb, err := CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	suite.lb = lb
}

func (suite *balanceLeaderSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *balanceLeaderSchedulerTestSuite) schedule() []*operator.Operator {
	ops, _ := suite.lb.Schedule(suite.tc, false)
	return ops
}

func (suite *balanceLeaderSchedulerTestSuite) dryRun() []plan.Plan {
	_, plans := suite.lb.Schedule(suite.tc, true)
	return plans
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceLimit() {
	re := suite.Require()
	suite.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	suite.tc.AddLeaderStore(1, 1)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 0)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	re.Empty(suite.schedule())

	// Stores:     1    2    3    4
	// Leaders:    16   0    0    0
	// Region1:    L    F    F    F
	suite.tc.UpdateLeaderCount(1, 16)
	re.NotEmpty(suite.schedule())

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	suite.tc.UpdateLeaderCount(1, 7)
	suite.tc.UpdateLeaderCount(2, 8)
	suite.tc.UpdateLeaderCount(3, 9)
	suite.tc.UpdateLeaderCount(4, 10)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	re.Empty(suite.schedule())
	plans := suite.dryRun()
	re.NotEmpty(plans)
	re.Equal(3, plans[0].GetStep())
	re.Equal(plan.StatusStoreScoreDisallowed, int(plans[0].GetStatus().StatusCode))

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	suite.tc.UpdateLeaderCount(4, 16)
	re.NotEmpty(suite.schedule())
	exhaustTransferLeaderInLimit(suite.T(), suite.tc, 1, 2, 3, 4)
	re.Empty(suite.schedule())
	suite.tc.SetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited)
	suite.tc.ResetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited/time.Minute.Seconds())
	ops := suite.schedule()
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpLeader, 4, 2)
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceLeaderSchedulePolicy() {
	re := suite.Require()
	// Stores:          1       2       3       4
	// Leader Count:    10      10      10      10
	// Leader Size :    10000   100    	100    	100
	// Region1:         L       F       F       F
	suite.tc.AddLeaderStore(1, 10, 10000*units.MiB)
	suite.tc.AddLeaderStore(2, 10, 100*units.MiB)
	suite.tc.AddLeaderStore(3, 10, 100*units.MiB)
	suite.tc.AddLeaderStore(4, 10, 100*units.MiB)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	re.Equal(constant.ByCount.String(), suite.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	re.Empty(suite.schedule())
	plans := suite.dryRun()
	re.NotEmpty(plans)
	re.Equal(3, plans[0].GetStep())
	re.Equal(plan.StatusStoreScoreDisallowed, int(plans[0].GetStatus().StatusCode))

	suite.tc.SetLeaderSchedulePolicy(constant.BySize.String())
	re.NotEmpty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceLeaderTolerantRatio() {
	re := suite.Require()
	suite.tc.SetTolerantSizeRatio(2.5)
	// test schedule leader by count, with tolerantSizeRatio=2.5
	// Stores:          1       2       3       4
	// Leader Count:    14->15  10      10      10
	// Leader Size :    100     100     100     100
	// Region1:         L       F       F       F
	suite.tc.AddLeaderStore(1, 14, 100)
	suite.tc.AddLeaderStore(2, 10, 100)
	suite.tc.AddLeaderStore(3, 10, 100)
	suite.tc.AddLeaderStore(4, 10, 100)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	re.Equal(constant.ByCount.String(), suite.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	re.Empty(suite.schedule())
	re.Equal(14, suite.tc.GetStore(1).GetLeaderCount())
	suite.tc.AddLeaderStore(1, 15, 100)
	re.Equal(15, suite.tc.GetStore(1).GetLeaderCount())
	re.NotEmpty(suite.schedule())
	suite.tc.SetTolerantSizeRatio(6) // (15-10)<6
	re.Empty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestScheduleWithOpInfluence() {
	re := suite.Require()
	suite.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   14
	// Region1:    F    F    F    L
	suite.tc.AddLeaderStore(1, 7)
	suite.tc.AddLeaderStore(2, 8)
	suite.tc.AddLeaderStore(3, 9)
	suite.tc.AddLeaderStore(4, 14)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	op := suite.schedule()[0]
	re.NotNil(op)
	suite.oc.SetOperator(op)
	// After considering the scheduled operator, leaders of store1 and store4 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when leader difference is not greater than 5.
	re.Equal(constant.ByCount.String(), suite.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	re.NotEmpty(suite.schedule())
	suite.tc.SetLeaderSchedulePolicy(constant.BySize.String())
	re.Empty(suite.schedule())

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   13
	// Region1:    F    F    F    L
	suite.tc.UpdateLeaderCount(1, 8)
	suite.tc.UpdateLeaderCount(2, 8)
	suite.tc.UpdateLeaderCount(3, 9)
	suite.tc.UpdateLeaderCount(4, 13)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	re.Empty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestTransferLeaderOut() {
	re := suite.Require()
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   12
	suite.tc.AddLeaderStore(1, 7)
	suite.tc.AddLeaderStore(2, 8)
	suite.tc.AddLeaderStore(3, 9)
	suite.tc.AddLeaderStore(4, 12)
	suite.tc.SetTolerantSizeRatio(0.1)
	for i := uint64(1); i <= 7; i++ {
		suite.tc.AddLeaderRegion(i, 4, 1, 2, 3)
	}

	// balance leader: 4->1, 4->1, 4->2
	regions := make(map[uint64]struct{})
	targets := map[uint64]uint64{
		1: 2,
		2: 1,
	}
	for range 20 {
		if len(suite.schedule()) == 0 {
			continue
		}
		if op := suite.schedule()[0]; op != nil {
			if _, ok := regions[op.RegionID()]; !ok {
				suite.oc.SetOperator(op)
				regions[op.RegionID()] = struct{}{}
				tr := op.Step(0).(operator.TransferLeader)
				re.Equal(uint64(4), tr.FromStore)
				targets[tr.ToStore]--
			}
		}
	}
	re.Len(regions, 3)
	for _, count := range targets {
		re.Zero(count)
	}
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceFilter() {
	re := suite.Require()
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    F    F    F    L
	suite.tc.AddLeaderStore(1, 1)
	suite.tc.AddLeaderStore(2, 2)
	suite.tc.AddLeaderStore(3, 3)
	suite.tc.AddLeaderStore(4, 16)
	suite.tc.AddLeaderRegion(1, 4, 1, 2, 3)

	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 1)
	// Test stateFilter.
	// if store 4 is offline, we should consider it
	// because it still provides services
	suite.tc.SetStoreOffline(4)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 1)
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	suite.tc.SetStoreDown(1)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 2)
	plans := suite.dryRun()
	re.NotEmpty(plans)
	re.Equal(0, plans[0].GetStep())
	re.Equal(plan.StatusStoreDown, int(plans[0].GetStatus().StatusCode))
	re.Equal(uint64(1), plans[0].GetResource(0))

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	suite.tc.SetStoreBusy(2, true)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 3)

	// Test disconnectFilter.
	// If store 3 is disconnected, no operator can be created.
	suite.tc.SetStoreDisconnect(3)
	re.Empty(suite.schedule())
}

func (suite *balanceLeaderSchedulerTestSuite) TestLeaderWeight() {
	re := suite.Require()
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F
	suite.tc.SetTolerantSizeRatio(2.5)
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.UpdateStoreLeaderWeight(1, 0.5)
	suite.tc.UpdateStoreLeaderWeight(2, 0.9)
	suite.tc.UpdateStoreLeaderWeight(3, 1)
	suite.tc.UpdateStoreLeaderWeight(4, 2)
	suite.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 1, 4)
	suite.tc.UpdateLeaderCount(4, 30)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 1, 3)
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalancePolicy() {
	re := suite.Require()
	// Stores:       1    2     3    4
	// LeaderCount: 20   66     6   20
	// LeaderSize:  66   20    20    6
	suite.tc.AddLeaderStore(1, 20, 600*units.MiB)
	suite.tc.AddLeaderStore(2, 66, 200*units.MiB)
	suite.tc.AddLeaderStore(3, 6, 20*units.MiB)
	suite.tc.AddLeaderStore(4, 20, 1*units.MiB)
	suite.tc.AddLeaderRegion(1, 2, 1, 3, 4)
	suite.tc.AddLeaderRegion(2, 1, 2, 3, 4)
	suite.tc.SetLeaderSchedulePolicy("count")
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 2, 3)
	suite.tc.SetLeaderSchedulePolicy("size")
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 1, 4)
}

func (suite *balanceLeaderSchedulerTestSuite) TestBalanceSelector() {
	re := suite.Require()
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	suite.tc.AddLeaderStore(1, 1)
	suite.tc.AddLeaderStore(2, 2)
	suite.tc.AddLeaderStore(3, 3)
	suite.tc.AddLeaderStore(4, 16)
	suite.tc.AddLeaderRegion(1, 4, 2, 3)
	suite.tc.AddLeaderRegion(2, 3, 1, 2)
	// store4 has max leader score, store1 has min leader score.
	// The scheduler try to move a leader out of 16 first.
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    14   15   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	suite.tc.UpdateLeaderCount(2, 14)
	suite.tc.UpdateLeaderCount(3, 15)
	// Cannot move leader out of store4, move a leader into store1.
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 3, 1)

	// Stores:     1    2    3    4
	// Leaders:    1    2    15   16
	// Region1:    -    F    L    F
	// Region2:    L    F    F    -
	suite.tc.AddLeaderStore(2, 2)
	suite.tc.AddLeaderRegion(1, 3, 2, 4)
	suite.tc.AddLeaderRegion(2, 1, 2, 3)
	// No leader in store16, no follower in store1. Now source and target are store3 and store2.
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 3, 2)

	// Stores:     1    2    3    4
	// Leaders:    9    10   10   11
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.AddLeaderRegion(1, 4, 2, 3)
	suite.tc.AddLeaderRegion(2, 1, 2, 3)
	// The cluster is balanced.
	re.Empty(suite.schedule())

	// store3's leader drops:
	// Stores:     1    2    3    4
	// Leaders:    11   13   0    16
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	suite.tc.AddLeaderStore(1, 11)
	suite.tc.AddLeaderStore(2, 13)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 16)
	operatorutil.CheckTransferLeader(re, suite.schedule()[0], operator.OpKind(0), 4, 3)
}

type balanceLeaderRangeSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *operator.Controller
}

func TestBalanceLeaderRangeSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceLeaderRangeSchedulerTestSuite))
}

func (suite *balanceLeaderRangeSchedulerTestSuite) SetupTest() {
	suite.cancel, _, suite.tc, suite.oc = prepareSchedulersTest()
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestSingleRangeBalance() {
	re := suite.Require()
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.UpdateStoreLeaderWeight(1, 0.5)
	suite.tc.UpdateStoreLeaderWeight(2, 0.9)
	suite.tc.UpdateStoreLeaderWeight(3, 1)
	suite.tc.UpdateStoreLeaderWeight(4, 2)
	suite.tc.AddLeaderRegionWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	ops, _ := lb.Schedule(suite.tc, false)
	re.NotEmpty(ops)
	re.Len(ops, 1)
	re.Len(ops[0].Counters, 1)
	re.Len(ops[0].FinishedCounters, 2)
	re.True(ops[0].NeedStoreHealthCheck())
	lb, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"h", "n"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"b", "f"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", "a"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"g", ""}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", "f"}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	lb, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"b", ""}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)

	kye := keyutil.NewKeyRange("a", "g")
	suite.tc.KeyRangeManager.Append([]keyutil.KeyRange{kye})
	lb, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	suite.tc.KeyRangeManager.Delete([]keyutil.KeyRange{kye})
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestMultiRangeBalance() {
	re := suite.Require()
	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// Region1:    L       F       F       F
	for i := uint64(1); i <= 4; i++ {
		suite.tc.AddLeaderStore(i, 10)
	}
	suite.tc.UpdateStoreLeaderWeight(1, 0.5)
	suite.tc.UpdateStoreLeaderWeight(2, 0.9)
	suite.tc.UpdateStoreLeaderWeight(3, 1)
	suite.tc.UpdateStoreLeaderWeight(4, 2)
	suite.tc.AddLeaderRegionWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", "g", "o", "t"}))
	re.NoError(err)
	ops, _ := lb.Schedule(suite.tc, false)
	re.Equal(uint64(1), ops[0].RegionID())
	r := suite.tc.GetRegion(1)
	suite.tc.RemoveRegion(r)
	suite.tc.RemoveRegionFromSubTree(r)
	suite.tc.AddLeaderRegionWithRange(2, "p", "r", 1, 2, 3, 4)
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Equal(uint64(2), ops[0].RegionID())
	r = suite.tc.GetRegion(2)
	suite.tc.RemoveRegion(r)
	suite.tc.RemoveRegionFromSubTree(r)

	suite.tc.AddLeaderRegionWithRange(3, "u", "w", 1, 2, 3, 4)
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
	r = suite.tc.GetRegion(3)
	suite.tc.RemoveRegion(r)
	suite.tc.RemoveRegionFromSubTree(r)
	suite.tc.AddLeaderRegionWithRange(4, "", "", 1, 2, 3, 4)
	re.NoError(err)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Empty(ops)
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestBatchBalance() {
	re := suite.Require()
	suite.tc.AddLeaderStore(1, 100)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 100)
	suite.tc.AddLeaderStore(5, 100)
	suite.tc.AddLeaderStore(6, 0)

	suite.tc.AddLeaderRegionWithRange(uint64(102), "102a", "102z", 1, 2, 3)
	suite.tc.AddLeaderRegionWithRange(uint64(103), "103a", "103z", 4, 5, 6)
	lb, err := CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	ops, _ := lb.Schedule(suite.tc, false)
	re.Len(ops, 2)
	for i := 1; i <= 50; i++ {
		suite.tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("%da", i), fmt.Sprintf("%dz", i), 1, 2, 3)
	}
	for i := 51; i <= 100; i++ {
		suite.tc.AddLeaderRegionWithRange(uint64(i), fmt.Sprintf("%da", i), fmt.Sprintf("%dz", i), 4, 5, 6)
	}
	suite.tc.AddLeaderRegionWithRange(uint64(101), "101a", "101z", 5, 4, 3)
	ops, _ = lb.Schedule(suite.tc, false)
	re.Len(ops, 4)
	regions := make(map[uint64]struct{})
	for _, op := range ops {
		regions[op.RegionID()] = struct{}{}
	}
	re.Len(regions, 4)
}

func TestBalanceLeaderMaxBatchSchedule(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, MaxBalanceLeaderBatchSize*10)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	for i := 1; i <= MaxBalanceLeaderBatchSize*20; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 2, 3)
	}

	lb, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	lb.(*balanceLeaderScheduler).conf.Batch = MaxBalanceLeaderBatchSize

	testutil.Eventually(re, func() bool {
		ops, _ := lb.Schedule(tc, false)
		return len(ops) == MaxBalanceLeaderBatchSize
	})
}

func (suite *balanceLeaderRangeSchedulerTestSuite) TestReSortStores() {
	re := suite.Require()
	suite.tc.AddLeaderStore(1, 104)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 100)
	suite.tc.AddLeaderStore(5, 100)
	suite.tc.AddLeaderStore(6, 0)
	stores := suite.tc.GetStores()
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetID() < stores[j].GetID()
	})

	deltaMap := make(map[uint64]int64)
	getScore := func(store *core.StoreInfo) float64 {
		return store.LeaderScore(0, deltaMap[store.GetID()])
	}
	candidateStores := make([]*core.StoreInfo, 0)
	// order by score desc.
	cs := newCandidateStores(append(candidateStores, stores...), false, getScore)
	// in candidate,the order stores:1(104),5(100),4(100),6,3,2
	// store 4 should in pos 2
	re.Equal(2, cs.binarySearch(stores[3]))

	// store 1 should in pos 0
	store1 := stores[0]
	re.Zero(cs.binarySearch(store1))
	deltaMap[store1.GetID()] = -1 // store 1
	cs.resortStoreWithPos(0)
	// store 1 should still in pos 0.
	re.Equal(uint64(1), cs.stores[0].GetID())
	curIndex := cs.binarySearch(store1)
	re.Zero(curIndex)
	deltaMap[1] = -4
	// store 1 update the scores to 104-4=100
	// the order stores should be:5(100),4(100),1(100),6,3,2
	cs.resortStoreWithPos(curIndex)
	re.Equal(uint64(1), cs.stores[2].GetID())
	re.Equal(2, cs.binarySearch(store1))
	// the top store is : 5(100)
	topStore := cs.stores[0]
	topStorePos := cs.binarySearch(topStore)
	deltaMap[topStore.GetID()] = -1
	cs.resortStoreWithPos(topStorePos)

	// after recorder, the order stores should be: 4(100),1(100),5(99),6,3,2
	re.Equal(uint64(1), cs.stores[1].GetID())
	re.Equal(1, cs.binarySearch(store1))
	re.Equal(topStore.GetID(), cs.stores[2].GetID())
	re.Equal(2, cs.binarySearch(topStore))

	bottomStore := cs.stores[5]
	deltaMap[bottomStore.GetID()] = 4
	cs.resortStoreWithPos(5)

	// the order stores should be: 4(100),1(100),5(99),2(5),6,3
	re.Equal(bottomStore.GetID(), cs.stores[3].GetID())
	re.Equal(3, cs.binarySearch(bottomStore))
}

func TestBalanceLeaderLimit(t *testing.T) {
	re := require.New(t)
	checkBalanceLeaderLimit(re, false /* disable placement rules */)
	checkBalanceLeaderLimit(re, true /* enable placement rules */)
}

func checkBalanceLeaderLimit(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
	tc.SetTolerantSizeRatio(2.5)
	// Add stores 1,2,3,4,5.
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 0)
	tc.AddRegionStore(3, 0)
	tc.AddRegionStore(4, 0)
	tc.AddRegionStore(5, 0)
	var (
		id      uint64
		regions []*metapb.Region
	)
	for i := range 50 {
		peers := []*metapb.Peer{
			{Id: id + 1, StoreId: 1},
			{Id: id + 2, StoreId: 2},
			{Id: id + 3, StoreId: 3},
		}
		regions = append(regions, &metapb.Region{
			Id:       id + 4,
			Peers:    peers,
			StartKey: fmt.Appendf(nil, "s_%02d", i),
			EndKey:   fmt.Appendf(nil, "s_%02d", i+1),
		})
		id += 4
	}

	regions[49].EndKey = []byte("")
	for _, meta := range regions {
		leader := rand.IntN(4) % 3
		regionInfo := core.NewRegionInfo(
			meta,
			meta.Peers[leader],
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		origin, overlaps, rangeChanged := tc.SetRegion(regionInfo)
		tc.UpdateSubTree(regionInfo, origin, overlaps, rangeChanged)
	}

	for range 100 {
		_, err := tc.AllocPeer(1)
		re.NoError(err)
	}
	for i := 1; i <= 5; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}

	// test not allow schedule leader
	tc.SetLeaderScheduleLimit(0)
	hb, err := CreateScheduler(types.ScatterRangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ScatterRangeScheduler, []string{"s_00", "s_50", "t"}))
	re.NoError(err)

	scheduleAndApplyOperator(tc, hb, 100)
	maxLeaderCount := 0
	minLeaderCount := 99
	for i := 1; i <= 5; i++ {
		leaderCount := tc.GetStoreLeaderCount(uint64(i))
		if leaderCount < minLeaderCount {
			minLeaderCount = leaderCount
		}
		if leaderCount > maxLeaderCount {
			maxLeaderCount = leaderCount
		}
		regionCount = tc.GetStoreRegionCount(uint64(i))
		re.LessOrEqual(regionCount, 32)
	}
	re.Greater(maxLeaderCount-minLeaderCount, 10)
}

func BenchmarkCandidateStores(b *testing.B) {
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()

	for id := uint64(1); id < uint64(10000); id++ {
		leaderCount := int(rand.Int32N(10000))
		tc.AddLeaderStore(id, leaderCount)
	}

	for b.Loop() {
		updateAndResortStoresInCandidateStores(tc)
	}
}

func updateAndResortStoresInCandidateStores(tc *mockcluster.Cluster) {
	deltaMap := make(map[uint64]int64)
	getScore := func(store *core.StoreInfo) float64 {
		return store.LeaderScore(0, deltaMap[store.GetID()])
	}
	cs := newCandidateStores(tc.GetStores(), false, getScore)
	stores := tc.GetStores()
	// update score for store and reorder
	for id, store := range stores {
		offsets := cs.binarySearchStores(store)
		if id%2 == 1 {
			deltaMap[store.GetID()] = int64(rand.Int32N(10000))
		} else {
			deltaMap[store.GetID()] = int64(-rand.Int32N(10000))
		}
		cs.resortStoreWithPos(offsets[0])
	}
}
