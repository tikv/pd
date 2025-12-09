// Copyright 2025 TiKV Project Authors.
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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestPlacementRule(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	rule1 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_0",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Leader,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z1"}},
		},
	}
	rule2 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_1",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Follower,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z2"}},
		},
	}

	rule3 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_2",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Learner,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z3"}},
		},
	}

	re.NoError(tc.SetRules([]*placement.Rule{rule1, rule2, rule3}))
	re.NoError(tc.GetRuleManager().DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID))

	sc := newBalanceRangeScheduler(oc, &balanceRangeSchedulerConfig{}).(*balanceRangeScheduler)
	job := &balanceRangeSchedulerJob{
		Engine: core.EngineTiKV,
		Rule:   core.LeaderScatter,
		Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("100", "110")},
	}
	for i, label := range []map[string]string{{"region": "z1"}, {"region": "z2"}, {"region": "z3"}} {
		store := core.NewStoreInfoWithLabel(uint64(i+1), label)
		store = store.Clone(core.SetLastHeartbeatTS(time.Now()))
		tc.PutStore(store)
	}

	for i := 1; i <= 100; i++ {
		starKey, endKey := 100+i-1, 100+i
		tc.AddLeaderRegionWithRange(uint64(i), strconv.Itoa(starKey), strconv.Itoa(endKey), 1, 2, 3)
	}

	// only store-1 can match the leader rule
	err := sc.prepare(tc, operator.NewOpInfluence(), job)
	re.Error(err)

	// all store can match the rules
	job.Rule = core.PeerScatter
	err = sc.prepare(tc, operator.NewOpInfluence(), job)
	re.NoError(err)
	re.Len(sc.stores, 3)

	// only store-1 can match the learner rule
	job.Rule = core.LearnerScatter
	err = sc.prepare(tc, operator.NewOpInfluence(), job)
	re.Error(err)
}

func TestIsBalanced(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"leader-scatter", "tikv", "1h", "test", "", ""}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)
	for i := 1; i <= 3; i++ {
		tc.AddLeaderStore(uint64(i), 1)
	}
	tc.AddLeaderRegionWithRange(1, fmt.Sprintf("%20d", 1), fmt.Sprintf("%20d", 2), 1, 2, 3)

	// only one region, so it is balanced
	re.False(sc.IsScheduleAllowed(tc))
	re.True(sc.isBalanced())
	re.Equal(finished, sc.job.Status)
	// add more regions
	for i := range 10 {
		tc.AddLeaderRegionWithRange(uint64(i+2), fmt.Sprintf("%20d", i), fmt.Sprintf("%20d", i+1), 1, 2, 3)
	}
	now := time.Now()
	re.NoError(sc.conf.addJob(&balanceRangeSchedulerJob{
		Engine:  core.EngineTiKV,
		Rule:    core.LeaderScatter,
		Ranges:  []keyutil.KeyRange{keyutil.NewKeyRange("", "")},
		Start:   &now,
		Timeout: 10 * time.Minute,
	}))
	km := tc.GetKeyRangeManager()
	re.True(km.IsEmpty())
	re.True(sc.IsScheduleAllowed(tc))
	re.False(sc.isBalanced())
	re.Equal(running, sc.job.Status)
	re.False(km.IsEmpty())

	// cancel this job
	re.NoError(sc.conf.deleteJob(1))
	re.Equal(cancelled, sc.job.Status)
	re.False(km.IsEmpty())
	// no any pending jobs
	re.False(sc.IsScheduleAllowed(tc))
	// must clean the job status
	re.True(km.IsEmpty())
}

func TestPrepareBalanceRange(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sc := newBalanceRangeScheduler(oc, &balanceRangeSchedulerConfig{}).(*balanceRangeScheduler)
	for i := 1; i <= 3; i++ {
		tc.AddLeaderStore(uint64(i), 0)
	}
	tc.AddLeaderRegionWithRange(1, "100", "110", 1, 2, 3)
	job := &balanceRangeSchedulerJob{
		Engine: core.EngineTiKV,
		Rule:   core.LeaderScatter,
		Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("100", "110")},
	}
	err := sc.prepare(tc, operator.NewOpInfluence(), job)
	re.NoError(err)
	re.Len(sc.stores, 3)
	re.Len(sc.scoreMap, 3)
	re.Equal(float64(1), sc.score(1))
	re.Equal(1.0/3.0, sc.expectScoreMap[1])
}

func TestTIKVEngine(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"leader-scatter", "tikv", "1h", "test", "100", "300"}))
	re.NoError(err)
	// no any stores
	re.False(scheduler.IsScheduleAllowed(tc))
	km := tc.GetKeyRangeManager()
	kr := keyutil.NewKeyRange("", "")
	ranges := km.GetNonOverlappingKeyRanges(&kr)
	re.Len(ranges, 2)
	re.Equal(ranges[0], keyutil.NewKeyRange("", "100"))
	re.Equal(ranges[1], keyutil.NewKeyRange("300", ""))

	re.NoError(err)
	ops, _ := scheduler.Schedule(tc, false)
	re.Empty(ops)

	// add regions:
	// store-1: 3 leader regions
	// store-2: 2 leader regions
	// store-3: 0 leader regions
	for i := 1; i <= 3; i++ {
		tc.AddLeaderStore(uint64(i), 0)
	}
	tc.AddLeaderRegionWithRange(1, "100", "110", 1, 2, 3)
	tc.AddLeaderRegionWithRange(2, "110", "120", 1, 2, 3)
	tc.AddLeaderRegionWithRange(3, "120", "140", 1, 2, 3)
	tc.AddLeaderRegionWithRange(4, "140", "160", 2, 1, 3)
	tc.AddLeaderRegionWithRange(5, "160", "180", 2, 1, 3)
	// case1: transfer leader from store 1 to store 3
	scheduler, err = CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"leader-scatter", "tikv", "1h", "test", "100", "300"}))
	re.NoError(err)
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	op := ops[0]
	re.Equal("3.00", op.GetAdditionalInfo("sourceScore"))
	re.Equal("0.00", op.GetAdditionalInfo("targetScore"))
	re.Contains(op.Brief(), "transfer leader: store 1 to 3")

	// case2: move leader from store 1 to store 4
	tc.AddLeaderRegionWithRange(6, "160", "180", 3, 1, 3)
	tc.AddLeaderStore(4, 0)
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	op = ops[0]
	re.Equal("3.00", op.GetAdditionalInfo("sourceScore"))
	re.Equal("0.00", op.GetAdditionalInfo("targetScore"))
	re.Contains(op.Brief(), "mv peer: store [1] to [4]")
	re.Equal("transfer leader from store 1 to store 4", op.Step(2).String())
}

func TestLocationLabel(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	rule1 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_0",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    1,
		Role:     placement.Leader,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z1"}},
		},
		LocationLabels: []string{"zone"},
	}
	rule2 := &placement.Rule{
		GroupID:  "TiDB_DDL_145",
		ID:       "table_rule_145_1",
		Index:    40,
		StartKey: []byte("100"),
		EndKey:   []byte("200"),
		Count:    2,
		Role:     placement.Follower,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "region", Op: "in", Values: []string{"z2", "z3"}},
		},
		LocationLabels: []string{"zone"},
	}

	re.NoError(tc.SetRules([]*placement.Rule{rule1, rule2}))
	re.NoError(tc.GetRuleManager().DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID))

	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"peer-scatter", "tikv", "1h", "test", "100", "300"}))
	re.NoError(err)
	tc.AddLabelsStore(1, 0, map[string]string{"region": "z1", "zone": "z1"})
	tc.AddLabelsStore(2, 0, map[string]string{"region": "z2", "zone": "z2"})
	tc.AddLabelsStore(3, 0, map[string]string{"region": "z2", "zone": "z2"})
	tc.AddLabelsStore(4, 0, map[string]string{"region": "z3", "zone": "z3"})
	tc.AddLabelsStore(5, 0, map[string]string{"region": "z3", "zone": "z3"})
	for i := range 100 {
		follower1 := 2 + i%4
		follower2 := 2 + (i+1)%4
		tc.AddLeaderRegionWithRange(uint64(i), strconv.Itoa(100+i), strconv.Itoa(100+i+1),
			1, uint64(follower1), uint64(follower2))
	}
	// case1: store 1 has 100 peers, the others has 50 peer, it suiter for the location label setting.
	re.False(scheduler.IsScheduleAllowed(tc))
	op, _ := scheduler.Schedule(tc, true)
	re.Empty(op)

	// case2: store1 has 110 peers, but store2 and store4 has 60 peer, it should move some peers from store2 or store 4
	//	to store5 and store3.
	for i := range 10 {
		tc.AddLeaderRegionWithRange(uint64(100+i), strconv.Itoa(200+i), strconv.Itoa(200+i+1),
			1, 2, 4)
	}
	scheduler, err = CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"peer-scatter", "tikv", "1h", "test", "100", "300"}))
	re.NoError(err)
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ := scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	re.Len(ops, 1)
	re.Contains(ops[0].Brief(), "mv peer")

	// case3: add 10 down stores, scheduler should ignore this unhealthy ,it should not move peer to this unhealthy store.
	for i := range 10 {
		store := core.NewStoreInfoWithLabel(uint64(i+6), map[string]string{"region": "z3", "zone": "z3"})
		opt := core.SetLastHeartbeatTS(time.Now().Add(-time.Hour * 24 * 10))
		tc.PutStore(store.Clone(opt))
	}
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	re.Len(ops, 1)
	re.Contains(ops[0].Brief(), "mv peer")
}

func TestTIFLASHEngine(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	// 3 tikv and 3 tiflash
	tikvCount := 3
	for i := 1; i <= tikvCount; i++ {
		tc.AddLeaderStore(uint64(i), 0)
	}
	for i := tikvCount + 1; i <= tikvCount+3; i++ {
		tc.AddLabelsStore(uint64(i), 0, map[string]string{core.EngineKey: core.EngineTiFlash})
	}
	tc.AddRegionWithLearner(uint64(1), 1, []uint64{2, 3}, []uint64{4})

	startKey := fmt.Sprintf("%20d0", 1)
	endKey := fmt.Sprintf("%20d0", 10)
	err := tc.SetRule(&placement.Rule{
		GroupID:  "tiflash",
		ID:       "1",
		Role:     placement.Learner,
		Count:    1,
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
		LabelConstraints: []placement.LabelConstraint{
			{Key: "engine", Op: "in", Values: []string{"tiflash"}},
		},
	})
	re.NoError(err)

	// generate a balance range scheduler with tiflash engine
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"learner-scatter", "tiflash", "1h", "test", startKey, endKey}))
	re.NoError(err)
	// tiflash-4 only has 1 region, so it doesn't need to balance
	re.False(scheduler.IsScheduleAllowed(tc))
	ops, _ := scheduler.Schedule(tc, false)
	re.Empty(ops)

	// add 2 learner on tiflash-4
	for i := 2; i <= 3; i++ {
		tc.AddRegionWithLearner(uint64(i), 1, []uint64{2, 3}, []uint64{4})
	}
	scheduler, err = CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"learner-scatter", "tiflash", "1h", "test", startKey, endKey}))
	re.NoError(err)
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, false)
	re.NotEmpty(ops)
	op := ops[0]
	re.Equal("3.00", op.GetAdditionalInfo("sourceScore"))
	re.Equal("0.00", op.GetAdditionalInfo("targetScore"))
	re.Equal("1.00", op.GetAdditionalInfo("sourceExpectScore"))
	re.Equal("1.00", op.GetAdditionalInfo("targetExpectScore"))
	re.Contains(op.Brief(), "mv peer: store [4] to")
}

func TestCodecConfig(t *testing.T) {
	re := require.New(t)
	job := &balanceRangeSchedulerJob{
		Alias:  "test.t",
		Engine: core.EngineTiKV,
		Rule:   core.LeaderScatter,
		JobID:  1,
		Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("a", "b")},
	}

	conf := &balanceRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		jobs:            []*balanceRangeSchedulerJob{job},
	}
	conf.init("test", storage.NewStorageWithMemoryBackend(), conf)
	re.NoError(conf.save())
	var conf1 balanceRangeSchedulerConfig
	re.NoError(conf.load(&conf1))
	re.Equal(conf1.jobs, conf.jobs)

	job1 := &balanceRangeSchedulerJob{
		Alias:  "test.t2",
		Engine: core.EngineTiKV,
		Rule:   core.LeaderScatter,
		Status: running,
		Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("a", "b")},
		JobID:  2,
	}
	re.NoError(conf.addJob(job1))
	re.Error(conf.addJob(job1))
	re.NoError(conf.load(&conf1))
	re.Equal(conf1.jobs, conf.jobs)

	data, err := conf.MarshalJSON()
	re.NoError(err)
	conf2 := &balanceRangeSchedulerConfig{
		jobs: make([]*balanceRangeSchedulerJob, 0),
	}
	re.NoError(conf2.UnmarshalJSON(data))
	re.Equal(conf2.jobs, conf.jobs)
}

func TestJobExpired(t *testing.T) {
	now := time.Now()
	for _, data := range []struct {
		finishedTime time.Time
		expired      bool
	}{
		{
			finishedTime: now.Add(-reserveDuration - 10*time.Second),
			expired:      true,
		},
		{
			finishedTime: now.Add(-reserveDuration + 10*time.Second),
			expired:      false,
		},
	} {
		job := &balanceRangeSchedulerJob{
			Finish: &data.finishedTime,
		}
		require.Equal(t, data.expired, job.expired(reserveDuration))
	}
}

func TestJobGC(t *testing.T) {
	re := require.New(t)
	conf := &balanceRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		jobs:            make([]*balanceRangeSchedulerJob, 0),
	}
	conf.init("test", storage.NewStorageWithMemoryBackend(), conf)
	now := time.Now()
	job := &balanceRangeSchedulerJob{
		Finish: &now,
	}
	re.NoError(conf.addJob(job))
	re.NoError(conf.deleteJob(1))
	re.NoError(conf.gc())
	re.Len(conf.jobs, 1)

	expiredTime := now.Add(-reserveDuration - 10*time.Second)
	conf.jobs[0].Finish = &expiredTime
	re.NoError(conf.gc())
	re.Empty(conf.jobs)
}

func TestPersistFail(t *testing.T) {
	re := require.New(t)
	persisFail := "github.com/tikv/pd/pkg/schedule/schedulers/persistFail"
	re.NoError(failpoint.Enable(persisFail, "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable(persisFail))
	}()
	job := &balanceRangeSchedulerJob{
		Engine: core.EngineTiKV,
		Rule:   core.LeaderScatter,
		JobID:  1,
		Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("a", "b")},
	}
	conf := &balanceRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		jobs:            []*balanceRangeSchedulerJob{job},
	}
	conf.init("test", storage.NewStorageWithMemoryBackend(), conf)
	errMsg := "fail to persist"
	newJob := &balanceRangeSchedulerJob{
		Alias: "test.t",
	}
	re.ErrorContains(conf.addJob(newJob), errMsg)
	re.Len(conf.jobs, 1)

	re.ErrorContains(conf.deleteJob(1), errMsg)
	re.NotEqual(cancelled, conf.jobs[0].Status)

	re.ErrorContains(conf.begin(0), errMsg)
	re.NotEqual(running, conf.jobs[0].Status)

	conf.jobs[0].Status = running
	re.ErrorContains(conf.finish(0), errMsg)
	re.NotEqual(finished, conf.jobs[0].Status)

	conf.jobs[0].Status = cancelled
	finishedTime := time.Now().Add(-reserveDuration - 10*time.Second)
	conf.jobs[0].Finish = &finishedTime
	re.ErrorContains(conf.gc(), errMsg)
	re.Len(conf.jobs, 1)
}
