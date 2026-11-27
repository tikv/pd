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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func checkOperator(re *require.Assertions, op *operator.Operator, sourceScore string, targetScore string) {
	val, exist := op.GetAdditionalInfo("sourceScore")
	re.True(exist)
	re.Equal(sourceScore, val)
	val, exist = op.GetAdditionalInfo("targetScore")
	re.True(exist)
	re.Equal(targetScore, val)
}

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
	exhaustTransferLeaderInLimit(t, tc, 1, 2, 3)
	// case1: transfer leader from store 1 to store 3
	scheduler, err = CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"leader-scatter", "tikv", "1h", "test", "100", "300"}))
	re.NoError(err)
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, true)
	re.Empty(ops)

	tc.SetStoreLimit(3, storelimit.TransferLeaderIn, storelimit.Unlimited)
	tc.ResetStoreLimit(3, storelimit.TransferLeaderIn, storelimit.Unlimited/time.Minute.Seconds())
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	op := ops[0]
	checkOperator(re, op, "3.00", "0.00")
	re.Contains(op.Brief(), "transfer leader: store 1 to 3")

	// case2: move leader from store 1 to store 4
	tc.AddLeaderRegionWithRange(6, "160", "180", 3, 1, 3)
	tc.AddLeaderStore(4, 0)
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	op = ops[0]
	checkOperator(re, op, "3.00", "0.00")
	re.Contains(op.Brief(), "mv peer: store [1] to [4]")
	re.Equal("transfer leader from store 1 to store 4", op.Step(2).String())
}

func TestSkipAffinityRegions(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

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
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler,
			[]string{"leader-scatter", "tikv", "1h", "test", "100", "300"}))
	re.NoError(err)

	// All regions in the range belong to an affinity group, so none of them should be scheduled.
	affinityManager := tc.GetAffinityManager()
	groupID := "test_group"
	re.NoError(affinityManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{
		GroupID:   groupID,
		KeyRanges: []keyutil.KeyRange{keyutil.NewKeyRange("100", "300")},
	}}))
	_, err = affinityManager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
	re.NoError(err)
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ := scheduler.Schedule(tc, true)
	re.Empty(ops)

	// The regions can be scheduled again after the affinity group is removed.
	re.NoError(affinityManager.DeleteAffinityGroups([]string{groupID}, true))
	re.True(scheduler.IsScheduleAllowed(tc))
	ops, _ = scheduler.Schedule(tc, true)
	re.Len(ops, 1)
	re.Contains(ops[0].Brief(), "transfer leader: store 1 to 3")
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
	// case1: store 1 has 100 peers, the others has 50 peer, it is suited for the location label setting.
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
	checkOperator(re, op, "3.00", "0.00")
	sourceExpectScore, exist := op.GetAdditionalInfo("sourceExpectScore")
	re.True(exist)
	re.Equal("1.00", sourceExpectScore)
	targetExpectScore, exist := op.GetAdditionalInfo("targetExpectScore")
	re.True(exist)
	re.Equal("1.00", targetExpectScore)
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
	re.Equal(uint64(0), job.JobID)
	re.NoError(conf.deleteJob(0))
	re.NoError(conf.gcLocked())
	re.Len(conf.jobs, 1)

	// the last job is kept even if it's expired
	expiredTime := now.Add(-reserveDuration - 10*time.Second)
	conf.jobs[0].Finish = &expiredTime
	re.NoError(conf.gcLocked())
	re.Len(conf.jobs, 1)

	// the job id never goes backward after gc
	conf.jobs[0].Finish = &now
	re.NoError(conf.addJob(&balanceRangeSchedulerJob{Alias: "job-1"}))
	re.NoError(conf.addJob(&balanceRangeSchedulerJob{Alias: "job-2"}))
	re.NoError(conf.deleteJob(1))
	re.NoError(conf.deleteJob(2))
	conf.jobs[0].Finish = &expiredTime
	conf.jobs[1].Finish = &expiredTime
	conf.jobs[2].Finish = &expiredTime
	re.NoError(conf.gcLocked())
	re.Len(conf.jobs, 1)
	re.Equal(uint64(2), conf.jobs[0].JobID)
	newJob := &balanceRangeSchedulerJob{Alias: "job-3"}
	re.NoError(conf.addJob(newJob))
	re.Equal(uint64(3), newJob.JobID)
}

func TestAddBalanceRangeJobCollectsExpiredHistory(t *testing.T) {
	re := require.New(t)
	expired := time.Now().Add(-reserveDuration - time.Hour)
	conf := &balanceRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		jobs: []*balanceRangeSchedulerJob{
			{JobID: 10, Alias: "old-10", Status: finished, Finish: &expired},
			{JobID: 11, Alias: "old-11", Status: cancelled, Finish: &expired},
			{JobID: 12, Alias: "old-12", Status: finished, Finish: &expired},
		},
	}
	conf.init("test", storage.NewStorageWithMemoryBackend(), conf)
	re.NoError(conf.save())
	job := &balanceRangeSchedulerJob{Alias: "new"}
	re.NoError(conf.addJob(job))
	re.Equal(uint64(13), job.JobID)
	re.Len(conf.jobs, 2)
	re.Equal(uint64(12), conf.jobs[0].JobID)
	var persisted []*balanceRangeSchedulerJob
	re.NoError(conf.load(&persisted))
	re.Len(persisted, 2)
	re.Equal(uint64(12), persisted[0].JobID)
	re.Equal(uint64(13), persisted[1].JobID)
	re.Equal(pending, persisted[1].Status)
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

	re.ErrorContains(conf.beginLocked(0, nil), errMsg)
	re.NotEqual(running, conf.jobs[0].Status)

	conf.jobs[0].Status = running
	re.ErrorContains(conf.finishLocked(0, nil), errMsg)
	re.NotEqual(finished, conf.jobs[0].Status)

	conf.jobs[0].Status = cancelled
	finishedTime := time.Now().Add(-reserveDuration - 10*time.Second)
	conf.jobs[0].Finish = &finishedTime
	// the last job is never gc-ed, so add another one
	conf.jobs = append(conf.jobs, &balanceRangeSchedulerJob{JobID: 2, Status: pending})
	re.ErrorContains(conf.gcLocked(), errMsg)
	re.Len(conf.jobs, 2)
	// GC failure must prevent adding a new job and leave the queue unchanged.
	re.ErrorContains(conf.addJob(&balanceRangeSchedulerJob{Alias: "after-gc"}), errMsg)
	re.Len(conf.jobs, 2)
	re.Equal(uint64(1), conf.jobs[0].JobID)
	re.Equal(uint64(2), conf.jobs[1].JobID)
}

func TestAddBalanceRangeJobWithInvalidFieldType(t *testing.T) {
	re := require.New(t)
	conf := &balanceRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		jobs:            make([]*balanceRangeSchedulerJob, 0),
	}
	conf.init("test", storage.NewStorageWithMemoryBackend(), conf)
	handler := &balanceRangeSchedulerHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	count := 0
	checkFn := func(data []byte, pass bool) {
		req := httptest.NewRequest(http.MethodPut, "/job", bytes.NewReader(data))
		resp := httptest.NewRecorder()
		re.NotPanics(func() {
			handler.addJob(resp, req)
		})
		if pass {
			re.Equal(http.StatusOK, resp.Code)
			count++
			re.Len(conf.jobs, count)
		} else {
			re.Equal(http.StatusBadRequest, resp.Code)
		}
		re.Len(conf.jobs, count)
	}

	// invalid engine type
	body, err := json.Marshal(map[string]any{
		"alias":     "a",
		"engine":    1,
		"rule":      "leader-scatter",
		"start-key": "100",
		"end-key":   "200",
	})
	re.NoError(err)
	checkFn(body, false)

	// invalid timeout type
	body, err = json.Marshal(map[string]any{
		"alias":     "a",
		"engine":    "tikv",
		"rule":      "leader-scatter",
		"start-key": "100",
		"end-key":   "200",
		"timeout":   "123",
	})
	re.NoError(err)
	checkFn(body, false)

	// normal case
	body, err = json.Marshal(map[string]any{
		"alias":     "a",
		"engine":    "tikv",
		"rule":      "leader-scatter",
		"start-key": "100",
		"end-key":   "200",
		"timeout":   "123s",
	})
	re.NoError(err)
	checkFn(body, true)

	// invalidate case
	body, err = json.Marshal(map[string]any{
		"alias":     "a",
		"engine":    "tikv",
		"rule":      "leader-scatter",
		"start-key": "100",
		"end-key":   "200",
		"timeout":   "0s",
	})
	re.NoError(err)
	checkFn(body, false)
}

func TestBalanceRangeUpdateJobStatus(t *testing.T) {
	re := require.New(t)
	create := time.Date(2026, time.November, 11, 1, 2, 3, 123456789, time.UTC)
	conf := &balanceRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		jobs: []*balanceRangeSchedulerJob{{
			JobID:  1,
			Alias:  "test",
			Create: create,
			Rule:   core.LeaderScatter,
			Engine: core.EngineTiKV,
			Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("a", "b")},
			Status: pending,
		}},
	}
	s := storage.NewStorageWithMemoryBackend()
	conf.init(types.BalanceRangeScheduler.String(), s, conf)
	loadStatus := func() JobStatus {
		var jobs []*balanceRangeSchedulerJob
		re.NoError(conf.load(&jobs))
		re.Len(jobs, 1)
		return jobs[0].Status
	}
	handler := newBalanceRangeHandler(conf)
	put := func(body string) int {
		req := httptest.NewRequest(http.MethodPut, "/job/status", bytes.NewBufferString(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w.Code
	}
	putStatus := func(jobID uint64, status string, create time.Time) int {
		body, err := json.Marshal(map[string]any{"job-id": jobID, "status": status, "create": create})
		re.NoError(err)
		return put(string(body))
	}

	re.Equal(http.StatusBadRequest, put(`{"job-id":1,"status":"running"}`))
	re.Equal(http.StatusBadRequest, put(`{"job-id":1,"create":null,"status":"running"}`))
	re.Equal(http.StatusBadRequest, put(`{"job-id":1,"create":"invalid","status":"running"}`))
	re.Equal(pending, conf.jobs[0].Status)
	re.Equal(http.StatusOK, putStatus(1, "running", create))
	re.Equal(running, conf.jobs[0].Status)
	re.Equal(running, loadStatus())
	start := conf.jobs[0].Start
	re.NotNil(start)
	// retry is idempotent
	re.Equal(http.StatusOK, putStatus(1, "running", create))
	re.Same(start, conf.jobs[0].Start)
	// Identity is checked even when the requested status is already current.
	re.Equal(http.StatusConflict, putStatus(1, "running", create.Add(-time.Nanosecond)))
	re.Same(start, conf.jobs[0].Start)

	re.Equal(http.StatusOK, putStatus(1, "finished", create))
	re.Equal(finished, conf.jobs[0].Status)
	re.Equal(finished, loadStatus())
	re.NotNil(conf.jobs[0].Finish)

	// the status can't move backward
	re.Equal(http.StatusConflict, putStatus(1, "running", create))
	re.Equal(errJobStatusConflict, errors.Cause(conf.updateJobStatus(1, create, running)))
	re.Equal(finished, loadStatus())
	// a cancelled job can't be finished
	conf.jobs[0].Status = cancelled
	re.Equal(http.StatusConflict, putStatus(1, "finished", create))

	// 404 is reserved for the PD that doesn't support this API
	re.Equal(http.StatusConflict, putStatus(2, "running", create))
	re.True(errs.ErrScheduleConfigNotExist.Equal(conf.updateJobStatus(2, create, running)))
	re.Equal(http.StatusBadRequest, put(`{"status":"running"}`))
	re.Equal(http.StatusBadRequest, putStatus(1, "cancelled", create))
	re.Equal(http.StatusBadRequest, putStatus(1, "pending", create))

	// A delayed completion must not finish a recreated job with the same ID and alias.
	conf.jobs[0].Status = pending
	conf.jobs[0].Start = nil
	conf.jobs[0].Finish = nil
	conf.jobs[0].Create = create.Add(time.Nanosecond)
	re.NoError(conf.save())
	re.Equal(http.StatusConflict, putStatus(1, "finished", create))
	re.Equal(pending, loadStatus())
	re.Nil(conf.jobs[0].Start)
	re.Nil(conf.jobs[0].Finish)
	// Matching creation times remain valid after a JSON round trip and in another timezone.
	re.Equal(http.StatusOK, putStatus(1, "finished", conf.jobs[0].Create.In(time.FixedZone("offset", 3600))))
	re.Equal(finished, conf.jobs[0].Status)
	re.Equal(finished, loadStatus())
	re.NotNil(conf.jobs[0].Start)
	re.NotNil(conf.jobs[0].Finish)
}

func TestBalanceRangeMergeJobs(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	later := now.Add(time.Minute)
	running0 := &balanceRangeSchedulerJob{JobID: 0, Status: running, Start: &now}
	pending1 := &balanceRangeSchedulerJob{JobID: 1, Status: pending}
	running2 := &balanceRangeSchedulerJob{JobID: 2, Status: running, Start: &now}
	finished3 := &balanceRangeSchedulerJob{JobID: 3, Status: finished, Start: &now, Finish: &now}
	conf := &balanceRangeSchedulerConfig{
		jobs: []*balanceRangeSchedulerJob{finished3, running0, pending1, running2},
	}
	conf.mergeLocked([]*balanceRangeSchedulerJob{
		// the watched snapshot is authoritative, even if its status ranks lower
		{JobID: 0, Status: pending},
		// newer status is applied
		{JobID: 1, Status: running, Start: &later},
		// cancelled by the user
		{JobID: 2, Status: cancelled, Start: &now, Finish: &later},
		// new job is appended, job 3 is removed by the gc
		{JobID: 4, Status: pending},
	})
	re.Len(conf.jobs, 4)
	re.NotSame(running0, conf.jobs[0])
	re.Equal(pending, conf.jobs[0].Status)
	re.Equal(running, conf.jobs[1].Status)
	re.Same(&later, conf.jobs[1].Start)
	re.Equal(cancelled, conf.jobs[2].Status)
	re.Same(&later, conf.jobs[2].Finish)
	// Publishing a snapshot never mutates jobs held by a running scheduling round.
	re.Equal(running, running0.Status)
	re.Equal(pending, pending1.Status)
	re.Equal(running, running2.Status)
	re.Equal(uint64(4), conf.jobs[3].JobID)
	re.Equal(pending, conf.jobs[3].Status)
}

type mockJobStatusUpdater struct {
	*mockcluster.Cluster
	err      error
	updates  []string
	attempts int
}

func (m *mockJobStatusUpdater) UpdateBalanceRangeJobStatus(jobID uint64, _ time.Time, status string) error {
	m.attempts++
	if m.err != nil {
		return m.err
	}
	m.updates = append(m.updates, fmt.Sprintf("%d:%s", jobID, status))
	return nil
}

func TestBalanceRangeJobStatusRetryBackoff(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc, err: errors.New("PD is unavailable")}
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "test", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)
	for i := range 10 {
		// Expire the previous deadline without sleeping.
		sc.nextRetry = time.Time{}
		beforeRequest := time.Now()
		re.False(sc.IsScheduleAllowed(cluster))
		re.Equal(i+1, cluster.attempts)
		expected := min(minJobStatusRetryInterval*time.Duration(1<<i), maxJobStatusRetryInterval)
		re.Equal(expected, sc.retryInterval)
		re.False(sc.nextRetry.Before(beforeRequest.Add(expected)))
		// Suppression is independent of pauses after the failed request.
		sc.nextRetry = time.Now().Add(time.Hour)
		re.False(sc.IsScheduleAllowed(cluster))
		re.Equal(i+1, cluster.attempts)
		re.Equal(pending, sc.conf.jobs[0].Status)
	}
	// Recovery clears the failure budget but still waits for watch publication.
	cluster.err = nil
	sc.nextRetry = time.Time{}
	re.False(sc.IsScheduleAllowed(cluster))
	re.Zero(sc.retryInterval)
	re.Equal(pending, sc.conf.jobs[0].Status)
	// A new watch snapshot bypasses the old request/retirement deadline.
	cluster.err = errors.New("PD is unavailable")
	sc.conf.mergeLocked(sc.conf.cloneLocked())
	re.False(sc.IsScheduleAllowed(cluster))
	re.Equal(minJobStatusRetryInterval, sc.retryInterval)
	// Watch-driven deletion and range cleanup must not wait for HTTP retry.
	job := sc.conf.jobs[0]
	tc.GetKeyRangeManager().Append(job.Ranges)
	sc.rangeJob = job
	sc.conf.mergeLocked(nil)
	re.False(sc.IsScheduleAllowed(cluster))
	re.True(tc.GetKeyRangeManager().IsEmpty())
}

func TestBalanceRangeJobStatusUpdater(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc, err: errors.New("pd is unavailable")}
	km := tc.GetKeyRangeManager()
	s := storage.NewStorageWithMemoryBackend()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, s,
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "test", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)

	// the job keeps pending if the status can't be persisted
	re.False(sc.IsScheduleAllowed(cluster))
	re.Equal(pending, sc.conf.jobs[0].Status)
	re.True(km.IsEmpty())
	re.Empty(cluster.updates)

	// A successful request does not change either cache before the watcher runs.
	cluster.err = nil
	sc.nextRetry = time.Time{}
	re.False(sc.IsScheduleAllowed(cluster))
	re.Equal([]string{"0:running"}, cluster.updates)
	re.Equal(pending, sc.conf.jobs[0].Status)
	re.Nil(sc.conf.jobs[0].Start)
	re.True(km.IsEmpty())
	re.False(sc.IsScheduleAllowed(cluster))
	re.Equal([]string{"0:running"}, cluster.updates)
	data, err := s.LoadSchedulerConfig(types.BalanceRangeScheduler.String())
	re.NoError(err)
	re.Empty(data)

	// Publish PD's persisted state as the config watcher does.
	jobs := sc.conf.cloneLocked()
	start := time.Now()
	jobs[0].Status = running
	jobs[0].Start = &start
	encoded, err := EncodeConfig(jobs)
	re.NoError(err)
	re.NoError(s.SaveSchedulerConfig(types.BalanceRangeScheduler.String(), encoded))
	re.NoError(sc.ReloadConfig())
	sc.IsScheduleAllowed(cluster)
	re.Equal(running, sc.conf.jobs[0].Status)
	re.False(km.IsEmpty())

	// a new scheduler, e.g. after the primary changes, appends the key ranges of the running job again
	encoded, err = sc.EncodeConfig()
	re.NoError(err)
	km.Delete(sc.conf.jobs[0].Ranges)
	re.True(km.IsEmpty())
	scheduler, err = CreateScheduler(types.BalanceRangeScheduler, oc, s, ConfigJSONDecoder(encoded))
	re.NoError(err)
	sc = scheduler.(*balanceRangeScheduler)
	sc.IsScheduleAllowed(cluster)
	re.Equal([]string{"0:running"}, cluster.updates)
	re.False(km.IsEmpty())

	// the job is cancelled by the PD and synced by the watcher, its key ranges must be cleaned
	sc.conf.mergeLocked([]*balanceRangeSchedulerJob{{JobID: 0, Status: cancelled, Start: &start, Finish: &start}})
	re.False(sc.IsScheduleAllowed(cluster))
	re.True(km.IsEmpty())
}

func TestBalanceRangeCleanConfig(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "test", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)
	km := tc.GetKeyRangeManager()
	sc.IsScheduleAllowed(tc)
	re.Equal(running, sc.conf.jobs[0].Status)
	re.False(km.IsEmpty())

	// the key ranges of the running job must be cleaned after the scheduler is removed
	sc.CleanConfig(tc)
	re.True(km.IsEmpty())
}

func TestBalanceRangeJobFinishedByOthers(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "test", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)
	km := tc.GetKeyRangeManager()
	sc.IsScheduleAllowed(tc)
	re.Equal(running, sc.conf.jobs[0].Status)
	re.False(km.IsEmpty())

	// the job is finished through the API instead of the scheduler itself
	re.NoError(sc.conf.updateJobStatus(0, sc.conf.jobs[0].Create, finished))
	re.False(sc.IsScheduleAllowed(tc))
	re.True(km.IsEmpty())
}

func TestBalanceRangeJobStatusUnsupported(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc, err: errors.Annotate(ErrBalanceRangeJobStatusUnsupported, "404")}
	km := tc.GetKeyRangeManager()
	s := storage.NewStorageWithMemoryBackend()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, s,
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "test", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)

	// An unsupported PD API must not fall back to writing either local cache.
	for range 2 {
		re.False(sc.IsScheduleAllowed(cluster))
		re.Equal(pending, sc.conf.jobs[0].Status)
		re.Nil(sc.conf.jobs[0].Start)
		re.True(km.IsEmpty())
		data, err := s.LoadSchedulerConfig(types.BalanceRangeScheduler.String())
		re.NoError(err)
		re.Empty(data)
	}

	// The same rule applies to completion of an already running job.
	jobs := sc.conf.cloneLocked()
	start := time.Now().Add(-2 * time.Hour)
	jobs[0].Status = running
	jobs[0].Start = &start
	sc.conf.mergeLocked(jobs)
	re.False(sc.IsScheduleAllowed(cluster))
	re.Equal(running, sc.conf.jobs[0].Status)
	re.Nil(sc.conf.jobs[0].Finish)
	re.False(km.IsEmpty())
	data, err := s.LoadSchedulerConfig(types.BalanceRangeScheduler.String())
	re.NoError(err)
	re.Empty(data)
}

func TestBalanceRangeJobRemovedByOthers(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "test", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)
	km := tc.GetKeyRangeManager()
	sc.IsScheduleAllowed(tc)
	re.Equal(running, sc.conf.jobs[0].Status)
	re.False(km.IsEmpty())
	oldRange := sc.conf.jobs[0].Ranges[0]

	// the running job is removed by the watcher, its key ranges must be cleaned
	sc.conf.Lock()
	sc.conf.mergeLocked(nil)
	sc.conf.Unlock()
	re.False(sc.IsScheduleAllowed(tc))
	re.True(km.IsEmpty())
	re.Nil(sc.rangeJob)

	// the running job is replaced by another job, the key ranges of the old job must be cleaned
	sc.conf.Lock()
	sc.conf.mergeLocked([]*balanceRangeSchedulerJob{{
		JobID:   1,
		Rule:    core.LeaderScatter,
		Engine:  core.EngineTiKV,
		Timeout: time.Hour,
		Ranges:  []keyutil.KeyRange{keyutil.NewKeyRange("300", "400")},
		Status:  pending,
	}})
	sc.conf.Unlock()
	sc.IsScheduleAllowed(tc)
	re.Equal(uint64(1), sc.rangeJob.JobID)
	re.False(km.IsEmpty())
	re.Equal([]keyutil.KeyRange{oldRange}, km.GetNonOverlappingKeyRanges(&oldRange))
}

func TestBalanceRangeWaitForWatchedCompletion(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired=%t", expired), func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			cluster := &mockJobStatusUpdater{Cluster: tc}
			s := storage.NewStorageWithMemoryBackend()
			scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, s,
				ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "test", "100", "200"}))
			re.NoError(err)
			sc := scheduler.(*balanceRangeScheduler)
			// A single region is balanced across these stores.
			for i := uint64(1); i <= 3; i++ {
				tc.AddLeaderStore(i, 0)
			}
			tc.AddLeaderRegionWithRange(1, "100", "200", 1, 2, 3)
			jobs := sc.conf.cloneLocked()
			start := time.Now()
			if expired {
				start = start.Add(-2 * time.Hour)
			}
			jobs[0].Status = running
			jobs[0].Start = &start
			encoded, err := EncodeConfig(jobs)
			re.NoError(err)
			re.NoError(s.SaveSchedulerConfig(sc.GetName(), encoded))
			re.NoError(sc.ReloadConfig())

			// Even after PD acknowledges completion, the watched job stays running
			// and the range remains reserved until the watcher publishes finished.
			cluster.err = errors.New("PD is unavailable")
			re.False(sc.IsScheduleAllowed(cluster))
			re.Equal(1, cluster.attempts)
			re.Equal(minJobStatusRetryInterval, sc.retryInterval)
			sc.nextRetry = time.Now().Add(time.Hour)
			re.False(sc.IsScheduleAllowed(cluster))
			re.Equal(1, cluster.attempts)
			re.False(tc.GetKeyRangeManager().IsEmpty())
			cluster.err = nil
			sc.nextRetry = time.Time{}
			re.False(sc.IsScheduleAllowed(cluster))
			re.Equal([]string{"0:finished"}, cluster.updates)
			re.Equal(running, sc.conf.jobs[0].Status)
			re.Nil(sc.conf.jobs[0].Finish)
			re.False(tc.GetKeyRangeManager().IsEmpty())
			data, err := s.LoadSchedulerConfig(sc.GetName())
			re.NoError(err)
			re.Equal(string(encoded), data)
			re.False(sc.IsScheduleAllowed(cluster))
			re.Equal([]string{"0:finished"}, cluster.updates)
			re.False(tc.GetKeyRangeManager().IsEmpty())

			finish := time.Now()
			jobs[0].Status = finished
			jobs[0].Finish = &finish
			encoded, err = EncodeConfig(jobs)
			re.NoError(err)
			re.NoError(s.SaveSchedulerConfig(sc.GetName(), encoded))
			re.NoError(sc.ReloadConfig())
			re.False(sc.IsScheduleAllowed(cluster))
			re.Equal(finished, sc.conf.jobs[0].Status)
			re.True(tc.GetKeyRangeManager().IsEmpty())
		})
	}
}

func TestBalanceRangeReloadReplacementAndDeletion(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc}
	s := storage.NewStorageWithMemoryBackend()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, s,
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "old", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)
	jobs := sc.conf.cloneLocked()
	start := time.Now()
	jobs[0].Status = running
	jobs[0].Start = &start
	encoded, err := EncodeConfig(jobs)
	re.NoError(err)
	re.NoError(s.SaveSchedulerConfig(sc.GetName(), encoded))
	re.NoError(sc.ReloadConfig())
	sc.IsScheduleAllowed(cluster)
	old := sc.conf.jobs[0]
	re.False(tc.GetKeyRangeManager().IsEmpty())

	// A recreated scheduler may reuse ID 0 with a different alias and range.
	jobs = []*balanceRangeSchedulerJob{{
		JobID: 0, Alias: "replacement", Engine: core.EngineTiKV, Rule: core.LeaderScatter,
		Timeout: time.Hour, Ranges: []keyutil.KeyRange{keyutil.NewKeyRange("300", "400")}, Status: pending,
	}}
	encoded, err = EncodeConfig(jobs)
	re.NoError(err)
	re.NoError(s.SaveSchedulerConfig(sc.GetName(), encoded))
	re.NoError(sc.ReloadConfig())
	re.False(sc.IsScheduleAllowed(cluster))
	re.NotSame(old, sc.conf.jobs[0])
	re.Equal("replacement", sc.conf.jobs[0].Alias)
	re.Equal(jobs[0].Ranges, sc.conf.jobs[0].Ranges)
	re.Equal(pending, sc.conf.jobs[0].Status)
	re.Equal([]string{"0:running"}, cluster.updates)
	re.True(tc.GetKeyRangeManager().IsEmpty())

	// The config watcher must also be able to publish a deleted config.
	re.NoError(s.RemoveSchedulerConfig(sc.GetName()))
	re.NoError(sc.ReloadConfig())
	re.Empty(sc.conf.jobs)
	re.False(sc.IsScheduleAllowed(cluster))
	re.True(tc.GetKeyRangeManager().IsEmpty())
}

func TestBalanceRangeReservationAcrossWatchedSnapshots(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	cluster := &mockJobStatusUpdater{Cluster: tc}
	s := storage.NewStorageWithMemoryBackend()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, s,
		ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "same-alias", "100", "200"}))
	re.NoError(err)
	sc := scheduler.(*balanceRangeScheduler)
	publish := func(jobs []*balanceRangeSchedulerJob) {
		data, err := EncodeConfig(jobs)
		re.NoError(err)
		re.NoError(s.SaveSchedulerConfig(sc.GetName(), data))
		re.NoError(sc.ReloadConfig())
	}
	jobs := sc.conf.cloneLocked()
	start := time.Now()
	jobs[0].Status = running
	jobs[0].Start = &start
	publish(jobs)
	sc.IsScheduleAllowed(cluster)
	old := sc.rangeJob
	re.NotNil(old)
	re.False(tc.GetKeyRangeManager().IsEmpty())

	// Cleanup clears scores, so an unchanged score also detects an unnecessary
	// release/re-registration that would be invisible in the final range set.
	sc.scoreMap = map[uint64]float64{42: 7}
	score := balanceRangeGauge.WithLabelValues("42", "score")
	score.Set(7)
	defer score.Set(0)
	publish(sc.conf.cloneLocked())
	re.NotSame(old, sc.conf.jobs[0])
	sc.IsScheduleAllowed(cluster)
	re.Same(sc.conf.jobs[0], sc.rangeJob)
	re.InDelta(7, testutil.ToFloat64(score), 0.001)
	re.False(tc.GetKeyRangeManager().IsEmpty())
	re.Empty(cluster.updates)

	// Reusing both ID and alias does not reuse the reservation when Create changes.
	jobs = sc.conf.cloneLocked()
	jobs[0].Create = jobs[0].Create.Add(time.Nanosecond)
	jobs[0].Status = pending
	jobs[0].Start = nil
	publish(jobs)
	re.False(sc.IsScheduleAllowed(cluster))
	re.Equal([]string{"0:running"}, cluster.updates)
	re.Nil(sc.rangeJob)
	re.True(tc.GetKeyRangeManager().IsEmpty())
	re.Equal(pending, sc.conf.jobs[0].Status)
	re.Nil(sc.conf.jobs[0].Start)
	re.InDelta(0, testutil.ToFloat64(score), 0.001)

	// HTTP acknowledgement alone must not reserve the new job's ranges.
	re.False(sc.IsScheduleAllowed(cluster))
	re.True(tc.GetKeyRangeManager().IsEmpty())
	jobs[0].Status = running
	jobs[0].Start = &start
	publish(jobs)
	sc.IsScheduleAllowed(cluster)
	re.Same(sc.conf.jobs[0], sc.rangeJob)
	re.False(tc.GetKeyRangeManager().IsEmpty())
}
