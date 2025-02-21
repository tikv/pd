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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

func TestGetPeers(t *testing.T) {
	re := require.New(t)
	learner := &metapb.Peer{StoreId: 1, Id: 1, Role: metapb.PeerRole_Learner}
	leader := &metapb.Peer{StoreId: 2, Id: 2}
	follower1 := &metapb.Peer{StoreId: 3, Id: 3}
	follower2 := &metapb.Peer{StoreId: 4, Id: 4}
	region := core.NewRegionInfo(&metapb.Region{Id: 100, Peers: []*metapb.Peer{
		leader, follower1, follower2, learner,
	}}, leader, core.WithLearners([]*metapb.Peer{learner}))
	for _, v := range []struct {
		role  string
		peers []*metapb.Peer
	}{
		{
			role:  "leader",
			peers: []*metapb.Peer{leader},
		},
		{
			role:  "follower",
			peers: []*metapb.Peer{follower1, follower2},
		},
		{
			role:  "learner",
			peers: []*metapb.Peer{learner},
		},
		{
			role:  "witness",
			peers: nil,
		},
	} {
		role := NewRole(v.role)
		re.Equal(v.peers, role.getPeers(region))
	}
}

func TestJobStatus(t *testing.T) {
	s := storage.NewStorageWithMemoryBackend()
	re := require.New(t)
	conf := &balanceRangeSchedulerConfig{
		schedulerConfig: &baseSchedulerConfig{},
		jobs:            make([]*balanceRangeSchedulerJob, 1),
	}
	conf.init(string(types.BalanceRangeScheduler), s, conf)
	for _, v := range []struct {
		jobStatus JobStatus
		begin     bool
		finish    bool
	}{
		{
			pending,
			true,
			false,
		},
		{
			running,
			false,
			true,
		},
		{
			finished,
			false,
			false,
		},
	} {
		job := &balanceRangeSchedulerJob{
			Status: v.jobStatus,
		}
		conf.jobs[0] = job
		if v.begin {
			re.Equal(running, conf.begin(0).Status)
		} else {
			re.Nil(conf.begin(0))
		}
		job.Status = v.jobStatus
		if v.finish {
			re.Equal(finished, conf.finish(0).Status)
		} else {
			re.Nil(conf.finish(0))
		}
	}
	idx, job := conf.peek()
	re.Equal(0, idx)
	re.Nil(job)
	conf.jobs[0] = &balanceRangeSchedulerJob{
		Status: running,
	}
	idx, job = conf.peek()
	re.Equal(0, idx)
	re.NotNil(job)
}

func TestBalanceRangePlan(t *testing.T) {
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
		Role:   leader,
		Ranges: []core.KeyRange{core.NewKeyRange("100", "110")},
	}
	plan, err := sc.prepare(tc, *operator.NewOpInfluence(), job)
	re.NoError(err)
	re.NotNil(plan)
	re.Len(plan.stores, 3)
	re.Len(plan.scoreMap, 3)
	re.Equal(int64(1), plan.scoreMap[1])
	re.Equal(int64(1), plan.tolerate)
}

func TestTIKVEngine(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader", "tikv", "1h", "test", "100", "200"}))
	re.NoError(err)
	ops, _ := scheduler.Schedule(tc, true)
	re.Empty(ops)
	for i := 1; i <= 3; i++ {
		tc.AddLeaderStore(uint64(i), 0)
	}
	// add regions:
	// store-1: 3 leader regions
	// store-2: 2 leader regions
	// store-3: 1 leader regions
	tc.AddLeaderRegionWithRange(1, "100", "110", 1, 2, 3)
	tc.AddLeaderRegionWithRange(2, "110", "120", 1, 2, 3)
	tc.AddLeaderRegionWithRange(3, "120", "140", 1, 2, 3)
	tc.AddLeaderRegionWithRange(4, "140", "160", 2, 1, 3)
	tc.AddLeaderRegionWithRange(5, "160", "180", 2, 1, 3)
	tc.AddLeaderRegionWithRange(5, "180", "200", 3, 1, 2)
	// case1: transfer leader from store 1 to store 3
	ops, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	op := ops[0]
	re.Equal("3", op.GetAdditionalInfo("sourceScore"))
	re.Equal("1", op.GetAdditionalInfo("targetScore"))
	re.Contains(op.Brief(), "transfer leader: store 1 to")
	tc.AddLeaderStore(4, 0)

	// case2: move peer from store 1 to store 4
	ops, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(ops)
	op = ops[0]
	re.Equal("3", op.GetAdditionalInfo("sourceScore"))
	re.Equal("0", op.GetAdditionalInfo("targetScore"))
	re.Contains(op.Brief(), "mv peer: store [1] to [4]")
}

func TestTIFLASHEngine(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tikvCount := 3
	// 3 tikv and 3 tiflash
	for i := 1; i <= tikvCount; i++ {
		tc.AddLeaderStore(uint64(i), 0)
	}
	for i := tikvCount + 1; i <= tikvCount+3; i++ {
		tc.AddLabelsStore(uint64(i), 0, map[string]string{"engine": "tiflash"})
	}
	tc.AddRegionWithLearner(uint64(1), 1, []uint64{2, 3}, []uint64{4})

	startKey := fmt.Sprintf("%20d0", 1)
	endKey := fmt.Sprintf("%20d0", 10)
	tc.RuleManager.SetRule(&placement.Rule{
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

	// generate a balance range scheduler with tiflash engine
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"learner", "tiflash", "1h", "test", startKey, endKey}))
	re.NoError(err)
	// tiflash-4 only has 1 region, so it doesn't need to balance
	ops, _ := scheduler.Schedule(tc, false)
	re.Empty(ops)

	// add 2 learner on tiflash-4
	for i := 2; i <= 3; i++ {
		tc.AddRegionWithLearner(uint64(i), 1, []uint64{2, 3}, []uint64{4})
	}
	ops, _ = scheduler.Schedule(tc, false)
	re.NotEmpty(ops)
	op := ops[0]
	re.Equal("3", op.GetAdditionalInfo("sourceScore"))
	re.Equal("0", op.GetAdditionalInfo("targetScore"))
	re.Equal("1", op.GetAdditionalInfo("tolerate"))
	re.Contains(op.Brief(), "mv peer: store [4] to")
}

func TestFetchAllRegions(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	for i := 1; i <= 3; i++ {
		tc.AddLeaderStore(uint64(i), 0)
	}
	for i := 1; i <= 100; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 2, 3)
	}

	ranges := core.NewKeyRangesWithSize(1)
	ranges.Append([]byte(""), []byte(""))
	regions := fetchAllRegions(tc, ranges)
	re.Len(regions, 100)

	ranges = core.NewKeyRangesWithSize(1)
	region := tc.GetRegion(50)
	ranges.Append([]byte(""), region.GetStartKey())
	ranges.Append(region.GetStartKey(), []byte(""))
	regions = fetchAllRegions(tc, ranges)
	re.Len(regions, 100)
}
