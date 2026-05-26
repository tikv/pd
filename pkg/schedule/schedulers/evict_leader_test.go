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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestEvictLeader(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add regions 1, 2, 3 with leaders in stores 1, 2, 3
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 2, 1)
	tc.AddLeaderRegion(3, 3, 1)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	re.True(sl.IsScheduleAllowed(tc))
	ops, _ := sl.Schedule(tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2, 3})
	re.False(ops[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 1, []uint64{2, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})))
	re.True(ops[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 2, []uint64{1, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})))
}

func TestEvictLeaderWithUnhealthyPeer(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add region 1, which has 3 peers. 1 is leader. 2 is healthy or pending, 3 is healthy or down.
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.MockRegionInfo(1, 1, []uint64{2, 3}, nil, nil)
	withDownPeer := core.WithDownPeers([]*pdpb.PeerStats{{
		Peer:        region.GetPeers()[2],
		DownSeconds: 1000,
	}})
	withPendingPeer := core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[1]})

	// only pending
	tc.PutRegion(region.Clone(withPendingPeer))
	ops, _ := sl.Schedule(tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{3})
	// only down
	tc.PutRegion(region.Clone(withDownPeer))
	ops, _ = sl.Schedule(tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	// pending + down
	tc.PutRegion(region.Clone(withPendingPeer, withDownPeer))
	ops, _ = sl.Schedule(tc, false)
	re.Empty(ops)
}

func TestConfigClone(t *testing.T) {
	re := require.New(t)

	emptyConf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]keyutil.KeyRange)}
	con2 := emptyConf.clone()
	re.Empty(con2.getKeyRangesByID(1))

	con2.StoreIDWithRanges[1], _ = getKeyRanges([]string{"a", "b", "c", "d"})
	con3 := con2.clone()
	re.Equal(len(con3.getRanges(1)), len(con2.getRanges(1)))

	con3.StoreIDWithRanges[1][0].StartKey = []byte("aaa")
	con4 := con3.clone()
	re.True(bytes.Equal(con4.StoreIDWithRanges[1][0].StartKey, con3.StoreIDWithRanges[1][0].StartKey))

	con4.Batch = 10
	con5 := con4.clone()
	re.Equal(con5.getBatch(), con4.getBatch())
}

func TestBatchEvict(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// the random might be the same, so we add 1000 regions to make sure the batch is full
	for i := 1; i <= 10000; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 2, 3)
	}
	tc.AddLeaderRegion(6, 2, 1, 3)
	tc.AddLeaderRegion(7, 3, 1, 2)

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	re.True(sl.IsScheduleAllowed(tc))
	testutil.Eventually(re, func() bool {
		ops, _ := sl.Schedule(tc, false)
		return len(ops) == 3
	})
	sl.(*evictLeaderScheduler).conf.Batch = 5
	testutil.Eventually(re, func() bool {
		ops, _ := sl.Schedule(tc, false)
		return len(ops) == 5
	})
	sl.(*evictLeaderScheduler).conf.Batch = maxEvictLeaderBatchSize
	testutil.Eventually(re, func() bool {
		ops, _ := sl.Schedule(tc, false)
		return len(ops) == maxEvictLeaderBatchSize
	})
}

func TestEvictLeaderBatchLimit(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)

	body, err := json.Marshal(map[string]any{"batch": maxEvictLeaderBatchSize})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusOK, resp.Code)
	re.Equal(maxEvictLeaderBatchSize, sl.(*evictLeaderScheduler).conf.getBatch())

	body, err = json.Marshal(map[string]any{"batch": maxEvictLeaderBatchSize + 1})
	re.NoError(err)
	req = httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp = httptest.NewRecorder()
	sl.ServeHTTP(resp, req)
	re.Equal(http.StatusBadRequest, resp.Code)
	re.Equal(maxEvictLeaderBatchSize, sl.(*evictLeaderScheduler).conf.getBatch())
}

func TestEvictLeaderAddWaitingOperatorLimit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxWaiting int
		expected   int
	}{
		{name: "default-waiting-limit", maxWaiting: 0, expected: 5},
		{name: "batch-sized-waiting-limit", maxWaiting: maxEvictLeaderBatchSize, expected: maxEvictLeaderBatchSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster, oc, sl := prepareEvictLeaderBatchScenario(t, tc.maxWaiting)

			var ops []*operator.Operator
			testutil.Eventually(re, func() bool {
				ops, _ = sl.Schedule(cluster, false)
				return len(ops) == maxEvictLeaderBatchSize
			})
			re.Equal(tc.expected, oc.AddWaitingOperator(ops...))
		})
	}
}

func BenchmarkEvictLeaderScheduleBatch(b *testing.B) {
	tc, _, sl := prepareEvictLeaderBatchScenario(b, 0)

	totalOps := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ops, _ := sl.Schedule(tc, false)
		if len(ops) == 0 {
			b.Fatal("expected evict leader operators")
		}
		totalOps += len(ops)
	}
	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(totalOps)/elapsed, "ops/s")
	b.ReportMetric(float64(totalOps)*60/elapsed, "ops/min")
}

func prepareEvictLeaderBatchScenario(tb testing.TB, maxWaiting int) (*mockcluster.Cluster, *operator.Controller, Scheduler) {
	restoreLogger := log.ReplaceGlobals(zap.NewNop(), nil)
	tb.Cleanup(restoreLogger)

	cancel, opt, tc, oc := prepareSchedulersTest(false)
	tb.Cleanup(cancel)
	if maxWaiting > 0 {
		cfg := opt.GetScheduleConfig().Clone()
		cfg.SchedulerMaxWaitingOperator = uint64(maxWaiting)
		opt.SetScheduleConfig(cfg)
	}

	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	for i := 1; i <= 100000; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 2, 3)
	}

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	if err != nil {
		tb.Fatal(err)
	}
	sl.(*evictLeaderScheduler).conf.Batch = maxEvictLeaderBatchSize
	return tc, oc, sl
}

func TestEvictLeaderSchedulerCompatibility(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	saveConf := &evictLeaderSchedulerConfig{
		StoreIDWithRanges: map[uint64][]keyutil.KeyRange{
			1: {keyutil.KeyRange{StartKey: []byte(""), EndKey: []byte("")}},
		},
	}

	configJSON, err := json.Marshal(saveConf)
	re.NoError(err)

	// Save the serialized config to storage
	err = tc.GetStorage().SaveSchedulerConfig(string(types.EvictLeaderScheduler), configJSON)
	re.NoError(err)

	scheduleNames, configs, err := tc.GetStorage().LoadAllSchedulerConfigs()
	re.NoError(err)
	re.Len(scheduleNames, 1)
	data := configs[0]
	es, err := CreateScheduler(types.EvictLeaderScheduler, oc, tc.GetStorage(), ConfigJSONDecoder([]byte(data)), func(string) error { return nil })
	re.NoError(err)
	re.Equal(3, es.(*evictLeaderScheduler).conf.Batch)
	re.NotEmpty(es.(*evictLeaderScheduler).conf.StoreIDWithRanges[1])
}
