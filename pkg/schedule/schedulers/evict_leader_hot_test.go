// Copyright 2026 TiKV Project Authors.
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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

type testHotPeerStatsProvider struct {
	responses map[utils.RWType]map[uint64][]*statistics.HotPeerStat
	calls     map[utils.RWType]int
	requests  map[utils.RWType][][]uint64
}

func newTestHotPeerStatsProvider() *testHotPeerStatsProvider {
	return &testHotPeerStatsProvider{
		responses: map[utils.RWType]map[uint64][]*statistics.HotPeerStat{
			utils.Read:  {},
			utils.Write: {},
		},
		calls:    make(map[utils.RWType]int),
		requests: make(map[utils.RWType][][]uint64),
	}
}

func (p *testHotPeerStatsProvider) GetHotPeerStatsForStores(
	rw utils.RWType,
	storeIDs []uint64,
) map[uint64][]*statistics.HotPeerStat {
	p.calls[rw]++
	p.requests[rw] = append(p.requests[rw], append([]uint64(nil), storeIDs...))
	return p.responses[rw]
}

func TestHotLeaderCandidatesRefreshAndPriority(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, _ := prepareSchedulersTest()
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 2)
	read21 := newTestHotPeerStat(t, cluster, utils.Read, 21, 2, 2, 1)
	write11 := newTestHotPeerStat(t, cluster, utils.Write, 11, 1, 1, 2)
	write12 := newTestHotPeerStat(t, cluster, utils.Write, 12, 1, 1, 2)
	write21OnStore1 := newTestHotPeerStat(t, cluster, utils.Write, 21, 1, 1, 2)
	write22 := newTestHotPeerStat(t, cluster, utils.Write, 22, 2, 2, 1)
	nonLeader99 := newTestHotPeerStat(t, cluster, utils.Write, 99, 3, 1, 1, 2)

	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{
		1: {read11, read21}, // read21 is deliberately under the wrong Store key.
		2: {read21},
	}
	provider.responses[utils.Write] = map[uint64][]*statistics.HotPeerStat{
		1: {write11, write12, write21OnStore1, nonLeader99},
		2: {write22},
	}

	now := time.Unix(100, 0)
	candidates := newHotLeaderCandidates()
	candidates.now = func() time.Time { return now }
	candidates.refresh(provider, []uint64{1, 2})

	re.Equal(1, provider.calls[utils.Read])
	re.Equal(1, provider.calls[utils.Write])
	re.ElementsMatch([]uint64{1, 2}, provider.requests[utils.Read][0])
	re.ElementsMatch([]uint64{1, 2}, provider.requests[utils.Write][0])
	re.Equal(map[uint64]uint64{11: 1, 21: 2}, popAllHotLeaderCandidates(candidates, utils.Read, []uint64{1, 2}))
	re.Equal(map[uint64]uint64{12: 1, 22: 2}, popAllHotLeaderCandidates(candidates, utils.Write, []uint64{1, 2}))
}

func TestHotLeaderCandidatesRefreshRateLimit(t *testing.T) {
	re := require.New(t)
	provider := newTestHotPeerStatsProvider()
	now := time.Unix(200, 0)
	candidates := newHotLeaderCandidates()
	candidates.now = func() time.Time { return now }

	candidates.refresh(provider, []uint64{1})
	re.Equal(1, provider.calls[utils.Read])
	re.Equal(1, provider.calls[utils.Write])
	re.Equal([]uint64{1}, provider.requests[utils.Read][0])

	now = now.Add(500 * time.Millisecond)
	candidates.refresh(provider, []uint64{1})
	re.Equal(1, provider.calls[utils.Read])
	re.Equal(1, provider.calls[utils.Write])

	candidates.refresh(provider, []uint64{1, 2})
	re.Equal(2, provider.calls[utils.Read])
	re.Equal(2, provider.calls[utils.Write])
	re.Equal([]uint64{2}, provider.requests[utils.Read][1])

	now = now.Add(500 * time.Millisecond)
	candidates.refresh(provider, []uint64{1, 2})
	re.Equal(3, provider.calls[utils.Read])
	re.Equal(3, provider.calls[utils.Write])
	re.Equal([]uint64{1}, provider.requests[utils.Read][2])
}

func TestHotLeaderCandidatesRefreshFailureRetainsCandidates(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, _ := prepareSchedulersTest()
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 2)
	write12 := newTestHotPeerStat(t, cluster, utils.Write, 12, 1, 1, 2)
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{1: {read11}}
	provider.responses[utils.Write] = map[uint64][]*statistics.HotPeerStat{1: {write12}}

	now := time.Unix(300, 0)
	candidates := newHotLeaderCandidates()
	candidates.now = func() time.Time { return now }
	candidates.refresh(provider, []uint64{1})

	provider.responses[utils.Read] = nil
	provider.responses[utils.Write] = map[uint64][]*statistics.HotPeerStat{}
	now = now.Add(time.Second)
	candidates.refresh(provider, []uint64{1})

	re.Equal(map[uint64]uint64{11: 1}, popAllHotLeaderCandidates(candidates, utils.Read, []uint64{1}))
	re.Empty(popAllHotLeaderCandidates(candidates, utils.Write, []uint64{1}))
	re.Equal(2, provider.calls[utils.Read])
	re.Equal(2, provider.calls[utils.Write])

	candidates.refresh(provider, []uint64{1})
	re.Equal(2, provider.calls[utils.Read])
	re.Equal(2, provider.calls[utils.Write])
	now = now.Add(time.Second)
	candidates.refresh(provider, []uint64{1})
	re.Equal(3, provider.calls[utils.Read])
	re.Equal(3, provider.calls[utils.Write])
}

func TestHotLeaderCandidatesPrunesStores(t *testing.T) {
	re := require.New(t)
	provider := newTestHotPeerStatsProvider()
	now := time.Unix(400, 0)
	candidates := newHotLeaderCandidates()
	candidates.now = func() time.Time { return now }

	candidates.refresh(provider, []uint64{1})
	re.Contains(candidates.stores, uint64(1))
	re.NotContains(candidates.stores, uint64(2))

	candidates.refresh(provider, []uint64{2})
	re.NotContains(candidates.stores, uint64(1))
	re.Contains(candidates.stores, uint64(2))
	re.Equal(2, provider.calls[utils.Read])
	re.Equal([]uint64{2}, provider.requests[utils.Read][1])

	candidates.refresh(provider, nil)
	re.Empty(candidates.stores)
	re.Equal(2, provider.calls[utils.Read])
	re.Equal(2, provider.calls[utils.Write])
}

type testHotLeaderCluster struct {
	*mockcluster.Cluster
	provider       hotPeerStatsProvider
	fullStatsCalls int
}

func (c *testHotLeaderCluster) GetHotPeerStats(rw utils.RWType) map[uint64][]*statistics.HotPeerStat {
	c.fullStatsCalls++
	return c.Cluster.GetHotPeerStats(rw)
}

func (c *testHotLeaderCluster) GetHotPeerStatsForStores(
	rw utils.RWType,
	storeIDs []uint64,
) map[uint64][]*statistics.HotPeerStat {
	return c.provider.GetHotPeerStatsForStores(rw, storeIDs)
}

// regionScalingHotLeaderCluster advertises the total cluster Region count without
// materializing cold Regions. Its counters make any broad Region access visible,
// so the tests and benchmark measure only the fixed targeted-hot working set.
type regionScalingHotLeaderCluster struct {
	*testHotLeaderCluster
	totalRegionCount   int
	fullRegionScans    int
	ordinarySelections int
}

func (c *regionScalingHotLeaderCluster) GetTotalRegionCount() int {
	return c.totalRegionCount
}

func (c *regionScalingHotLeaderCluster) ScanRegions(_, _ []byte, _ int) []*core.RegionInfo {
	c.fullRegionScans++
	return nil
}

func (c *regionScalingHotLeaderCluster) BatchScanRegions(
	_ *keyutil.KeyRanges,
	_ ...core.BatchScanRegionsOptionFunc,
) ([]*core.RegionInfo, error) {
	c.fullRegionScans++
	return nil, nil
}

func (c *regionScalingHotLeaderCluster) GetRegionCount(_, _ []byte) int {
	c.fullRegionScans++
	return c.totalRegionCount
}

func (c *regionScalingHotLeaderCluster) RandLeaderRegions(
	_ uint64,
	_ []keyutil.KeyRange,
) []*core.RegionInfo {
	c.ordinarySelections++
	return nil
}

type fixedHotPeerStatsProvider map[utils.RWType]map[uint64][]*statistics.HotPeerStat

func (p fixedHotPeerStatsProvider) GetHotPeerStatsForStores(
	rw utils.RWType,
	_ []uint64,
) map[uint64][]*statistics.HotPeerStat {
	return p[rw]
}

func TestEvictLeaderSchedulerUsesHotLeaderCandidates(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, opController := prepareSchedulersTest()
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 2)
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{1: {read11}}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}
	scheduler := newEvictLeaderScheduler(opController, newTestEvictLeaderConfig(1, 1))

	ops, _ := scheduler.Schedule(hotCluster, false)
	re.Equal(1, provider.calls[utils.Read])
	re.Equal(1, provider.calls[utils.Write])
	re.Len(ops, 1)
	re.Equal(uint64(11), ops[0].RegionID())
	re.Contains(ops[0].Counters, evictLeaderPickReadHotCounter)
}

func TestEvictSlowStoreSchedulerHotCandidatesFollowCurrentStore(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, opController := prepareSchedulersTest()
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 3)
	read21 := newTestHotPeerStat(t, cluster, utils.Read, 21, 2, 2, 3)
	for _, storeID := range []uint64{1, 2} {
		store := cluster.GetStore(storeID)
		cluster.PutStore(store.Clone(func(store *core.StoreInfo) {
			store.GetStoreStats().SlowScore = slowStoreEvictThreshold
		}))
	}
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{
		1: {read11},
		2: {read21},
	}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}
	conf := initEvictSlowStoreSchedulerConfig()
	conf.EvictedStores = []uint64{1}
	conf.Batch = 1
	scheduler := newEvictSlowStoreScheduler(opController, conf)

	ops, _ := scheduler.Schedule(hotCluster, false)
	re.Equal(1, provider.calls[utils.Read])
	re.Equal(1, provider.calls[utils.Write])
	re.Len(ops, 1)
	re.Equal(uint64(11), ops[0].RegionID())
	re.Equal([]uint64{1}, provider.requests[utils.Read][0])

	conf.EvictedStores = []uint64{2}
	ops, _ = scheduler.Schedule(hotCluster, false)
	re.Len(ops, 1)
	re.Equal(uint64(21), ops[0].RegionID())
	re.Equal(2, provider.calls[utils.Read])
	re.Equal(2, provider.calls[utils.Write])
	re.Equal([]uint64{2}, provider.requests[utils.Read][1])
}

func TestScheduleEvictHotLeaderDoesNotScanTotalRegions(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, opController := prepareSchedulersTest()
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 2)
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{1: {read11}}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}
	scalingCluster := &regionScalingHotLeaderCluster{
		testHotLeaderCluster: hotCluster,
		totalRegionCount:     10_000_000,
	}

	ops := scheduleEvictHotLeaderBatch(
		"test-no-total-region-scan", scalingCluster, newTestEvictLeaderConfig(1, 1),
		opController, newHotLeaderCandidates(),
	)
	re.Len(ops, 1)
	re.Equal(uint64(11), ops[0].RegionID())
	re.Equal(1, provider.calls[utils.Read])
	re.Equal(1, provider.calls[utils.Write])
	re.Zero(hotCluster.fullStatsCalls)
	re.Zero(scalingCluster.fullRegionScans)
	re.Zero(scalingCluster.ordinarySelections)
}

func TestScheduleEvictHotLeaderPriority(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, opController := prepareSchedulersTest()
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 2)
	write12 := newTestHotPeerStat(t, cluster, utils.Write, 12, 1, 1, 2)
	cluster.AddLeaderRegion(13, 1, 2)
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{1: {read11}}
	provider.responses[utils.Write] = map[uint64][]*statistics.HotPeerStat{1: {write12}}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}

	ops := scheduleEvictHotLeaderBatch(
		"test-evict-hot-leader", hotCluster, newTestEvictLeaderConfig(3, 1),
		opController, newHotLeaderCandidates(),
	)
	re.Len(ops, 3)
	re.Equal([]uint64{11, 12, 13}, []uint64{ops[0].RegionID(), ops[1].RegionID(), ops[2].RegionID()})
	re.Contains(ops[0].Counters, evictLeaderPickReadHotCounter)
	re.Contains(ops[1].Counters, evictLeaderPickWriteHotCounter)
}

func TestScheduleEvictHotLeaderGlobalReadPriority(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, opController := prepareSchedulersTest()
	defer cancel()

	write11 := newTestHotPeerStat(t, cluster, utils.Write, 11, 1, 1, 3)
	read21 := newTestHotPeerStat(t, cluster, utils.Read, 21, 2, 2, 3)
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{2: {read21}}
	provider.responses[utils.Write] = map[uint64][]*statistics.HotPeerStat{1: {write11}}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}

	ops := scheduleEvictHotLeaderBatch(
		"test-global-read-priority", hotCluster, newTestEvictLeaderConfig(1, 1, 2),
		opController, newHotLeaderCandidates(),
	)
	re.Len(ops, 1)
	re.Equal(uint64(21), ops[0].RegionID())
	re.Contains(ops[0].Counters, evictLeaderPickReadHotCounter)
}

func TestScheduleEvictHotLeaderRevalidatesCandidates(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*require.Assertions, *mockcluster.Cluster, *operator.Controller) uint64
		selectAlready bool
	}{
		{
			name: "missing-region",
			setup: func(_ *require.Assertions, _ *mockcluster.Cluster, _ *operator.Controller) uint64 {
				return 999
			},
		},
		{
			name: "leader-moved",
			setup: func(_ *require.Assertions, cluster *mockcluster.Cluster, _ *operator.Controller) uint64 {
				cluster.AddLeaderRegion(1, 2, 1)
				return 1
			},
		},
		{
			name: "active-operator",
			setup: func(re *require.Assertions, cluster *mockcluster.Cluster, opController *operator.Controller) uint64 {
				region := cluster.AddLeaderRegion(1, 1, 2)
				op, err := operator.CreateTransferLeaderOperator(
					"existing", cluster, region, 2, []uint64{2}, operator.OpLeader,
				)
				re.NoError(err)
				re.True(opController.AddOperator(op))
				return 1
			},
		},
		{
			name: "pending-peer",
			setup: func(_ *require.Assertions, cluster *mockcluster.Cluster, _ *operator.Controller) uint64 {
				region := cluster.AddLeaderRegion(1, 1, 2)
				cluster.PutRegion(region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)})))
				return 1
			},
		},
		{
			name: "down-peer",
			setup: func(_ *require.Assertions, cluster *mockcluster.Cluster, _ *operator.Controller) uint64 {
				region := cluster.AddLeaderRegion(1, 1, 2)
				cluster.PutRegion(region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{
					Peer: region.GetStorePeer(2),
				}})))
				return 1
			},
		},
		{
			name: "no-target-follower",
			setup: func(_ *require.Assertions, cluster *mockcluster.Cluster, _ *operator.Controller) uint64 {
				cluster.AddLeaderRegion(1, 1)
				return 1
			},
		},
		{
			name: "selected-in-current-batch",
			setup: func(_ *require.Assertions, cluster *mockcluster.Cluster, _ *operator.Controller) uint64 {
				cluster.AddLeaderRegion(1, 1, 2)
				return 1
			},
			selectAlready: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			cancel, cluster, opController := prepareHotLeaderValidationTest()
			defer cancel()
			regionID := test.setup(re, cluster, opController)
			selected := make(map[uint64]struct{})
			if test.selectAlready {
				selected[regionID] = struct{}{}
			}
			op := scheduleEvictHotLeader(
				"test-revalidate", cluster, newTestEvictLeaderConfig(1, 1),
				opController, selected, 1, regionID,
			)
			re.Nil(op)
		})
	}

	t.Run("valid", func(t *testing.T) {
		re := require.New(t)
		cancel, cluster, opController := prepareHotLeaderValidationTest()
		defer cancel()
		cluster.AddLeaderRegion(1, 1, 2)
		op := scheduleEvictHotLeader(
			"test-revalidate", cluster, newTestEvictLeaderConfig(1, 1),
			opController, map[uint64]struct{}{}, 1, 1,
		)
		operatorutil.CheckMultiTargetTransferLeader(re, op, operator.OpLeader, 1, []uint64{2})
	})
}

func TestScheduleEvictHotLeaderRespectsRanges(t *testing.T) {
	re := require.New(t)
	region := core.NewRegionInfo(&metapb.Region{
		Id:       1,
		StartKey: []byte("b"),
		EndKey:   []byte("d"),
	}, nil)
	re.True(regionIsInKeyRanges(region, []keyutil.KeyRange{keyutil.NewKeyRange("a", "e")}))
	re.True(regionIsInKeyRanges(region, []keyutil.KeyRange{keyutil.NewKeyRange("b", "")}))
	re.False(regionIsInKeyRanges(region, []keyutil.KeyRange{keyutil.NewKeyRange("b", "c")}))
	re.False(regionIsInKeyRanges(region, []keyutil.KeyRange{keyutil.NewKeyRange("c", "e")}))

	region = region.Clone(core.WithEndKey(nil))
	re.True(regionIsInKeyRanges(region, []keyutil.KeyRange{keyutil.NewKeyRange("b", "")}))
	re.False(regionIsInKeyRanges(region, []keyutil.KeyRange{keyutil.NewKeyRange("b", "z")}))
}

func TestScheduleEvictHotLeaderAvoidsDuplicateBatchRegions(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, opController := prepareSchedulersTest()
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 2)
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{1: {read11}}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}
	ops := scheduleEvictHotLeaderBatch(
		"test-no-duplicate", hotCluster, newTestEvictLeaderConfig(2, 1),
		opController, newHotLeaderCandidates(),
	)
	re.Len(ops, 1)
	re.Equal(uint64(11), ops[0].RegionID())
}

func TestScheduleEvictHotLeaderDoesNotFallbackToActiveRegion(t *testing.T) {
	re := require.New(t)
	cancel, _, cluster, opController := prepareSchedulersTest(false)
	defer cancel()

	read11 := newTestHotPeerStat(t, cluster, utils.Read, 11, 1, 1, 2)
	cluster.AddLeaderRegion(13, 1, 2)
	existing, err := operator.CreateTransferLeaderOperator(
		"existing", cluster, cluster.GetRegion(11), 2, []uint64{2}, operator.OpLeader,
	)
	re.NoError(err)
	re.True(opController.AddOperator(existing))
	provider := newTestHotPeerStatsProvider()
	provider.responses[utils.Read] = map[uint64][]*statistics.HotPeerStat{1: {read11}}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}

	ops := scheduleEvictHotLeaderBatch(
		"test-active-fallback", hotCluster, newTestEvictLeaderConfig(2, 1),
		opController, newHotLeaderCandidates(),
	)
	re.Len(ops, 1)
	re.Equal(uint64(13), ops[0].RegionID())
}

func TestScheduleEvictHotLeaderFallsBackToOrdinaryAndUnhealthy(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		re := require.New(t)
		cancel, cluster, opController := prepareHotLeaderValidationTest()
		defer cancel()
		cluster.AddLeaderRegion(1, 1, 2)
		provider := newTestHotPeerStatsProvider()
		hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}
		ops := scheduleEvictHotLeaderBatch(
			"test-ordinary-fallback", hotCluster, newTestEvictLeaderConfig(1, 1),
			opController, newHotLeaderCandidates(),
		)
		re.Len(ops, 1)
		operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	})

	t.Run("unhealthy", func(t *testing.T) {
		re := require.New(t)
		cancel, cluster, opController := prepareHotLeaderValidationTest()
		defer cancel()
		region := cluster.AddLeaderRegion(1, 1, 2, 3)
		cluster.PutRegion(region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)})))
		provider := newTestHotPeerStatsProvider()
		hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}
		ops := scheduleEvictHotLeaderBatch(
			"test-unhealthy-fallback", hotCluster, newTestEvictLeaderConfig(1, 1),
			opController, newHotLeaderCandidates(),
		)
		re.Len(ops, 1)
		operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{3})
	})
}

func prepareHotLeaderValidationTest() (func(), *mockcluster.Cluster, *operator.Controller) {
	cancel, _, cluster, opController := prepareSchedulersTest(false)
	for storeID := uint64(1); storeID <= 3; storeID++ {
		cluster.AddLeaderStore(storeID, 0)
	}
	return cancel, cluster, opController
}

func newTestEvictLeaderConfig(batch int, storeIDs ...uint64) *evictLeaderSchedulerConfig {
	storeIDWithRanges := make(map[uint64][]keyutil.KeyRange, len(storeIDs))
	for _, storeID := range storeIDs {
		storeIDWithRanges[storeID] = []keyutil.KeyRange{keyutil.NewKeyRange("", "")}
	}
	return &evictLeaderSchedulerConfig{
		StoreIDWithRanges: storeIDWithRanges,
		Batch:             batch,
	}
}

func newTestHotPeerStat(
	t testing.TB,
	cluster *mockcluster.Cluster,
	kind utils.RWType,
	regionID, leaderStoreID, wantedStoreID uint64,
	otherStoreIDs ...uint64,
) *statistics.HotPeerStat {
	t.Helper()
	if cluster.GetStore(leaderStoreID) == nil {
		cluster.AddRegionStore(leaderStoreID, 0)
	}
	for _, storeID := range otherStoreIDs {
		if cluster.GetStore(storeID) == nil {
			cluster.AddRegionStore(storeID, 0)
		}
	}
	const hotLoad = uint64(1 << 30)
	var stats []*statistics.HotPeerStat
	if kind == utils.Read {
		stats = cluster.AddRegionLeaderWithReadInfo(
			regionID, leaderStoreID, hotLoad, 0, 0,
			uint64(utils.StoreHeartBeatReportInterval), otherStoreIDs, 1,
		)
	} else {
		stats = cluster.AddLeaderRegionWithWriteInfo(
			regionID, leaderStoreID, hotLoad, 0, 0,
			uint64(utils.RegionHeartBeatReportInterval), otherStoreIDs, 1,
		)
	}
	for _, stat := range stats {
		if stat.StoreID == wantedStoreID {
			require.Equal(t, wantedStoreID == leaderStoreID, stat.IsLeader())
			return stat
		}
	}
	require.FailNow(t, "hot peer stat not found", "region %d store %d", regionID, wantedStoreID)
	return nil
}

func popAllHotLeaderCandidates(
	candidates *hotLeaderCandidates,
	rw utils.RWType,
	storeIDs []uint64,
) map[uint64]uint64 {
	popped := make(map[uint64]uint64)
	for {
		storeID, regionID, ok := candidates.pop(rw, storeIDs)
		if !ok {
			return popped
		}
		popped[regionID] = storeID
	}
}

func BenchmarkScheduleEvictHotLeaderByTotalRegionCount(b *testing.B) {
	const hotLeaderCount = 5_000
	cancel, _, cluster, opController := prepareSchedulersTest()
	b.Cleanup(cancel)

	readStats := make([]*statistics.HotPeerStat, 0, hotLeaderCount)
	for regionID := uint64(1); regionID <= hotLeaderCount; regionID++ {
		readStats = append(readStats, newTestHotPeerStat(
			b, cluster, utils.Read, regionID, 1, 1, 2,
		))
	}
	provider := fixedHotPeerStatsProvider{
		utils.Read:  {1: readStats},
		utils.Write: {},
	}
	hotCluster := &testHotLeaderCluster{Cluster: cluster, provider: provider}
	conf := newTestEvictLeaderConfig(1, 1)

	for _, totalRegionCount := range []int{10_000, 100_000, 1_000_000, 10_000_000} {
		b.Run(fmt.Sprintf("total_regions=%d", totalRegionCount), func(b *testing.B) {
			scalingCluster := &regionScalingHotLeaderCluster{
				testHotLeaderCluster: hotCluster,
				totalRegionCount:     totalRegionCount,
			}
			candidates := newHotLeaderCandidates()
			now := time.Unix(1_000, 0)
			candidates.now = func() time.Time { return now }
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(hotLeaderCount, "hot_leaders")
			b.ReportMetric(float64(totalRegionCount), "total_regions")

			for range b.N {
				// Force the once-per-second refresh path on every iteration. Cache-hit
				// scheduling calls do less work than the path measured here.
				now = now.Add(hotLeaderCandidateRefreshInterval)
				ops := scheduleEvictHotLeaderBatch(
					"benchmark-evict-hot-leader", scalingCluster, conf,
					opController, candidates,
				)
				if len(ops) != 1 {
					b.Fatalf("expected one operator, got %d", len(ops))
				}
			}

			b.StopTimer()
			if hotCluster.fullStatsCalls != 0 ||
				scalingCluster.fullRegionScans != 0 ||
				scalingCluster.ordinarySelections != 0 {
				b.Fatalf(
					"unexpected broad access: full-stats=%d full-scans=%d ordinary=%d",
					hotCluster.fullStatsCalls,
					scalingCluster.fullRegionScans,
					scalingCluster.ordinarySelections,
				)
			}
		})
	}
}
