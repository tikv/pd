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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
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

func newTestHotPeerStat(
	t *testing.T,
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
