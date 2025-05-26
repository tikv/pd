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
	"math/rand"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
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

	emptyConf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange)}
	con2 := emptyConf.clone()
	re.Empty(con2.getKeyRangesByID(1))

	con2.StoreIDWithRanges[1], _ = getKeyRanges([]string{"a", "b", "c", "d"})
	con3 := con2.clone()
	re.Len(con3.getRanges(1), len(con2.getRanges(1)))

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
}

func TestEvictHotLeader(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWrittenBytes(1, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{3, []uint64{2, 1, 3}, 0.0, 0, 0},
		{4, []uint64{2, 1, 3}, 0.0, 0, 0},
		{5, []uint64{2, 1, 3}, 0.0, 0, 0},
		{6, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{7, []uint64{2, 1, 3}, 0.0, 0, 0},
		{8, []uint64{2, 1, 3}, 1.0 * units.MiB, 2 * units.MiB, 0},
		{9, []uint64{2, 1, 3}, 0.0, 0, 0},
		{10, []uint64{2, 1, 3}, 2.0 * units.MiB, 3 * units.MiB, 0},
		{11, []uint64{2, 1, 3}, 0.0, 0, 0},
		{12, []uint64{2, 1, 3}, 0.0, 0, 0},
		{13, []uint64{2, 1, 3}, 1.5 * units.MiB, 4 * units.MiB, 0},
		{14, []uint64{2, 1, 3}, 0.0, 0, 0},
	})

	// hot read has lower priority than hot write
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{15, []uint64{2, 1, 3}, 0.0, 0, 0},
		{16, []uint64{2, 1, 3}, 1.5 * units.MiB, 4 * units.MiB, 0},
		{17, []uint64{2, 1, 3}, 0.0, 0, 0},
	})

	sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"2"}), func(string) error { return nil })
	re.NoError(err)
	re.True(sl.IsScheduleAllowed(tc))
	ops, _ := sl.Schedule(tc, false)
	for _, op := range ops {
		switch op.RegionID() {
		case 3, 4, 5, 7, 9, 11, 12, 14, 15, 16, 17:
			re.FailNow("unexpected region", op.RegionID())
		default:
		}
	}
	re.Len(ops, 3)
}

func BenchmarkScheduleEvictLeaderBatch(b *testing.B) {
	cases := []struct {
		name       string
		stores     int
		regions    int
		hotRegions int
		writeRate  float64
	}{
		{"cold", 10, 500000, 0, 0},
		{"hot", 10, 500000, 5000, 2.0},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cancel, _, cluster, oc := prepareSchedulersTest()
			defer cancel()

			// Add stores with write statistics
			for i := 1; i <= tc.stores; i++ {
				cluster.AddLeaderStore(uint64(i), 0)
				if tc.writeRate > 0 {
					writtenBytes := uint64(tc.writeRate * float64(units.MiB) * utils.StoreHeartBeatReportInterval)
					cluster.UpdateStorageWrittenBytes(uint64(i), writtenBytes)
					cluster.UpdateStorageWrittenKeys(uint64(i), writtenBytes)
				}
			}

			hotRegions := make([]testRegionInfo, 0, tc.hotRegions)
			if tc.hotRegions > 0 {
				for i := 1; i <= tc.hotRegions; i++ {
					peers := []uint64{1} // leader在store 1
					for len(peers) < 3 {
						sid := uint64(rand.Intn(tc.stores) + 1)
						if !containsUint64(peers, sid) {
							peers = append(peers, sid)
						}
					}
					hotRegions = append(hotRegions, testRegionInfo{
						id:        uint64(i),
						peers:     peers,
						byteRate:  tc.writeRate * units.MiB,
						keyRate:   tc.writeRate * units.MiB * 2,
						queryRate: 0,
					})
				}
			}

			// Add regions with leaders
			for i := 1; i <= tc.regions; i++ {
				peers := []uint64{uint64(rand.Intn(tc.stores) + 1)}
				for len(peers) < 3 {
					sid := uint64(rand.Intn(tc.stores) + 1)
					if !containsUint64(peers, sid) {
						peers = append(peers, sid)
					}
				}
				cluster.AddLeaderRegion(uint64(i), peers[0], peers[1], peers[2])
			}

			if len(hotRegions) > 0 {
				addRegionInfo(cluster, utils.Write, hotRegions)
			}

			// Create scheduler and configure store 1 as evict target
			sl, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(),
				ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}),
				func(string) error { return nil })
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sl.Schedule(cluster, false)
			}
		})
	}
}

func containsUint64(arr []uint64, val uint64) bool {
	for _, a := range arr {
		if a == val {
			return true
		}
	}
	return false
}
