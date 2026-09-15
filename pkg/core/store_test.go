// Copyright 2017 TiKV Project Authors.
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

package core

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestDistinctScore(t *testing.T) {
	re := require.New(t)
	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3"}

	var stores []*StoreInfo
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				storeID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				storeLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				store := NewStoreInfoWithLabel(storeID, storeLabels)
				stores = append(stores, store)

				// Number of stores in different zones.
				numZones := i * len(racks) * len(hosts)
				// Number of stores in the same zone but in different racks.
				numRacks := j * len(hosts)
				// Number of stores in the same rack but in different hosts.
				numHosts := k
				score := (numZones*replicaBaseScore+numRacks)*replicaBaseScore + numHosts
				re.Equal(float64(score), DistinctScore(labels, stores, store))
			}
		}
	}
	store := NewStoreInfoWithLabel(100, nil)
	re.Equal(float64(0), DistinctScore(labels, stores, store))
}

func TestCloneStore(_ *testing.T) {
	meta := &metapb.Store{Id: 1, Address: "mock://tikv-1:1", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}}
	store := NewStoreInfo(meta)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for time.Since(start) <= time.Second {
			store.GetMeta().GetState()
		}
	}()
	go func() {
		defer wg.Done()
		for time.Since(start) <= time.Second {
			store.Clone(
				SetStoreState(metapb.StoreState_Up),
				SetLastHeartbeatTS(time.Now()),
			)
		}
	}()
	wg.Wait()
}

func TestCloneMetaStore(t *testing.T) {
	re := require.New(t)
	store := &metapb.Store{Id: 1, Address: "mock://tikv-1:1", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}}
	store2 := typeutil.DeepClone(NewStoreInfo(store).meta, StoreFactory)
	re.Equal(store2.Labels, store.Labels)
	store2.Labels[0].Value = "changed value"
	re.NotEqual(store2.Labels, store.Labels)
}

// requireTxnProtocolVersionRange asserts the presence and the value of the txn
// protocol version range carried by the given store.
func requireTxnProtocolVersionRange(re *require.Assertions, store *StoreInfo, expected *metapb.TxnProtocolVersionRange) {
	actual := store.GetMeta().GetTxnProtocolVersionRange()
	if expected == nil {
		re.Nil(actual)
		return
	}
	re.NotNil(actual)
	re.Equal(expected.GetMin(), actual.GetMin())
	re.Equal(expected.GetMax(), actual.GetMax())
}

func TestStoreTxnProtocolVersionRange(t *testing.T) {
	re := require.New(t)
	meta := &metapb.Store{Id: 1, Address: "mock://tikv-1:1", Version: "6.5.0"}
	store := NewStoreInfo(meta)
	re.Nil(store.GetMeta().GetTxnProtocolVersionRange())

	// Set, replace and lower the range by the dedicated option.
	store = store.Clone(SetStoreTxnProtocolVersionRange(&metapb.TxnProtocolVersionRange{Min: 0, Max: 2}))
	requireTxnProtocolVersionRange(re, store, &metapb.TxnProtocolVersionRange{Min: 0, Max: 2})
	store = store.Clone(SetStoreTxnProtocolVersionRange(&metapb.TxnProtocolVersionRange{Min: 0, Max: 1}))
	requireTxnProtocolVersionRange(re, store, &metapb.TxnProtocolVersionRange{Min: 0, Max: 1})

	// An explicit empty range keeps its presence.
	store = store.Clone(SetStoreTxnProtocolVersionRange(&metapb.TxnProtocolVersionRange{}))
	re.NotNil(store.GetMeta().GetTxnProtocolVersionRange())
	requireTxnProtocolVersionRange(re, store, &metapb.TxnProtocolVersionRange{})

	// A missing range clears the stored one.
	store = store.Clone(SetStoreTxnProtocolVersionRange(nil))
	re.Nil(store.GetMeta().GetTxnProtocolVersionRange())
}

func TestStoreTxnProtocolVersionRangePresenceAndSnapshotIsolation(t *testing.T) {
	re := require.New(t)
	store := NewStoreInfo(&metapb.Store{Id: 1, Address: "mock://tikv-1:1"})
	re.Nil(store.GetMeta().GetTxnProtocolVersionRange())

	// An explicit empty range survives the round trip of a full store clone.
	emptyRange := store.Clone(SetStoreTxnProtocolVersionRange(&metapb.TxnProtocolVersionRange{}))
	re.NotNil(emptyRange.GetMeta().GetTxnProtocolVersionRange())
	re.Zero(emptyRange.GetMeta().GetTxnProtocolVersionRange().GetMin())
	re.Zero(emptyRange.GetMeta().GetTxnProtocolVersionRange().GetMax())
	re.NotNil(typeutil.DeepClone(emptyRange.GetMeta(), StoreFactory).GetTxnProtocolVersionRange())

	// Mutating the input range does not affect the generated snapshot.
	input := &metapb.TxnProtocolVersionRange{Min: 0, Max: 1}
	withRange := store.Clone(SetStoreTxnProtocolVersionRange(input))
	requireTxnProtocolVersionRange(re, withRange, &metapb.TxnProtocolVersionRange{Min: 0, Max: 1})
	input.Min, input.Max = 3, 4
	requireTxnProtocolVersionRange(re, withRange, &metapb.TxnProtocolVersionRange{Min: 0, Max: 1})

	// Applying the option does not modify the previous snapshot.
	requireTxnProtocolVersionRange(re, store, nil)
	cleared := withRange.Clone(SetStoreTxnProtocolVersionRange(nil))
	re.Nil(cleared.GetMeta().GetTxnProtocolVersionRange())
	requireTxnProtocolVersionRange(re, withRange, &metapb.TxnProtocolVersionRange{Min: 0, Max: 1})

	// ShallowClone keeps the meta as is until the option is applied.
	shallow := withRange.ShallowClone(SetStoreTxnProtocolVersionRange(&metapb.TxnProtocolVersionRange{Min: 1, Max: 2}))
	requireTxnProtocolVersionRange(re, shallow, &metapb.TxnProtocolVersionRange{Min: 1, Max: 2})
	requireTxnProtocolVersionRange(re, withRange, &metapb.TxnProtocolVersionRange{Min: 0, Max: 1})

	// The range keeps the same presence and value when it is written back to the cache.
	storesInfo := NewStoresInfo()
	storesInfo.PutStore(withRange)
	opts := []StoreCreateOption{SetStoreTxnProtocolVersionRange(&metapb.TxnProtocolVersionRange{Min: 3, Max: 4})}
	storesInfo.PutStore(withRange.Clone(opts...), opts...)
	requireTxnProtocolVersionRange(re, storesInfo.GetStore(1), &metapb.TxnProtocolVersionRange{Min: 3, Max: 4})
}

func TestSetStoreMetaTxnProtocolVersionRange(t *testing.T) {
	re := require.New(t)
	newMeta := func(versionRange *metapb.TxnProtocolVersionRange) *metapb.Store {
		return &metapb.Store{
			Id:                      1,
			Address:                 "mock://tikv-1:2",
			Version:                 "6.6.0",
			DeployPath:              "test/store1-new",
			TxnProtocolVersionRange: versionRange,
		}
	}

	store := NewStoreInfo(&metapb.Store{Id: 1, Address: "mock://tikv-1:1", Version: "6.5.0", NodeState: metapb.NodeState_Serving})
	heartbeat := time.Now().Add(-time.Minute)
	store = store.Clone(SetLastHeartbeatTS(heartbeat))

	// SetStoreMeta copies the range reported by the new meta.
	updated := store.Clone(SetStoreMeta(newMeta(&metapb.TxnProtocolVersionRange{Min: 0, Max: 2})))
	requireTxnProtocolVersionRange(re, updated, &metapb.TxnProtocolVersionRange{Min: 0, Max: 2})
	re.Equal("mock://tikv-1:2", updated.GetMeta().GetAddress())
	re.Equal("test/store1-new", updated.GetMeta().GetDeployPath())

	// An explicit empty range keeps its presence.
	emptyRange := store.Clone(SetStoreMeta(newMeta(&metapb.TxnProtocolVersionRange{})))
	re.NotNil(emptyRange.GetMeta().GetTxnProtocolVersionRange())
	requireTxnProtocolVersionRange(re, emptyRange, &metapb.TxnProtocolVersionRange{})

	// A missing range clears the stored one without touching LastHeartbeat nor
	// the other fields which are not maintained by SetStoreMeta.
	cleared := updated.Clone(SetStoreMeta(newMeta(nil)))
	re.Nil(cleared.GetMeta().GetTxnProtocolVersionRange())
	re.Equal(heartbeat.UnixNano(), cleared.GetMeta().GetLastHeartbeat())
	re.Equal(updated.GetMeta().GetNodeState(), cleared.GetMeta().GetNodeState())
}

func BenchmarkStoreClone(b *testing.B) {
	meta := &metapb.Store{Id: 1,
		Address: "mock://tikv-1:1",
		Labels:  []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}}
	store := NewStoreInfo(meta)
	b.ResetTimer()
	for t := range b.N {
		store.Clone(SetLeaderCount(t))
	}
}

func TestRegionScore(t *testing.T) {
	re := require.New(t)
	stats := &pdpb.StoreStats{}
	stats.Capacity = 512 * units.MiB  // 512 MB
	stats.Available = 100 * units.MiB // 100 MB
	stats.UsedSize = 0

	store := NewStoreInfo(
		&metapb.Store{Id: 1},
		SetStoreStats(stats),
		SetRegionSize(1),
	)
	score := store.RegionScore("v1", 0.7, 0.9, 0)
	// Region score should never be NaN, or /store API would fail.
	re.False(math.IsNaN(score))
}

func TestLowSpaceRatio(t *testing.T) {
	re := require.New(t)
	store := NewStoreInfo(&metapb.Store{Id: 1})

	store.rawStats.Capacity = initialMinSpace << 4
	store.rawStats.Available = store.rawStats.Capacity >> 3

	re.False(store.IsLowSpace(0.8))
	store.regionCount = 101
	re.True(store.IsLowSpace(0.8))
	store.rawStats.Available = store.rawStats.Capacity >> 2
	re.False(store.IsLowSpace(0.8))
	store.rawStats.Capacity = 0
	re.False(store.IsLowSpace(0.8))
}

func TestLowSpaceScoreV2(t *testing.T) {
	re := require.New(t)
	testdata := []struct {
		bigger *StoreInfo
		small  *StoreInfo
		delta  int64
	}{
		{
			// store1 and store2 has same store available ratio and store1 less 50 GB
			bigger: newStoreInfoWithAvailable(1, 20*units.GiB, 100*units.GiB, 1.4),
			small:  newStoreInfoWithAvailable(2, 200*units.GiB, 1000*units.GiB, 1.4),
		},
		{
			// store1 and store2 has same available space and less than 50 GB
			bigger: newStoreInfoWithAvailable(1, 10*units.GiB, 1000*units.GiB, 1.4),
			small:  newStoreInfoWithAvailable(2, 10*units.GiB, 100*units.GiB, 1.4),
		},
		{
			// store1 and store2 has same available ratio less than 0.2
			bigger: newStoreInfoWithAvailable(1, 20*units.GiB, 1000*units.GiB, 1.4),
			small:  newStoreInfoWithAvailable(2, 10*units.GiB, 500*units.GiB, 1.4),
		},
		{
			// store1 and store2 has same available ratio
			// but the store1 ratio less than store2 ((50-10)/50=0.8<(200-100)/200=0.5)
			bigger: newStoreInfoWithAvailable(1, 10*units.GiB, 100*units.GiB, 1.4),
			small:  newStoreInfoWithAvailable(2, 100*units.GiB, 1000*units.GiB, 1.4),
		},
		{
			// store1 and store2 has same usedSize and capacity
			// but the bigger's amp is bigger
			bigger: newStoreInfoWithAvailable(1, 10*units.GiB, 100*units.GiB, 1.5),
			small:  newStoreInfoWithAvailable(2, 10*units.GiB, 100*units.GiB, 1.4),
		},
		{
			// store1 and store2 has same capacity and regionSize (40g)
			// but store1 has less available space size
			bigger: newStoreInfoWithAvailable(1, 60*units.GiB, 100*units.GiB, 1),
			small:  newStoreInfoWithAvailable(2, 80*units.GiB, 100*units.GiB, 2),
		},
		{
			// store1 and store2 has same capacity and store2 (40g) has twice usedSize than store1 (20g)
			// but store1 has higher amp, so store1(60g) has more regionSize (40g)
			bigger: newStoreInfoWithAvailable(1, 80*units.GiB, 100*units.GiB, 3),
			small:  newStoreInfoWithAvailable(2, 60*units.GiB, 100*units.GiB, 1),
		},
		{
			// store1's capacity is less than store2's capacity, but store2 has more available space,
			bigger: newStoreInfoWithAvailable(1, 2*units.GiB, 100*units.GiB, 3),
			small:  newStoreInfoWithAvailable(2, 100*units.GiB, 10*1000*units.GiB, 3),
		},
		{
			// store2 has extra file size (70GB), it can balance region from store1 to store2.
			// See https://github.com/tikv/pd/issues/5790
			small:  newStoreInfoWithDisk(1, 400*units.MiB, 6930*units.GiB, 7000*units.GiB, 400),
			bigger: newStoreInfoWithAvailable(2, 1500*units.GiB, 7000*units.GiB, 1.32),
			delta:  37794,
		},
	}
	for _, v := range testdata {
		score1 := v.bigger.regionScoreV2(-v.delta, 0.8)
		score2 := v.small.regionScoreV2(v.delta, 0.8)
		re.Greater(score1, score2)
	}
}

// TestNewStore tests the new store in big cluster
// See https://github.com/tikv/pd/issues/9145
func TestNewStore(t *testing.T) {
	// The amp of the new store is small
	small := newStoreInfoWithAvailable(1, 2900*units.GiB, 3000*units.GiB, 0.03)
	bigger := newStoreInfoWithAvailable(2, 2000*units.GiB, 3000*units.GiB, 3)
	delta := int64(80000)
	score1 := small.regionScoreV2(delta, 0.8)
	score2 := bigger.regionScoreV2(-delta, 0.8)
	require.Less(t, score1, score2)

	// The amp of the new store is recovered
	small = newStoreInfoWithAvailable(1, 2000*units.GiB, 3000*units.GiB, 3)
	score1 = small.regionScoreV2(delta, 0.8)
	require.Greater(t, score1, score2)
}

// newStoreInfoWithAvailable is created with available and capacity
func newStoreInfoWithAvailable(id, available, capacity uint64, amp float64) *StoreInfo {
	stats := &pdpb.StoreStats{}
	stats.Capacity = capacity
	stats.Available = available
	usedSize := capacity - available
	stats.UsedSize = usedSize
	regionSize := (float64(usedSize) * amp) / units.MiB
	store := NewStoreInfo(
		&metapb.Store{
			Id: id,
		},
		SetStoreStats(stats),
		SetRegionCount(int(regionSize/96)),
		SetRegionSize(int64(regionSize)),
	)
	return store
}

// newStoreInfoWithDisk is created with all disk infos.
func newStoreInfoWithDisk(id, used, available, capacity, regionSize uint64) *StoreInfo {
	stats := &pdpb.StoreStats{}
	stats.Capacity = capacity
	stats.Available = available
	stats.UsedSize = used
	store := NewStoreInfo(
		&metapb.Store{
			Id: id,
		},
		SetStoreStats(stats),
		SetRegionCount(int(regionSize/96)),
		SetRegionSize(int64(regionSize)),
	)
	return store
}

func TestPutStore(t *testing.T) {
	store := newStoreInfoWithAvailable(1, 20*units.GiB, 100*units.GiB, 1.4)
	storesInfo := NewStoresInfo()
	storesInfo.PutStore(store)
	re := require.New(t)
	re.Equal(store, storesInfo.GetStore(store.GetID()))

	opts := []StoreCreateOption{SetStoreState(metapb.StoreState_Up)}
	store = store.Clone(opts...)
	re.NotEqual(store, storesInfo.GetStore(store.GetID()))
	storesInfo.PutStore(store, opts...)
	re.Equal(store, storesInfo.GetStore(store.GetID()))

	opts = []StoreCreateOption{
		SetStoreStats(&pdpb.StoreStats{
			Capacity:  100 * units.GiB,
			Available: 20 * units.GiB,
			UsedSize:  80 * units.GiB,
		}),
		SetLastHeartbeatTS(time.Now()),
	}
	store = store.Clone(opts...)
	re.NotEqual(store, storesInfo.GetStore(store.GetID()))
	storesInfo.PutStore(store, opts...)
	re.Equal(store, storesInfo.GetStore(store.GetID()))
}

func TestStoreInfoIsTiFlash(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		name                   string
		labels                 map[string]string
		expectedTiKV           bool
		expectedTiFlashWrite   bool
		expectedTiFlashCompute bool
	}{
		{
			name:                   "TiKV node without engine label",
			labels:                 map[string]string{},
			expectedTiKV:           true,
			expectedTiFlashWrite:   false,
			expectedTiFlashCompute: false,
		},
		{
			name: "TiKV node with engine label",
			labels: map[string]string{
				EngineKey: EngineTiKV,
			},
			expectedTiKV:           true,
			expectedTiFlashWrite:   false,
			expectedTiFlashCompute: false,
		},
		{
			name: "TiFlash classic node or write node",
			labels: map[string]string{
				EngineKey: EngineTiFlash,
			},
			expectedTiKV:           false,
			expectedTiFlashWrite:   true,
			expectedTiFlashCompute: false,
		},
		{
			name: "TiFlash compute node",
			labels: map[string]string{
				EngineKey: EngineTiFlashCompute,
			},
			expectedTiKV:           false,
			expectedTiFlashWrite:   false,
			expectedTiFlashCompute: true,
		},
		{
			name: "TiFlash node with additional labels",
			labels: map[string]string{
				EngineKey: EngineTiFlash,
				"zone":    "zone1",
				"rack":    "rack1",
				"host":    "host1",
			},
			expectedTiKV:           false,
			expectedTiFlashWrite:   true,
			expectedTiFlashCompute: false,
		},
		{
			name: "TiFlash compute node with additional labels",
			labels: map[string]string{
				EngineKey: EngineTiFlashCompute,
				"zone":    "zone2",
				"rack":    "rack2",
				"host":    "host2",
			},
			expectedTiKV:           false,
			expectedTiFlashWrite:   false,
			expectedTiFlashCompute: true,
		},
		{
			name: "Store with wrong engine value",
			labels: map[string]string{
				EngineKey: "unknown_engine",
			},
			expectedTiKV:           false,
			expectedTiFlashWrite:   false,
			expectedTiFlashCompute: false,
		},
		{
			name: "Store with case sensitive engine label",
			labels: map[string]string{
				EngineKey: "TiFlash", // uppercase, should not match
			},
			expectedTiKV:           false,
			expectedTiFlashWrite:   false,
			expectedTiFlashCompute: false,
		},
		{
			name: "Store with empty engine value",
			labels: map[string]string{
				EngineKey: "",
			},
			expectedTiKV:           true,
			expectedTiFlashWrite:   false,
			expectedTiFlashCompute: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(*testing.T) {
			store := NewStoreInfoWithLabel(1, tc.labels)
			result := store.IsTiKV()
			re.Equal(tc.expectedTiKV, result, "Expected to return %v for test case: %s", tc.expectedTiKV, tc.name)
			result = store.IsTiFlashWrite()
			re.Equal(tc.expectedTiFlashWrite, result, "Expected to return %v for test case: %s", tc.expectedTiFlashWrite, tc.name)
			result = store.IsTiFlashCompute()
			re.Equal(tc.expectedTiFlashCompute, result, "Expected to return %v for test case: %s", tc.expectedTiFlashCompute, tc.name)
		})
	}
}

func TestGetAvgNetworkSlowScore(t *testing.T) {
	re := require.New(t)

	type args struct {
		scores map[uint64]map[uint64]uint64 // storeID -> networkSlowScores
	}
	type want struct {
		storeID uint64
		avg     uint64
	}
	testCases := []struct {
		name string
		args args
		want []want // Support multiple storeID checks in one test case
	}{
		{
			name: "Store not found",
			args: args{
				scores: map[uint64]map[uint64]uint64{},
			},
			want: []want{
				{storeID: 1, avg: 0},
				{storeID: 123, avg: 0},
			},
		},
		{
			name: "Store exists, but no network slow scores",
			args: args{
				scores: map[uint64]map[uint64]uint64{
					1: {},
				},
			},
			want: []want{
				{storeID: 1, avg: 0}, // Only one store, no other stores to calculate avg with
			},
		},
		{
			name: "Multiple stores with various scores",
			args: args{
				scores: map[uint64]map[uint64]uint64{
					1: {2: 7}, // store1's scores to other stores (excluding itself)
					2: {1: 5}, // store2's scores to other stores (excluding itself)
				},
			},
			want: []want{
				{storeID: 1, avg: 7}, // store1 excludes itself, only counts store2, networkSlowScores[2]=7
				{storeID: 2, avg: 5}, // store2 excludes itself, only counts store1, networkSlowScores[1]=5
			},
		},
		{
			name: "Multiple stores with missing scores (should default to 1)",
			args: args{
				scores: map[uint64]map[uint64]uint64{
					1: {2: 7},  // store1's scores (excluding itself)
					2: {1: 5},  // store2's scores (excluding itself)
					3: {1: 10}, // store3's scores, only has score for store1 (excluding itself)
				},
			},
			want: []want{
				{storeID: 1, avg: 4}, // store1 excludes itself, counts store2(7) and store3(default 1), avg=(7+1)/2=4
				{storeID: 2, avg: 3}, // store2 excludes itself, counts store1(5) and store3(default 1), avg=(5+1)/2=3
				{storeID: 3, avg: 5}, // store3 excludes itself, counts store1(10) and store2(default 1), avg=(10+1)/2=5
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(*testing.T) {
			storesInfo := NewStoresInfo()
			for id, scores := range tc.args.scores {
				store := NewStoreInfo(&metapb.Store{Id: id})
				store.rawStats.NetworkSlowScores = scores
				storesInfo.PutStore(store)
			}
			for _, w := range tc.want {
				re.Equal(w.avg, storesInfo.GetAvgNetworkSlowScore(w.storeID))
			}
		})
	}
}
