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

package core

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopologyWeight(t *testing.T) {
	re := require.New(t)

	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3", "h4"}

	var stores []*StoreInfo
	var testStore *StoreInfo
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
				if i == 0 && j == 0 && k == 0 {
					testStore = store
				}
				stores = append(stores, store)
			}
		}
	}

	re.Equal(1.0/3/3/4, GetStoreTopoWeight(testStore, stores, labels, 3))
}

func TestTopologyWeight1(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := NewStoreInfoWithLabel(2, map[string]string{"dc": "dc2", "zone": "zone2", "host": "host2"})
	store3 := NewStoreInfoWithLabel(3, map[string]string{"dc": "dc3", "zone": "zone3", "host": "host3"})
	store4 := NewStoreInfoWithLabel(4, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store5 := NewStoreInfoWithLabel(5, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host2"})
	store6 := NewStoreInfoWithLabel(6, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host3"})
	stores := []*StoreInfo{store1, store2, store3, store4, store5, store6}

	re.Equal(1.0/3, GetStoreTopoWeight(store2, stores, labels, 3))
	re.Equal(1.0/3/4, GetStoreTopoWeight(store1, stores, labels, 3))
	re.Equal(1.0/3/4, GetStoreTopoWeight(store6, stores, labels, 3))
}

func TestTopologyWeight2(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := NewStoreInfoWithLabel(2, map[string]string{"dc": "dc2"})
	store3 := NewStoreInfoWithLabel(3, map[string]string{"dc": "dc3"})
	store4 := NewStoreInfoWithLabel(4, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host1"})
	store5 := NewStoreInfoWithLabel(5, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host1"})
	stores := []*StoreInfo{store1, store2, store3, store4, store5}

	re.Equal(1.0/3, GetStoreTopoWeight(store2, stores, labels, 3))
	re.Equal(1.0/3/3, GetStoreTopoWeight(store1, stores, labels, 3))
}

func TestTopologyWeight3(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := NewStoreInfoWithLabel(2, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host2"})
	store3 := NewStoreInfoWithLabel(3, map[string]string{"dc": "dc1", "zone": "zone3", "host": "host3"})
	store4 := NewStoreInfoWithLabel(4, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host4"})
	store5 := NewStoreInfoWithLabel(5, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host5"})
	store6 := NewStoreInfoWithLabel(6, map[string]string{"dc": "dc2", "zone": "zone5", "host": "host6"})

	store7 := NewStoreInfoWithLabel(7, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host7"})
	store8 := NewStoreInfoWithLabel(8, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host8"})
	store9 := NewStoreInfoWithLabel(9, map[string]string{"dc": "dc2", "zone": "zone4", "host": "host9"})
	store10 := NewStoreInfoWithLabel(10, map[string]string{"dc": "dc2", "zone": "zone5", "host": "host10"})
	stores := []*StoreInfo{store1, store2, store3, store4, store5, store6, store7, store8, store9, store10}

	re.Equal(1.0/5/2, GetStoreTopoWeight(store7, stores, labels, 5))
	re.Equal(1.0/5/4, GetStoreTopoWeight(store8, stores, labels, 5))
	re.Equal(1.0/5/4, GetStoreTopoWeight(store9, stores, labels, 5))
	re.Equal(1.0/5/2, GetStoreTopoWeight(store10, stores, labels, 5))
}

func TestTopologyWeight4(t *testing.T) {
	re := require.New(t)

	labels := []string{"dc", "zone", "host"}
	store1 := NewStoreInfoWithLabel(1, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host1"})
	store2 := NewStoreInfoWithLabel(2, map[string]string{"dc": "dc1", "zone": "zone1", "host": "host2"})
	store3 := NewStoreInfoWithLabel(3, map[string]string{"dc": "dc1", "zone": "zone2", "host": "host3"})
	store4 := NewStoreInfoWithLabel(4, map[string]string{"dc": "dc2", "zone": "zone1", "host": "host4"})

	stores := []*StoreInfo{store1, store2, store3, store4}

	re.Equal(1.0/3/2, GetStoreTopoWeight(store1, stores, labels, 3))
	re.Equal(1.0/3, GetStoreTopoWeight(store3, stores, labels, 3))
	re.Equal(1.0/3, GetStoreTopoWeight(store4, stores, labels, 3))
}

func TestTopologyWeightWithPartialValidLabels(t *testing.T) {
	re := require.New(t)

	labels := []string{"zone", "rack"}
	store1 := NewStoreInfoWithLabel(1, map[string]string{"zone": "z1", "rack": "r1"})
	store2 := NewStoreInfoWithLabel(2, map[string]string{"zone": "z1", "rack": "r2"})
	store3 := NewStoreInfoWithLabel(3, map[string]string{"zone": "z2", "rack": "r3"})
	store4 := NewStoreInfoWithLabel(4, map[string]string{"zone": "z2", "rack": "r4"})
	stores := []*StoreInfo{store1, store2, store3, store4}

	re.Equal(1.0/2/2, GetStoreTopoWeight(store1, stores, labels, 3))
}

func TestTopologyWeightWithEmptyStoreLabelValue(t *testing.T) {
	re := require.New(t)

	store := NewStoreInfoWithLabel(1, map[string]string{"zone": ""})

	_ = GetStoreTopoWeight(store, []*StoreInfo{store}, []string{"zone"}, 3)

	re.Equal("zone", store.GetLabels()[0].Key)
}

func TestTopologyWeightWithManyLocationLabels(t *testing.T) {
	re := require.New(t)

	labels := make([]string, 17)
	storeLabels := make(map[string]string, len(labels))
	for i := range labels {
		labels[i] = fmt.Sprintf("label-%d", i)
		storeLabels[labels[i]] = fmt.Sprintf("value-%d", i)
	}
	store := NewStoreInfoWithLabel(1, storeLabels)

	re.NotPanics(func() {
		_ = GetStoreTopoWeight(store, []*StoreInfo{store}, labels, 3)
	})
}

// generateTestStores creates n stores with different labels for testing.
func generateTestStores(n int) []*StoreInfo {
	stores := make([]*StoreInfo, 0, n)
	for i := range n {
		store := NewStoreInfoWithLabel(uint64(i), map[string]string{
			"zone": "zone" + string(rune('a'+i%3)),
			"rack": "rack" + string(rune('a'+i%5)),
			"host": "host" + string(rune('a'+i%7)),
		})
		stores = append(stores, store)
	}
	return stores
}

func BenchmarkBuildTopology(b *testing.B) {
	testCases := []struct {
		name       string
		storeCount int
	}{
		{"Small-10-Stores", 10},
		{"Medium-100-Stores", 100},
		{"Large-1000-Stores", 1000},
	}

	locationLabels := []string{"zone", "rack", "host"}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stores := generateTestStores(tc.storeCount)
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				topology := getTopology()
				buildTopology(
					topology,
					stores[0],
					stores,
					locationLabels,
					3,
				)
				putTopology(topology)
			}
		})
	}
}

var benchmarkStoreTopoWeight float64

func BenchmarkGetStoreTopoWeight(b *testing.B) {
	testCases := []struct {
		name       string
		storeCount int
	}{
		{"Small-10-Stores", 10},
		{"Medium-100-Stores", 100},
		{"Large-1000-Stores", 1000},
	}

	locationLabels := []string{"zone", "rack", "host"}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stores := generateTestStores(tc.storeCount)
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				benchmarkStoreTopoWeight = GetStoreTopoWeight(
					stores[0],
					stores,
					locationLabels,
					4,
				)
			}
		})
	}
}
