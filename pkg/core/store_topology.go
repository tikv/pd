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
	"sync"

	"github.com/tikv/pd/pkg/slice"
)

// GetStoreTopoWeight calculates the topology weight of a store based on its labels and the labels of other stores.
func GetStoreTopoWeight(store *StoreInfo, stores []*StoreInfo, locationLabels []string, count int) float64 {
	topology, validLabels, sameLocationStoreNum, isMatch := buildTopology(store, stores, locationLabels, count)
	defer putTopology(topology)
	weight := 1.0
	topo := topology
	if isMatch {
		return weight / float64(count) / sameLocationStoreNum
	}

	for _, label := range locationLabels {
		value := getStoreLabelValue(store.GetMeta(), label)
		if _, ok := topo[value]; ok {
			if slice.Contains(validLabels, label) {
				weight /= float64(len(topo))
			}
			topo = topo[value].(map[string]any)
		} else {
			break
		}
	}

	return weight / sameLocationStoreNum
}

// Object pool for store topology.
var topologyPool = sync.Pool{
	New: func() any {
		return make(map[string]any, 8)
	},
}

// buildTopology builds the store topology graph and returns:
// - topology: pooled store topology map
// - validLabels: filtered valid location labels
// - sameLocationStoreNum: number of stores in the same location
// - isMatch: whether the location matches exactly
func buildTopology(s *StoreInfo, stores []*StoreInfo, locationLabels []string, count int) (map[string]any, []string, float64, bool) {
	topology := getTopology()
	var inlineLabelCount [16]int
	var labelCount []int
	if len(locationLabels) <= len(inlineLabelCount) {
		labelCount = inlineLabelCount[:len(locationLabels)]
	} else {
		labelCount = make([]int, len(locationLabels))
	}

	sameLocationStoreNum := 1.0

	for _, store := range stores {
		if store.IsServing() || store.IsPreparing() {
			updateTopology(topology, store, locationLabels, labelCount)
		}
	}

	validLabels := locationLabels
	var isMatch bool
	for i, c := range labelCount {
		if c == 0 {
			validLabels = validLabels[:i]
			break
		}
		if count/c == 0 {
			validLabels = validLabels[:i]
			break
		}
		if count/c == 1 && count%c == 0 {
			validLabels = validLabels[:i+1]
			isMatch = true
			break
		}
	}

	for _, store := range stores {
		if store.GetID() == s.GetID() {
			continue
		}
		if s.CompareLocation(store, validLabels) == -1 {
			sameLocationStoreNum++
		}
	}

	return topology, validLabels, sameLocationStoreNum, isMatch
}

// updateTopology records a store's topology in the `topology` variable.
func updateTopology(topology map[string]any, store *StoreInfo, locationLabels []string, labelCount []int) {
	if len(locationLabels) == 0 {
		return
	}

	topo := topology
	for i, label := range locationLabels {
		value := getStoreLabelValue(store.GetMeta(), label)
		if _, exist := topo[value]; !exist {
			m := getTopology()
			topo[value] = m
			labelCount[i]++
		}
		topo = topo[value].(map[string]any)
	}
}

func cleanTopology(topology map[string]any) {
	for k, v := range topology {
		if subTopo, ok := v.(map[string]any); ok {
			cleanTopology(subTopo)
			topologyPool.Put(subTopo)
		}
		delete(topology, k)
	}
}

func getTopology() map[string]any {
	topology := topologyPool.Get().(map[string]any)
	cleanTopology(topology)
	return topology
}

func putTopology(topology map[string]any) {
	cleanTopology(topology)
	topologyPool.Put(topology)
}
