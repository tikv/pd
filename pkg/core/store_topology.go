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

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/slice"
)

// GetStoreTopoWeight calculates the topology weight of a store based on its labels and the labels of other stores.
func GetStoreTopoWeight(store *StoreInfo, stores []*StoreInfo, locationLabels []string, count int) float64 {
	topology := getTopology()
	defer putTopology(topology)
	validLabels, sameLocationStoreNum, isMatch := buildTopology(topology, store, stores, locationLabels, count)
	weight := 1.0
	topo := topology
	if isMatch {
		return weight / float64(count) / sameLocationStoreNum
	}

	sortedLabels := getSortedLabels(store.GetLabels(), locationLabels)
	for _, label := range sortedLabels.pairs {
		if _, ok := topo[label.Value]; ok {
			if slice.Contains(validLabels, label.Key) {
				weight /= float64(len(topo))
			}
			topo = topo[label.Value].(map[string]any)
		} else {
			break
		}
	}
	putSortedLabels(sortedLabels)

	return weight / sameLocationStoreNum
}

var (
	storeLabelPool = sync.Pool{
		New: func() any {
			return &metapb.StoreLabel{}
		},
	}
)

// LabelPairs is pre-allocated buffer for sorting labels.
type LabelPairs struct {
	pairs        []*metapb.StoreLabel
	pooledLabels []*metapb.StoreLabel
}

var labelPairsPool = sync.Pool{
	New: func() any {
		return &LabelPairs{
			pairs: make([]*metapb.StoreLabel, 0, 8), // pre-allocate common size
		}
	},
}

// getSortedLabels returns sorted store labels. Missing labels are borrowed from the pool.
func getSortedLabels(storeLabels []*metapb.StoreLabel, locationLabels []string) *LabelPairs {
	labelPairs := labelPairsPool.Get().(*LabelPairs)

	if cap(labelPairs.pairs) < len(locationLabels) {
		labelPairs.pairs = make([]*metapb.StoreLabel, 0, len(locationLabels))
	}
	labelPairs.pairs = labelPairs.pairs[:0]
	labelPairs.pooledLabels = labelPairs.pooledLabels[:0]

	for _, ll := range locationLabels {
		find := false
		for _, sl := range storeLabels {
			if ll == sl.Key {
				labelPairs.pairs = append(labelPairs.pairs, sl)
				find = true
				break
			}
		}

		if !find {
			label := storeLabelPool.Get().(*metapb.StoreLabel)
			label.Key = ll
			label.Value = ""
			labelPairs.pairs = append(labelPairs.pairs, label)
			labelPairs.pooledLabels = append(labelPairs.pooledLabels, label)
		}
	}

	return labelPairs
}

// putSortedLabels puts the label pairs back to pool.
func putSortedLabels(pairs *LabelPairs) {
	for _, label := range pairs.pooledLabels {
		label.Key = ""
		label.Value = ""
		storeLabelPool.Put(label)
	}

	pairs.pairs = pairs.pairs[:0]
	pairs.pooledLabels = pairs.pooledLabels[:0]

	labelPairsPool.Put(pairs)
}

var (
	// Object pool for store topology.
	topologyPool = sync.Pool{
		New: func() any {
			return make(map[string]any, 8)
		},
	}

	// Object pool for label counter.
	labelCountPool = sync.Pool{
		New: func() any {
			labelCount := make([]int, 16)
			return &labelCount
		},
	}
)

// buildTopology builds the store topology graph and returns:
// - validLabels: filtered valid location labels
// - sameLocationStoreNum: number of stores in the same location
// - isMatch: whether the location matches exactly
func buildTopology(topology map[string]any, s *StoreInfo, stores []*StoreInfo, locationLabels []string, count int) ([]string, float64, bool) {
	labelCount := labelCountPool.Get().(*[]int)
	defer labelCountPool.Put(labelCount)

	if cap(*labelCount) < len(locationLabels) {
		*labelCount = make([]int, len(locationLabels))
	} else {
		*labelCount = (*labelCount)[:len(locationLabels)]
	}
	for i := range *labelCount {
		(*labelCount)[i] = 0
	}

	sameLocationStoreNum := 1.0

	for _, store := range stores {
		if store.IsServing() || store.IsPreparing() {
			sortedLabels := getSortedLabels(store.GetLabels(), locationLabels)
			updateTopology(topology, sortedLabels, (*labelCount))
			putSortedLabels(sortedLabels)
		}
	}

	validLabels := locationLabels
	var isMatch bool
	for i, c := range *labelCount {
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

	return validLabels, sameLocationStoreNum, isMatch
}

// updateTopology records stores' topology in the `topology` variable.
func updateTopology(topology map[string]any, sortedLabels *LabelPairs, labelCount []int) {
	if sortedLabels == nil || len(sortedLabels.pairs) == 0 {
		return
	}

	topo := topology
	for i, l := range sortedLabels.pairs {
		if _, exist := topo[l.Value]; !exist {
			m := getTopology()
			topo[l.Value] = m
			labelCount[i]++
		}
		topo = topo[l.Value].(map[string]any)
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
