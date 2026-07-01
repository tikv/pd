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
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/slice"
)

// GetStoreTopoWeight calculates the topology weight of a store based on its labels and the labels of other stores.
func GetStoreTopoWeight(store *StoreInfo, stores []*StoreInfo, locationLabels []string, count int) float64 {
	topology, validLabels, sameLocationStoreNum, isMatch := buildTopology(store, stores, locationLabels, count)
	weight := 1.0
	topo := topology
	if isMatch {
		return weight / float64(count) / sameLocationStoreNum
	}

	storeLabels := getSortedLabels(store.GetLabels(), locationLabels)
	for _, label := range storeLabels {
		if _, ok := topo[label.Value]; ok {
			if slice.Contains(validLabels, label.Key) {
				weight /= float64(len(topo))
			}
			topo = topo[label.Value].(map[string]any)
		} else {
			break
		}
	}

	return weight / sameLocationStoreNum
}

func buildTopology(s *StoreInfo, stores []*StoreInfo, locationLabels []string, count int) (map[string]any, []string, float64, bool) {
	topology := make(map[string]any)
	sameLocationStoreNum := 1.0
	totalLabelCount := make([]int, len(locationLabels))
	for _, store := range stores {
		if store.IsServing() || store.IsPreparing() {
			labelCount := updateTopology(topology, getSortedLabels(store.GetLabels(), locationLabels))
			for i, c := range labelCount {
				totalLabelCount[i] += c
			}
		}
	}

	validLabels := locationLabels
	var isMatch bool
	for i, c := range totalLabelCount {
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

func getSortedLabels(storeLabels []*metapb.StoreLabel, locationLabels []string) []*metapb.StoreLabel {
	var sortedLabels []*metapb.StoreLabel
	for _, ll := range locationLabels {
		find := false
		for _, sl := range storeLabels {
			if ll == sl.Key {
				sortedLabels = append(sortedLabels, sl)
				find = true
				break
			}
		}
		// TODO: we need to improve this logic to make the label calculation more accurate if the user has the wrong label settings.
		if !find {
			sortedLabels = append(sortedLabels, &metapb.StoreLabel{Key: ll, Value: ""})
		}
	}
	return sortedLabels
}

// updateTopology records stores' topology in the `topology` variable.
func updateTopology(topology map[string]any, sortedLabels []*metapb.StoreLabel) []int {
	labelCount := make([]int, len(sortedLabels))
	if len(sortedLabels) == 0 {
		return labelCount
	}
	topo := topology
	for i, l := range sortedLabels {
		if _, exist := topo[l.Value]; !exist {
			topo[l.Value] = make(map[string]any)
			labelCount[i] += 1
		}
		topo = topo[l.Value].(map[string]any)
	}
	return labelCount
}
