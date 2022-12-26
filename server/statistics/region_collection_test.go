// Copyright 2018 TiKV Project Authors.
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

package statistics

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/core"
)

func TestRegionLabelIsolationLevel(t *testing.T) {
	re := require.New(t)
	locationLabels := []string{"zone", "rack", "host"}
	labelLevelStats := NewLabelStatistics()
	labelsSet := [][]map[string]string{
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r1", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by host when location labels is ["zone", "rack", "host"]
			// cannot be isolated when location labels is ["zone", "rack"]
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by zone
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z3", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r3", "host": "h3"},
		},
		{
			// cannot be isolated
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
		},
		{
			// isolated by rack
			{"rack": "r1", "host": "h1"},
			{"rack": "r2", "host": "h2"},
			{"rack": "r3", "host": "h3"},
		},
		{
			// isolated by host
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "host": "h3"},
		},
	}
	res := []string{"rack", "host", "zone", "rack", "none", "rack", "host"}
	counter := map[string]int{"none": 1, "host": 2, "rack": 3, "zone": 1}
	regionID := 1
	f := func(labels []map[string]string, res string, locationLabels []string) {
		metaStores := []*metapb.Store{
			{Id: 1, Address: "mock://tikv-1"},
			{Id: 2, Address: "mock://tikv-2"},
			{Id: 3, Address: "mock://tikv-3"},
		}
		stores := make([]*core.StoreInfo, 0, len(labels))
		for i, m := range metaStores {
			var newLabels []*metapb.StoreLabel
			for k, v := range labels[i] {
				newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
			}
			s := core.NewStoreInfo(m, core.SetStoreLabels(newLabels))

			stores = append(stores, s)
		}
		region := core.NewRegionInfo(&metapb.Region{Id: uint64(regionID)}, nil)
		label := GetRegionLabelIsolation(stores, locationLabels)
		labelLevelStats.Observe(region, stores, locationLabels)
		re.Equal(res, label)
		regionID++
	}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		re.Equal(res, labelLevelStats.labelCounter[i])
	}

	label := GetRegionLabelIsolation(nil, locationLabels)
	re.Equal(nonIsolation, label)
	label = GetRegionLabelIsolation(nil, nil)
	re.Equal(nonIsolation, label)
	store := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "mock://tikv-1"}, core.SetStoreLabels([]*metapb.StoreLabel{{Key: "foo", Value: "bar"}}))
	label = GetRegionLabelIsolation([]*core.StoreInfo{store}, locationLabels)
	re.Equal("zone", label)

	regionID = 1
	res = []string{"rack", "none", "zone", "rack", "none", "rack", "none"}
	counter = map[string]int{"none": 3, "host": 0, "rack": 3, "zone": 1}
	locationLabels = []string{"zone", "rack"}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		re.Equal(res, labelLevelStats.labelCounter[i])
	}
}
