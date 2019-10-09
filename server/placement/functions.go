// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"math"

	"github.com/pingcap/pd/server/core"
)

// Score calculates the score of the constraint expression. The greater score is
// better. A nagative score means the constraint is not satisfied.
func (c Constraint) Score(region *core.RegionInfo, cluster Cluster) int {
	v0, v := c.Value, c.eval(region, cluster)
	switch c.Op {
	case "=":
		return -int(math.Abs(float64(v - v0)))
	case "<=":
		return v0 - v
	case ">=":
		return v - v0
	case "<":
		return v0 - v - 1
	case ">":
		return v - v0 - 1
	}
	return -1
}

func (c Constraint) eval(region *core.RegionInfo, cluster Cluster) int {
	switch c.Function {
	case "count":
		return c.evalCount(region, cluster)
	case "label_values":
		return c.evalLabelValues(region, cluster)
	case "count_leader":
		return c.evalCountLeader(region, cluster)
	case "isolation_level":
		return c.evalIsolationLevel(region, cluster)
	}
	return 0
}

func (c Constraint) evalCount(region *core.RegionInfo, cluster Cluster) int {
	stores := c.filterStores(cluster.GetRegionStores(region))
	return len(stores)
}

func (c Constraint) evalLabelValues(region *core.RegionInfo, cluster Cluster) int {
	stores := c.filterStores(cluster.GetRegionStores(region))
	return c.countLabelValues(stores, c.Labels)
}

func (c Constraint) evalCountLeader(region *core.RegionInfo, cluster Cluster) int {
	leaderStore := cluster.GetStore(region.GetLeader().GetStoreId())
	if leaderStore != nil && c.matchStore(leaderStore) {
		return 1
	}
	return 0
}

func (c Constraint) evalIsolationLevel(region *core.RegionInfo, cluster Cluster) int {
	stores := c.filterStores(cluster.GetRegionStores(region))
	for i := range c.Labels {
		if c.countLabelValues(stores, c.Labels[:i+1]) == len(stores) {
			return len(c.Labels) - i
		}
	}
	return 0
}

func (c Constraint) filterStores(stores []*core.StoreInfo) []*core.StoreInfo {
	res := stores[:0]
	for _, s := range stores {
		if c.matchStore(s) {
			res = append(res, s)
		}
	}
	return res
}

func (c Constraint) matchStore(store *core.StoreInfo) bool {
	for _, f := range c.Filters {
		if store.GetLabelValue(f.Key) != f.Value {
			return false
		}
	}
	return true
}

func (c Constraint) labelValues(store *core.StoreInfo, labels []string) string {
	var str string
	for _, label := range labels {
		str = str + store.GetLabelValue(label) + "/" // Symbol '/' never be included in a label value.
	}
	return str
}

func (c Constraint) countLabelValues(stores []*core.StoreInfo, labels []string) int {
	if len(labels) == 0 {
		return 0
	}
	set := make(map[string]struct{})
	for _, s := range stores {
		set[c.labelValues(s, labels)] = struct{}{}
	}
	return len(set)
}
