// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"bytes"
	"time"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

const (
	scanLimit                    = 1000
	adjacentResourceLimit        = 6
	minAdjacentSchedulerInterval = time.Second
	maxAdjacentSchedulerInterval = 20 * time.Second
)

func init() {
	schedule.RegisterScheduler("adjacent-region", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newBalanceAdjacentRegionScheduler(opt), nil
	})
}

type balanceAdjacentRegionScheduler struct {
	opt      schedule.Options
	limit    uint64
	selector schedule.Selector
	lastKey  []byte
	ids      []uint64
}

// newBalanceAdjacentRegionScheduler creates a scheduler that tends to disperse adjacent region
// on each store.
func newBalanceAdjacentRegionScheduler(opt schedule.Options) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
		schedule.NewSnapshotCountFilter(opt),
		schedule.NewStorageThresholdFilter(opt),
	}
	return &balanceAdjacentRegionScheduler{
		opt:      opt,
		limit:    adjacentResourceLimit,
		selector: schedule.NewRandomSelector(filters),
		lastKey:  []byte(""),
	}

}

func (l *balanceAdjacentRegionScheduler) GetName() string {
	return "balance-adjacent-region-scheduler"
}

func (l *balanceAdjacentRegionScheduler) GetMinInterval() time.Duration {
	return minAdjacentSchedulerInterval
}

func (l *balanceAdjacentRegionScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, maxAdjacentSchedulerInterval, linearGrowth)
}

func (l *balanceAdjacentRegionScheduler) GetResourceKind() core.ResourceKind {
	return core.AdjacentKind
}

func (l *balanceAdjacentRegionScheduler) GetType() string {
	return "adjacent-region"
}

func (l *balanceAdjacentRegionScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.opt.GetRegionScheduleLimit())
}

func (l *balanceAdjacentRegionScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (l *balanceAdjacentRegionScheduler) Cleanup(cluster schedule.Cluster) {}

func (l *balanceAdjacentRegionScheduler) Schedule(cluster schedule.Cluster) *schedule.Operator {
	if l.ids == nil {
		l.ids = make([]uint64, 0, len(cluster.GetStores()))
	}

	regions := cluster.ScanRegions(l.lastKey, scanLimit)
	adjacentRegions := make([]*core.RegionInfo, 0, scanLimit)
	for _, r := range regions {
		l.lastKey = r.StartKey
		if len(adjacentRegions) == 0 {
			adjacentRegions = append(adjacentRegions, r)
			continue
		}

		// append if the region are adjacent
		lastRegion := adjacentRegions[len(adjacentRegions)-1]
		if lastRegion.Leader.GetStoreId() == r.Leader.GetStoreId() && bytes.Equal(lastRegion.EndKey, r.StartKey) {
			adjacentRegions = append(adjacentRegions, r)
			continue
		}

		if len(adjacentRegions) == 1 {
			adjacentRegions[0] = r
		} else {
			// got adjacent regions
			break
		}
	}

	// scan to the end
	if len(regions) <= 1 {
		l.lastKey = []byte("")
	}

	if len(adjacentRegions) >= 2 {
		// There is no more continuous adjacent region with last key
		if adjacentRegions[0].GetId() != regions[0].GetId() {
			l.ids = l.ids[:0]
		}
		r1 := adjacentRegions[0]
		r2 := adjacentRegions[1]
		op := l.disperseLeader(cluster, r1, r2)
		if op == nil {
			op = l.dispersePeer(cluster, r1)
		}
		l.lastKey = r2.StartKey
		return op
	}
	l.ids = l.ids[:0]
	return nil
}

func (l *balanceAdjacentRegionScheduler) disperseLeader(cluster schedule.Cluster, before *core.RegionInfo, after *core.RegionInfo) *schedule.Operator {
	diffPeers := before.GetDiffFollowers(after)
	if len(diffPeers) == 0 {
		return nil
	}
	storesInfo := make([]*core.StoreInfo, 0, len(diffPeers))
	for _, p := range diffPeers {
		storesInfo = append(storesInfo, cluster.GetStore(p.GetStoreId()))
	}
	target := l.selector.SelectTarget(storesInfo)
	if target == nil {
		return nil
	}
	step := schedule.TransferLeader{FromStore: before.Leader.GetStoreId(), ToStore: target.GetId()}
	return schedule.NewOperator("balance-adjacent-leader", before.GetId(), core.AdjacentKind, step)
}

func (l *balanceAdjacentRegionScheduler) dispersePeer(cluster schedule.Cluster, region *core.RegionInfo) *schedule.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	leaderStoreID := region.Leader.GetStoreId()
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(leaderStoreID)
	scoreGuard := schedule.NewDistinctScoreFilter(l.opt.GetLocationLabels(), stores, source)
	excludeStores := region.GetStoreIds()
	for _, storeID := range l.ids {
		if _, ok := excludeStores[storeID]; !ok {
			excludeStores[storeID] = struct{}{}
		}
	}

	filters := []schedule.Filter{
		schedule.NewExcludedFilter(nil, excludeStores),
		scoreGuard,
	}
	target := l.selector.SelectTarget(cluster.GetStores(), filters...)
	if target == nil {
		return nil
	}
	newPeer, err := cluster.AllocPeer(target.GetId())
	if err != nil {
		return nil
	}
	if newPeer == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_peer").Inc()
		return nil
	}

	// record the store id and exclude it in next time
	l.ids = append(l.ids, newPeer.GetStoreId())

	return schedule.CreateMovePeerOperator("balance-adjacent-peer", region, core.AdjacentKind, leaderStoreID, newPeer.GetStoreId(), newPeer.GetId())
}
