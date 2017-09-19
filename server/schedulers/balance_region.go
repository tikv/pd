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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("balanceRegion", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opt), nil
	})
}

const storeCacheInterval = 30 * time.Second

type balanceRegionScheduler struct {
	opt      schedule.Options
	cache    *cache.TTLUint64
	limit    uint64
	selector schedule.Selector
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opt schedule.Options) schedule.Scheduler {
	ttlCache := cache.NewIDTTL(storeCacheInterval, 4*storeCacheInterval)
	filters := []schedule.Filter{
		schedule.NewCacheFilter(ttlCache),
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
		schedule.NewSnapshotCountFilter(opt),
		schedule.NewStorageThresholdFilter(opt),
	}

	return &balanceRegionScheduler{
		opt:      opt,
		cache:    ttlCache,
		limit:    1,
		selector: schedule.NewBalanceSelector(core.RegionKind, filters),
	}
}

func (s *balanceRegionScheduler) GetName() string {
	return "balance-region-scheduler"
}

func (s *balanceRegionScheduler) GetInterval() time.Duration {
	return schedule.MinScheduleInterval
}

func (s *balanceRegionScheduler) GetResourceKind() core.ResourceKind {
	return core.RegionKind
}

func (s *balanceRegionScheduler) GetResourceLimit() uint64 {
	return minUint64(s.limit, s.opt.GetRegionScheduleLimit())
}

func (s *balanceRegionScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (s *balanceRegionScheduler) Cleanup(cluster schedule.Cluster) {}

func (s *balanceRegionScheduler) Schedule(cluster schedule.Cluster) *schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	// Select a peer from the store with most regions.
	region, oldPeer := scheduleRemovePeer(cluster, s.GetName(), s.selector)
	if region == nil {
		return nil
	}

	// We don't schedule region with abnormal number of replicas.
	if len(region.GetPeers()) != s.opt.GetMaxReplicas() {
		schedulerCounter.WithLabelValues(s.GetName(), "abnormal_replica").Inc()
		return nil
	}

	// Skip hot regions.
	if cluster.IsRegionHot(region.GetId()) {
		schedulerCounter.WithLabelValues(s.GetName(), "region_hot").Inc()
		return nil
	}

	op := s.transferPeer(cluster, region, oldPeer)
	if op == nil {
		// We can't transfer peer from this store now, so we add it to the cache
		// and skip it for a while.
		s.cache.Put(oldPeer.GetStoreId())
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return op
}

func (s *balanceRegionScheduler) transferPeer(cluster schedule.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer) *schedule.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(oldPeer.GetStoreId())
	scoreGuard := schedule.NewDistinctScoreFilter(s.opt.GetLocationLabels(), stores, source)

	checker := schedule.NewReplicaChecker(s.opt, cluster)
	newPeer := checker.SelectBestPeerToAddReplica(region, scoreGuard)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_peer").Inc()
		return nil
	}

	target := cluster.GetStore(newPeer.GetStoreId())
	if !shouldBalance(source, target, s.GetResourceKind()) {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	s.limit = adjustBalanceLimit(cluster, s.GetResourceKind())

	return schedule.CreateMovePeerOperator("balanceRegion", region, core.RegionKind, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
}

// GetCache returns interval id cache in the scheduler. This is for test only.
// TODO: remove it after moving tests into this directory.
func (s *balanceRegionScheduler) GetCache() *cache.TTLUint64 {
	return s.cache
}
