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

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("shuffleRegion", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newShuffleRegionScheduler(opt), nil
	})
}

type shuffleRegionScheduler struct {
	opt      schedule.Options
	selector schedule.Selector
}

// newShuffleRegionScheduler creates an admin scheduler that shuffles regions
// between stores.
func newShuffleRegionScheduler(opt schedule.Options) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}

	return &shuffleRegionScheduler{
		opt:      opt,
		selector: schedule.NewRandomSelector(filters),
	}
}

func (s *shuffleRegionScheduler) GetName() string {
	return "shuffle-region-scheduler"
}

func (s *shuffleRegionScheduler) GetInterval() time.Duration {
	return schedule.MinScheduleInterval
}

func (s *shuffleRegionScheduler) GetResourceKind() core.ResourceKind {
	return core.RegionKind
}

func (s *shuffleRegionScheduler) GetResourceLimit() uint64 {
	return s.opt.GetRegionScheduleLimit()
}

func (s *shuffleRegionScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (s *shuffleRegionScheduler) Cleanup(cluster schedule.Cluster) {}

func (s *shuffleRegionScheduler) Schedule(cluster schedule.Cluster) *schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region, oldPeer := scheduleRemovePeer(cluster, s.GetName(), s.selector)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
		return nil
	}

	excludedFilter := schedule.NewExcludedFilter(nil, region.GetStoreIds())
	newPeer := scheduleAddPeer(cluster, s.selector, excludedFilter)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_new_peer").Inc()
		return nil
	}

	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return schedule.CreateMovePeerOperator("shuffleRegion", region, core.RegionKind, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
}
