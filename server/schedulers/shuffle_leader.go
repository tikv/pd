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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("shuffleLeader", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newShuffleLeaderScheduler(opt), nil
	})
}

type shuffleLeaderScheduler struct {
	opt      schedule.Options
	selector schedule.Selector
	selected *metapb.Peer
}

// newShuffleLeaderScheduler creates an admin scheduler that shuffles leaders
// between stores.
func newShuffleLeaderScheduler(opt schedule.Options) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}

	return &shuffleLeaderScheduler{
		opt:      opt,
		selector: schedule.NewRandomSelector(filters),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return "shuffle-leader-scheduler"
}

func (s *shuffleLeaderScheduler) GetInterval() time.Duration {
	return schedule.MinScheduleInterval
}

func (s *shuffleLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (s *shuffleLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *shuffleLeaderScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (s *shuffleLeaderScheduler) Cleanup(cluster schedule.Cluster) {}

func (s *shuffleLeaderScheduler) Schedule(cluster schedule.Cluster) *schedule.Operator {
	// We shuffle leaders between stores:
	// 1. select a store randomly.
	// 2. transfer a leader from the store to another store.
	// 3. transfer a leader to the store from another store.
	// These will not change store's leader count, but swap leaders between stores.

	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	// Select a store and transfer a leader from it.
	if s.selected == nil {
		region, newLeader := scheduleTransferLeader(cluster, s.GetName(), s.selector)
		if region == nil {
			return nil
		}
		// Mark the selected store.
		s.selected = region.Leader
		schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
		step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: newLeader.GetStoreId()}
		return schedule.NewOperator("shuffleLeader", region.GetId(), core.LeaderKind, step)
	}

	// Reset the selected store.
	storeID := s.selected.GetStoreId()
	s.selected = nil

	// Transfer a leader to the selected store.
	region := cluster.RandFollowerRegion(storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: storeID}
	return schedule.NewOperator("shuffleSelectedLeader", region.GetId(), core.LeaderKind, step)
}
