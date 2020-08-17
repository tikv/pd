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
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("evict-leader", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		if len(args) != 1 {
			return nil, errors.New("evict-leader needs 1 argument")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newEvictLeaderScheduler(opController, id), nil
	})
}

type evictLeaderScheduler struct {
	*baseScheduler
	name     string
	storeID  uint64
	selector *schedule.RandomSelector
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *schedule.OperatorController, storeID uint64) schedule.Scheduler {
	name := fmt.Sprintf("evict-leader-scheduler-%d", storeID)
	filters := []schedule.Filter{
		schedule.StoreStateFilter{ActionScope: name, TransferLeader: true},
	}
	base := newBaseScheduler(opController)
	return &evictLeaderScheduler{
		baseScheduler: base,
		name:          name,
		storeID:       storeID,
		selector:      schedule.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.name
}

func (s *evictLeaderScheduler) GetType() string {
	return "evict-leader"
}

func (s *evictLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return cluster.BlockStore(s.storeID)
}

func (s *evictLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandLeaderRegion(s.storeID, core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_leader").Inc()
		return nil
	}
	target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region))
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_target_store").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	op := schedule.CreateTransferLeaderOperator("evict-leader", region, region.GetLeader().GetStoreId(), target.GetID(), schedule.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*schedule.Operator{op}
}
