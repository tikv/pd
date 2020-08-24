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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"math/rand"

	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("random-merge", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newRandomMergeScheduler(opController), nil
	})
}

const randomMergeName = "random-merge-scheduler"

type randomMergeScheduler struct {
	name string
	*baseScheduler
	selector *schedule.RandomSelector
}

// newRandomMergeScheduler creates an admin scheduler that randomly picks two adjacent regions
// then merges them.
func newRandomMergeScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.StoreStateFilter{ActionScope: randomMergeName, MoveRegion: true},
	}
	base := newBaseScheduler(opController)
	return &randomMergeScheduler{
		name:          randomMergeName,
		baseScheduler: base,
		selector:      schedule.NewRandomSelector(filters),
	}
}

func (s *randomMergeScheduler) GetName() string {
	return s.name
}

func (s *randomMergeScheduler) GetType() string {
	return "random-merge"
}

func (s *randomMergeScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpMerge) < cluster.GetMergeScheduleLimit()
}

func (s *randomMergeScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	stores := cluster.GetStores()
	store := s.selector.SelectSource(cluster, stores)
	if store == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_store").Inc()
		return nil
	}
	region := cluster.RandLeaderRegion(store.GetID(), core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
		return nil
	}

	other, target := cluster.GetAdjacentRegions(region)
	if !cluster.GetEnableOneWayMerge() && ((rand.Int()%2 == 0 && other != nil) || target == nil) {
		target = other
	}
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_adjacent").Inc()
		return nil
	}

	ops, err := schedule.CreateMergeRegionOperator("random-merge", cluster, region, target, schedule.OpAdmin)
	if err != nil {
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return ops
}
