// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"fmt"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/pkg/errors"
)

func init() {
	schedule.RegisterScheduler("user-evict-leader", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		if len(args) != 1 {
			return nil, errors.New("user-evict-leader needs 1 argument")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newEvictLeaderScheduler(opController, id), nil
	})
}

func SchedulerType() string {
	return "user-evict-leader"
}

func SchedulerArgs() []string {
	args := []string{"1"}
	return args
}

type evictLeaderScheduler struct {
	*userBaseScheduler
	name     string
	storeID  uint64
	selector *selector.RandomSelector
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *schedule.OperatorController, storeID uint64) schedule.Scheduler {
	name := fmt.Sprintf("user-evict-leader-scheduler-%d", storeID)
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: name, TransferLeader: true},
	}
	base := newUserBaseScheduler(opController)
	return &evictLeaderScheduler{
		userBaseScheduler: base,
		name:              name,
		storeID:           storeID,
		selector:          selector.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.name
}

func (s *evictLeaderScheduler) GetType() string {
	return "user-evict-leader"
}

func (s *evictLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return cluster.BlockStore(s.storeID)
}

func (s *evictLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	region := cluster.RandLeaderRegion(s.storeID, core.HealthRegion())
	if region == nil {
		log.Error("no leader")
		return nil
	}
	target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region))
	if target == nil {
		log.Error("no target store")
		return nil
	}
	op := operator.CreateTransferLeaderOperator("user-evict-leader", region, region.GetLeader().GetStoreId(), target.GetID(), operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
