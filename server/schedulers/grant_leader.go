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

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pkg/errors"
)

func init() {
	schedule.RegisterArgsToMapper("grant-leader", func(args []string) (schedule.ConfigMapper, error) {
		if len(args) != 1 {
			return nil, errors.New("should specify the store-id")
		}
		mapper := make(schedule.ConfigMapper)
		mapper["store-id"] = args[0]
		return mapper, nil
	})

	schedule.RegisterScheduler("grant-leader", func(opController *schedule.OperatorController, storage *core.Storage, mapper schedule.ConfigMapper) (schedule.Scheduler, error) {
		if len(mapper) != 1 {
			return nil, errors.New("grant-leader needs 1 argument")
		}
		id, err := strconv.ParseUint(mapper["store-id"].(string), 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		conf := &grandLeaderConf{
			name:    fmt.Sprintf("grant-leader-scheduler-%d", id),
			storeID: id,
		}
		storage.SaveScheduleConfig(conf.name, conf)
		return newGrantLeaderScheduler(opController, conf), nil
	})
}

type grandLeaderConf struct {
	name    string
	storeID uint64
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*baseScheduler
	conf *grandLeaderConf
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(opController *schedule.OperatorController, conf *grandLeaderConf) schedule.Scheduler {
	base := newBaseScheduler(opController)
	return &grantLeaderScheduler{
		baseScheduler: base,
		conf:          conf,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.conf.name
}

func (s *grantLeaderScheduler) GetType() string {
	return "grant-leader"
}
func (s *grantLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return cluster.BlockStore(s.conf.storeID)
}

func (s *grantLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.conf.storeID)
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandFollowerRegion(s.conf.storeID, core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	op := operator.CreateTransferLeaderOperator("grant-leader", region, region.GetLeader().GetStoreId(), s.conf.storeID, operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
