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

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pkg/errors"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("user-grant-leader", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("should specify the store-id")
			}

			conf, ok := v.(*grandLeaderConfig)
			if !ok {
				return errors.New("the config does not exist")
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.WithStack(err)
			}
			conf.StoreID = id
			conf.Name = fmt.Sprintf("user-grant-leader-scheduler-%d", id)
			return nil
		}
	})

	schedule.RegisterScheduler("user-grant-leader", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &grandLeaderConfig{}
		decoder(conf)
		return newGrantLeaderScheduler(opController, conf), nil
	})
}

// SchedulerType return type of the scheduler
func SchedulerType() string {
	return "user-grant-leader"
}

// SchedulerArgs provides parameters for the scheduler
func SchedulerArgs() []string {
	args := []string{"7"}
	return args
}

type grandLeaderConfig struct {
	Name    string `json:"name"`
	StoreID uint64 `json:"store-id"`
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*userBaseScheduler
	conf *grandLeaderConfig
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(opController *schedule.OperatorController, conf *grandLeaderConfig) schedule.Scheduler {
	base := newUserBaseScheduler(opController)
	return &grantLeaderScheduler{
		userBaseScheduler: base,
		conf:              conf,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *grantLeaderScheduler) GetType() string {
	return "user-grant-leader"
}

func (s *grantLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *grantLeaderScheduler) Prepare(cluster opt.Cluster) error {
	return cluster.BlockStore(s.conf.StoreID)
}

func (s *grantLeaderScheduler) Cleanup(cluster opt.Cluster) {
	cluster.UnblockStore(s.conf.StoreID)
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	region := cluster.RandFollowerRegion(s.conf.StoreID, core.HealthRegion())
	if region == nil {
		return nil
	}
	op := operator.CreateTransferLeaderOperator("user-grant-leader", region, region.GetLeader().GetStoreId(), s.conf.StoreID, operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
