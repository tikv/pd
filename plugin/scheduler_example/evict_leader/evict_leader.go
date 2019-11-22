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
	"net/url"
	"strconv"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/pkg/errors"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("user-evict-leader", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("should specify the store-id")
			}
			conf, ok := v.(*evictLeaderSchedulerConfig)
			if !ok {
				return errors.New("the config does not exist")
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.WithStack(err)
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return errors.WithStack(err)
			}
			name := fmt.Sprintf("user-evict-leader-scheduler-%d", id)
			conf.StoreID = id
			conf.Name = name
			conf.Ranges = ranges
			return nil

		}
	})

	schedule.RegisterScheduler("user-evict-leader", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{}
		decoder(conf)
		return newEvictLeaderScheduler(opController, conf), nil
	})
}

// SchedulerType return type of the scheduler
func SchedulerType() string {
	return "user-evict-leader"
}

// SchedulerArgs provides parameters for the scheduler
func SchedulerArgs() []string {
	args := []string{"1"}
	return args
}

type evictLeaderSchedulerConfig struct {
	Name    string          `json:"name"`
	StoreID uint64          `json:"store-id"`
	Ranges  []core.KeyRange `json:"ranges"`
}

type evictLeaderScheduler struct {
	*userBaseScheduler
	conf     *evictLeaderSchedulerConfig
	selector *selector.RandomSelector
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *schedule.OperatorController, conf *evictLeaderSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: conf.Name, TransferLeader: true},
	}

	base := newUserBaseScheduler(opController)
	return &evictLeaderScheduler{
		userBaseScheduler: base,
		conf:              conf,
		selector:          selector.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *evictLeaderScheduler) GetType() string {
	return "user-evict-leader"
}

func (s *evictLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *evictLeaderScheduler) Prepare(cluster opt.Cluster) error {
	return cluster.BlockStore(s.conf.StoreID)
}

func (s *evictLeaderScheduler) Cleanup(cluster opt.Cluster) {
	cluster.UnblockStore(s.conf.StoreID)
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	region := cluster.RandLeaderRegion(s.conf.StoreID, s.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		return nil
	}
	target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region))
	if target == nil {
		return nil
	}
	op := operator.CreateTransferLeaderOperator("user-evict-leader", region, region.GetLeader().GetStoreId(), target.GetID(), operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, err
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, err
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}
