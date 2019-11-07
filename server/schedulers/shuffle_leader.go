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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/pkg/errors"
)

const shuffleLeaderName = "shuffle-leader-scheduler"

func init() {
	schedule.RegisterSliceDecoderBuilder("shuffle-leader", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleLeaderSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return errors.WithStack(err)
			}
			conf.Ranges = ranges
			conf.Name = shuffleLeaderName
			return nil
		}
	})

	schedule.RegisterScheduler("shuffle-leader", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleLeaderSchedulerConfig{}
		decoder(conf)
		return newShuffleLeaderScheduler(opController, conf), nil
	})
}

type shuffleLeaderSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type shuffleLeaderScheduler struct {
	*baseScheduler
	conf     *shuffleLeaderSchedulerConfig
	selector *selector.RandomSelector
}

// newShuffleLeaderScheduler creates an admin scheduler that shuffles leaders
// between stores.
func newShuffleLeaderScheduler(opController *schedule.OperatorController, conf *shuffleLeaderSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: conf.Name, TransferLeader: true},
	}
	base := newBaseScheduler(opController)
	return &shuffleLeaderScheduler{
		baseScheduler: base,
		conf:          conf,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleLeaderScheduler) GetType() string {
	return "shuffle-leader"
}

func (s *shuffleLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *shuffleLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	// We shuffle leaders between stores by:
	// 1. random select a valid store.
	// 2. transfer a leader to the store.
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	targetStore := s.selector.SelectTarget(cluster, stores)
	if targetStore == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
		return nil
	}
	region := cluster.RandFollowerRegion(targetStore.GetID(), s.conf.Ranges, core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	op := operator.CreateTransferLeaderOperator("shuffle-leader", region, region.GetLeader().GetId(), targetStore.GetID(), operator.OpAdmin)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
