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
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/schedule/selector"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// ShuffleLeaderName is shuffle leader scheduler name.
	ShuffleLeaderName = "shuffle-leader-scheduler"
	// ShuffleLeaderType is shuffle leader scheduler type.
	ShuffleLeaderType = "shuffle-leader"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleLeaderType, func(args []string) schedule.ConfigDecoder {
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
			conf.Name = ShuffleLeaderName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleLeaderType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleLeaderScheduler(opController, conf), nil
	})
}

type shuffleLeaderSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type shuffleLeaderScheduler struct {
	*BaseScheduler
	conf     *shuffleLeaderSchedulerConfig
	selector *selector.RandomSelector
}

// newShuffleLeaderScheduler creates an admin scheduler that shuffles leaders
// between stores.
func newShuffleLeaderScheduler(opController *schedule.OperatorController, conf *shuffleLeaderSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: conf.Name, TransferLeader: true},
		filter.NewSpecialUseFilter(conf.Name),
	}
	base := NewBaseScheduler(opController)
	return &shuffleLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleLeaderScheduler) GetType() string {
	return ShuffleLeaderType
}

func (s *shuffleLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
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
	region := cluster.RandFollowerRegion(targetStore.GetID(), s.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
		return nil
	}
	op, err := operator.CreateTransferLeaderOperator(ShuffleLeaderType, cluster, region, region.GetLeader().GetId(), targetStore.GetID(), operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create shuffle leader operator", zap.Error(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	return []*operator.Operator{op}
}
