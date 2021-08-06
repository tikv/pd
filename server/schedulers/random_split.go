// Copyright 2021 TiKV Project Authors.
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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

const (
	// RandomSplitName is random split scheduler name.
	RandomSplitName = "random-split-scheduler"
	// RandomSplitType is random split scheduler type.
	RandomSplitType = "random-split"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(RandomSplitType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*randomSplitSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = RandomSplitName
			return nil
		}
	})
	schedule.RegisterScheduler(RandomSplitType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &randomSplitSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newRandomSplitScheduler(opController, conf), nil
	})
}

type randomSplitSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type randomSplitScheduler struct {
	*BaseScheduler
	conf    *randomSplitSchedulerConfig
	filters []filter.Filter
}

// newRandomSplitScheduler creates an admin scheduler that randomly picks a region then splits it.
// Splitting a region is not a heavy operation. Its cost is similar to transferring a leader, so we use the
// same limit and config as shuffle-leader-scheduler.
func newRandomSplitScheduler(opController *schedule.OperatorController, conf *randomSplitSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: conf.Name, TransferLeader: true},
		filter.NewSpecialUseFilter(conf.Name),
	}
	base := NewBaseScheduler(opController)
	return &randomSplitScheduler{
		BaseScheduler: base,
		conf:          conf,
		filters:       filters,
	}
}

func (s *randomSplitScheduler) GetName() string {
	return s.conf.Name
}

func (s *randomSplitScheduler) GetType() string {
	return RandomSplitType
}

func (s *randomSplitScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *randomSplitScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpSplit) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpSplit.String()).Inc()
	}
	return allowed
}

func (s *randomSplitScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	store := filter.NewCandidates(cluster.GetStores()).
		FilterSource(cluster.GetOpts(), s.filters...).
		RandomPick()
	if store == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-store").Inc()
		return nil
	}
	region := cluster.RandLeaderRegion(store.GetID(), s.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil
	}

	if !s.allowSplit(cluster, region) {
		schedulerCounter.WithLabelValues(s.GetName(), "not-allowed").Inc()
		return nil
	}

	op, err := operator.CreateSplitRegionOperator(RandomSplitType, region, operator.OpAdmin, pdpb.CheckPolicy_APPROXIMATE, nil)
	if err != nil {
		log.Debug("fail to create split region operator", errs.ZapError(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	return []*operator.Operator{op}
}

func (s *randomSplitScheduler) allowSplit(cluster opt.Cluster, region *core.RegionInfo) bool {
	if !opt.IsRegionHealthy(cluster, region) {
		return false
	}
	if !opt.IsRegionReplicated(cluster, region) {
		return false
	}
	if cluster.IsRegionHot(region) {
		return false
	}
	return true
}
