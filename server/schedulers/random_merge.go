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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"math/rand"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/checker"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

const (
	// RandomMergeName is random merge scheduler name.
	RandomMergeName = "random-merge-scheduler"
	// RandomMergeType is random merge scheduler type.
	RandomMergeType = "random-merge"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(RandomMergeType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*randomMergeSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = RandomMergeName
			return nil
		}
	})
	schedule.RegisterScheduler(RandomMergeType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &randomMergeSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newRandomMergeScheduler(opController, conf), nil
	})
}

type randomMergeSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type randomMergeScheduler struct {
	*BaseScheduler
	conf *randomMergeSchedulerConfig
}

// newRandomMergeScheduler creates an admin scheduler that randomly picks two adjacent regions
// then merges them.
func newRandomMergeScheduler(opController *schedule.OperatorController, conf *randomMergeSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	return &randomMergeScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
}

func (s *randomMergeScheduler) GetName() string {
	return s.conf.Name
}

func (s *randomMergeScheduler) GetType() string {
	return RandomMergeType
}

func (s *randomMergeScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *randomMergeScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetMergeScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpMerge.String()).Inc()
	}
	return allowed
}

func (s *randomMergeScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	store := filter.NewCandidates(cluster.GetStores()).
		FilterSource(cluster.GetOpts(), &filter.StoreStateFilter{ActionScope: s.conf.Name, MoveRegion: true}).
		RandomPick()
	if store == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-source-store").Inc()
		return nil
	}
	region := cluster.RandLeaderRegion(store.GetID(), s.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil
	}

	other, target := cluster.GetAdjacentRegions(region)
	if !cluster.GetOpts().IsOneWayMergeEnabled() && ((rand.Int()%2 == 0 && other != nil) || target == nil) {
		target = other
	}
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
		return nil
	}

	if !s.allowMerge(cluster, region, target) {
		schedulerCounter.WithLabelValues(s.GetName(), "not-allowed").Inc()
		return nil
	}

	ops, err := operator.CreateMergeRegionOperator(RandomMergeType, cluster, region, target, operator.OpMerge)
	if err != nil {
		log.Debug("fail to create merge region operator", errs.ZapError(err))
		return nil
	}
	ops[0].Counters = append(ops[0].Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	return ops
}

func (s *randomMergeScheduler) allowMerge(cluster opt.Cluster, region, target *core.RegionInfo) bool {
	if !opt.IsRegionHealthy(cluster, region) || !opt.IsRegionHealthy(cluster, target) {
		return false
	}
	if !opt.IsRegionReplicated(cluster, region) || !opt.IsRegionReplicated(cluster, target) {
		return false
	}
	if cluster.IsRegionHot(region) || cluster.IsRegionHot(target) {
		return false
	}
	return checker.AllowMerge(cluster, region, target)
}
