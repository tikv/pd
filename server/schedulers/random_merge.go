// Copyright 2018 PingCAP, Inc.
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

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/pkg/errors"
)

const randomMergeName = "random-merge-scheduler"

func init() {
	schedule.RegisterSliceDecoderBuilder("random-merge", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*randomMergeSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return errors.WithStack(err)
			}
			conf.Ranges = ranges
			conf.Name = randomMergeName
			return nil
		}
	})
	schedule.RegisterScheduler("random-merge", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &randomMergeSchedulerConfig{}
		decoder(conf)
		return newRandomMergeScheduler(opController, conf), nil
	})
}

type randomMergeSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type randomMergeScheduler struct {
	*baseScheduler
	conf     *randomMergeSchedulerConfig
	selector *selector.RandomSelector
}

// newRandomMergeScheduler creates an admin scheduler that randomly picks two adjacent regions
// then merges them.
func newRandomMergeScheduler(opController *schedule.OperatorController, conf *randomMergeSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: conf.Name, MoveRegion: true},
	}
	base := newBaseScheduler(opController)
	return &randomMergeScheduler{
		baseScheduler: base,
		conf:          conf,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *randomMergeScheduler) GetName() string {
	return s.conf.Name
}

func (s *randomMergeScheduler) GetType() string {
	return "random-merge"
}

func (s *randomMergeScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *randomMergeScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpMerge) < cluster.GetMergeScheduleLimit()
}

func (s *randomMergeScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	stores := cluster.GetStores()
	store := s.selector.SelectSource(cluster, stores)
	if store == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-source-store").Inc()
		return nil
	}
	region := cluster.RandLeaderRegion(store.GetID(), s.conf.Ranges, core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil
	}

	other, target := cluster.GetAdjacentRegions(region)
	if !cluster.IsOneWayMergeEnabled() && ((rand.Int()%2 == 0 && other != nil) || target == nil) {
		target = other
	}
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
		return nil
	}

	ops, err := operator.CreateMergeRegionOperator("random-merge", cluster, region, target, operator.OpAdmin)
	if err != nil {
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	return ops
}
