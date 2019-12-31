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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// ShuffleRegionName is shuffle region scheduler name.
	ShuffleRegionName = "shuffle-region-scheduler"
	// ShuffleRegionType is shuffle region scheduler type.
	ShuffleRegionType = "shuffle-region"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleRegionSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return errors.WithStack(err)
			}
			conf.Ranges = ranges
			conf.Name = ShuffleRegionName
			return nil
		}
	})
	schedule.RegisterScheduler(ShuffleRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleRegionSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleRegionScheduler(opController, conf), nil
	})
}

type shuffleRegionSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type shuffleRegionScheduler struct {
	*BaseScheduler
	conf     *shuffleRegionSchedulerConfig
	selector *selector.RandomSelector
}

// newShuffleRegionScheduler creates an admin scheduler that shuffles regions
// between stores.
func newShuffleRegionScheduler(opController *schedule.OperatorController, conf *shuffleRegionSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: conf.Name, MoveRegion: true},
	}
	base := NewBaseScheduler(opController)
	return &shuffleRegionScheduler{
		BaseScheduler: base,
		conf:          conf,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *shuffleRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleRegionScheduler) GetType() string {
	return ShuffleRegionType
}

func (s *shuffleRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *shuffleRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region, oldPeer := s.scheduleRemovePeer(cluster)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil
	}

	excludedFilter := filter.NewExcludedFilter(s.GetName(), nil, region.GetStoreIds())
	newPeer := s.scheduleAddPeer(cluster, excludedFilter)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-new-peer").Inc()
		return nil
	}

	op, err := operator.CreateMovePeerOperator(ShuffleRegionType, cluster, region, operator.OpAdmin, oldPeer.GetStoreId(), newPeer)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}

func (s *shuffleRegionScheduler) scheduleRemovePeer(cluster opt.Cluster) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.GetStores()

	source := s.selector.SelectSource(cluster, stores)
	if source == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-source-store").Inc()
		return nil, nil
	}

	region := cluster.RandFollowerRegion(source.GetID(), s.conf.Ranges, opt.HealthRegion(cluster))
	if region == nil {
		region = cluster.RandLeaderRegion(source.GetID(), s.conf.Ranges, opt.HealthRegion(cluster))
	}
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetID())
}

func (s *shuffleRegionScheduler) scheduleAddPeer(cluster opt.Cluster, filter filter.Filter) *metapb.Peer {
	stores := cluster.GetStores()

	target := s.selector.SelectTarget(cluster, stores, filter)
	if target == nil {
		return nil
	}

	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		log.Error("failed to allocate peer", zap.Error(err))
		return nil
	}

	return newPeer
}
