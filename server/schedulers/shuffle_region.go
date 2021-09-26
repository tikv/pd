// Copyright 2017 TiKV Project Authors.
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
	"net/http"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
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
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Roles = allRoles
			return nil
		}
	})
	schedule.RegisterScheduler(ShuffleRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleRegionSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleRegionScheduler(opController, conf), nil
	})
}

type shuffleRegionScheduler struct {
	*BaseScheduler
	conf    *shuffleRegionSchedulerConfig
	filters []filter.Filter
}

// newShuffleRegionScheduler creates an admin scheduler that shuffles regions
// between stores.
func newShuffleRegionScheduler(opController *schedule.OperatorController, conf *shuffleRegionSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: ShuffleRegionName, MoveRegion: true},
		filter.NewSpecialUseFilter(ShuffleRegionName),
	}
	base := NewBaseScheduler(opController)
	return &shuffleRegionScheduler{
		BaseScheduler: base,
		conf:          conf,
		filters:       filters,
	}
}

func (s *shuffleRegionScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.conf.ServeHTTP(w, r)
}

func (s *shuffleRegionScheduler) GetName() string {
	return ShuffleRegionName
}

func (s *shuffleRegionScheduler) GetType() string {
	return ShuffleRegionType
}

func (s *shuffleRegionScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.EncodeConfig()
}

func (s *shuffleRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	return allowed
}

func (s *shuffleRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region, oldPeer := s.scheduleRemovePeer(cluster)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil
	}

	newPeer := s.scheduleAddPeer(cluster, region, oldPeer)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-new-peer").Inc()
		return nil
	}

	op, err := operator.CreateMovePeerOperator(ShuffleRegionType, cluster, region, operator.OpRegion, oldPeer.GetStoreId(), newPeer)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}

func (s *shuffleRegionScheduler) scheduleRemovePeer(cluster opt.Cluster) (*core.RegionInfo, *metapb.Peer) {
	candidates := filter.NewCandidates(cluster.GetStores()).
		FilterSource(cluster.GetOpts(), s.filters...).
		Shuffle()

	for _, source := range candidates.Stores {
		var region *core.RegionInfo
		if s.conf.IsRoleAllow(roleFollower) {
			region = cluster.RandFollowerRegion(source.GetID(), s.conf.GetRanges(), opt.HealthRegion(cluster), opt.ReplicatedRegion(cluster))
		}
		if region == nil && s.conf.IsRoleAllow(roleLeader) {
			region = cluster.RandLeaderRegion(source.GetID(), s.conf.GetRanges(), opt.HealthRegion(cluster), opt.ReplicatedRegion(cluster))
		}
		if region == nil && s.conf.IsRoleAllow(roleLearner) {
			region = cluster.RandLearnerRegion(source.GetID(), s.conf.GetRanges(), opt.HealthRegion(cluster), opt.ReplicatedRegion(cluster))
		}
		if region != nil {
			return region, region.GetStorePeer(source.GetID())
		}
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-source-store").Inc()
	return nil, nil
}

func (s *shuffleRegionScheduler) scheduleAddPeer(cluster opt.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer) *metapb.Peer {
	scoreGuard := filter.NewPlacementSafeguard(s.GetName(), cluster, region, cluster.GetStore(oldPeer.GetStoreId()))
	excludedFilter := filter.NewExcludedFilter(s.GetName(), nil, region.GetStoreIds())

	target := filter.NewCandidates(cluster.GetStores()).
		FilterTarget(cluster.GetOpts(), s.filters...).
		FilterTarget(cluster.GetOpts(), scoreGuard, excludedFilter).
		RandomPick()
	if target == nil {
		return nil
	}
	return &metapb.Peer{StoreId: target.GetID(), Role: oldPeer.GetRole()}
}
