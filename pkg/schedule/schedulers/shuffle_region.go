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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
)

const (
	// ShuffleRegionName is shuffle region scheduler name.
	ShuffleRegionName = "shuffle-region-scheduler"
	// ShuffleRegionType is shuffle region scheduler type.
	ShuffleRegionType = "shuffle-region"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	shuffleRegionCounter                   = schedulerCounter.WithLabelValues(ShuffleRegionName, "schedule")
	shuffleRegionNewOperatorCounter        = schedulerCounter.WithLabelValues(ShuffleRegionName, "new-operator")
	shuffleRegionNoRegionCounter           = schedulerCounter.WithLabelValues(ShuffleRegionName, "no-region")
	shuffleRegionNoNewPeerCounter          = schedulerCounter.WithLabelValues(ShuffleRegionName, "no-new-peer")
	shuffleRegionCreateOperatorFailCounter = schedulerCounter.WithLabelValues(ShuffleRegionName, "create-operator-fail")
	shuffleRegionNoSourceStoreCounter      = schedulerCounter.WithLabelValues(ShuffleRegionName, "no-source-store")
)

type shuffleRegionScheduler struct {
	*BaseScheduler
	conf    *shuffleRegionSchedulerConfig
	filters []filter.Filter
}

// newShuffleRegionScheduler creates an admin scheduler that shuffles regions
// between stores.
func newShuffleRegionScheduler(opController *operator.Controller, conf *shuffleRegionSchedulerConfig) Scheduler {
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: ShuffleRegionName, MoveRegion: true, OperatorLevel: constant.Low},
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

func (s *shuffleRegionScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	cfgData, err := s.conf.storage.LoadSchedulerConfig(s.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &shuffleRegionSchedulerConfig{}
	if err := DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
	}
	s.conf.Roles = newCfg.Roles
	s.conf.Ranges = newCfg.Ranges
	return nil
}

func (s *shuffleRegionScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpRegion) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	return allowed
}

func (s *shuffleRegionScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	shuffleRegionCounter.Inc()
	region, oldPeer := s.scheduleRemovePeer(cluster)
	if region == nil {
		shuffleRegionNoRegionCounter.Inc()
		return nil, nil
	}

	newPeer := s.scheduleAddPeer(cluster, region, oldPeer)
	if newPeer == nil {
		shuffleRegionNoNewPeerCounter.Inc()
		return nil, nil
	}

	op, err := operator.CreateMovePeerOperator(ShuffleRegionType, cluster, region, operator.OpRegion, oldPeer.GetStoreId(), newPeer)
	if err != nil {
		shuffleRegionCreateOperatorFailCounter.Inc()
		return nil, nil
	}
	op.Counters = append(op.Counters, shuffleRegionNewOperatorCounter)
	op.SetPriorityLevel(constant.Low)
	return []*operator.Operator{op}, nil
}

func (s *shuffleRegionScheduler) scheduleRemovePeer(cluster sche.SchedulerCluster) (*core.RegionInfo, *metapb.Peer) {
	candidates := filter.NewCandidates(cluster.GetStores()).
		FilterSource(cluster.GetSchedulerConfig(), nil, nil, s.filters...).
		Shuffle()

	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	ranges := s.conf.GetRanges()
	for _, source := range candidates.Stores {
		var region *core.RegionInfo
		if s.conf.IsRoleAllow(roleFollower) {
			region = filter.SelectOneRegion(cluster.RandFollowerRegions(source.GetID(), ranges), nil,
				pendingFilter, downFilter, replicaFilter)
		}
		if region == nil && s.conf.IsRoleAllow(roleLeader) {
			region = filter.SelectOneRegion(cluster.RandLeaderRegions(source.GetID(), ranges), nil,
				pendingFilter, downFilter, replicaFilter)
		}
		if region == nil && s.conf.IsRoleAllow(roleLearner) {
			region = filter.SelectOneRegion(cluster.RandLearnerRegions(source.GetID(), ranges), nil,
				pendingFilter, downFilter, replicaFilter)
		}
		if region != nil {
			return region, region.GetStorePeer(source.GetID())
		}
		shuffleRegionNoRegionCounter.Inc()
	}

	shuffleRegionNoSourceStoreCounter.Inc()
	return nil, nil
}

func (s *shuffleRegionScheduler) scheduleAddPeer(cluster sche.SchedulerCluster, region *core.RegionInfo, oldPeer *metapb.Peer) *metapb.Peer {
	store := cluster.GetStore(oldPeer.GetStoreId())
	if store == nil {
		return nil
	}
	scoreGuard := filter.NewPlacementSafeguard(s.GetName(), cluster.GetSchedulerConfig(), cluster.GetBasicCluster(), cluster.GetRuleManager(), region, store, nil)
	excludedFilter := filter.NewExcludedFilter(s.GetName(), nil, region.GetStoreIDs())

	target := filter.NewCandidates(cluster.GetStores()).
		FilterTarget(cluster.GetSchedulerConfig(), nil, nil, append(s.filters, scoreGuard, excludedFilter)...).
		RandomPick()
	if target == nil {
		return nil
	}
	return &metapb.Peer{StoreId: target.GetID(), Role: oldPeer.GetRole()}
}
