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
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
)

type labelSchedulerConfig struct {
	schedulerConfig

	Ranges []core.KeyRange `json:"ranges"`
	// TODO: When we prepare to use Ranges, we will need to implement the ReloadConfig function for this scheduler.
}

type labelScheduler struct {
	*BaseScheduler
	conf *labelSchedulerConfig
}

// LabelScheduler is mainly based on the store's label information for scheduling.
// Now only used for reject leader schedule, that will move the leader out of
// the store with the specific label.
func newLabelScheduler(opController *operator.Controller, conf *labelSchedulerConfig) Scheduler {
	return &labelScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.LabelScheduler, conf),
		conf:          conf,
	}
}

// EncodeConfig implements the Scheduler interface.
func (s *labelScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *labelScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *labelScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	labelCounter.Inc()
	stores := cluster.GetStores()
	rejectLeaderStores := make(map[uint64]struct{})
	for _, s := range stores {
		if cluster.GetSchedulerConfig().CheckLabelProperty(config.RejectLeader, s.GetLabels()) {
			rejectLeaderStores[s.GetID()] = struct{}{}
		}
	}
	if len(rejectLeaderStores) == 0 {
		labelSkipCounter.Inc()
		return nil, nil
	}
	log.Debug("label scheduler reject leader store list", zap.Reflect("stores", rejectLeaderStores))
	for id := range rejectLeaderStores {
		if region := filter.SelectOneRegion(cluster.RandLeaderRegions(id, s.conf.Ranges), nil); region != nil {
			log.Debug("label scheduler selects region to transfer leader", zap.Uint64("region-id", region.GetID()))
			excludeStores := make(map[uint64]struct{})
			for _, p := range region.GetDownPeers() {
				excludeStores[p.GetPeer().GetStoreId()] = struct{}{}
			}
			for _, p := range region.GetPendingPeers() {
				excludeStores[p.GetStoreId()] = struct{}{}
			}
			f := filter.NewExcludedFilter(s.GetName(), nil, excludeStores)

			target := filter.NewCandidates(s.R, cluster.GetFollowerStores(region)).
				FilterTarget(cluster.GetSchedulerConfig(), nil, nil, &filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, OperatorLevel: constant.Medium}, f).
				RandomPick()
			if target == nil {
				log.Debug("label scheduler no target found for region", zap.Uint64("region-id", region.GetID()))
				labelNoTargetCounter.Inc()
				continue
			}

			op, err := operator.CreateTransferLeaderOperator("label-reject-leader", cluster, region, target.GetID(), []uint64{}, operator.OpLeader)
			if err != nil {
				log.Debug("fail to create transfer label reject leader operator", errs.ZapError(err))
				return nil, nil
			}
			op.Counters = append(op.Counters, labelNewOperatorCounter)
			return []*operator.Operator{op}, nil
		}
	}
	labelNoRegionCounter.Inc()
	return nil, nil
}
