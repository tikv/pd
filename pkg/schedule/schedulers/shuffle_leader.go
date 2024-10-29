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
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
)

type shuffleLeaderSchedulerConfig struct {
	schedulerConfig

	Ranges []core.KeyRange `json:"ranges"`
	// TODO: When we prepare to use Ranges, we will need to implement the ReloadConfig function for this scheduler.
}

type shuffleLeaderScheduler struct {
	*BaseScheduler
	conf    *shuffleLeaderSchedulerConfig
	filters []filter.Filter
}

// newShuffleLeaderScheduler creates an admin scheduler that shuffles leaders
// between stores.
func newShuffleLeaderScheduler(opController *operator.Controller, conf *shuffleLeaderSchedulerConfig) Scheduler {
	base := NewBaseScheduler(opController, types.ShuffleLeaderScheduler, conf)
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: base.GetName(), TransferLeader: true, OperatorLevel: constant.Low},
		filter.NewSpecialUseFilter(base.GetName()),
	}
	return &shuffleLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		filters:       filters,
	}
}

// EncodeConfig implements the Scheduler interface.
func (s *shuffleLeaderScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *shuffleLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *shuffleLeaderScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	// We shuffle leaders between stores by:
	// 1. random select a valid store.
	// 2. transfer a leader to the store.
	shuffleLeaderCounter.Inc()
	targetStore := filter.NewCandidates(s.R, cluster.GetStores()).
		FilterTarget(cluster.GetSchedulerConfig(), nil, nil, s.filters...).
		RandomPick()
	if targetStore == nil {
		shuffleLeaderNoTargetStoreCounter.Inc()
		return nil, nil
	}
	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	region := filter.SelectOneRegion(cluster.RandFollowerRegions(targetStore.GetID(), s.conf.Ranges), nil, pendingFilter, downFilter)
	if region == nil {
		shuffleLeaderNoFollowerCounter.Inc()
		return nil, nil
	}
	op, err := operator.CreateTransferLeaderOperator(s.GetName(), cluster, region, targetStore.GetID(), []uint64{}, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create shuffle leader operator", errs.ZapError(err))
		return nil, nil
	}
	op.SetPriorityLevel(constant.Low)
	op.Counters = append(op.Counters, shuffleLeaderNewOperatorCounter)
	return []*operator.Operator{op}, nil
}
