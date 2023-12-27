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
)

const (
	// ShuffleLeaderName is shuffle leader scheduler name.
	ShuffleLeaderName = "shuffle-leader-scheduler"
	// ShuffleLeaderType is shuffle leader scheduler type.
	ShuffleLeaderType = "shuffle-leader"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	shuffleLeaderCounter              = schedulerCounter.WithLabelValues(ShuffleLeaderName, "schedule")
	shuffleLeaderNewOperatorCounter   = schedulerCounter.WithLabelValues(ShuffleLeaderName, "new-operator")
	shuffleLeaderNoTargetStoreCounter = schedulerCounter.WithLabelValues(ShuffleLeaderName, "no-target-store")
	shuffleLeaderNoFollowerCounter    = schedulerCounter.WithLabelValues(ShuffleLeaderName, "no-follower")
)

type shuffleLeaderSchedulerConfig struct {
	Name   string          `json:"name"`
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
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: conf.Name, TransferLeader: true, OperatorLevel: constant.Low},
		filter.NewSpecialUseFilter(conf.Name),
	}
	base := NewBaseScheduler(opController)
	return &shuffleLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		filters:       filters,
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleLeaderScheduler) GetType() string {
	return ShuffleLeaderType
}

func (s *shuffleLeaderScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

func (s *shuffleLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *shuffleLeaderScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	// We shuffle leaders between stores by:
	// 1. random select a valid store.
	// 2. transfer a leader to the store.
	shuffleLeaderCounter.Inc()
	targetStore := filter.NewCandidates(cluster.GetStores()).
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
	op, err := operator.CreateTransferLeaderOperator(ShuffleLeaderType, cluster, region, region.GetLeader().GetId(), targetStore.GetID(), []uint64{}, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create shuffle leader operator", errs.ZapError(err))
		return nil, nil
	}
	op.SetPriorityLevel(constant.Low)
	op.Counters = append(op.Counters, shuffleLeaderNewOperatorCounter)
	return []*operator.Operator{op}, nil
}
