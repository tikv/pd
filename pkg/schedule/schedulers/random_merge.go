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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/checker"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
)

const (
	// RandomMergeName is random merge scheduler name.
	RandomMergeName = "random-merge-scheduler"
	// RandomMergeType is random merge scheduler type.
	RandomMergeType = "random-merge"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	randomMergeCounter              = schedulerCounter.WithLabelValues(RandomMergeName, "schedule")
	randomMergeNewOperatorCounter   = schedulerCounter.WithLabelValues(RandomMergeName, "new-operator")
	randomMergeNoSourceStoreCounter = schedulerCounter.WithLabelValues(RandomMergeName, "no-source-store")
	randomMergeNoRegionCounter      = schedulerCounter.WithLabelValues(RandomMergeName, "no-region")
	randomMergeNoTargetStoreCounter = schedulerCounter.WithLabelValues(RandomMergeName, "no-target-store")
	randomMergeNotAllowedCounter    = schedulerCounter.WithLabelValues(RandomMergeName, "not-allowed")
)

type randomMergeSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
	// TODO: When we prepare to use Ranges, we will need to implement the ReloadConfig function for this scheduler.
}

type randomMergeScheduler struct {
	*BaseScheduler
	conf *randomMergeSchedulerConfig
}

// newRandomMergeScheduler creates an admin scheduler that randomly picks two adjacent regions
// then merges them.
func newRandomMergeScheduler(opController *operator.Controller, conf *randomMergeSchedulerConfig) Scheduler {
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
	return EncodeConfig(s.conf)
}

func (s *randomMergeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpMerge) < cluster.GetSchedulerConfig().GetMergeScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpMerge.String()).Inc()
	}
	return allowed
}

func (s *randomMergeScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	randomMergeCounter.Inc()

	store := filter.NewCandidates(cluster.GetStores()).
		FilterSource(cluster.GetSchedulerConfig(), nil, nil, &filter.StoreStateFilter{ActionScope: s.conf.Name, MoveRegion: true, OperatorLevel: constant.Low}).
		RandomPick()
	if store == nil {
		randomMergeNoSourceStoreCounter.Inc()
		return nil, nil
	}
	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	region := filter.SelectOneRegion(cluster.RandLeaderRegions(store.GetID(), s.conf.Ranges), nil, pendingFilter, downFilter)
	if region == nil {
		randomMergeNoRegionCounter.Inc()
		return nil, nil
	}

	other, target := cluster.GetAdjacentRegions(region)
	if !cluster.GetSchedulerConfig().IsOneWayMergeEnabled() && ((rand.Int()%2 == 0 && other != nil) || target == nil) {
		target = other
	}
	if target == nil {
		randomMergeNoTargetStoreCounter.Inc()
		return nil, nil
	}

	if !s.allowMerge(cluster, region, target) {
		randomMergeNotAllowedCounter.Inc()
		return nil, nil
	}

	ops, err := operator.CreateMergeRegionOperator(RandomMergeType, cluster, region, target, operator.OpMerge)
	if err != nil {
		log.Debug("fail to create merge region operator", errs.ZapError(err))
		return nil, nil
	}
	ops[0].SetPriorityLevel(constant.Low)
	ops[1].SetPriorityLevel(constant.Low)
	ops[0].Counters = append(ops[0].Counters, randomMergeNewOperatorCounter)
	return ops, nil
}

func (s *randomMergeScheduler) allowMerge(cluster sche.SchedulerCluster, region, target *core.RegionInfo) bool {
	if !filter.IsRegionHealthy(region) || !filter.IsRegionHealthy(target) {
		return false
	}
	if !filter.IsRegionReplicated(cluster, region) || !filter.IsRegionReplicated(cluster, target) {
		return false
	}
	if cluster.IsRegionHot(region) || cluster.IsRegionHot(target) {
		return false
	}
	return checker.AllowMerge(cluster, region, target)
}
