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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/statistics"
	"go.uber.org/zap"
)

const (
	// ShuffleHotRegionName is shuffle hot region scheduler name.
	ShuffleHotRegionName = "shuffle-hot-region-scheduler"
	// ShuffleHotRegionType is shuffle hot region scheduler type.
	ShuffleHotRegionType = "shuffle-hot-region"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	shuffleHotRegionCounter            = schedulerCounter.WithLabelValues(ShuffleHotRegionName, "schedule")
	shuffleHotRegionNewOperatorCounter = schedulerCounter.WithLabelValues(ShuffleHotRegionName, "new-operator")
	shuffleHotRegionSkipCounter        = schedulerCounter.WithLabelValues(ShuffleHotRegionName, "skip")
)

type shuffleHotRegionSchedulerConfig struct {
	Name  string `json:"name"`
	Limit uint64 `json:"limit"`
}

// ShuffleHotRegionScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random store, and then transfer the leader to
// the hot peer.
type shuffleHotRegionScheduler struct {
	*baseHotScheduler
	conf *shuffleHotRegionSchedulerConfig
}

// newShuffleHotRegionScheduler creates an admin scheduler that random balance hot regions
func newShuffleHotRegionScheduler(opController *schedule.OperatorController, conf *shuffleHotRegionSchedulerConfig) schedule.Scheduler {
	base := newBaseHotScheduler(opController)
	ret := &shuffleHotRegionScheduler{
		baseHotScheduler: base,
		conf:             conf,
	}
	return ret
}

func (s *shuffleHotRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleHotRegionScheduler) GetType() string {
	return ShuffleHotRegionType
}

func (s *shuffleHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleHotRegionScheduler) IsScheduleAllowed(cluster sche.ClusterInformer) bool {
	hotRegionAllowed := s.OpController.OperatorCount(operator.OpHotRegion) < s.conf.Limit
	regionAllowed := s.OpController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit()
	leaderAllowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !hotRegionAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpHotRegion.String()).Inc()
	}
	if !regionAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	if !leaderAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return hotRegionAllowed && regionAllowed && leaderAllowed
}

func (s *shuffleHotRegionScheduler) Schedule(cluster sche.ClusterInformer, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	shuffleHotRegionCounter.Inc()
	rw := s.randomRWType()
	s.prepareForBalance(rw, cluster)
	operators := s.randomSchedule(cluster, s.stLoadInfos[buildResourceType(rw, constant.LeaderKind)])
	return operators, nil
}

func (s *shuffleHotRegionScheduler) randomSchedule(cluster sche.ClusterInformer, loadDetail map[uint64]*statistics.StoreLoadDetail) []*operator.Operator {
	for _, detail := range loadDetail {
		if len(detail.HotPeers) < 1 {
			continue
		}
		i := s.r.Intn(len(detail.HotPeers))
		r := detail.HotPeers[i]
		// select src region
		srcRegion := cluster.GetRegion(r.RegionID)
		if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
			continue
		}
		srcStoreID := srcRegion.GetLeader().GetStoreId()
		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID), errs.ZapError(errs.ErrGetSourceStore))
		}

		filters := []filter.Filter{
			&filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true, OperatorLevel: constant.Low},
			filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIDs(), srcRegion.GetStoreIDs()),
			filter.NewPlacementSafeguard(s.GetName(), cluster.GetOpts(), cluster.GetBasicCluster(), cluster.GetRuleManager(), srcRegion, srcStore, nil),
		}
		stores := cluster.GetStores()
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if !filter.Target(cluster.GetOpts(), store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetID())
		}
		if len(destStoreIDs) == 0 {
			return nil
		}
		// random pick a dest store
		destStoreID := destStoreIDs[s.r.Intn(len(destStoreIDs))]
		if destStoreID == 0 {
			return nil
		}
		srcPeer := srcRegion.GetStorePeer(srcStoreID)
		if srcPeer == nil {
			return nil
		}
		destPeer := &metapb.Peer{StoreId: destStoreID}
		op, err := operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStoreID, destPeer)
		if err != nil {
			log.Debug("fail to create move leader operator", errs.ZapError(err))
			return nil
		}
		op.SetPriorityLevel(constant.Low)
		op.Counters = append(op.Counters, shuffleHotRegionNewOperatorCounter)
		return []*operator.Operator{op}
	}
	shuffleHotRegionSkipCounter.Inc()
	return nil
}
