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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
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
	syncutil.RWMutex
	storage endpoint.ConfigStorage
	Name    string `json:"name"`
	Limit   uint64 `json:"limit"`
}

func (conf *shuffleHotRegionSchedulerConfig) getSchedulerName() string {
	return conf.Name
}

func (conf *shuffleHotRegionSchedulerConfig) Clone() *shuffleHotRegionSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	return &shuffleHotRegionSchedulerConfig{
		Name:  conf.Name,
		Limit: conf.Limit,
	}
}

func (conf *shuffleHotRegionSchedulerConfig) persistLocked() error {
	name := conf.getSchedulerName()
	data, err := EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(name, data)
}

func (conf *shuffleHotRegionSchedulerConfig) getLimit() uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Limit
}

// ShuffleHotRegionScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random store, and then transfer the leader to
// the hot peer.
type shuffleHotRegionScheduler struct {
	*baseHotScheduler
	conf    *shuffleHotRegionSchedulerConfig
	handler http.Handler
}

// newShuffleHotRegionScheduler creates an admin scheduler that random balance hot regions
func newShuffleHotRegionScheduler(opController *operator.Controller, conf *shuffleHotRegionSchedulerConfig) Scheduler {
	base := newBaseHotScheduler(opController)
	handler := newShuffleHotRegionHandler(conf)
	ret := &shuffleHotRegionScheduler{
		baseHotScheduler: base,
		conf:             conf,
		handler:          handler,
	}
	return ret
}

func (s *shuffleHotRegionScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *shuffleHotRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleHotRegionScheduler) GetType() string {
	return ShuffleHotRegionType
}

func (s *shuffleHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

func (s *shuffleHotRegionScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	cfgData, err := s.conf.storage.LoadSchedulerConfig(s.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &shuffleHotRegionSchedulerConfig{}
	if err = DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
	}
	s.conf.Limit = newCfg.Limit
	return nil
}

func (s *shuffleHotRegionScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	hotRegionAllowed := s.OpController.OperatorCount(operator.OpHotRegion) < s.conf.getLimit()
	conf := cluster.GetSchedulerConfig()
	regionAllowed := s.OpController.OperatorCount(operator.OpRegion) < conf.GetRegionScheduleLimit()
	leaderAllowed := s.OpController.OperatorCount(operator.OpLeader) < conf.GetLeaderScheduleLimit()
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

func (s *shuffleHotRegionScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	shuffleHotRegionCounter.Inc()
	rw := s.randomRWType()
	s.prepareForBalance(rw, cluster)
	operators := s.randomSchedule(cluster, s.stLoadInfos[buildResourceType(rw, constant.LeaderKind)])
	return operators, nil
}

func (s *shuffleHotRegionScheduler) randomSchedule(cluster sche.SchedulerCluster, loadDetail map[uint64]*statistics.StoreLoadDetail) []*operator.Operator {
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
			filter.NewPlacementSafeguard(s.GetName(), cluster.GetSchedulerConfig(), cluster.GetBasicCluster(), cluster.GetRuleManager(), srcRegion, srcStore, nil),
		}
		stores := cluster.GetStores()
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if !filter.Target(cluster.GetSchedulerConfig(), store, filters) {
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

type shuffleHotRegionHandler struct {
	rd     *render.Render
	config *shuffleHotRegionSchedulerConfig
}

func (handler *shuffleHotRegionHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	limit, ok := input["limit"].(float64)
	if !ok {
		handler.rd.JSON(w, http.StatusBadRequest, "invalid limit")
		return
	}
	handler.config.Lock()
	defer handler.config.Unlock()
	previous := handler.config.Limit
	handler.config.Limit = uint64(limit)
	err := handler.config.persistLocked()
	if err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		handler.config.Limit = previous
		return
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *shuffleHotRegionHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func newShuffleHotRegionHandler(config *shuffleHotRegionSchedulerConfig) http.Handler {
	h := &shuffleHotRegionHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	return router
}
