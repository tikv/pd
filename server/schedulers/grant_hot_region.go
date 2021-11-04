// Copyright 2021 TiKV Project Authors.
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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/unrolled/render"
)

const (
	// GrantHotRegionName is grant hot region scheduler name.
	GrantHotRegionName = "grant-hot-region-scheduler"
	// GrantHotRegionType is grant hot region scheduler type.
	GrantHotRegionType = "grant-hot-region"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(GrantHotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 2 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}

			conf, ok := v.(*grantHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			leadID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
			}

			storeIDs := make([]uint64, 0)
			for _, id := range strings.Split(args[1], ",") {
				storeID, err := strconv.ParseUint(id, 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
				}
				storeIDs = append(storeIDs, storeID)
			}
			if !contains(leadID, storeIDs) {
				return errs.ErrSchedulerConfig
			}
			conf.StoreLeadID = leadID
			conf.StoreIDs = storeIDs

			return nil
		}
	})

	schedule.RegisterScheduler(GrantHotRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &grantHotRegionSchedulerConfig{StoreIDs: make([]uint64, 0), storage: storage}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantHotRegionScheduler(opController, conf), nil
	})
}

type grantHotRegionSchedulerConfig struct {
	mu          sync.RWMutex
	storage     *core.Storage
	cluster     opt.Cluster
	StoreIDs    []uint64 `json:"store-id"`
	StoreLeadID uint64   `json:"store-leader-id"`
}

func (conf *grantHotRegionSchedulerConfig) Clone() *grantHotRegionSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &grantHotRegionSchedulerConfig{
		StoreIDs:    conf.StoreIDs,
		StoreLeadID: conf.StoreLeadID,
	}
}

func (conf *grantHotRegionSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *grantHotRegionSchedulerConfig) getSchedulerName() string {
	return GrantHotRegionName
}

// grantLeaderScheduler transfers all hot peers to peers  and transfer leader to the fixed store
type grantHotRegionScheduler struct {
	*BaseScheduler
	r           *rand.Rand
	conf        *grantHotRegionSchedulerConfig
	handler     http.Handler
	types       []rwType
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail
}

// newGrantHotRegionScheduler creates an admin scheduler that transfers hot region peer to fixed store and hot region leader to one store.
func newGrantHotRegionScheduler(opController *schedule.OperatorController, conf *grantHotRegionSchedulerConfig) *grantHotRegionScheduler {
	base := NewBaseScheduler(opController)
	handler := newGrantHotRegionHandler(conf)
	ret := &grantHotRegionScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		types:         []rwType{read, write},
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
	return ret
}

func (s *grantHotRegionScheduler) GetName() string {
	return GrantHotRegionName
}

func (s *grantHotRegionScheduler) GetType() string {
	return GrantHotRegionType
}

func (s *grantHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *grantHotRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	log.Debug("grant hot region scheduler check allow")
	regionAllowed := s.OpController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit()
	leaderAllowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !regionAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	if !leaderAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return regionAllowed && leaderAllowed
}

func (s *grantHotRegionScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

type grantHotRegionHandler struct {
	rd     *render.Render
	config *grantHotRegionSchedulerConfig
}

func (handler *grantHotRegionHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	ids, ok := input["store-id"].(string)
	if !ok {
		_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrSchedulerConfig)
		return
	}
	storeIDs := make([]uint64, 0)
	for _, v := range strings.Split(ids, ",") {
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrBytesToUint64)
			return
		}
		storeIDs = append(storeIDs, id)
	}
	leaderID, err := strconv.ParseUint(input["store-leader-id"].(string), 10, 64)
	if err != nil {
		_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrBytesToUint64)
		return
	}
	if !contains(leaderID, storeIDs) {
		_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrSchedulerConfig)
		return
	}

	handler.config.StoreIDs, handler.config.StoreLeadID = storeIDs, leaderID
	if err = handler.config.Persist(); err != nil {
		handler.config.StoreLeadID = 0
		_ = handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *grantHotRegionHandler) ListConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.Clone()
	_ = handler.rd.JSON(w, http.StatusOK, conf)
}

func newGrantHotRegionHandler(config *grantHotRegionSchedulerConfig) http.Handler {
	h := &grantHotRegionHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	return router
}

func (s *grantHotRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	i := s.r.Int() % len(s.types)
	return s.dispatch(s.types[i], cluster)
}

func (s *grantHotRegionScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	storeInfos := summaryStoreInfos(cluster)
	storesLoads := cluster.GetStoresLoads()
	isTraceRegionFlow := cluster.GetOpts().IsTraceRegionFlow()

	switch typ {
	case read:
		s.stLoadInfos[readPeer] = summaryStoresLoad(
			storeInfos,
			storesLoads,
			cluster.RegionReadStats(),
			isTraceRegionFlow,
			read, core.RegionKind)
		return s.randomSchedule(cluster, s.stLoadInfos[readPeer])
	case write:
		s.stLoadInfos[writePeer] = summaryStoresLoad(
			storeInfos,
			storesLoads,
			cluster.RegionWriteStats(),
			isTraceRegionFlow,
			write, core.RegionKind)
		return s.randomSchedule(cluster, s.stLoadInfos[writePeer])
	}
	return nil
}

func (s *grantHotRegionScheduler) randomSchedule(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) []*operator.Operator {
	for srcStoreID, detail := range loadDetail {
		if len(detail.HotPeers) < 1 || srcStoreID == s.conf.StoreLeadID {
			continue
		}
		for _, peer := range detail.HotPeers {
			if contains(srcStoreID, s.conf.StoreIDs) {
				op, err := s.transfer(cluster, peer.RegionID, srcStoreID, false)
				if err != nil {
					log.Debug("fail to create transfer hot region operator", zap.Uint64("regionid", peer.RegionID),
						zap.Uint64("src store id", srcStoreID), errs.ZapError(err))
					return nil
				}
				return []*operator.Operator{op}
			}
			if peer.IsLeader() && srcStoreID != s.conf.StoreLeadID {
				op, err := s.transfer(cluster, peer.RegionID, srcStoreID, true)
				if err != nil {
					log.Debug("fail to create transfer hot region operator", zap.Uint64("regionid", peer.RegionID),
						zap.Uint64("src store id", srcStoreID), errs.ZapError(err))
					return nil
				}
				return []*operator.Operator{op}
			}
		}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	return nil
}

func (s *grantHotRegionScheduler) transfer(cluster opt.Cluster, regionID uint64, srcStoreID uint64, isLeader bool) (op *operator.Operator, err error) {
	srcRegion := cluster.GetRegion(regionID)
	if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
		return nil, errs.ErrRegionRuleNotFound
	}
	srcStore := cluster.GetStore(srcStoreID)
	if srcStore == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID), errs.ZapError(errs.ErrGetSourceStore))
		return nil, errs.ErrStoreNotFound
	}
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
		filter.NewPlacementSafeguard(s.GetName(), cluster, srcRegion, srcStore),
	}

	destStoreIDs := make([]uint64, 0, len(s.conf.StoreIDs))
	var candidate []uint64
	if isLeader {
		filters = append(filters, &filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true})
		candidate = []uint64{s.conf.StoreLeadID}
	} else {
		filters = append(filters, &filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true})
		candidate = s.conf.StoreIDs
	}
	for _, storeID := range candidate {
		store := cluster.GetStore(storeID)
		if !filter.Target(cluster.GetOpts(), store, filters) {
			continue
		}
		destStoreIDs = append(destStoreIDs, storeID)
	}
	if len(destStoreIDs) == 0 {
		return nil, errs.ErrCheckerNotFound
	}

	srcPeer := srcStore.GetMeta()
	if srcPeer == nil {
		return nil, errs.ErrStoreNotFound
	}

	dstStore := &metapb.Peer{StoreId: destStoreIDs[0]}

	if isLeader {
		return operator.CreateTransferLeaderOperator(GrantHotRegionType, cluster, srcRegion, srcRegion.GetLeader().GetStoreId(), dstStore.StoreId, operator.OpLeader)
	} else {
		return operator.CreateMovePeerOperator("grant-move-hot-leader", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStore.GetID(), dstStore)
	}
}

func contains(id uint64, ids []uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
