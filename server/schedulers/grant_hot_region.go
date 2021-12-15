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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
	"github.com/unrolled/render"
	"go.uber.org/zap"
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
			if !conf.setStore(leadID, storeIDs) {
				return errs.ErrSchedulerConfig
			}
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

func (conf *grantHotRegionSchedulerConfig) setStore(leaderID uint64, peers []uint64) bool {
	conf.mu.Lock()
	defer conf.mu.Unlock()
	ret := slice.AnyOf(peers, func(i int) bool {
		return leaderID == peers[i]
	})
	if ret {
		conf.StoreLeadID = leaderID
		conf.StoreIDs = peers
	}
	return ret
}

func (conf *grantHotRegionSchedulerConfig) GetStoreLeadID() uint64 {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.StoreLeadID
}

func (conf *grantHotRegionSchedulerConfig) SetStoreLeadID(id uint64) {
	conf.mu.Lock()
	defer conf.mu.Unlock()
	conf.StoreLeadID = id
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

func (conf *grantHotRegionSchedulerConfig) has(storeID uint64) bool {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return slice.AnyOf(conf.StoreIDs, func(i int) bool {
		return storeID == conf.StoreIDs[i]
	})
}

// grantLeaderScheduler transfers all hot peers to peers  and transfer leader to the fixed store
type grantHotRegionScheduler struct {
	*BaseScheduler
	r           *rand.Rand
	conf        *grantHotRegionSchedulerConfig
	handler     http.Handler
	types       []statistics.RWType
	stLoadInfos [resourceTypeLen]map[uint64]*statistics.StoreLoadDetail
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
		types:         []statistics.RWType{statistics.Read, statistics.Write},
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*statistics.StoreLoadDetail{}
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

// IsScheduleAllowed returns whether the scheduler is allowed to schedule.
// TODO it should check if there is any scheduelr such as evict or hot region scheduler
func (s *grantHotRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
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
	if !handler.config.setStore(leaderID, storeIDs) {
		_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrSchedulerConfig)
		return
	}

	if err = handler.config.Persist(); err != nil {
		handler.config.SetStoreLeadID(0)
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

func (s *grantHotRegionScheduler) dispatch(typ statistics.RWType, cluster opt.Cluster) []*operator.Operator {
	storeInfos := statistics.SummaryStoreInfos(cluster)
	storesLoads := cluster.GetStoresLoads()
	isTraceRegionFlow := cluster.GetOpts().IsTraceRegionFlow()

	var stLoadInfos map[uint64]*statistics.StoreLoadDetail
	switch typ {
	case statistics.Read:
		stLoadInfos = summaryStoresLoad(
			storeInfos,
			storesLoads,
			cluster.RegionReadStats(),
			isTraceRegionFlow,
			statistics.Read, core.RegionKind)
	case statistics.Write:
		stLoadInfos = summaryStoresLoad(
			storeInfos,
			storesLoads,
			cluster.RegionWriteStats(),
			isTraceRegionFlow,
			statistics.Write, core.RegionKind)
	}
	infos := make([]*statistics.StoreLoadDetail, len(stLoadInfos))
	index := 0
	for _, info := range stLoadInfos {
		infos[index] = info
		index++
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].LoadPred.Current.Loads[statistics.ByteDim] > infos[j].LoadPred.Current.Loads[statistics.ByteDim]
	})
	return s.randomSchedule(cluster, infos)
}

func (s *grantHotRegionScheduler) randomSchedule(cluster opt.Cluster, infos []*statistics.StoreLoadDetail) (ops []*operator.Operator) {
	isleader := s.r.Int()%2 == 1
	for _, detail := range infos {
		srcStoreID := detail.Info.Store.GetID()
		if isleader {
			if s.conf.has(srcStoreID) || len(detail.HotPeers) < 1 {
				continue
			}
		} else {
			if !s.conf.has(srcStoreID) || srcStoreID == s.conf.GetStoreLeadID() {
				continue
			}
		}

		for _, peer := range detail.HotPeers {
			if s.OpController.GetOperator(peer.RegionID) != nil {
				continue
			}
			op, err := s.transfer(cluster, peer.RegionID, srcStoreID, isleader)
			if err != nil {
				log.Debug("fail to create grant hot region operator", zap.Uint64("region-id", peer.RegionID),
					zap.Uint64("src store id", srcStoreID), errs.ZapError(err))
				continue
			}
			return []*operator.Operator{op}
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
		filter.NewPlacementSafeguard(s.GetName(), cluster, srcRegion, srcStore),
	}

	destStoreIDs := make([]uint64, 0, len(s.conf.StoreIDs))
	var candidate []uint64
	if isLeader {
		filters = append(filters, &filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true})
		candidate = []uint64{s.conf.GetStoreLeadID()}
	} else {
		filters = append(filters, &filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()))
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
	i := s.r.Int() % len(destStoreIDs)
	dstStore := &metapb.Peer{StoreId: destStoreIDs[i]}

	if isLeader {
		op, err = operator.CreateTransferLeaderOperator(GrantHotRegionType+"-leader", cluster, srcRegion, srcRegion.GetLeader().GetStoreId(), dstStore.StoreId, operator.OpLeader)
	} else {
		op, err = operator.CreateMovePeerOperator(GrantHotRegionType+"-move", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStore.GetID(), dstStore)
	}
	return
}
