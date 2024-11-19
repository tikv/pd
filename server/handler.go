// Copyright 2016 TiKV Project Authors.
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

package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule"
	sc "github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

// SchedulerConfigHandlerPath is the api router path of the schedule config handler.
var SchedulerConfigHandlerPath = "/api/v1/scheduler-config"

type server struct {
	*Server
}

// GetCoordinator returns the coordinator.
func (s *server) GetCoordinator() *schedule.Coordinator {
	c := s.GetRaftCluster()
	if c == nil {
		return nil
	}
	return c.GetCoordinator()
}

// GetCluster returns RaftCluster.
func (s *server) GetCluster() sche.SchedulerCluster {
	return s.GetRaftCluster()
}

// Handler is a helper to export methods to handle API/RPC requests.
type Handler struct {
	*handler.Handler
	s               *Server
	opt             *config.PersistOptions
	pluginChMap     map[string]chan string
	pluginChMapLock syncutil.RWMutex
}

func newHandler(s *Server) *Handler {
	h := handler.NewHandler(&server{
		Server: s,
	})
	return &Handler{
		Handler:         h,
		s:               s,
		opt:             s.persistOptions,
		pluginChMap:     make(map[string]chan string),
		pluginChMapLock: syncutil.RWMutex{},
	}
}

// GetRaftCluster returns RaftCluster.
func (h *Handler) GetRaftCluster() (*cluster.RaftCluster, error) {
	rc := h.s.GetRaftCluster()
	if rc == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return rc, nil
}

// IsSchedulerExisted returns whether scheduler is existed.
func (h *Handler) IsSchedulerExisted(name string) (bool, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return false, err
	}
	return rc.GetCoordinator().GetSchedulersController().IsSchedulerExisted(name)
}

// GetScheduleConfig returns ScheduleConfig.
func (h *Handler) GetScheduleConfig() *sc.ScheduleConfig {
	return h.s.GetScheduleConfig()
}

// GetHotRegionsWriteInterval gets interval for PD to store Hot Region information..
func (h *Handler) GetHotRegionsWriteInterval() time.Duration {
	return h.opt.GetHotRegionsWriteInterval()
}

// GetHotRegionsReservedDays gets days hot region information is kept.
func (h *Handler) GetHotRegionsReservedDays() uint64 {
	return h.opt.GetHotRegionsReservedDays()
}

// HistoryHotRegionsRequest wrap request condition from tidb.
// it is request from tidb
type HistoryHotRegionsRequest struct {
	StartTime      int64    `json:"start_time,omitempty"`
	EndTime        int64    `json:"end_time,omitempty"`
	RegionIDs      []uint64 `json:"region_ids,omitempty"`
	StoreIDs       []uint64 `json:"store_ids,omitempty"`
	PeerIDs        []uint64 `json:"peer_ids,omitempty"`
	IsLearners     []bool   `json:"is_learners,omitempty"`
	IsLeaders      []bool   `json:"is_leaders,omitempty"`
	HotRegionTypes []string `json:"hot_region_type,omitempty"`
}

// GetAllRequestHistoryHotRegion gets all hot region info in HistoryHotRegion form.
func (h *Handler) GetAllRequestHistoryHotRegion(request *HistoryHotRegionsRequest) (*storage.HistoryHotRegions, error) {
	var hotRegionTypes = storage.HotRegionTypes
	if len(request.HotRegionTypes) != 0 {
		hotRegionTypes = request.HotRegionTypes
	}
	iter := h.GetHistoryHotRegionIter(hotRegionTypes, request.StartTime, request.EndTime)
	var results []*storage.HistoryHotRegion
	regionSet, storeSet, peerSet, learnerSet, leaderSet :=
		make(map[uint64]bool), make(map[uint64]bool),
		make(map[uint64]bool), make(map[bool]bool), make(map[bool]bool)
	for _, id := range request.RegionIDs {
		regionSet[id] = true
	}
	for _, id := range request.StoreIDs {
		storeSet[id] = true
	}
	for _, id := range request.PeerIDs {
		peerSet[id] = true
	}
	for _, isLearner := range request.IsLearners {
		learnerSet[isLearner] = true
	}
	for _, isLeader := range request.IsLeaders {
		leaderSet[isLeader] = true
	}
	var next *storage.HistoryHotRegion
	var err error
	for next, err = iter.Next(); next != nil && err == nil; next, err = iter.Next() {
		if len(regionSet) != 0 && !regionSet[next.RegionID] {
			continue
		}
		if len(storeSet) != 0 && !storeSet[next.StoreID] {
			continue
		}
		if len(peerSet) != 0 && !peerSet[next.PeerID] {
			continue
		}
		if !learnerSet[next.IsLearner] {
			continue
		}
		if !leaderSet[next.IsLeader] {
			continue
		}
		results = append(results, next)
	}
	return &storage.HistoryHotRegions{
		HistoryHotRegion: results,
	}, err
}

// AddScheduler adds a scheduler.
func (h *Handler) AddScheduler(tp types.CheckerSchedulerType, args ...string) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	var removeSchedulerCb func(string) error
	if c.IsServiceIndependent(constant.SchedulingServiceName) {
		removeSchedulerCb = c.GetCoordinator().GetSchedulersController().RemoveSchedulerHandler
	} else {
		removeSchedulerCb = c.GetCoordinator().GetSchedulersController().RemoveScheduler
	}
	s, err := schedulers.CreateScheduler(tp, c.GetOperatorController(), h.s.storage, schedulers.ConfigSliceDecoder(tp, args), removeSchedulerCb)
	if err != nil {
		return err
	}
	log.Info("create scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args))
	if c.IsServiceIndependent(constant.SchedulingServiceName) {
		if err = c.AddSchedulerHandler(s, args...); err != nil {
			log.Error("can not add scheduler handler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args), errs.ZapError(err))
			return err
		}
		log.Info("add scheduler handler successfully", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args))
	} else {
		if err = c.AddScheduler(s, args...); err != nil {
			log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args), errs.ZapError(err))
			return err
		}
		log.Info("add scheduler successfully", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args))
	}
	if err = h.opt.Persist(c.GetStorage()); err != nil {
		log.Error("can not persist scheduler config", errs.ZapError(err))
		return err
	}
	log.Info("persist scheduler config successfully", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args))
	return nil
}

// RemoveScheduler removes a scheduler by name.
func (h *Handler) RemoveScheduler(name string) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	if c.IsServiceIndependent(constant.SchedulingServiceName) {
		if err = c.RemoveSchedulerHandler(name); err != nil {
			log.Error("can not remove scheduler handler", zap.String("scheduler-name", name), errs.ZapError(err))
		} else {
			log.Info("remove scheduler handler successfully", zap.String("scheduler-name", name))
		}
	} else {
		if err = c.RemoveScheduler(name); err != nil {
			log.Error("can not remove scheduler", zap.String("scheduler-name", name), errs.ZapError(err))
		} else {
			log.Info("remove scheduler successfully", zap.String("scheduler-name", name))
		}
	}
	return err
}

// SetAllStoresLimit is used to set limit of all stores.
func (h *Handler) SetAllStoresLimit(ratePerMin float64, limitType storelimit.Type) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	return c.SetAllStoresLimit(limitType, ratePerMin)
}

// SetAllStoresLimitTTL is used to set limit of all stores with ttl
func (h *Handler) SetAllStoresLimitTTL(ratePerMin float64, limitType storelimit.Type, ttl time.Duration) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	return c.SetAllStoresLimitTTL(limitType, ratePerMin, ttl)
}

// SetLabelStoresLimit is used to set limit of label stores.
func (h *Handler) SetLabelStoresLimit(ratePerMin float64, limitType storelimit.Type, labels []*metapb.StoreLabel) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	for _, store := range c.GetStores() {
		for _, label := range labels {
			for _, sl := range store.GetLabels() {
				if label.Key == sl.Key && label.Value == sl.Value {
					// TODO: need to handle some of stores are persisted, and some of stores are not.
					_ = c.SetStoreLimit(store.GetID(), limitType, ratePerMin)
				}
			}
		}
	}
	return nil
}

// SetStoreLimit is used to set the limit of a store.
func (h *Handler) SetStoreLimit(storeID uint64, ratePerMin float64, limitType storelimit.Type) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	return c.SetStoreLimit(storeID, limitType, ratePerMin)
}

// GetRegionsByType gets the region with specified type.
func (h *Handler) GetRegionsByType(typ statistics.RegionStatisticType) ([]*core.RegionInfo, error) {
	c := h.s.GetRaftCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	return c.GetRegionStatsByType(typ), nil
}

// GetSchedulerConfigHandler gets the handler of schedulers.
func (h *Handler) GetSchedulerConfigHandler() (http.Handler, error) {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	for name, handler := range c.GetSchedulerHandlers() {
		prefix := path.Join(pdRootPath, SchedulerConfigHandlerPath, name)
		urlPath := prefix + "/"
		mux.Handle(urlPath, http.StripPrefix(prefix, handler))
	}
	return mux, nil
}

// ResetTS resets the ts with specified tso.
func (h *Handler) ResetTS(ts uint64, ignoreSmaller, skipUpperBoundCheck bool, _ uint32) error {
	log.Info("reset-ts",
		zap.Uint64("new-ts", ts),
		zap.Bool("ignore-smaller", ignoreSmaller),
		zap.Bool("skip-upper-bound-check", skipUpperBoundCheck))
	tsoAllocator, err := h.s.tsoAllocatorManager.GetAllocator(tso.GlobalDCLocation)
	if err != nil {
		return err
	}
	if tsoAllocator == nil {
		return errs.ErrServerNotStarted
	}
	return tsoAllocator.SetTSO(ts, ignoreSmaller, skipUpperBoundCheck)
}

// GetProgressByID returns the progress details for a given store ID.
func (h *Handler) GetProgressByID(storeID string) (action string, p, ls, cs float64, err error) {
	return h.s.GetRaftCluster().GetProgressByID(storeID)
}

// GetProgressByAction returns the progress details for a given action.
func (h *Handler) GetProgressByAction(action string) (p, ls, cs float64, err error) {
	return h.s.GetRaftCluster().GetProgressByAction(action)
}

// PluginLoad loads the plugin referenced by the pluginPath
func (h *Handler) PluginLoad(pluginPath string) error {
	h.pluginChMapLock.Lock()
	defer h.pluginChMapLock.Unlock()
	cluster, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	c := cluster.GetCoordinator()
	ch := make(chan string)
	h.pluginChMap[pluginPath] = ch

	// make sure path is in data dir
	filePath, err := filepath.Abs(pluginPath)
	if err != nil || !isPathInDirectory(filePath, h.s.GetConfig().DataDir) {
		return errs.ErrFilePathAbs.Wrap(err)
	}

	c.LoadPlugin(pluginPath, ch)
	return nil
}

// PluginUnload unloads the plugin referenced by the pluginPath
func (h *Handler) PluginUnload(pluginPath string) error {
	h.pluginChMapLock.Lock()
	defer h.pluginChMapLock.Unlock()
	if ch, ok := h.pluginChMap[pluginPath]; ok {
		ch <- schedule.PluginUnload
		return nil
	}
	return errs.ErrPluginNotFound.FastGenByArgs(pluginPath)
}

// GetAddr returns the server urls for clients.
func (h *Handler) GetAddr() string {
	return h.s.GetAddr()
}

// SetStoreLimitTTL set storeLimit with ttl
func (h *Handler) SetStoreLimitTTL(data string, value float64, ttl time.Duration) error {
	return h.s.SaveTTLConfig(map[string]any{
		data: value,
	}, ttl)
}

// IsLeader return true if this server is leader
func (h *Handler) IsLeader() bool {
	return h.s.member.IsLeader()
}

// GetHistoryHotRegions get hot region info in HistoryHotRegion form.
func (h *Handler) GetHistoryHotRegions(typ utils.RWType) ([]storage.HistoryHotRegion, error) {
	hotRegions, err := h.GetHotRegions(typ)
	if hotRegions == nil || err != nil {
		return nil, err
	}
	hotPeers := hotRegions.AsPeer
	return h.packHotRegions(hotPeers, typ.String())
}

func (h *Handler) packHotRegions(hotPeersStat statistics.StoreHotPeersStat, hotRegionType string) (historyHotRegions []storage.HistoryHotRegion, err error) {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil, err
	}
	for _, hotPeersStat := range hotPeersStat {
		stats := hotPeersStat.Stats
		for _, hotPeerStat := range stats {
			region := c.GetRegion(hotPeerStat.RegionID)
			if region == nil {
				continue
			}
			meta := region.GetMeta()
			meta, err := encryption.EncryptRegion(meta, h.s.encryptionKeyManager)
			if err != nil {
				return nil, err
			}
			stat := storage.HistoryHotRegion{
				// store in ms.
				// TODO: distinguish store heartbeat interval and region heartbeat interval
				// read statistic from store heartbeat, write statistic from region heartbeat
				UpdateTime:     int64(region.GetInterval().GetEndTimestamp() * 1000),
				RegionID:       hotPeerStat.RegionID,
				StoreID:        hotPeerStat.StoreID,
				PeerID:         region.GetStorePeer(hotPeerStat.StoreID).GetId(),
				IsLeader:       hotPeerStat.IsLeader,
				IsLearner:      core.IsLearner(region.GetPeer(hotPeerStat.StoreID)),
				HotDegree:      int64(hotPeerStat.HotDegree),
				FlowBytes:      hotPeerStat.ByteRate,
				KeyRate:        hotPeerStat.KeyRate,
				QueryRate:      hotPeerStat.QueryRate,
				StartKey:       string(region.GetStartKey()),
				EndKey:         string(region.GetEndKey()),
				EncryptionMeta: meta.GetEncryptionMeta(),
				HotRegionType:  hotRegionType,
			}
			historyHotRegions = append(historyHotRegions, stat)
		}
	}
	return
}

// GetHistoryHotRegionIter return a iter which iter all qualified item .
func (h *Handler) GetHistoryHotRegionIter(
	hotRegionTypes []string,
	startTime, endTime int64,
) storage.HotRegionStorageIterator {
	iter := h.s.hotRegionStorage.NewIterator(hotRegionTypes, startTime, endTime)
	return iter
}

// RedirectSchedulerUpdate update scheduler config. Export this func to help handle damaged store.
func (h *Handler) RedirectSchedulerUpdate(name string, storeID float64) error {
	input := make(map[string]any)
	input["name"] = name
	input["store_id"] = storeID
	updateURL, err := url.JoinPath(h.GetAddr(), "pd", SchedulerConfigHandlerPath, name, "config")
	if err != nil {
		return err
	}
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return apiutil.PostJSONIgnoreResp(h.s.GetHTTPClient(), updateURL, body)
}
