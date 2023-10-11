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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	sc "github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
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

func (s *server) GetCoordinator() *schedule.Coordinator {
	return s.GetRaftCluster().GetCoordinator()
}

func (s *server) GetCluster() sche.SharedCluster {
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

// GetStores returns all stores in the cluster.
func (h *Handler) GetStores() ([]*core.StoreInfo, error) {
	rc := h.s.GetRaftCluster()
	if rc == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	storeMetas := rc.GetMetaStores()
	stores := make([]*core.StoreInfo, 0, len(storeMetas))
	for _, s := range storeMetas {
		storeID := s.GetId()
		store := rc.GetStore(storeID)
		if store == nil {
			return nil, errs.ErrStoreNotFound.FastGenByArgs(storeID)
		}
		stores = append(stores, store)
	}
	return stores, nil
}

// GetHotWriteRegions gets all hot write regions stats.
func (h *Handler) GetHotWriteRegions() *statistics.StoreHotPeersInfos {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil
	}
	return c.GetHotWriteRegions()
}

// GetHotBuckets returns all hot buckets stats.
func (h *Handler) GetHotBuckets(regionIDs ...uint64) map[uint64][]*buckets.BucketStat {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil
	}
	degree := c.GetOpts().GetHotRegionCacheHitsThreshold()
	return c.BucketsStats(degree, regionIDs...)
}

// GetHotReadRegions gets all hot read regions stats.
func (h *Handler) GetHotReadRegions() *statistics.StoreHotPeersInfos {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil
	}
	return c.GetHotReadRegions()
}

// GetHotRegionsWriteInterval gets interval for PD to store Hot Region information..
func (h *Handler) GetHotRegionsWriteInterval() time.Duration {
	return h.opt.GetHotRegionsWriteInterval()
}

// GetHotRegionsReservedDays gets days hot region information is kept.
func (h *Handler) GetHotRegionsReservedDays() uint64 {
	return h.opt.GetHotRegionsReservedDays()
}

// GetStoresLoads gets all hot write stores stats.
func (h *Handler) GetStoresLoads() map[uint64][]float64 {
	rc := h.s.GetRaftCluster()
	if rc == nil {
		return nil
	}
	return rc.GetStoresLoads()
}

// AddScheduler adds a scheduler.
func (h *Handler) AddScheduler(name string, args ...string) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	var removeSchedulerCb func(string) error
	if h.s.IsAPIServiceMode() {
		removeSchedulerCb = c.GetCoordinator().GetSchedulersController().RemoveSchedulerHandler
	} else {
		removeSchedulerCb = c.GetCoordinator().GetSchedulersController().RemoveScheduler
	}
	s, err := schedulers.CreateScheduler(name, c.GetOperatorController(), h.s.storage, schedulers.ConfigSliceDecoder(name, args), removeSchedulerCb)
	if err != nil {
		return err
	}
	log.Info("create scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args))
	if h.s.IsAPIServiceMode() {
		if err = c.AddSchedulerHandler(s, args...); err != nil {
			log.Error("can not add scheduler handler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args), errs.ZapError(err))
			return err
		}
		log.Info("add scheduler handler successfully", zap.String("scheduler-name", name), zap.Strings("scheduler-args", args))
	} else {
		if err = c.AddScheduler(s, args...); err != nil {
			log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args), errs.ZapError(err))
			return err
		}
		log.Info("add scheduler successfully", zap.String("scheduler-name", name), zap.Strings("scheduler-args", args))
	}
	if err = h.opt.Persist(c.GetStorage()); err != nil {
		log.Error("can not persist scheduler config", errs.ZapError(err))
		return err
	}
	log.Info("persist scheduler config successfully", zap.String("scheduler-name", name), zap.Strings("scheduler-args", args))
	return nil
}

// RemoveScheduler removes a scheduler by name.
func (h *Handler) RemoveScheduler(name string) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	if h.s.IsAPIServiceMode() {
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

// AddBalanceLeaderScheduler adds a balance-leader-scheduler.
func (h *Handler) AddBalanceLeaderScheduler() error {
	return h.AddScheduler(schedulers.BalanceLeaderType)
}

// AddBalanceWitnessScheduler adds a balance-witness-scheduler.
func (h *Handler) AddBalanceWitnessScheduler() error {
	return h.AddScheduler(schedulers.BalanceWitnessType)
}

// AddTransferWitnessLeaderScheduler adds a transfer-witness-leader-scheduler.
func (h *Handler) AddTransferWitnessLeaderScheduler() error {
	return h.AddScheduler(schedulers.TransferWitnessLeaderType)
}

// AddBalanceRegionScheduler adds a balance-region-scheduler.
func (h *Handler) AddBalanceRegionScheduler() error {
	return h.AddScheduler(schedulers.BalanceRegionType)
}

// AddBalanceHotRegionScheduler adds a balance-hot-region-scheduler.
func (h *Handler) AddBalanceHotRegionScheduler() error {
	return h.AddScheduler(schedulers.HotRegionType)
}

// AddEvictSlowTrendScheduler adds a evict-slow-trend-scheduler.
func (h *Handler) AddEvictSlowTrendScheduler() error {
	return h.AddScheduler(schedulers.EvictSlowTrendType)
}

// AddLabelScheduler adds a label-scheduler.
func (h *Handler) AddLabelScheduler() error {
	return h.AddScheduler(schedulers.LabelType)
}

// AddScatterRangeScheduler adds a balance-range-leader-scheduler
func (h *Handler) AddScatterRangeScheduler(args ...string) error {
	return h.AddScheduler(schedulers.ScatterRangeType, args...)
}

// AddGrantLeaderScheduler adds a grant-leader-scheduler.
func (h *Handler) AddGrantLeaderScheduler(storeID uint64) error {
	return h.AddScheduler(schedulers.GrantLeaderType, strconv.FormatUint(storeID, 10))
}

// AddEvictLeaderScheduler adds an evict-leader-scheduler.
func (h *Handler) AddEvictLeaderScheduler(storeID uint64) error {
	return h.AddScheduler(schedulers.EvictLeaderType, strconv.FormatUint(storeID, 10))
}

// AddShuffleLeaderScheduler adds a shuffle-leader-scheduler.
func (h *Handler) AddShuffleLeaderScheduler() error {
	return h.AddScheduler(schedulers.ShuffleLeaderType)
}

// AddShuffleRegionScheduler adds a shuffle-region-scheduler.
func (h *Handler) AddShuffleRegionScheduler() error {
	return h.AddScheduler(schedulers.ShuffleRegionType)
}

// AddShuffleHotRegionScheduler adds a shuffle-hot-region-scheduler.
func (h *Handler) AddShuffleHotRegionScheduler(limit uint64) error {
	return h.AddScheduler(schedulers.ShuffleHotRegionType, strconv.FormatUint(limit, 10))
}

// AddEvictSlowStoreScheduler adds a evict-slow-store-scheduler.
func (h *Handler) AddEvictSlowStoreScheduler() error {
	return h.AddScheduler(schedulers.EvictSlowStoreType)
}

// AddSplitBucketScheduler adds a split-bucket-scheduler.
func (h *Handler) AddSplitBucketScheduler() error {
	return h.AddScheduler(schedulers.SplitBucketType)
}

// AddRandomMergeScheduler adds a random-merge-scheduler.
func (h *Handler) AddRandomMergeScheduler() error {
	return h.AddScheduler(schedulers.RandomMergeType)
}

// AddGrantHotRegionScheduler adds a grant-hot-region-scheduler
func (h *Handler) AddGrantHotRegionScheduler(leaderID, peers string) error {
	return h.AddScheduler(schedulers.GrantHotRegionType, leaderID, peers)
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
	c.SetAllStoresLimitTTL(limitType, ratePerMin, ttl)
	return nil
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

// GetAllStoresLimit is used to get limit of all stores.
func (h *Handler) GetAllStoresLimit(limitType storelimit.Type) (map[uint64]sc.StoreLimitConfig, error) {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil, err
	}
	return c.GetAllStoresLimit(), nil
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

// SetStoreLimitScene sets the limit values for different scenes
func (h *Handler) SetStoreLimitScene(scene *storelimit.Scene, limitType storelimit.Type) {
	rc := h.s.GetRaftCluster()
	if rc == nil {
		return
	}
	rc.GetStoreLimiter().ReplaceStoreLimitScene(scene, limitType)
}

// GetStoreLimitScene returns the limit values for different scenes
func (h *Handler) GetStoreLimitScene(limitType storelimit.Type) *storelimit.Scene {
	rc := h.s.GetRaftCluster()
	if rc == nil {
		return nil
	}
	return rc.GetStoreLimiter().StoreLimitScene(limitType)
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
		return errs.ErrFilePathAbs.Wrap(err).FastGenWithCause()
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
	return h.s.SaveTTLConfig(map[string]interface{}{
		data: value,
	}, ttl)
}

// IsLeader return true if this server is leader
func (h *Handler) IsLeader() bool {
	return h.s.member.IsLeader()
}

// PackHistoryHotReadRegions get read hot region info in HistoryHotRegion form.
func (h *Handler) PackHistoryHotReadRegions() ([]storage.HistoryHotRegion, error) {
	hotReadRegions := h.GetHotReadRegions()
	if hotReadRegions == nil {
		return nil, nil
	}
	hotReadPeerRegions := hotReadRegions.AsPeer
	return h.packHotRegions(hotReadPeerRegions, storage.ReadType.String())
}

// PackHistoryHotWriteRegions get write hot region info in HistoryHotRegion from
func (h *Handler) PackHistoryHotWriteRegions() ([]storage.HistoryHotRegion, error) {
	hotWriteRegions := h.GetHotWriteRegions()
	if hotWriteRegions == nil {
		return nil, nil
	}
	hotWritePeerRegions := hotWriteRegions.AsPeer
	return h.packHotRegions(hotWritePeerRegions, storage.WriteType.String())
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
func (h *Handler) redirectSchedulerUpdate(name string, storeID float64) error {
	input := make(map[string]interface{})
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

// AddEvictOrGrant add evict leader scheduler or grant leader scheduler.
func (h *Handler) AddEvictOrGrant(storeID float64, name string) error {
	if exist, err := h.IsSchedulerExisted(name); !exist {
		if err != nil && !errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
			return err
		}
		switch name {
		case schedulers.EvictLeaderName:
			err = h.AddEvictLeaderScheduler(uint64(storeID))
		case schedulers.GrantLeaderName:
			err = h.AddGrantLeaderScheduler(uint64(storeID))
		}
		if err != nil {
			return err
		}
	} else {
		if err := h.redirectSchedulerUpdate(name, storeID); err != nil {
			return err
		}
		log.Info("update scheduler", zap.String("scheduler-name", name), zap.Uint64("store-id", uint64(storeID)))
	}
	return nil
}
