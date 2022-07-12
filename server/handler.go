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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/tso"
	"go.uber.org/zap"
)

var (
	// SchedulerConfigHandlerPath is the api router path of the schedule config handler.
	SchedulerConfigHandlerPath = "/api/v1/scheduler-config"

	// ErrServerNotStarted is error info for server not started.
	ErrServerNotStarted = errors.New("The server has not been started")
	// ErrOperatorNotFound is error info for operator not found.
	ErrOperatorNotFound = errors.New("operator not found")
	// ErrAddOperator is error info for already have an operator when adding operator.
	ErrAddOperator = errors.New("failed to add operator, maybe already have one")
	// ErrRegionNotAdjacent is error info for region not adjacent.
	ErrRegionNotAdjacent = errors.New("two regions are not adjacent")
	// ErrRegionNotFound is error info for region not found.
	ErrRegionNotFound = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
	// ErrRegionAbnormalPeer is error info for region has abnormal peer.
	ErrRegionAbnormalPeer = func(regionID uint64) error {
		return errors.Errorf("region %v has abnormal peer", regionID)
	}
	// ErrStoreNotFound is error info for store not found.
	ErrStoreNotFound = func(storeID uint64) error {
		return errors.Errorf("store %v not found", storeID)
	}
	// ErrPluginNotFound is error info for plugin not found.
	ErrPluginNotFound = func(pluginPath string) error {
		return errors.Errorf("plugin is not found: %s", pluginPath)
	}

	schedulerConfigPrefix = "pd/api/v1/scheduler-config"
)

// Handler is a helper to export methods to handle API/RPC requests.
type Handler struct {
	s               *Server
	opt             *config.PersistOptions
	pluginChMap     map[string]chan string
	pluginChMapLock syncutil.RWMutex
}

func newHandler(s *Server) *Handler {
	return &Handler{s: s, opt: s.persistOptions, pluginChMap: make(map[string]chan string), pluginChMapLock: syncutil.RWMutex{}}
}

// GetRaftCluster returns RaftCluster.
func (h *Handler) GetRaftCluster() (*cluster.RaftCluster, error) {
	rc := h.s.GetRaftCluster()
	if rc == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return rc, nil
}

// GetOperatorController returns OperatorController.
func (h *Handler) GetOperatorController() (*schedule.OperatorController, error) {
	rc := h.s.GetRaftCluster()
	if rc == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return rc.GetOperatorController(), nil
}

// IsSchedulerPaused returns whether scheduler is paused.
func (h *Handler) IsSchedulerPaused(name string) (bool, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return false, err
	}
	return rc.IsSchedulerPaused(name)
}

// IsSchedulerDisabled returns whether scheduler is disabled.
func (h *Handler) IsSchedulerDisabled(name string) (bool, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return false, err
	}
	return rc.IsSchedulerDisabled(name)
}

// IsSchedulerExisted returns whether scheduler is existed.
func (h *Handler) IsSchedulerExisted(name string) (bool, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return false, err
	}
	return rc.IsSchedulerExisted(name)
}

// GetScheduleConfig returns ScheduleConfig.
func (h *Handler) GetScheduleConfig() *config.ScheduleConfig {
	return h.s.GetScheduleConfig()
}

// GetSchedulers returns all names of schedulers.
func (h *Handler) GetSchedulers() ([]string, error) {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil, err
	}
	return c.GetSchedulers(), nil
}

// IsCheckerPaused returns if checker is paused
func (h *Handler) IsCheckerPaused(name string) (bool, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return false, err
	}
	return rc.IsCheckerPaused(name)
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
			return nil, ErrStoreNotFound(storeID)
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

//  GetHotRegionsReservedDays gets days hot region information is kept.
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

	s, err := schedule.CreateScheduler(name, c.GetOperatorController(), h.s.storage, schedule.ConfigSliceDecoder(name, args))
	if err != nil {
		return err
	}
	log.Info("create scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args))
	if err = c.AddScheduler(s, args...); err != nil {
		log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", args), errs.ZapError(err))
	} else if err = h.opt.Persist(c.GetStorage()); err != nil {
		log.Error("can not persist scheduler config", errs.ZapError(err))
	} else {
		log.Info("add scheduler successfully", zap.String("scheduler-name", name), zap.Strings("scheduler-args", args))
	}
	return err
}

// RemoveScheduler removes a scheduler by name.
func (h *Handler) RemoveScheduler(name string) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	if err = c.RemoveScheduler(name); err != nil {
		log.Error("can not remove scheduler", zap.String("scheduler-name", name), errs.ZapError(err))
	} else {
		log.Info("remove scheduler successfully", zap.String("scheduler-name", name))
	}
	return err
}

// PauseOrResumeScheduler pauses a scheduler for delay seconds or resume a paused scheduler.
// t == 0 : resume scheduler.
// t > 0 : scheduler delays t seconds.
func (h *Handler) PauseOrResumeScheduler(name string, t int64) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	if err = c.PauseOrResumeScheduler(name, t); err != nil {
		if t == 0 {
			log.Error("can not resume scheduler", zap.String("scheduler-name", name), errs.ZapError(err))
		} else {
			log.Error("can not pause scheduler", zap.String("scheduler-name", name), errs.ZapError(err))
		}
	} else {
		if t == 0 {
			log.Info("resume scheduler successfully", zap.String("scheduler-name", name))
		} else {
			log.Info("pause scheduler successfully", zap.String("scheduler-name", name), zap.Int64("pause-seconds", t))
		}
	}
	return err
}

// PauseOrResumeChecker pauses checker for delay seconds or resume checker
// t == 0 : resume checker.
// t > 0 : checker delays t seconds.
func (h *Handler) PauseOrResumeChecker(name string, t int64) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}
	if err = c.PauseOrResumeChecker(name, t); err != nil {
		if t == 0 {
			log.Error("can not resume checker", zap.String("checker-name", name), errs.ZapError(err))
		} else {
			log.Error("can not pause checker", zap.String("checker-name", name), errs.ZapError(err))
		}
	}
	return err
}

// AddBalanceLeaderScheduler adds a balance-leader-scheduler.
func (h *Handler) AddBalanceLeaderScheduler() error {
	return h.AddScheduler(schedulers.BalanceLeaderType)
}

// AddBalanceLeaderScheduler adds a balance-leader-scheduler.
func (h *Handler) AddBalanceWitnessScheduler() error {
	return h.AddScheduler(schedulers.BalanceWitnessType)
}

// AddBalanceRegionScheduler adds a balance-region-scheduler.
func (h *Handler) AddBalanceRegionScheduler() error {
	return h.AddScheduler(schedulers.BalanceRegionType)
}

// AddBalanceHotRegionScheduler adds a balance-hot-region-scheduler.
func (h *Handler) AddBalanceHotRegionScheduler() error {
	return h.AddScheduler(schedulers.HotRegionType)
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

// GetOperator returns the region operator.
func (h *Handler) GetOperator(regionID uint64) (*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}

	op := c.GetOperator(regionID)
	if op == nil {
		return nil, ErrOperatorNotFound
	}

	return op, nil
}

// GetOperatorStatus returns the status of the region operator.
func (h *Handler) GetOperatorStatus(regionID uint64) (*schedule.OperatorWithStatus, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}

	op := c.GetOperatorStatus(regionID)
	if op == nil {
		return nil, ErrOperatorNotFound
	}

	return op, nil
}

// RemoveOperator removes the region operator.
func (h *Handler) RemoveOperator(regionID uint64) error {
	c, err := h.GetOperatorController()
	if err != nil {
		return err
	}

	op := c.GetOperator(regionID)
	if op == nil {
		return ErrOperatorNotFound
	}

	_ = c.RemoveOperator(op)
	return nil
}

// GetOperators returns the running operators.
func (h *Handler) GetOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperators(), nil
}

// GetWaitingOperators returns the waiting operators.
func (h *Handler) GetWaitingOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetWaitingOperators(), nil
}

// GetAdminOperators returns the running admin operators.
func (h *Handler) GetAdminOperators() ([]*operator.Operator, error) {
	return h.GetOperatorsOfKind(operator.OpAdmin)
}

// GetLeaderOperators returns the running leader operators.
func (h *Handler) GetLeaderOperators() ([]*operator.Operator, error) {
	return h.GetOperatorsOfKind(operator.OpLeader)
}

// GetRegionOperators returns the running region operators.
func (h *Handler) GetRegionOperators() ([]*operator.Operator, error) {
	return h.GetOperatorsOfKind(operator.OpRegion)
}

// GetOperatorsOfKind returns the running operators of the kind.
func (h *Handler) GetOperatorsOfKind(mask operator.OpKind) ([]*operator.Operator, error) {
	ops, err := h.GetOperators()
	if err != nil {
		return nil, err
	}
	var results []*operator.Operator
	for _, op := range ops {
		if op.Kind()&mask != 0 {
			results = append(results, op)
		}
	}
	return results, nil
}

// GetHistory returns finished operators' history since start.
func (h *Handler) GetHistory(start time.Time) ([]operator.OpHistory, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetHistory(start), nil
}

// GetRecords returns finished operators since start.
func (h *Handler) GetRecords(from time.Time) ([]*operator.OpRecord, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	records := c.GetRecords(from)
	if len(records) == 0 {
		return nil, ErrOperatorNotFound
	}
	return records, nil
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
func (h *Handler) GetAllStoresLimit(limitType storelimit.Type) (map[uint64]config.StoreLimitConfig, error) {
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

// AddTransferLeaderOperator adds an operator to transfer leader to the store.
func (h *Handler) AddTransferLeaderOperator(regionID uint64, storeID uint64) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	newLeader := region.GetStoreVoter(storeID)
	if newLeader == nil {
		return errors.Errorf("region has no voter in store %v", storeID)
	}

	op, err := operator.CreateTransferLeaderOperator("admin-transfer-leader", c, region, region.GetLeader().GetStoreId(), newLeader.GetStoreId(), []uint64{}, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create transfer leader operator", errs.ZapError(err))
		return err
	}
	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddTransferRegionOperator adds an operator to transfer region to the stores.
func (h *Handler) AddTransferRegionOperator(regionID uint64, storeIDs map[uint64]placement.PeerRoleType) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	if c.GetOpts().IsPlacementRulesEnabled() {
		// Cannot determine role without peer role when placement rules enabled. Not supported now.
		for _, role := range storeIDs {
			if len(role) == 0 {
				return errors.New("transfer region without peer role is not supported when placement rules enabled")
			}
		}
	}
	for id := range storeIDs {
		if err := checkStoreState(c, id); err != nil {
			return err
		}
	}

	roles := make(map[uint64]placement.PeerRoleType)
	for id, peerRole := range storeIDs {
		if peerRole == "" {
			peerRole = placement.Voter
		}
		roles[id] = peerRole
	}
	op, err := operator.CreateMoveRegionOperator("admin-move-region", c, region, operator.OpAdmin, roles)
	if err != nil {
		log.Debug("fail to create move region operator", errs.ZapError(err))
		return err
	}
	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddTransferPeerOperator adds an operator to transfer peer.
func (h *Handler) AddTransferPeerOperator(regionID uint64, fromStoreID, toStoreID uint64) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	oldPeer := region.GetStorePeer(fromStoreID)
	if oldPeer == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	if err := checkStoreState(c, toStoreID); err != nil {
		return err
	}

	newPeer := &metapb.Peer{StoreId: toStoreID, Role: oldPeer.GetRole()}
	op, err := operator.CreateMovePeerOperator("admin-move-peer", c, region, operator.OpAdmin, fromStoreID, newPeer)
	if err != nil {
		log.Debug("fail to create move peer operator", errs.ZapError(err))
		return err
	}
	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// checkAdminAddPeerOperator checks adminAddPeer operator with given region ID and store ID.
func (h *Handler) checkAdminAddPeerOperator(regionID uint64, toStoreID uint64) (*cluster.RaftCluster, *core.RegionInfo, error) {
	c, err := h.GetRaftCluster()
	if err != nil {
		return nil, nil, err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return nil, nil, ErrRegionNotFound(regionID)
	}

	if region.GetStorePeer(toStoreID) != nil {
		return nil, nil, errors.Errorf("region already has peer in store %v", toStoreID)
	}

	if err := checkStoreState(c, toStoreID); err != nil {
		return nil, nil, err
	}

	return c, region, nil
}

// AddAddPeerOperator adds an operator to add peer.
func (h *Handler) AddAddPeerOperator(regionID uint64, toStoreID uint64) error {
	c, region, err := h.checkAdminAddPeerOperator(regionID, toStoreID)
	if err != nil {
		return err
	}

	newPeer := &metapb.Peer{StoreId: toStoreID}
	op, err := operator.CreateAddPeerOperator("admin-add-peer", c, region, newPeer, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create add peer operator", errs.ZapError(err))
		return err
	}
	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddAddLearnerOperator adds an operator to add learner.
func (h *Handler) AddAddLearnerOperator(regionID uint64, toStoreID uint64) error {
	c, region, err := h.checkAdminAddPeerOperator(regionID, toStoreID)
	if err != nil {
		return err
	}

	newPeer := &metapb.Peer{
		StoreId: toStoreID,
		Role:    metapb.PeerRole_Learner,
	}

	op, err := operator.CreateAddPeerOperator("admin-add-learner", c, region, newPeer, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create add learner operator", errs.ZapError(err))
		return err
	}
	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddRemovePeerOperator adds an operator to remove peer.
func (h *Handler) AddRemovePeerOperator(regionID uint64, fromStoreID uint64) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	if region.GetStorePeer(fromStoreID) == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	op, err := operator.CreateRemovePeerOperator("admin-remove-peer", c, operator.OpAdmin, region, fromStoreID)
	if err != nil {
		log.Debug("fail to create move peer operator", errs.ZapError(err))
		return err
	}
	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddMergeRegionOperator adds an operator to merge region.
func (h *Handler) AddMergeRegionOperator(regionID uint64, targetID uint64) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	target := c.GetRegion(targetID)
	if target == nil {
		return ErrRegionNotFound(targetID)
	}

	if !schedule.IsRegionHealthy(region) || !schedule.IsRegionReplicated(c, region) {
		return ErrRegionAbnormalPeer(regionID)
	}

	if !schedule.IsRegionHealthy(target) || !schedule.IsRegionReplicated(c, target) {
		return ErrRegionAbnormalPeer(targetID)
	}

	// for the case first region (start key is nil) with the last region (end key is nil) but not adjacent
	if (!bytes.Equal(region.GetStartKey(), target.GetEndKey()) || len(region.GetStartKey()) == 0) &&
		(!bytes.Equal(region.GetEndKey(), target.GetStartKey()) || len(region.GetEndKey()) == 0) {
		return ErrRegionNotAdjacent
	}

	ops, err := operator.CreateMergeRegionOperator("admin-merge-region", c, region, target, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create merge region operator", errs.ZapError(err))
		return err
	}
	if ok := c.GetOperatorController().AddOperator(ops...); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddSplitRegionOperator adds an operator to split a region.
func (h *Handler) AddSplitRegionOperator(regionID uint64, policyStr string, keys []string) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	policy, ok := pdpb.CheckPolicy_value[strings.ToUpper(policyStr)]
	if !ok {
		return errors.Errorf("check policy %s is not supported", policyStr)
	}

	var splitKeys [][]byte
	if pdpb.CheckPolicy(policy) == pdpb.CheckPolicy_USEKEY {
		for i := range keys {
			k, err := hex.DecodeString(keys[i])
			if err != nil {
				return errors.Errorf("split key %s is not in hex format", keys[i])
			}
			splitKeys = append(splitKeys, k)
		}
	}

	op, err := operator.CreateSplitRegionOperator("admin-split-region", region, operator.OpAdmin, pdpb.CheckPolicy(policy), splitKeys)
	if err != nil {
		return err
	}

	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddScatterRegionOperator adds an operator to scatter a region.
func (h *Handler) AddScatterRegionOperator(regionID uint64, group string) error {
	c, err := h.GetRaftCluster()
	if err != nil {
		return err
	}

	region := c.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	if c.IsRegionHot(region) {
		return errors.Errorf("region %d is a hot region", regionID)
	}

	op, err := c.GetRegionScatter().Scatter(region, group)
	if err != nil {
		return err
	}

	if op == nil {
		return nil
	}
	if ok := c.GetOperatorController().AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddScatterRegionsOperators add operators to scatter regions and return the processed percentage and error
func (h *Handler) AddScatterRegionsOperators(regionIDs []uint64, startRawKey, endRawKey, group string, retryLimit int) (int, error) {
	c, err := h.GetRaftCluster()
	if err != nil {
		return 0, err
	}
	var ops []*operator.Operator
	var failures map[uint64]error
	// If startKey and endKey are both defined, use them first.
	if len(startRawKey) > 0 && len(endRawKey) > 0 {
		startKey, err := hex.DecodeString(startRawKey)
		if err != nil {
			return 0, err
		}
		endKey, err := hex.DecodeString(endRawKey)
		if err != nil {
			return 0, err
		}
		ops, failures, err = c.GetRegionScatter().ScatterRegionsByRange(startKey, endKey, group, retryLimit)
		if err != nil {
			return 0, err
		}
	} else {
		ops, failures, err = c.GetRegionScatter().ScatterRegionsByID(regionIDs, group, retryLimit)
		if err != nil {
			return 0, err
		}
	}
	// If there existed any operator failed to be added into Operator Controller, add its regions into unProcessedRegions
	for _, op := range ops {
		op.AttachKind(operator.OpAdmin)
		if ok := c.GetOperatorController().AddOperator(op); !ok {
			failures[op.RegionID()] = fmt.Errorf("region %v failed to add operator", op.RegionID())
		}
	}
	percentage := 100
	if len(failures) > 0 {
		percentage = 100 - 100*len(failures)/(len(ops)+len(failures))
	}
	return percentage, nil
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

// GetOfflinePeer gets the region with offline peer.
func (h *Handler) GetOfflinePeer(typ statistics.RegionStatisticType) ([]*core.RegionInfo, error) {
	c := h.s.GetRaftCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	return c.GetOfflineRegionStatsByType(typ), nil
}

// ResetTS resets the ts with specified tso.
func (h *Handler) ResetTS(ts uint64) error {
	tsoAllocator, err := h.s.tsoAllocatorManager.GetAllocator(tso.GlobalDCLocation)
	if err != nil {
		return err
	}
	if tsoAllocator == nil {
		return ErrServerNotStarted
	}
	return tsoAllocator.SetTSO(ts)
}

// SetStoreLimitScene sets the limit values for different scenes
func (h *Handler) SetStoreLimitScene(scene *storelimit.Scene, limitType storelimit.Type) {
	cluster := h.s.GetRaftCluster()
	cluster.GetStoreLimiter().ReplaceStoreLimitScene(scene, limitType)
}

// GetStoreLimitScene returns the limit values for different scenes
func (h *Handler) GetStoreLimitScene(limitType storelimit.Type) *storelimit.Scene {
	cluster := h.s.GetRaftCluster()
	return cluster.GetStoreLimiter().StoreLimitScene(limitType)
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
	c.LoadPlugin(pluginPath, ch)
	return nil
}

// PluginUnload unloads the plugin referenced by the pluginPath
func (h *Handler) PluginUnload(pluginPath string) error {
	h.pluginChMapLock.Lock()
	defer h.pluginChMapLock.Unlock()
	if ch, ok := h.pluginChMap[pluginPath]; ok {
		ch <- cluster.PluginUnload
		return nil
	}
	return ErrPluginNotFound(pluginPath)
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

// IsLeader return ture if this server is leader
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
			var peerID uint64
			var isLearner bool
			for _, peer := range meta.Peers {
				if peer.StoreId == hotPeerStat.StoreID {
					peerID = peer.Id
					isLearner = core.IsLearner(peer)
					break
				}
			}
			stat := storage.HistoryHotRegion{
				// store in ms.
				UpdateTime:     hotPeerStat.LastUpdateTime.UnixNano() / int64(time.Millisecond),
				RegionID:       hotPeerStat.RegionID,
				StoreID:        hotPeerStat.StoreID,
				PeerID:         peerID,
				IsLeader:       peerID == region.GetLeader().Id,
				IsLearner:      isLearner,
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

func checkStoreState(rc *cluster.RaftCluster, storeID uint64) error {
	store := rc.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}
	if store.IsRemoved() {
		return errs.ErrStoreRemoved.FastGenByArgs(storeID)
	}
	if store.IsUnhealthy() {
		return errs.ErrStoreUnhealthy.FastGenByArgs(storeID)
	}
	return nil
}

// RedirectSchedulerUpdate update scheduler config. Export this func to help handle damaged store.
func (h *Handler) redirectSchedulerUpdate(name string, storeID float64) error {
	input := make(map[string]interface{})
	input["name"] = name
	input["store_id"] = storeID
	updateURL := fmt.Sprintf("%s/%s/%s/config", h.GetAddr(), schedulerConfigPrefix, name)
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

// GetPausedSchedulerDelayAt returns paused unix timestamp when a scheduler is paused
func (h *Handler) GetPausedSchedulerDelayAt(name string) (int64, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return -1, err
	}
	return rc.GetPausedSchedulerDelayAt(name)
}

// GetPausedSchedulerDelayUntil returns resume unix timestamp when a scheduler is paused
func (h *Handler) GetPausedSchedulerDelayUntil(name string) (int64, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return -1, err
	}
	return rc.GetPausedSchedulerDelayUntil(name)
}
