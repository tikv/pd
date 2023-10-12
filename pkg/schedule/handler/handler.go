// Copyright 2023 TiKV Project Authors.
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

package handler

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

// Server is the interface for handler about schedule.
// TODO: remove it after GetCluster is unified between PD server and Scheduling server.
type Server interface {
	GetCoordinator() *schedule.Coordinator
	GetCluster() sche.SchedulerCluster
}

// Handler is a handler to handle http request about schedule.
type Handler struct {
	Server
}

// NewHandler creates a new handler.
func NewHandler(server Server) *Handler {
	return &Handler{
		Server: server,
	}
}

// GetOperatorController returns OperatorController.
func (h *Handler) GetOperatorController() (*operator.Controller, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return co.GetOperatorController(), nil
}

// GetRegionScatterer returns RegionScatterer.
func (h *Handler) GetRegionScatterer() (*scatter.RegionScatterer, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return co.GetRegionScatterer(), nil
}

// GetOperator returns the region operator.
func (h *Handler) GetOperator(regionID uint64) (*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}

	op := c.GetOperator(regionID)
	if op == nil {
		return nil, errs.ErrOperatorNotFound
	}

	return op, nil
}

// GetOperatorStatus returns the status of the region operator.
func (h *Handler) GetOperatorStatus(regionID uint64) (*operator.OpWithStatus, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}

	op := c.GetOperatorStatus(regionID)
	if op == nil {
		return nil, errs.ErrOperatorNotFound
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
		return errs.ErrOperatorNotFound
	}

	_ = c.RemoveOperator(op, operator.AdminStop)
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

// GetOperatorsByKinds returns the running operators by kinds.
func (h *Handler) GetOperatorsByKinds(kinds []string) ([]*operator.Operator, error) {
	var (
		results []*operator.Operator
		ops     []*operator.Operator
		err     error
	)
	for _, kind := range kinds {
		switch kind {
		case operator.OpAdmin.String():
			ops, err = h.GetAdminOperators()
		case operator.OpLeader.String():
			ops, err = h.GetLeaderOperators()
		case operator.OpRegion.String():
			ops, err = h.GetRegionOperators()
		case operator.OpWaiting:
			ops, err = h.GetWaitingOperators()
		}
		if err != nil {
			return nil, err
		}
		results = append(results, ops...)
	}
	return results, nil
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
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperatorsOfKind(operator.OpAdmin), nil
}

// GetLeaderOperators returns the running leader operators.
func (h *Handler) GetLeaderOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperatorsOfKind(operator.OpLeader), nil
}

// GetRegionOperators returns the running region operators.
func (h *Handler) GetRegionOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperatorsOfKind(operator.OpRegion), nil
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
		return nil, errs.ErrOperatorNotFound
	}
	return records, nil
}

// HandleOperatorCreation processes the request and creates an operator based on the provided input.
// It supports various types of operators such as transfer-leader, transfer-region, add-peer, remove-peer, merge-region, split-region, scatter-region, and scatter-regions.
// The function validates the input, performs the corresponding operation, and returns the HTTP status code, response body, and any error encountered during the process.
func (h *Handler) HandleOperatorCreation(input map[string]interface{}) (int, interface{}, error) {
	name, ok := input["name"].(string)
	if !ok {
		return http.StatusBadRequest, nil, errors.Errorf("missing operator name")
	}
	switch name {
	case "transfer-leader":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		storeID, ok := input["to_store_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing store id to transfer leader to")
		}
		if err := h.AddTransferLeaderOperator(uint64(regionID), uint64(storeID)); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "transfer-region":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		storeIDs, ok := parseStoreIDsAndPeerRole(input["to_store_ids"], input["peer_roles"])
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("invalid store ids to transfer region to")
		}
		if len(storeIDs) == 0 {
			return http.StatusBadRequest, nil, errors.Errorf("missing store ids to transfer region to")
		}
		if err := h.AddTransferRegionOperator(uint64(regionID), storeIDs); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "transfer-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		fromID, ok := input["from_store_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("invalid store id to transfer peer from")
		}
		toID, ok := input["to_store_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("invalid store id to transfer peer to")
		}
		if err := h.AddTransferPeerOperator(uint64(regionID), uint64(fromID), uint64(toID)); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "add-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("invalid store id to transfer peer to")
		}
		if err := h.AddAddPeerOperator(uint64(regionID), uint64(storeID)); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "add-learner":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("invalid store id to transfer peer to")
		}
		if err := h.AddAddLearnerOperator(uint64(regionID), uint64(storeID)); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "remove-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("invalid store id to transfer peer to")
		}
		if err := h.AddRemovePeerOperator(uint64(regionID), uint64(storeID)); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "merge-region":
		regionID, ok := input["source_region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		targetID, ok := input["target_region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("invalid target region id to merge to")
		}
		if err := h.AddMergeRegionOperator(uint64(regionID), uint64(targetID)); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "split-region":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		policy, ok := input["policy"].(string)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing split policy")
		}
		var keys []string
		if ks, ok := input["keys"]; ok {
			for _, k := range ks.([]interface{}) {
				key, ok := k.(string)
				if !ok {
					return http.StatusBadRequest, nil, errors.Errorf("bad format keys")
				}
				keys = append(keys, key)
			}
		}
		if err := h.AddSplitRegionOperator(uint64(regionID), policy, keys); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "scatter-region":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("missing region id")
		}
		group, _ := input["group"].(string)
		if err := h.AddScatterRegionOperator(uint64(regionID), group); err != nil {
			return http.StatusInternalServerError, nil, err
		}
	case "scatter-regions":
		// support both receiving key ranges or regionIDs
		startKey, _ := input["start_key"].(string)
		endKey, _ := input["end_key"].(string)
		ids, ok := typeutil.JSONToUint64Slice(input["region_ids"])
		if !ok {
			return http.StatusBadRequest, nil, errors.Errorf("region_ids is invalid")
		}
		group, _ := input["group"].(string)
		// retry 5 times if retryLimit not defined
		retryLimit := 5
		if rl, ok := input["retry_limit"].(float64); ok {
			retryLimit = int(rl)
		}
		processedPercentage, err := h.AddScatterRegionsOperators(ids, startKey, endKey, group, retryLimit)
		errorMessage := ""
		if err != nil {
			errorMessage = err.Error()
		}
		s := struct {
			ProcessedPercentage int    `json:"processed-percentage"`
			Error               string `json:"error"`
		}{
			ProcessedPercentage: processedPercentage,
			Error:               errorMessage,
		}
		return http.StatusOK, s, nil
	default:
		return http.StatusBadRequest, nil, errors.Errorf("unknown operator")
	}
	return http.StatusOK, nil, nil
}

// AddTransferLeaderOperator adds an operator to transfer leader to the store.
func (h *Handler) AddTransferLeaderOperator(regionID uint64, storeID uint64) error {
	c := h.GetCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
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
	return h.addOperator(op)
}

// AddTransferRegionOperator adds an operator to transfer region to the stores.
func (h *Handler) AddTransferRegionOperator(regionID uint64, storeIDs map[uint64]placement.PeerRoleType) error {
	c := h.GetCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	if c.GetSharedConfig().IsPlacementRulesEnabled() {
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
	return h.addOperator(op)
}

// AddTransferPeerOperator adds an operator to transfer peer.
func (h *Handler) AddTransferPeerOperator(regionID uint64, fromStoreID, toStoreID uint64) error {
	c := h.GetCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	oldPeer := region.GetStorePeer(fromStoreID)
	if oldPeer == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	if err := checkStoreState(c, toStoreID); err != nil {
		return err
	}

	newPeer := &metapb.Peer{StoreId: toStoreID, Role: oldPeer.GetRole(), IsWitness: oldPeer.GetIsWitness()}
	op, err := operator.CreateMovePeerOperator("admin-move-peer", c, region, operator.OpAdmin, fromStoreID, newPeer)
	if err != nil {
		log.Debug("fail to create move peer operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// checkAdminAddPeerOperator checks adminAddPeer operator with given region ID and store ID.
func (h *Handler) checkAdminAddPeerOperator(regionID uint64, toStoreID uint64) (sche.SharedCluster, *core.RegionInfo, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return nil, nil, errs.ErrRegionNotFound.FastGenByArgs(regionID)
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
	return h.addOperator(op)
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
	return h.addOperator(op)
}

// AddRemovePeerOperator adds an operator to remove peer.
func (h *Handler) AddRemovePeerOperator(regionID uint64, fromStoreID uint64) error {
	c := h.GetCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	if region.GetStorePeer(fromStoreID) == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	op, err := operator.CreateRemovePeerOperator("admin-remove-peer", c, operator.OpAdmin, region, fromStoreID)
	if err != nil {
		log.Debug("fail to create move peer operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// AddMergeRegionOperator adds an operator to merge region.
func (h *Handler) AddMergeRegionOperator(regionID uint64, targetID uint64) error {
	c := h.GetCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	target := c.GetRegion(targetID)
	if target == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(targetID)
	}

	if !filter.IsRegionHealthy(region) || !filter.IsRegionReplicated(c, region) {
		return errs.ErrRegionAbnormalPeer.FastGenByArgs(regionID)
	}

	if !filter.IsRegionHealthy(target) || !filter.IsRegionReplicated(c, target) {
		return errs.ErrRegionAbnormalPeer.FastGenByArgs(targetID)
	}

	// for the case first region (start key is nil) with the last region (end key is nil) but not adjacent
	if (!bytes.Equal(region.GetStartKey(), target.GetEndKey()) || len(region.GetStartKey()) == 0) &&
		(!bytes.Equal(region.GetEndKey(), target.GetStartKey()) || len(region.GetEndKey()) == 0) {
		return errs.ErrRegionNotAdjacent
	}

	ops, err := operator.CreateMergeRegionOperator("admin-merge-region", c, region, target, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create merge region operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(ops...)
}

// AddSplitRegionOperator adds an operator to split a region.
func (h *Handler) AddSplitRegionOperator(regionID uint64, policyStr string, keys []string) error {
	c := h.GetCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
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

	return h.addOperator(op)
}

// AddScatterRegionOperator adds an operator to scatter a region.
func (h *Handler) AddScatterRegionOperator(regionID uint64, group string) error {
	c := h.GetCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	if c.IsRegionHot(region) {
		return errors.Errorf("region %d is a hot region", regionID)
	}

	s, err := h.GetRegionScatterer()
	if err != nil {
		return err
	}

	op, err := s.Scatter(region, group, false)
	if err != nil {
		return err
	}

	if op == nil {
		return nil
	}
	return h.addOperator(op)
}

// AddScatterRegionsOperators add operators to scatter regions and return the processed percentage and error
func (h *Handler) AddScatterRegionsOperators(regionIDs []uint64, startRawKey, endRawKey, group string, retryLimit int) (int, error) {
	s, err := h.GetRegionScatterer()
	if err != nil {
		return 0, err
	}
	opsCount := 0
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
		opsCount, failures, err = s.ScatterRegionsByRange(startKey, endKey, group, retryLimit)
		if err != nil {
			return 0, err
		}
	} else {
		opsCount, failures, err = s.ScatterRegionsByID(regionIDs, group, retryLimit, false)
		if err != nil {
			return 0, err
		}
	}
	percentage := 100
	if len(failures) > 0 {
		percentage = 100 - 100*len(failures)/(opsCount+len(failures))
	}
	return percentage, nil
}

func (h *Handler) addOperator(ops ...*operator.Operator) error {
	oc, err := h.GetOperatorController()
	if err != nil {
		return err
	}

	if ok := oc.AddOperator(ops...); !ok {
		return errors.WithStack(errs.ErrAddOperator)
	}
	return nil
}

func checkStoreState(c sche.SharedCluster, storeID uint64) error {
	store := c.GetStore(storeID)
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

func parseStoreIDsAndPeerRole(ids interface{}, roles interface{}) (map[uint64]placement.PeerRoleType, bool) {
	items, ok := ids.([]interface{})
	if !ok {
		return nil, false
	}
	storeIDToPeerRole := make(map[uint64]placement.PeerRoleType)
	storeIDs := make([]uint64, 0, len(items))
	for _, item := range items {
		id, ok := item.(float64)
		if !ok {
			return nil, false
		}
		storeIDs = append(storeIDs, uint64(id))
		storeIDToPeerRole[uint64(id)] = ""
	}

	peerRoles, ok := roles.([]interface{})
	// only consider roles having the same length with ids as the valid case
	if ok && len(peerRoles) == len(storeIDs) {
		for i, v := range storeIDs {
			switch pr := peerRoles[i].(type) {
			case string:
				storeIDToPeerRole[v] = placement.PeerRoleType(pr)
			default:
			}
		}
	}
	return storeIDToPeerRole, true
}

// GetCheckerStatus returns the status of the checker.
func (h *Handler) GetCheckerStatus(name string) (map[string]bool, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	isPaused, err := co.IsCheckerPaused(name)
	if err != nil {
		return nil, err
	}
	return map[string]bool{
		"paused": isPaused,
	}, nil
}

// GetSchedulerNames returns all names of schedulers.
func (h *Handler) GetSchedulerNames() ([]string, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return co.GetSchedulersController().GetSchedulerNames(), nil
}

type schedulerPausedPeriod struct {
	Name     string    `json:"name"`
	PausedAt time.Time `json:"paused_at"`
	ResumeAt time.Time `json:"resume_at"`
}

// GetSchedulerByStatus returns all names of schedulers by status.
func (h *Handler) GetSchedulerByStatus(status string, needTS bool) (interface{}, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	sc := co.GetSchedulersController()
	schedulers := sc.GetSchedulerNames()
	switch status {
	case "paused":
		var pausedSchedulers []string
		pausedPeriods := []schedulerPausedPeriod{}
		for _, scheduler := range schedulers {
			paused, err := sc.IsSchedulerPaused(scheduler)
			if err != nil {
				return nil, err
			}
			if paused {
				if needTS {
					s := schedulerPausedPeriod{
						Name:     scheduler,
						PausedAt: time.Time{},
						ResumeAt: time.Time{},
					}
					pausedAt, err := sc.GetPausedSchedulerDelayAt(scheduler)
					if err != nil {
						return nil, err
					}
					s.PausedAt = time.Unix(pausedAt, 0)
					resumeAt, err := sc.GetPausedSchedulerDelayUntil(scheduler)
					if err != nil {
						return nil, err
					}
					s.ResumeAt = time.Unix(resumeAt, 0)
					pausedPeriods = append(pausedPeriods, s)
				} else {
					pausedSchedulers = append(pausedSchedulers, scheduler)
				}
			}
		}
		if needTS {
			return pausedPeriods, nil
		}
		return pausedSchedulers, nil
	case "disabled":
		var disabledSchedulers []string
		for _, scheduler := range schedulers {
			disabled, err := sc.IsSchedulerDisabled(scheduler)
			if err != nil {
				return nil, err
			}
			if disabled {
				disabledSchedulers = append(disabledSchedulers, scheduler)
			}
		}
		return disabledSchedulers, nil
	default:
		return schedulers, nil
	}
}

// GetDiagnosticResult returns the diagnostic results of the specified scheduler.
func (h *Handler) GetDiagnosticResult(name string) (*schedulers.DiagnosticResult, error) {
	if _, ok := schedulers.DiagnosableSummaryFunc[name]; !ok {
		return nil, errs.ErrSchedulerUndiagnosable.FastGenByArgs(name)
	}
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	result, err := co.GetDiagnosticResult(name)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// PauseOrResumeScheduler pauses a scheduler for delay seconds or resume a paused scheduler.
// t == 0 : resume scheduler.
// t > 0 : scheduler delays t seconds.
func (h *Handler) PauseOrResumeScheduler(name string, t int64) (err error) {
	co := h.GetCoordinator()
	if co == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	if err = co.GetSchedulersController().PauseOrResumeScheduler(name, t); err != nil {
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
func (h *Handler) PauseOrResumeChecker(name string, t int64) (err error) {
	co := h.GetCoordinator()
	if co == nil {
		return errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	if err = co.PauseOrResumeChecker(name, t); err != nil {
		if t == 0 {
			log.Error("can not resume checker", zap.String("checker-name", name), errs.ZapError(err))
		} else {
			log.Error("can not pause checker", zap.String("checker-name", name), errs.ZapError(err))
		}
	}
	return err
}

// GetStore returns a store.
// If store does not exist, return error.
func (h *Handler) GetStore(storeID uint64) (*core.StoreInfo, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	store := c.GetStore(storeID)
	if store == nil {
		return nil, errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}
	return store, nil
}

// GetStores returns all stores in the cluster.
func (h *Handler) GetStores() ([]*core.StoreInfo, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	storeMetas := c.GetStores()
	stores := make([]*core.StoreInfo, 0, len(storeMetas))
	for _, store := range storeMetas {
		store, err := h.GetStore(store.GetID())
		if err != nil {
			return nil, err
		}
		stores = append(stores, store)
	}
	return stores, nil
}

// GetHotRegions gets hot regions' statistics by RWType and storeIDs.
// If storeIDs is empty, it returns all hot regions' statistics by RWType.
func (h *Handler) GetHotRegions(typ utils.RWType, storeIDs ...uint64) (*statistics.StoreHotPeersInfos, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return co.GetHotRegions(typ, storeIDs...), nil
}

// HotStoreStats is used to record the status of hot stores.
type HotStoreStats struct {
	BytesWriteStats map[uint64]float64 `json:"bytes-write-rate,omitempty"`
	BytesReadStats  map[uint64]float64 `json:"bytes-read-rate,omitempty"`
	KeysWriteStats  map[uint64]float64 `json:"keys-write-rate,omitempty"`
	KeysReadStats   map[uint64]float64 `json:"keys-read-rate,omitempty"`
	QueryWriteStats map[uint64]float64 `json:"query-write-rate,omitempty"`
	QueryReadStats  map[uint64]float64 `json:"query-read-rate,omitempty"`
}

// GetHotStores gets all hot stores stats.
func (h *Handler) GetHotStores() (*HotStoreStats, error) {
	stats := &HotStoreStats{
		BytesWriteStats: make(map[uint64]float64),
		BytesReadStats:  make(map[uint64]float64),
		KeysWriteStats:  make(map[uint64]float64),
		KeysReadStats:   make(map[uint64]float64),
		QueryWriteStats: make(map[uint64]float64),
		QueryReadStats:  make(map[uint64]float64),
	}
	stores, error := h.GetStores()
	if error != nil {
		return nil, error
	}
	storesLoads, error := h.GetStoresLoads()
	if error != nil {
		return nil, error
	}
	for _, store := range stores {
		id := store.GetID()
		if loads, ok := storesLoads[id]; ok {
			if store.IsTiFlash() {
				stats.BytesWriteStats[id] = loads[utils.StoreRegionsWriteBytes]
				stats.KeysWriteStats[id] = loads[utils.StoreRegionsWriteKeys]
			} else {
				stats.BytesWriteStats[id] = loads[utils.StoreWriteBytes]
				stats.KeysWriteStats[id] = loads[utils.StoreWriteKeys]
			}
			stats.BytesReadStats[id] = loads[utils.StoreReadBytes]
			stats.KeysReadStats[id] = loads[utils.StoreReadKeys]
			stats.QueryWriteStats[id] = loads[utils.StoreWriteQuery]
			stats.QueryReadStats[id] = loads[utils.StoreReadQuery]
		}
	}
	return stats, nil
}

// GetStoresLoads gets all hot write stores stats.
func (h *Handler) GetStoresLoads() (map[uint64][]float64, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return c.GetStoresLoads(), nil
}

// HotBucketsResponse is the response for hot buckets.
type HotBucketsResponse map[uint64][]*HotBucketsItem

// HotBucketsItem is the item of hot buckets.
type HotBucketsItem struct {
	StartKey   string `json:"start_key"`
	EndKey     string `json:"end_key"`
	HotDegree  int    `json:"hot_degree"`
	ReadBytes  uint64 `json:"read_bytes"`
	ReadKeys   uint64 `json:"read_keys"`
	WriteBytes uint64 `json:"write_bytes"`
	WriteKeys  uint64 `json:"write_keys"`
}

func convert(buckets *buckets.BucketStat) *HotBucketsItem {
	return &HotBucketsItem{
		StartKey:   core.HexRegionKeyStr(buckets.StartKey),
		EndKey:     core.HexRegionKeyStr(buckets.EndKey),
		HotDegree:  buckets.HotDegree,
		ReadBytes:  buckets.Loads[utils.RegionReadBytes],
		ReadKeys:   buckets.Loads[utils.RegionReadKeys],
		WriteBytes: buckets.Loads[utils.RegionWriteBytes],
		WriteKeys:  buckets.Loads[utils.RegionWriteKeys],
	}
}

// GetHotBuckets returns all hot buckets stats.
func (h *Handler) GetHotBuckets(regionIDs ...uint64) (HotBucketsResponse, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	degree := c.GetSharedConfig().GetHotRegionCacheHitsThreshold()
	stats := c.BucketsStats(degree, regionIDs...)
	ret := HotBucketsResponse{}
	for regionID, stats := range stats {
		ret[regionID] = make([]*HotBucketsItem, len(stats))
		for i, stat := range stats {
			ret[regionID][i] = convert(stat)
		}
	}
	return ret, nil
}

// GetRuleManager returns the rule manager.
func (h *Handler) GetRuleManager() (*placement.RuleManager, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped
	}
	if !c.GetSharedConfig().IsPlacementRulesEnabled() {
		return nil, errs.ErrPlacementDisabled
	}
	return c.GetRuleManager(), nil
}

// PreCheckForRegion checks if the region is valid.
func (h *Handler) PreCheckForRegion(regionStr string) (*core.RegionInfo, int, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, http.StatusInternalServerError, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	regionID, err := strconv.ParseUint(regionStr, 10, 64)
	if err != nil {
		return nil, http.StatusBadRequest, errs.ErrRegionInvalidID.FastGenByArgs()
	}
	region := c.GetRegion(regionID)
	if region == nil {
		return nil, http.StatusNotFound, errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}
	return region, http.StatusOK, nil
}

// CheckRegionPlacementRule checks if the region matches the placement rules.
func (h *Handler) CheckRegionPlacementRule(region *core.RegionInfo) (*placement.RegionFit, error) {
	c := h.GetCluster()
	if c == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	manager, err := h.GetRuleManager()
	if err != nil {
		return nil, err
	}
	return manager.FitRegion(c, region), nil
}
