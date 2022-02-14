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

package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/unrolled/render"
)

type operatorHandler struct {
	*server.Handler
	r *render.Render
}

func newOperatorHandler(handler *server.Handler, r *render.Render) *operatorHandler {
	return &operatorHandler{
		Handler: handler,
		r:       r,
	}
}

const (
	pRegionID = "region_id"
	pKind     = "kind"
)

// @Tags operator
// @Summary Get a Region's pending operator.
// @Param region_id path int true "A Region's Id"
// @Produce json
// @Success 200 {object} schedule.OperatorWithStatus
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /operators/{region_id} [get]
func (h *operatorHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)[pRegionID]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	op, err := h.GetOperatorStatus(regionID)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, op)
}

const (
	kindAdmin   = "admin"
	kindLeader  = "leader"
	kingRegion  = "region"
	kindWaiting = "waiting"
)

// @Tags operator
// @Summary List pending operators.
// @Param kind query string false "Specify the operator kind." Enums(admin, leader, region)
// @Produce json
// @Success 200 {array} operator.Operator
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /operators [get]
func (h *operatorHandler) List(w http.ResponseWriter, r *http.Request) {
	var (
		results []*operator.Operator
		ops     []*operator.Operator
		err     error
	)

	kinds, ok := r.URL.Query()[pKind]
	if !ok {
		results, err = h.GetOperators()
		if err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		for _, kind := range kinds {
			switch kind {
			case kindAdmin:
				ops, err = h.GetAdminOperators()
			case kindLeader:
				ops, err = h.GetLeaderOperators()
			case kingRegion:
				ops, err = h.GetRegionOperators()
			case kindWaiting:
				ops, err = h.GetWaitingOperators()
			}
			if err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			results = append(results, ops...)
		}
	}

	h.r.JSON(w, http.StatusOK, results)
}

const (
	opTransferLeader = "transfer-leader"
	opTransferRegion = "transfer-region"
	opTransferPeer   = "transfer-peer"
	opAddPeer        = "add-peer"
	opAddLearner     = "add-learner"
	opRemovePeer     = "remove-peer"
	opMergeRegion    = "merge-region"
	opSplitRegion    = "split-region"
	opScatterRegion  = "scatter-region"
	opScatterRegions = "scatter-regions"
)

const (
	pName           = "name"
	pToStoreID      = "to_store_id"
	pToStoreIDs     = "to_store_ids"
	pPeerRoles      = "peer_roles"
	pFromStoreID    = "from_store_id"
	pStoreID        = "store_id"
	pSourceRegionID = "source_region_id"
	pTargetRegionID = "target_region_id"
	pPolicy         = "policy"
	pGroup          = "group"
	pStartKey       = "start_key"
	pEndKey         = "end_key"
	pRegionIDs      = "region_ids"
	pRetryLimit     = "retry_limit"
	pKeys           = "keys"
)

// FIXME: details of input json body params
// @Tags operator
// @Summary Create an operator.
// @Accept json
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "The operator is created."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /operators [post]
func (h *operatorHandler) Post(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	name, ok := input[pName].(string)
	if !ok {
		respWithMissParam(w, h.r, pName)
		return
	}

	parseUInt64Param := func(name string) (uint64, bool) {
		v, ok := input[name].(float64)
		if !ok {
			respWithMissParam(w, h.r, name)
			return 0, ok
		}
		return uint64(v), ok
	}

	switch name {
	case opTransferLeader:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}

		storeID, ok := parseUInt64Param(pToStoreID)
		if !ok {
			return
		}
		if err := h.AddTransferLeaderOperator(regionID, storeID); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opTransferRegion:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}
		storeIDs, ok := parseStoreIDsAndPeerRole(input[pToStoreIDs], input[pPeerRoles])
		if !ok {
			respWithInvalidParam(w, h.r, fmt.Sprintf("%s(store ids to transfer region to)", pToStoreIDs))
			return
		}
		if len(storeIDs) == 0 {
			respWithMissParam(w, h.r, fmt.Sprintf("%s(store ids to transfer region to)", pToStoreIDs))
			return
		}
		if err := h.AddTransferRegionOperator(regionID, storeIDs); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opTransferPeer:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}
		fromID, ok := parseUInt64Param(pFromStoreID)
		if !ok {
			return
		}

		toID, ok := parseUInt64Param(pToStoreID)
		if !ok {
			return
		}

		if err := h.AddTransferPeerOperator(regionID, fromID, toID); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opAddPeer:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}
		storeID, ok := parseUInt64Param(pStoreID)
		if !ok {
			return
		}
		if err := h.AddAddPeerOperator(regionID, storeID); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opAddLearner:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}
		storeID, ok := parseUInt64Param(pStoreID)
		if !ok {
			return
		}
		if err := h.AddAddLearnerOperator(regionID, storeID); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opRemovePeer:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}
		storeID, ok := parseUInt64Param(pStoreID)
		if !ok {
			return
		}
		if err := h.AddRemovePeerOperator(regionID, storeID); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opMergeRegion:
		regionID, ok := parseUInt64Param(pSourceRegionID)
		if !ok {
			return
		}
		targetID, ok := parseUInt64Param(pTargetRegionID)
		if !ok {
			return
		}
		if err := h.AddMergeRegionOperator(regionID, targetID); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opSplitRegion:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}
		policy, ok := input[pPolicy].(string)
		if !ok {
			respWithMissParam(w, h.r, pPolicy)
			return
		}
		keys, ok := typeutil.JSONToStringSlice(input[pKeys])
		if !ok {
			respWithInvalidParam(w, h.r, pKeys)
			return
		}
		if err := h.AddSplitRegionOperator(regionID, policy, keys); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opScatterRegion:
		regionID, ok := parseUInt64Param(pRegionID)
		if !ok {
			return
		}
		group, _ := input[pGroup].(string)
		if err := h.AddScatterRegionOperator(regionID, group); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case opScatterRegions:
		// support both receiving key ranges or regionIDs
		startKey, _ := input[pStartKey].(string)
		endKey, _ := input[pEndKey].(string)

		ids, ok := typeutil.JSONToUint64Slice(input[pRegionIDs])
		if !ok {
			respWithInvalidParam(w, h.r, pRegionIDs)
			return
		}
		group, _ := input[pGroup].(string)
		// retry 5 times if retryLimit not defined
		retryLimit := 5
		if rl, ok := input[pRetryLimit].(float64); ok {
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
		h.r.JSON(w, http.StatusOK, &s)
		return
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown operator")
		return
	}
	h.r.JSON(w, http.StatusOK, "The operator is created.")
}

// @Tags operator
// @Summary Cancel a Region's pending operator.
// @Param region_id path int true "A Region's Id"
// @Produce json
// @Success 200 {string} string "The pending operator is canceled."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /operators/{region_id} [delete]
func (h *operatorHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)[pRegionID]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	if err = h.RemoveOperator(regionID); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, "The pending operator is canceled.")
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
