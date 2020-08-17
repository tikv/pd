// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/unrolled/render"
)

var errPlacementDisabled = errors.New("placement rules feature is disabled")

type ruleHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRulesHandler(svr *server.Server, rd *render.Render) *ruleHandler {
	return &ruleHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags rule
// @Summary List all rules of cluster.
// @Produce json
// @Success 200 {array} placement.Rule
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Router /config/rules [get]
func (h *ruleHandler) GetAll(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	rules := cluster.GetRuleManager().GetAllRules()
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags rule
// @Summary Set all rules for the cluster. If there is an error, modifications are promised to be rollback in memory, but may fail to rollback disk. You propabably want to request again to make rules in memory/disk consistent.
// @Produce json
// @Param rules body []placement.Rule true "Parameters of rules"
// @Success 200 {string} string "Update rules successfully."
// @Failure 400 {string} string "The input is invalid."
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config/rules [get]
func (h *ruleHandler) SetAll(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	var rules []*placement.Rule
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &rules); err != nil {
		return
	}
	for _, rule := range rules {
		if err := h.checkRule(rule); err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if err := cluster.GetRuleManager().SetRules(rules); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Update rules successfully.")
}

// @Tags rule
// @Summary List all rules of cluster by group.
// @Param group path string true "The name of group"
// @Produce json
// @Success 200 {array} placement.Rule
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Router /config/rules/group/{group} [get]
func (h *ruleHandler) GetAllByGroup(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	group := mux.Vars(r)["group"]
	rules := cluster.GetRuleManager().GetRulesByGroup(group)
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags rule
// @Summary List all rules of cluster by region.
// @Param region path string true "The name of region"
// @Produce json
// @Success 200 {array} placement.Rule
// @Failure 400 {string} string "The input is invalid."
// @Failure 404 {string} string "The region does not exist."
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Router /config/rules/region/{region} [get]
func (h *ruleHandler) GetAllByRegion(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	regionStr := mux.Vars(r)["region"]
	regionID, err := strconv.ParseUint(regionStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, "invalid region id")
		return
	}
	region := cluster.GetRegion(regionID)
	if region == nil {
		h.rd.JSON(w, http.StatusNotFound, server.ErrRegionNotFound(regionID).Error())
		return
	}
	rules := cluster.GetRuleManager().GetRulesForApplyRegion(region)
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags rule
// @Summary List all rules of cluster by key.
// @Param key path string true "The name of key"
// @Produce json
// @Success 200 {array} placement.Rule
// @Failure 400 {string} string "The input is invalid."
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Router /config/rules/key/{key} [get]
func (h *ruleHandler) GetAllByKey(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	keyHex := mux.Vars(r)["key"]
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, "key should be in hex format")
		return
	}
	rules := cluster.GetRuleManager().GetRulesByKey(key)
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags rule
// @Summary Get rule of cluster by group and id.
// @Param group path string true "The name of group"
// @Param id path string true "Rule Id"
// @Produce json
// @Success 200 {object} placement.Rule
// @Failure 404 {string} string "The rule does not exist."
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Router /config/rule/{group}/{id} [get]
func (h *ruleHandler) Get(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	group, id := mux.Vars(r)["group"], mux.Vars(r)["id"]
	rule := cluster.GetRuleManager().GetRule(group, id)
	if rule == nil {
		h.rd.JSON(w, http.StatusNotFound, nil)
		return
	}
	h.rd.JSON(w, http.StatusOK, rule)
}

// @Tags rule
// @Summary Update rule of cluster.
// @Accept json
// @Param rule body placement.Rule true "Parameters of rule"
// @Produce json
// @Success 200 {string} string "Update rule successfully."
// @Failure 400 {string} string "The input is invalid."
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config/rule [post]
func (h *ruleHandler) Set(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	var rule placement.Rule
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &rule); err != nil {
		return
	}
	if err := h.checkRule(&rule); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	oldRule := cluster.GetRuleManager().GetRule(rule.GroupID, rule.ID)
	if err := cluster.GetRuleManager().SetRule(&rule); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	cluster.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
	if oldRule != nil {
		cluster.AddSuspectKeyRange(oldRule.StartKey, oldRule.EndKey)
	}
	h.rd.JSON(w, http.StatusOK, "Update rule successfully.")
}

func (h *ruleHandler) checkRule(r *placement.Rule) error {
	start, err := hex.DecodeString(r.StartKeyHex)
	if err != nil {
		return errors.Wrap(err, "start key is not in hex format")
	}
	end, err := hex.DecodeString(r.EndKeyHex)
	if err != nil {
		return errors.Wrap(err, "end key is not hex format")
	}
	if len(end) > 0 && bytes.Compare(end, start) <= 0 {
		return errors.New("endKey should be greater than startKey")
	}

	keyType := h.svr.GetConfig().PDServerCfg.KeyType
	if keyType == core.Table.String() || keyType == core.Txn.String() {
		if len(start) > 0 {
			if _, _, err = codec.DecodeBytes(start); err != nil {
				return errors.Wrapf(err, "start key should be encoded in %s mode", keyType)
			}
		}
		if len(end) > 0 {
			if _, _, err = codec.DecodeBytes(end); err != nil {
				return errors.Wrapf(err, "end key should be encoded in %s mode", keyType)
			}
		}
	}

	return nil
}

// @Tags rule
// @Summary Delete rule of cluster.
// @Param group path string true "The name of group"
// @Param id path string true "Rule Id"
// @Produce json
// @Success 200 {string} string "Delete rule successfully."
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config/rule/{group}/{id} [delete]
func (h *ruleHandler) Delete(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	group, id := mux.Vars(r)["group"], mux.Vars(r)["id"]
	rule := cluster.GetRuleManager().GetRule(group, id)
	if err := cluster.GetRuleManager().DeleteRule(group, id); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if rule != nil {
		cluster.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
	}

	h.rd.JSON(w, http.StatusOK, "Delete rule successfully.")
}

// @Tags rule
// @Summary Batch operations for the cluster. Operations should be independent(different ID). If there is an error, modifications are promised to be rollback in memory, but may fail to rollback disk. You propabably want to request again to make rules in memory/disk consistent.
// @Produce json
// @Param operations body []placement.RuleOp true "Parameters of rule operations"
// @Success 200 {string} string "Batch operations successfully."
// @Failure 400 {string} string "The input is invalid."
// @Failure 412 {string} string "Placement rules feature is disabled."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config/rules/batch [post]
func (h *ruleHandler) Batch(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	var opts []placement.RuleOp
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &opts); err != nil {
		return
	}
	for _, opt := range opts {
		switch opt.Action {
		case placement.RuleOpAdd:
			if err := h.checkRule(opt.Rule); err != nil {
				h.rd.JSON(w, http.StatusBadRequest, err.Error())
				return
			}
		}
	}
	if err := cluster.GetRuleManager().Batch(opts); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Batch operations successfully.")
}
