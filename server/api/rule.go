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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type ruleHandler struct {
	*server.Handler
	svr *server.Server
	rd  *render.Render
}

func newRulesHandler(svr *server.Server, rd *render.Render) *ruleHandler {
	return &ruleHandler{
		Handler: svr.GetHandler(),
		svr:     svr,
		rd:      rd,
	}
}

// @Tags     rule
// @Summary  List all rules of cluster.
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules [get]
func (h *ruleHandler) GetAllRules(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	rules := manager.GetAllRules()
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags     rule
// @Summary  Set all rules for the cluster. If there is an error, modifications are promised to be rollback in memory, but may fail to rollback disk. You probably want to request again to make rules in memory/disk consistent.
// @Produce  json
// @Param    rules  body      []placement.Rule  true  "Parameters of rules"
// @Success  200    {string}  string            "Update rules successfully."
// @Failure  400    {string}  string            "The input is invalid."
// @Failure  412    {string}  string            "Placement rules feature is disabled."
// @Failure  500    {string}  string            "PD server failed to proceed the request."
// @Router   /config/rules [post]
func (h *ruleHandler) SetAllRules(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	var rules []*placement.Rule
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &rules); err != nil {
		return
	}
	for _, v := range rules {
		if err := h.syncReplicateConfigWithDefaultRule(v); err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if err := manager.SetKeyType(h.svr.GetConfig().PDServerCfg.KeyType).
		SetRules(rules); err != nil {
		if errs.ErrRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Update rules successfully.")
}

// @Tags     rule
// @Summary  List all rules of cluster by group.
// @Param    group  path  string  true  "The name of group"
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/group/{group} [get]
func (h *ruleHandler) GetRuleByGroup(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	group := mux.Vars(r)["group"]
	rules := manager.GetRulesByGroup(group)
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags     rule
// @Summary  List all rules of cluster by region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/region/{region} [get]
func (h *ruleHandler) GetRulesByRegion(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionStr := mux.Vars(r)["region"]
	region, code, err := h.PreCheckForRegion(regionStr)
	if err != nil {
		h.rd.JSON(w, code, err.Error())
		return
	}
	rules := manager.GetRulesForApplyRegion(region)
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags     rule
// @Summary  List rules and matched peers related to the given region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {object}  placement.RegionFit
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/region/{region}/detail [get]
func (h *ruleHandler) CheckRegionPlacementRule(w http.ResponseWriter, r *http.Request) {
	regionStr := mux.Vars(r)["region"]
	region, code, err := h.PreCheckForRegion(regionStr)
	if err != nil {
		h.rd.JSON(w, code, err.Error())
		return
	}
	regionFit, err := h.Handler.CheckRegionPlacementRule(region)
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, regionFit)
}

// @Tags     rule
// @Summary  List all rules of cluster by key.
// @Param    key  path  string  true  "The name of key"
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/key/{key} [get]
func (h *ruleHandler) GetRulesByKey(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	keyHex := mux.Vars(r)["key"]
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, errs.ErrKeyFormat.Error())
		return
	}
	rules := manager.GetRulesByKey(key)
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags     rule
// @Summary  Get rule of cluster by group and id.
// @Param    group  path  string  true  "The name of group"
// @Param    id     path  string  true  "Rule Id"
// @Produce  json
// @Success  200  {object}  placement.Rule
// @Failure  404  {string}  string  "The rule does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Router   /config/rule/{group}/{id} [get]
func (h *ruleHandler) GetRuleByGroupAndID(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	group, id := mux.Vars(r)["group"], mux.Vars(r)["id"]
	rule := manager.GetRule(group, id)
	if rule == nil {
		h.rd.JSON(w, http.StatusNotFound, errs.ErrRuleNotFound.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, rule)
}

// @Tags     rule
// @Summary  Update rule of cluster.
// @Accept   json
// @Param    rule  body  placement.Rule  true  "Parameters of rule"
// @Produce  json
// @Success  200  {string}  string  "Update rule successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule [post]
func (h *ruleHandler) SetRule(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	var rule placement.Rule
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &rule); err != nil {
		return
	}
	oldRule := manager.GetRule(rule.GroupID, rule.ID)
	if err := h.syncReplicateConfigWithDefaultRule(&rule); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := manager.SetKeyType(h.svr.GetConfig().PDServerCfg.KeyType).
		SetRule(&rule); err != nil {
		if errs.ErrRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	cluster := getCluster(r)
	cluster.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
	if oldRule != nil {
		cluster.AddSuspectKeyRange(oldRule.StartKey, oldRule.EndKey)
	}
	h.rd.JSON(w, http.StatusOK, "Update rule successfully.")
}

// sync replicate config with default-rule
func (h *ruleHandler) syncReplicateConfigWithDefaultRule(rule *placement.Rule) error {
	// sync default rule with replicate config
	if rule.GroupID == "pd" && rule.ID == "default" {
		cfg := h.svr.GetReplicationConfig().Clone()
		cfg.MaxReplicas = uint64(rule.Count)
		if err := h.svr.SetReplicationConfig(*cfg); err != nil {
			return err
		}
	}
	return nil
}

// @Tags     rule
// @Summary  Delete rule of cluster.
// @Param    group  path  string  true  "The name of group"
// @Param    id     path  string  true  "Rule Id"
// @Produce  json
// @Success  200  {string}  string  "Delete rule successfully."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule/{group}/{id} [delete]
func (h *ruleHandler) DeleteRuleByGroup(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	group, id := mux.Vars(r)["group"], mux.Vars(r)["id"]
	rule := manager.GetRule(group, id)
	if err := manager.DeleteRule(group, id); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if rule != nil {
		cluster := getCluster(r)
		cluster.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
	}

	h.rd.JSON(w, http.StatusOK, "Delete rule successfully.")
}

// @Tags     rule
// @Summary  Batch operations for the cluster. Operations should be independent(different ID). If there is an error, modifications are promised to be rollback in memory, but may fail to rollback disk. You probably want to request again to make rules in memory/disk consistent.
// @Produce  json
// @Param    operations  body      []placement.RuleOp  true  "Parameters of rule operations"
// @Success  200         {string}  string              "Batch operations successfully."
// @Failure  400         {string}  string              "The input is invalid."
// @Failure  412         {string}  string              "Placement rules feature is disabled."
// @Failure  500         {string}  string              "PD server failed to proceed the request."
// @Router   /config/rules/batch [post]
func (h *ruleHandler) BatchRules(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	var opts []placement.RuleOp
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &opts); err != nil {
		return
	}
	if err := manager.SetKeyType(h.svr.GetConfig().PDServerCfg.KeyType).
		Batch(opts); err != nil {
		if errs.ErrRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Batch operations successfully.")
}

// @Tags     rule
// @Summary  Get rule group config by group id.
// @Param    id  path  string  true  "Group Id"
// @Produce  json
// @Success  200  {object}  placement.RuleGroup
// @Failure  404  {string}  string  "The RuleGroup does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule_group/{id} [get]
func (h *ruleHandler) GetGroupConfig(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	id := mux.Vars(r)["id"]
	group := manager.GetRuleGroup(id)
	if group == nil {
		h.rd.JSON(w, http.StatusNotFound, nil)
		return
	}
	h.rd.JSON(w, http.StatusOK, group)
}

// @Tags     rule
// @Summary  Update rule group config.
// @Accept   json
// @Param    rule  body  placement.RuleGroup  true  "Parameters of rule group"
// @Produce  json
// @Success  200  {string}  string  "Update rule group config successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule_group [post]
func (h *ruleHandler) SetGroupConfig(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	var ruleGroup placement.RuleGroup
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &ruleGroup); err != nil {
		return
	}
	if err := manager.SetRuleGroup(&ruleGroup); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	cluster := getCluster(r)
	for _, rule := range manager.GetRulesByGroup(ruleGroup.ID) {
		cluster.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
	}
	h.rd.JSON(w, http.StatusOK, "Update rule group successfully.")
}

// @Tags     rule
// @Summary  Delete rule group config.
// @Param    id  path  string  true  "Group Id"
// @Produce  json
// @Success  200  {string}  string  "Delete rule group config successfully."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule_group/{id} [delete]
func (h *ruleHandler) DeleteGroupConfig(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	id := mux.Vars(r)["id"]
	err = manager.DeleteRuleGroup(id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	cluster := getCluster(r)
	for _, r := range manager.GetRulesByGroup(id) {
		cluster.AddSuspectKeyRange(r.StartKey, r.EndKey)
	}
	h.rd.JSON(w, http.StatusOK, "Delete rule group successfully.")
}

// @Tags     rule
// @Summary  List all rule group configs.
// @Produce  json
// @Success  200  {array}   placement.RuleGroup
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule_groups [get]
func (h *ruleHandler) GetAllGroupConfigs(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ruleGroups := manager.GetRuleGroups()
	h.rd.JSON(w, http.StatusOK, ruleGroups)
}

// @Tags     rule
// @Summary  List all rules and groups configuration.
// @Produce  json
// @Success  200  {array}   placement.GroupBundle
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/placement-rule [get]
func (h *ruleHandler) GetPlacementRules(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	bundles := manager.GetAllGroupBundles()
	h.rd.JSON(w, http.StatusOK, bundles)
}

// @Tags     rule
// @Summary  Update all rules and groups configuration.
// @Param    partial  query  bool  false  "if partially update rules"  default(false)
// @Produce  json
// @Success  200  {string}  string  "Update rules and groups successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/placement-rule [post]
func (h *ruleHandler) SetPlacementRules(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	var groups []placement.GroupBundle
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &groups); err != nil {
		return
	}
	_, partial := r.URL.Query()["partial"]
	if err := manager.SetKeyType(h.svr.GetConfig().PDServerCfg.KeyType).
		SetAllGroupBundles(groups, !partial); err != nil {
		if errs.ErrRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Update rules and groups successfully.")
}

// @Tags     rule
// @Summary  Get group config and all rules belong to the group.
// @Param    group  path  string  true  "The name of group"
// @Produce  json
// @Success  200  {object}  placement.GroupBundle
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/placement-rule/{group} [get]
func (h *ruleHandler) GetPlacementRuleByGroup(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	g := mux.Vars(r)["group"]
	group := manager.GetGroupBundle(g)
	h.rd.JSON(w, http.StatusOK, group)
}

// @Tags     rule
// @Summary  Get group config and all rules belong to the group.
// @Param    group   path   string  true   "The name or name pattern of group"
// @Param    regexp  query  bool    false  "Use regular expression"  default(false)
// @Produce  plain
// @Success  200  {string}  string  "Delete group and rules successfully."
// @Failure  400  {string}  string  "Bad request."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/placement-rule [delete]
func (h *ruleHandler) DeletePlacementRuleByGroup(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	group := mux.Vars(r)["group"]
	group, err = url.PathUnescape(group)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	_, regex := r.URL.Query()["regexp"]
	if err := manager.DeleteGroupBundle(group, regex); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Delete group and rules successfully.")
}

// @Tags     rule
// @Summary  Update group and all rules belong to it.
// @Produce  json
// @Success  200  {string}  string  "Update group and rules successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/placement-rule/{group} [post]
func (h *ruleHandler) SetPlacementRuleByGroup(w http.ResponseWriter, r *http.Request) {
	manager, err := h.Handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		h.rd.JSON(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	groupID := mux.Vars(r)["group"]
	var group placement.GroupBundle
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &group); err != nil {
		return
	}
	if len(group.ID) == 0 {
		group.ID = groupID
	}
	if group.ID != groupID {
		h.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("group id %s does not match request URI %s", group.ID, groupID))
		return
	}
	if err := manager.SetKeyType(h.svr.GetConfig().PDServerCfg.KeyType).
		SetGroupBundle(group); err != nil {
		if errs.ErrRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Update group and rules successfully.")
}
