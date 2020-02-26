// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/pd/v4/pkg/apiutil"
	"github.com/pingcap/pd/v4/pkg/codec"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pkg/errors"
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

func (h *ruleHandler) GetAll(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	rules := cluster.GetRuleManager().GetAllRules()
	h.rd.JSON(w, http.StatusOK, rules)
}

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
	if err := cluster.GetRuleManager().SetRule(&rule); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
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
	if len(start) > 0 && bytes.Compare(end, start) <= 0 {
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

func (h *ruleHandler) Delete(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r.Context())
	if !cluster.IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errPlacementDisabled.Error())
		return
	}
	group, id := mux.Vars(r)["group"], mux.Vars(r)["id"]
	if err := cluster.GetRuleManager().DeleteRule(group, id); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
