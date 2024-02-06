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

package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type regionLabelHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionLabelHandler(s *server.Server, rd *render.Render) *regionLabelHandler {
	return &regionLabelHandler{
		svr: s,
		rd:  rd,
	}
}

// @Tags     region_label
// @Summary  List all label rules of cluster.
// @Produce  json
// @Success  200  {array}  labeler.LabelRule
// @Router   /config/region-label/rules [get]
func (h *regionLabelHandler) GetAllRegionLabelRules(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	rules := cluster.GetRegionLabeler().GetAllLabelRules()
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags     region_label
// @Summary  Update region label rules in batch.
// @Accept   json
// @Param    patch  body  labeler.LabelRulePatch  true  "Patch to update rules"
// @Produce  json
// @Success  200  {string}  string  "Update region label rules successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rules [patch]
func (h *regionLabelHandler) PatchRegionLabelRules(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	var patch labeler.LabelRulePatch
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &patch); err != nil {
		return
	}
	if err := cluster.GetRegionLabeler().Patch(patch); err != nil {
		if errs.ErrRegionRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Update region label rules successfully.")
}

// @Tags     region_label
// @Summary  Get label rules of cluster by ids.
// @Param    body  body  []string  true  "IDs of query rules"
// @Produce  json
// @Success  200  {array}   labeler.LabelRule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rules/ids [get]
func (h *regionLabelHandler) GetRegionLabelRulesByIDs(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	var ids []string
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &ids); err != nil {
		return
	}
	rules, err := cluster.GetRegionLabeler().GetLabelRules(ids)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, rules)
}

// @Tags     region_label
// @Summary  Get label rule of cluster by id.
// @Param    id  path  string  true  "Rule Id"
// @Produce  json
// @Success  200  {object}  labeler.LabelRule
// @Failure  404  {string}  string  "The rule does not exist."
// @Router   /config/region-label/rule/{id} [get]
func (h *regionLabelHandler) GetRegionLabelRuleByID(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	id, err := url.PathUnescape(mux.Vars(r)["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	rule := cluster.GetRegionLabeler().GetLabelRule(id)
	if rule == nil {
		h.rd.JSON(w, http.StatusNotFound, nil)
		return
	}
	h.rd.JSON(w, http.StatusOK, rule)
}

// @Tags     region_label
// @Summary  Delete label rule of cluster by id.
// @Param    id  path  string  true  "Rule Id"
// @Produce  json
// @Success  200  {string}  string  "Delete rule successfully."
// @Failure  404  {string}  string  "The rule does not exist."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rule/{id} [delete]
func (h *regionLabelHandler) DeleteRegionLabelRule(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	id, err := url.PathUnescape(mux.Vars(r)["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	err = cluster.GetRegionLabeler().DeleteLabelRule(id)
	if err != nil {
		if errs.ErrRegionRuleNotFound.Equal(err) {
			h.rd.JSON(w, http.StatusNotFound, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	h.rd.Text(w, http.StatusOK, "Delete rule successfully.")
}

// @Tags     region_label
// @Summary  Update region label rule of cluster.
// @Accept   json
// @Param    rule  body  labeler.LabelRule  true  "Parameters of label rule"
// @Produce  json
// @Success  200  {string}  string  "Update rule successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rule [post]
func (h *regionLabelHandler) SetRegionLabelRule(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	var rule labeler.LabelRule
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &rule); err != nil {
		return
	}
	if err := cluster.GetRegionLabeler().SetLabelRule(&rule); err != nil {
		if errs.ErrRegionRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Update region label rule successfully.")
}

// @Tags     region_label
// @Summary  Get label of a region.
// @Param    id   path  integer  true  "Region Id"
// @Param    key  path  string   true  "Label key"
// @Produce  json
// @Success  200  {string}  string
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Router   /region/id/{id}/label/{key} [get]
func (h *regionLabelHandler) GetRegionLabelByKey(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	regionID, labelKey := mux.Vars(r)["id"], mux.Vars(r)["key"]
	id, err := strconv.ParseUint(regionID, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	region := cluster.GetRegion(id)
	if region == nil {
		h.rd.JSON(w, http.StatusNotFound, nil)
		return
	}
	labelValue := cluster.GetRegionLabeler().GetRegionLabel(region, labelKey)
	h.rd.JSON(w, http.StatusOK, labelValue)
}

// @Tags     region_label
// @Summary  Get labels of a region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {string}  string
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Router   /region/id/{id}/labels [get]
func (h *regionLabelHandler) GetRegionLabels(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	regionID, err := strconv.ParseUint(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	region := cluster.GetRegion(regionID)
	if region == nil {
		h.rd.JSON(w, http.StatusNotFound, nil)
		return
	}
	labels := cluster.GetRegionLabeler().GetRegionLabels(region)
	h.rd.JSON(w, http.StatusOK, labels)
}
