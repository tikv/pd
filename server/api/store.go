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
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

// MetaStore contains meta information about a store.
type MetaStore struct {
	*metapb.Store
	StateName string `json:"state_name"`
}

// SlowTrend contains slow trend information about a store.
type SlowTrend struct {
	// CauseValue is the slow trend detecting raw input, it changes by the performance and pressure along time of the store.
	// The value itself is not important, what matter is:
	//   - The comparition result from store to store.
	//   - The change magnitude along time (represented by CauseRate).
	// Currently it's one of store's internal latency (duration of waiting in the task queue of raftstore.store).
	CauseValue float64 `json:"cause_value"`
	// CauseRate is for mesuring the change magnitude of CauseValue of the store,
	//   - CauseRate > 0 means the store is become slower currently
	//   - CauseRate < 0 means the store is become faster currently
	//   - CauseRate == 0 means the store's performance and pressure does not have significant changes
	CauseRate float64 `json:"cause_rate"`
	// ResultValue is the current gRPC QPS of the store.
	ResultValue float64 `json:"result_value"`
	// ResultRate is for mesuring the change magnitude of ResultValue of the store.
	ResultRate float64 `json:"result_rate"`
}

// StoreStatus contains status about a store.
type StoreStatus struct {
	Capacity           typeutil.ByteSize  `json:"capacity"`
	Available          typeutil.ByteSize  `json:"available"`
	UsedSize           typeutil.ByteSize  `json:"used_size"`
	LeaderCount        int                `json:"leader_count"`
	LeaderWeight       float64            `json:"leader_weight"`
	LeaderScore        float64            `json:"leader_score"`
	LeaderSize         int64              `json:"leader_size"`
	RegionCount        int                `json:"region_count"`
	RegionWeight       float64            `json:"region_weight"`
	RegionScore        float64            `json:"region_score"`
	RegionSize         int64              `json:"region_size"`
	LearnerCount       int                `json:"learner_count,omitempty"`
	WitnessCount       int                `json:"witness_count,omitempty"`
	SlowScore          uint64             `json:"slow_score,omitempty"`
	SlowTrend          *SlowTrend         `json:"slow_trend,omitempty"`
	SendingSnapCount   uint32             `json:"sending_snap_count,omitempty"`
	ReceivingSnapCount uint32             `json:"receiving_snap_count,omitempty"`
	IsBusy             bool               `json:"is_busy,omitempty"`
	StartTS            *time.Time         `json:"start_ts,omitempty"`
	LastHeartbeatTS    *time.Time         `json:"last_heartbeat_ts,omitempty"`
	Uptime             *typeutil.Duration `json:"uptime,omitempty"`
}

// StoreInfo contains information about a store.
type StoreInfo struct {
	Store  *MetaStore   `json:"store"`
	Status *StoreStatus `json:"status"`
}

const (
	disconnectedName = "Disconnected"
	downStateName    = "Down"
)

func newStoreInfo(opt *sc.ScheduleConfig, store *core.StoreInfo) *StoreInfo {
	var slowTrend *SlowTrend
	coreSlowTrend := store.GetSlowTrend()
	if coreSlowTrend != nil {
		slowTrend = &SlowTrend{coreSlowTrend.CauseValue, coreSlowTrend.CauseRate, coreSlowTrend.ResultValue, coreSlowTrend.ResultRate}
	}
	s := &StoreInfo{
		Store: &MetaStore{
			Store:     store.GetMeta(),
			StateName: store.GetState().String(),
		},
		Status: &StoreStatus{
			Capacity:           typeutil.ByteSize(store.GetCapacity()),
			Available:          typeutil.ByteSize(store.GetAvailable()),
			UsedSize:           typeutil.ByteSize(store.GetUsedSize()),
			LeaderCount:        store.GetLeaderCount(),
			LeaderWeight:       store.GetLeaderWeight(),
			LeaderScore:        store.LeaderScore(constant.StringToSchedulePolicy(opt.LeaderSchedulePolicy), 0),
			LeaderSize:         store.GetLeaderSize(),
			RegionCount:        store.GetRegionCount(),
			RegionWeight:       store.GetRegionWeight(),
			RegionScore:        store.RegionScore(opt.RegionScoreFormulaVersion, opt.HighSpaceRatio, opt.LowSpaceRatio, 0),
			RegionSize:         store.GetRegionSize(),
			LearnerCount:       store.GetLearnerCount(),
			WitnessCount:       store.GetWitnessCount(),
			SlowScore:          store.GetSlowScore(),
			SlowTrend:          slowTrend,
			SendingSnapCount:   store.GetSendingSnapCount(),
			ReceivingSnapCount: store.GetReceivingSnapCount(),
			IsBusy:             store.IsBusy(),
		},
	}

	if store.GetStoreStats() != nil {
		startTS := store.GetStartTime()
		s.Status.StartTS = &startTS
	}
	if lastHeartbeat := store.GetLastHeartbeatTS(); !lastHeartbeat.IsZero() {
		s.Status.LastHeartbeatTS = &lastHeartbeat
	}
	if upTime := store.GetUptime(); upTime > 0 {
		duration := typeutil.NewDuration(upTime)
		s.Status.Uptime = &duration
	}

	if store.GetState() == metapb.StoreState_Up {
		if store.DownTime() > opt.MaxStoreDownTime.Duration {
			s.Store.StateName = downStateName
		} else if store.IsDisconnected() {
			s.Store.StateName = disconnectedName
		}
	}
	return s
}

// StoresInfo records stores' info.
type StoresInfo struct {
	Count  int          `json:"count"`
	Stores []*StoreInfo `json:"stores"`
}

type storeHandler struct {
	handler *server.Handler
	rd      *render.Render
}

func newStoreHandler(handler *server.Handler, rd *render.Render) *storeHandler {
	return &storeHandler{
		handler: handler,
		rd:      rd,
	}
}

// @Tags        store
// @Summary  Get a store's information.
// @Param    id  path  integer  true  "Store Id"
// @Produce     json
// @Success  200  {object}  StoreInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The store does not exist."
// @Failure     500  {string}  string  "PD server failed to proceed the request."
// @Router   /store/{id} [get]
func (h *storeHandler) GetStore(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	storeID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}

	store := rc.GetStore(storeID)
	if store == nil {
		h.rd.JSON(w, http.StatusNotFound, errs.ErrStoreNotFound.FastGenByArgs(storeID).Error())
		return
	}

	storeInfo := newStoreInfo(h.handler.GetScheduleConfig(), store)
	h.rd.JSON(w, http.StatusOK, storeInfo)
}

// @Tags     store
// @Summary  Take down a store from the cluster.
// @Param    id     path   integer  true  "Store Id"
// @Param    force  query  string   true  "force"  Enums(true, false)
// @Produce  json
// @Success  200  {string}  string  "The store is set as Offline."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The store does not exist."
// @Failure  410  {string}  string  "The store has already been removed."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /store/{id} [delete]
func (h *storeHandler) DeleteStore(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	storeID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}

	_, force := r.URL.Query()["force"]
	err := rc.RemoveStore(storeID, force)

	if err != nil {
		h.responseStoreErr(w, err, storeID)
		return
	}

	h.rd.JSON(w, http.StatusOK, "The store is set as Offline.")
}

// @Tags     store
// @Summary  Set the store's state.
// @Param    id     path   integer  true  "Store Id"
// @Param    state  query  string   true  "state"  Enums(Up, Offline)
// @Produce  json
// @Success  200  {string}  string  "The store's state is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The store does not exist."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /store/{id}/state [post]
func (h *storeHandler) SetStoreState(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	storeID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}
	stateStr := r.URL.Query().Get("state")
	var err error
	if strings.EqualFold(stateStr, metapb.StoreState_Up.String()) {
		err = rc.UpStore(storeID)
	} else if strings.EqualFold(stateStr, metapb.StoreState_Offline.String()) {
		err = rc.RemoveStore(storeID, false)
	} else {
		err = errors.Errorf("invalid state %v", stateStr)
	}

	if err != nil {
		h.responseStoreErr(w, err, storeID)
		return
	}

	h.rd.JSON(w, http.StatusOK, "The store's state is updated.")
}

func (h *storeHandler) responseStoreErr(w http.ResponseWriter, err error, storeID uint64) {
	if errors.ErrorEqual(err, errs.ErrStoreNotFound.FastGenByArgs(storeID)) {
		h.rd.JSON(w, http.StatusNotFound, err.Error())
		return
	}

	if errors.ErrorEqual(err, errs.ErrStoreRemoved.FastGenByArgs(storeID)) {
		h.rd.JSON(w, http.StatusGone, err.Error())
		return
	}

	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
	}
}

// FIXME: details of input json body params
// @Tags     store
// @Summary  Set the store's label.
// @Param    id    path  integer  true  "Store Id"
// @Param    body  body  object   true  "Labels in json format"
// @Produce  json
// @Success  200  {string}  string  "The store's label is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /store/{id}/label [post]
func (h *storeHandler) SetStoreLabel(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	storeID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}

	var input map[string]string
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	labels := make([]*metapb.StoreLabel, 0, len(input))
	for k, v := range input {
		labels = append(labels, &metapb.StoreLabel{
			Key:   k,
			Value: v,
		})
	}

	if err := sc.ValidateLabels(labels); err != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(err))
		return
	}

	_, force := r.URL.Query()["force"]
	if err := rc.UpdateStoreLabels(storeID, labels, force); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, "The store's label is updated.")
}

// @Tags     store
// @Summary  delete the store's label.
// @Param    id    path  integer  true  "Store Id"
// @Param    body  body  object   true  "Labels in json format"
// @Produce  json
// @Success  200  {string}  string  "The store's label is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /store/{id}/label [delete]
func (h *storeHandler) DeleteStoreLabel(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	storeID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}

	var labelKey string
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &labelKey); err != nil {
		return
	}
	if err := sc.ValidateLabelKey(labelKey); err != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(err))
		return
	}
	if err := rc.DeleteStoreLabel(storeID, labelKey); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, fmt.Sprintf("The label %s is deleted for store %d.", labelKey, storeID))
}

// FIXME: details of input json body params
// @Tags     store
// @Summary  Set the store's leader/region weight.
// @Param    id    path  integer  true  "Store Id"
// @Param    body  body  object   true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The store's label is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /store/{id}/weight [post]
func (h *storeHandler) SetStoreWeight(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	storeID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}

	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	leaderVal, ok := input["leader"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "leader weight unset")
		return
	}
	regionVal, ok := input["region"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "region weight unset")
		return
	}
	leader, ok := leaderVal.(float64)
	if !ok || leader < 0 {
		h.rd.JSON(w, http.StatusBadRequest, "bad format leader weight")
		return
	}
	region, ok := regionVal.(float64)
	if !ok || region < 0 {
		h.rd.JSON(w, http.StatusBadRequest, "bad format region weight")
		return
	}

	if err := rc.SetStoreWeight(storeID, leader, region); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, "The store's label is updated.")
}

// FIXME: details of input json body params
// @Tags     store
// @Summary  Set the store's limit.
// @Param    ttlSecond  query  integer  false  "ttl param is only for BR and lightning now. Don't use it."
// @Param    id         path   integer  true   "Store Id"
// @Param    body       body   object   true   "json params"
// @Produce  json
// @Success  200  {string}  string  "The store's label is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /store/{id}/limit [post]
func (h *storeHandler) SetStoreLimit(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	storeID, errParse := apiutil.ParseUint64VarsField(vars, "id")
	if errParse != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errParse))
		return
	}

	store := rc.GetStore(storeID)
	if store == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errs.ErrStoreNotFound.FastGenByArgs(storeID).Error())
		return
	}

	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	rateVal, ok := input["rate"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "rate unset")
		return
	}
	ratePerMin, ok := rateVal.(float64)
	if !ok || ratePerMin <= 0 {
		h.rd.JSON(w, http.StatusBadRequest, "invalid rate which should be larger than 0")
		return
	}

	typeValues, err := getStoreLimitType(input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	var ttl int
	if ttlSec := r.URL.Query().Get("ttlSecond"); ttlSec != "" {
		var err error
		ttl, err = strconv.Atoi(ttlSec)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	for _, typ := range typeValues {
		if ttl > 0 {
			key := fmt.Sprintf("add-peer-%v", storeID)
			if typ == storelimit.RemovePeer {
				key = fmt.Sprintf("remove-peer-%v", storeID)
			}
			h.handler.SetStoreLimitTTL(key, ratePerMin, time.Duration(ttl)*time.Second)
			continue
		}
		if err := h.handler.SetStoreLimit(storeID, ratePerMin, typ); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	h.rd.JSON(w, http.StatusOK, "The store's label is updated.")
}

type storesHandler struct {
	*server.Handler
	rd *render.Render
}

func newStoresHandler(handler *server.Handler, rd *render.Render) *storesHandler {
	return &storesHandler{
		Handler: handler,
		rd:      rd,
	}
}

// @Tags     store
// @Summary  Remove tombstone records in the cluster.
// @Produce  json
// @Success  200  {string}  string  "Remove tombstone successfully."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /stores/remove-tombstone [delete]
func (h *storesHandler) RemoveTombStone(w http.ResponseWriter, r *http.Request) {
	err := getCluster(r).RemoveTombStoneRecords()
	if err != nil {
		apiutil.ErrorResp(h.rd, w, err)
		return
	}

	h.rd.JSON(w, http.StatusOK, "Remove tombstone successfully.")
}

// FIXME: details of input json body params
// @Tags     store
// @Summary  Set limit of all stores in the cluster.
// @Accept   json
// @Param    ttlSecond  query  integer  false  "ttl param is only for BR and lightning now. Don't use it."
// @Param    body       body   object   true   "json params"
// @Produce  json
// @Success  200  {string}  string  "Set store limit successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /stores/limit [post]
func (h *storesHandler) SetAllStoresLimit(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	rateVal, ok := input["rate"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "rate unset")
		return
	}
	ratePerMin, ok := rateVal.(float64)
	if !ok || ratePerMin <= 0 {
		h.rd.JSON(w, http.StatusBadRequest, "invalid rate which should be larger than 0")
		return
	}

	typeValues, err := getStoreLimitType(input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	var ttl int
	if ttlSec := r.URL.Query().Get("ttlSecond"); ttlSec != "" {
		var err error
		ttl, err = strconv.Atoi(ttlSec)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	if _, ok := input["labels"]; !ok {
		for _, typ := range typeValues {
			if ttl > 0 {
				if err := h.SetAllStoresLimitTTL(ratePerMin, typ, time.Duration(ttl)*time.Second); err != nil {
					h.rd.JSON(w, http.StatusInternalServerError, err.Error())
					return
				}
			} else {
				if err := h.Handler.SetAllStoresLimit(ratePerMin, typ); err != nil {
					h.rd.JSON(w, http.StatusInternalServerError, err.Error())
					return
				}
			}
		}
	} else {
		labelMap := input["labels"].(map[string]interface{})
		labels := make([]*metapb.StoreLabel, 0, len(input))
		for k, v := range labelMap {
			labels = append(labels, &metapb.StoreLabel{
				Key:   k,
				Value: v.(string),
			})
		}

		if err := sc.ValidateLabels(labels); err != nil {
			apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(err))
			return
		}
		for _, typ := range typeValues {
			if err := h.SetLabelStoresLimit(ratePerMin, typ, labels); err != nil {
				h.rd.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	}

	h.rd.JSON(w, http.StatusOK, "Set store limit successfully.")
}

// FIXME: details of output json body
// @Tags     store
// @Summary  Get limit of all stores in the cluster.
// @Param    include_tombstone  query  bool  false  "include Tombstone"  default(false)
// @Produce  json
// @Success  200  {object}  string
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /stores/limit [get]
func (h *storesHandler) GetAllStoresLimit(w http.ResponseWriter, r *http.Request) {
	limits := h.GetScheduleConfig().StoreLimit
	includeTombstone := false
	var err error
	if includeStr := r.URL.Query().Get("include_tombstone"); includeStr != "" {
		includeTombstone, err = strconv.ParseBool(includeStr)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if !includeTombstone {
		returned := make(map[uint64]sc.StoreLimitConfig, len(limits))
		rc := getCluster(r)
		for storeID, v := range limits {
			store := rc.GetStore(storeID)
			if store == nil || store.IsRemoved() {
				continue
			}
			returned[storeID] = v
		}
		h.rd.JSON(w, http.StatusOK, returned)
		return
	}
	h.rd.JSON(w, http.StatusOK, limits)
}

// @Tags     store
// @Summary  Set limit scene in the cluster.
// @Accept   json
// @Param    body  body  storelimit.Scene  true  "Store limit scene"
// @Produce  json
// @Success  200  {string}  string  "Set store limit scene successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /stores/limit/scene [post]
func (h *storesHandler) SetStoreLimitScene(w http.ResponseWriter, r *http.Request) {
	typeName := r.URL.Query().Get("type")
	typeValue, err := parseStoreLimitType(typeName)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	scene := h.Handler.GetStoreLimitScene(typeValue)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &scene); err != nil {
		return
	}
	h.Handler.SetStoreLimitScene(scene, typeValue)
	h.rd.JSON(w, http.StatusOK, "Set store limit scene successfully.")
}

// @Tags     store
// @Summary  Get limit scene in the cluster.
// @Produce  json
// @Success  200  {string}  string  "Get store limit scene successfully."
// @Router   /stores/limit/scene [get]
func (h *storesHandler) GetStoreLimitScene(w http.ResponseWriter, r *http.Request) {
	typeName := r.URL.Query().Get("type")
	typeValue, err := parseStoreLimitType(typeName)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	scene := h.Handler.GetStoreLimitScene(typeValue)
	h.rd.JSON(w, http.StatusOK, scene)
}

// Progress contains status about a progress.
type Progress struct {
	Action       string  `json:"action"`
	StoreID      uint64  `json:"store_id,omitempty"`
	Progress     float64 `json:"progress"`
	CurrentSpeed float64 `json:"current_speed"`
	LeftSeconds  float64 `json:"left_seconds"`
}

// @Tags     stores
// @Summary  Get store progress in the cluster.
// @Produce  json
// @Success  200  {object}  Progress
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /stores/progress [get]
func (h *storesHandler) GetStoresProgress(w http.ResponseWriter, r *http.Request) {
	if v := r.URL.Query().Get("id"); v != "" {
		storeID, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(err))
			return
		}

		action, progress, leftSeconds, currentSpeed, err := h.Handler.GetProgressByID(v)
		if err != nil {
			h.rd.JSON(w, http.StatusNotFound, err.Error())
			return
		}
		sp := &Progress{
			StoreID:      storeID,
			Action:       action,
			Progress:     progress,
			CurrentSpeed: currentSpeed,
			LeftSeconds:  leftSeconds,
		}

		h.rd.JSON(w, http.StatusOK, sp)
		return
	}
	if v := r.URL.Query().Get("action"); v != "" {
		progress, leftSeconds, currentSpeed, err := h.Handler.GetProgressByAction(v)
		if err != nil {
			h.rd.JSON(w, http.StatusNotFound, err.Error())
			return
		}
		sp := &Progress{
			Action:       v,
			Progress:     progress,
			CurrentSpeed: currentSpeed,
			LeftSeconds:  leftSeconds,
		}

		h.rd.JSON(w, http.StatusOK, sp)
		return
	}
	h.rd.JSON(w, http.StatusBadRequest, "need query parameters")
}

// @Tags     store
// @Summary     Get all stores in the cluster.
// @Param       state  query  array  true  "Specify accepted store states."
// @Produce  json
// @Success     200  {object}  StoresInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router      /stores [get]
// @Deprecated  Better to use /stores/check instead.
func (h *storesHandler) GetAllStores(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	stores := rc.GetMetaStores()
	StoresInfo := &StoresInfo{
		Stores: make([]*StoreInfo, 0, len(stores)),
	}

	urlFilter, err := newStoreStateFilter(r.URL)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	stores = urlFilter.filter(stores)
	for _, s := range stores {
		storeID := s.GetId()
		store := rc.GetStore(storeID)
		if store == nil {
			h.rd.JSON(w, http.StatusInternalServerError, errs.ErrStoreNotFound.FastGenByArgs(storeID).Error())
			return
		}

		storeInfo := newStoreInfo(h.GetScheduleConfig(), store)
		StoresInfo.Stores = append(StoresInfo.Stores, storeInfo)
	}
	StoresInfo.Count = len(StoresInfo.Stores)

	h.rd.JSON(w, http.StatusOK, StoresInfo)
}

// @Tags     store
// @Summary  Get all stores by states in the cluster.
// @Param    state  query  array  true  "Specify accepted store states."
// @Produce  json
// @Success  200  {object}  StoresInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /stores/check [get]
func (h *storesHandler) GetStoresByState(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	stores := rc.GetMetaStores()
	StoresInfo := &StoresInfo{
		Stores: make([]*StoreInfo, 0, len(stores)),
	}

	lowerStateName := []string{strings.ToLower(downStateName), strings.ToLower(disconnectedName)}
	for _, v := range metapb.StoreState_name {
		lowerStateName = append(lowerStateName, strings.ToLower(v))
	}

	var queryStates []string
	if v, ok := r.URL.Query()["state"]; ok {
		for _, s := range v {
			stateName := strings.ToLower(s)
			if stateName != "" && !slice.Contains(lowerStateName, stateName) {
				h.rd.JSON(w, http.StatusBadRequest, "unknown StoreState: "+s)
				return
			} else if stateName != "" {
				queryStates = append(queryStates, stateName)
			}
		}
	}

	for _, s := range stores {
		storeID := s.GetId()
		store := rc.GetStore(storeID)
		if store == nil {
			h.rd.JSON(w, http.StatusInternalServerError, errs.ErrStoreNotFound.FastGenByArgs(storeID).Error())
			return
		}

		storeInfo := newStoreInfo(h.GetScheduleConfig(), store)
		if queryStates != nil && !slice.Contains(queryStates, strings.ToLower(storeInfo.Store.StateName)) {
			continue
		}
		StoresInfo.Stores = append(StoresInfo.Stores, storeInfo)
	}
	StoresInfo.Count = len(StoresInfo.Stores)

	h.rd.JSON(w, http.StatusOK, StoresInfo)
}

type storeStateFilter struct {
	accepts []metapb.StoreState
}

func newStoreStateFilter(u *url.URL) (*storeStateFilter, error) {
	var acceptStates []metapb.StoreState
	if v, ok := u.Query()["state"]; ok {
		for _, s := range v {
			state, err := strconv.Atoi(s)
			if err != nil {
				return nil, errors.WithStack(err)
			}

			storeState := metapb.StoreState(state)
			switch storeState {
			case metapb.StoreState_Up, metapb.StoreState_Offline, metapb.StoreState_Tombstone:
				acceptStates = append(acceptStates, storeState)
			default:
				return nil, errors.Errorf("unknown StoreState: %v", storeState)
			}
		}
	} else {
		// Accepts Up and Offline by default.
		acceptStates = []metapb.StoreState{metapb.StoreState_Up, metapb.StoreState_Offline}
	}

	return &storeStateFilter{
		accepts: acceptStates,
	}, nil
}

func (filter *storeStateFilter) filter(stores []*metapb.Store) []*metapb.Store {
	ret := make([]*metapb.Store, 0, len(stores))
	for _, s := range stores {
		state := s.GetState()
		for _, accept := range filter.accepts {
			if state == accept {
				ret = append(ret, s)
				break
			}
		}
	}
	return ret
}

func getStoreLimitType(input map[string]interface{}) ([]storelimit.Type, error) {
	typeNameIface, ok := input["type"]
	var err error
	if ok {
		typeName, ok := typeNameIface.(string)
		if !ok {
			err = errors.New("bad format type")
			return nil, err
		}
		typ, err := parseStoreLimitType(typeName)
		return []storelimit.Type{typ}, err
	}

	return []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer}, err
}

func parseStoreLimitType(typeName string) (storelimit.Type, error) {
	typeValue := storelimit.AddPeer
	var err error
	if typeName != "" {
		if value, ok := storelimit.TypeNameValue[typeName]; ok {
			typeValue = value
		} else {
			err = errors.New("unknown type")
		}
	}
	return typeValue, err
}
