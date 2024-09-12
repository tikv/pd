// Copyright 2017 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type hotStatusHandler struct {
	*server.Handler
	rd *render.Render
}

func newHotStatusHandler(handler *server.Handler, rd *render.Render) *hotStatusHandler {
	return &hotStatusHandler{
		Handler: handler,
		rd:      rd,
	}
}

// @Tags     hotspot
// @Summary  List the hot write regions.
// @Produce  json
// @Success  200  {object}  statistics.StoreHotPeersInfos
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/write [get]
func (h *hotStatusHandler) GetHotWriteRegions(w http.ResponseWriter, r *http.Request) {
	h.getHotRegions(utils.Write, w, r)
}

// @Tags     hotspot
// @Summary  List the hot read regions.
// @Produce  json
// @Success  200  {object}  statistics.StoreHotPeersInfos
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/read [get]
func (h *hotStatusHandler) GetHotReadRegions(w http.ResponseWriter, r *http.Request) {
	h.getHotRegions(utils.Read, w, r)
}

func (h *hotStatusHandler) getHotRegions(typ utils.RWType, w http.ResponseWriter, r *http.Request) {
	storeIDs := r.URL.Query()["store_id"]
	if len(storeIDs) < 1 {
		hotRegions, err := h.GetHotRegions(typ)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, hotRegions)
		return
	}

	var ids []uint64
	for _, storeID := range storeIDs {
		id, err := strconv.ParseUint(storeID, 10, 64)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("invalid store id: %s", storeID))
			return
		}
		_, err = h.GetStore(id)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		ids = append(ids, id)
	}

	hotRegions, err := h.GetHotRegions(typ, ids...)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, hotRegions)
}

// @Tags     hotspot
// @Summary  List the hot stores.
// @Produce  json
// @Success  200  {object}  handler.HotStoreStats
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/stores [get]
func (h *hotStatusHandler) GetHotStores(w http.ResponseWriter, _ *http.Request) {
	stats, err := h.Handler.GetHotStores()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, stats)
}

// @Tags     hotspot
// @Summary  List the hot buckets.
// @Produce  json
// @Success  200  {object}  handler.HotBucketsResponse
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/buckets [get]
func (h *hotStatusHandler) GetHotBuckets(w http.ResponseWriter, r *http.Request) {
	regionIDs := r.URL.Query()["region_id"]
	ids := make([]uint64, len(regionIDs))
	for i, regionID := range regionIDs {
		if id, err := strconv.ParseUint(regionID, 10, 64); err == nil {
			ids[i] = id
		}
	}
	ret, err := h.Handler.GetHotBuckets(ids...)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, ret)
}

// @Tags     hotspot
// @Summary  List the history hot regions.
// @Accept   json
// @Produce  json
// @Success  200  {object}  storage.HistoryHotRegions
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/history [get]
func (h *hotStatusHandler) GetHistoryHotRegions(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	historyHotRegionsRequest := &server.HistoryHotRegionsRequest{}
	err = json.Unmarshal(data, historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	results, err := h.GetAllRequestHistoryHotRegion(historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, results)
}
