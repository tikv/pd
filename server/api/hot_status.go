// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/statistics"
	"github.com/unrolled/render"
)

type hotStatusHandler struct {
	*server.Handler
	rd *render.Render
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

func newHotStatusHandler(handler *server.Handler, rd *render.Render) *hotStatusHandler {
	return &hotStatusHandler{
		Handler: handler,
		rd:      rd,
	}
}

// @Tags hotspot
// @Summary List the hot write regions.
// @Produce json
// @Success 200 {object} statistics.StoreHotPeersInfos
// @Router /hotspot/regions/write [get]
func (h *hotStatusHandler) GetHotWriteRegions(w http.ResponseWriter, r *http.Request) {
	storeIDs := r.URL.Query()["store_id"]
	if len(storeIDs) < 1 {
		h.rd.JSON(w, http.StatusOK, h.Handler.GetHotWriteRegions())
		return
	}

	rc, err := h.GetRaftCluster()
	if rc == nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	var ids []uint64
	for _, storeID := range storeIDs {
		id, err := strconv.ParseUint(storeID, 10, 64)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("invalid store id: %s", storeID))
			return
		}
		store := rc.GetStore(id)
		if store == nil {
			h.rd.JSON(w, http.StatusNotFound, server.ErrStoreNotFound(id).Error())
			return
		}
		ids = append(ids, id)
	}

	h.rd.JSON(w, http.StatusOK, rc.GetHotWriteRegions(ids...))
}

// @Tags hotspot
// @Summary List the hot read regions.
// @Produce json
// @Success 200 {object} statistics.StoreHotPeersInfos
// @Router /hotspot/regions/read [get]
func (h *hotStatusHandler) GetHotReadRegions(w http.ResponseWriter, r *http.Request) {
	storeIDs := r.URL.Query()["store_id"]
	if len(storeIDs) < 1 {
		h.rd.JSON(w, http.StatusOK, h.Handler.GetHotReadRegions())
		return
	}

	rc, err := h.GetRaftCluster()
	if rc == nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	var ids []uint64
	for _, storeID := range storeIDs {
		id, err := strconv.ParseUint(storeID, 10, 64)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("invalid store id: %s", storeID))
			return
		}
		store := rc.GetStore(id)
		if store == nil {
			h.rd.JSON(w, http.StatusNotFound, server.ErrStoreNotFound(id).Error())
			return
		}
		ids = append(ids, id)
	}

	h.rd.JSON(w, http.StatusOK, rc.GetHotReadRegions(ids...))
}

// @Tags hotspot
// @Summary List the hot stores.
// @Produce json
// @Success 200 {object} HotStoreStats
// @Router /hotspot/stores [get]
func (h *hotStatusHandler) GetHotStores(w http.ResponseWriter, r *http.Request) {
	stats := HotStoreStats{
		BytesWriteStats: make(map[uint64]float64),
		BytesReadStats:  make(map[uint64]float64),
		KeysWriteStats:  make(map[uint64]float64),
		KeysReadStats:   make(map[uint64]float64),
		QueryWriteStats: make(map[uint64]float64),
		QueryReadStats:  make(map[uint64]float64),
	}
	for id, loads := range h.GetStoresLoads() {
		stats.BytesWriteStats[id] = loads[statistics.StoreWriteBytes]
		stats.BytesReadStats[id] = loads[statistics.StoreReadBytes]
		stats.KeysWriteStats[id] = loads[statistics.StoreWriteKeys]
		stats.KeysReadStats[id] = loads[statistics.StoreReadKeys]
		stats.QueryWriteStats[id] = loads[statistics.StoreWriteQuery]
		stats.QueryReadStats[id] = loads[statistics.StoreReadQuery]
	}
	h.rd.JSON(w, http.StatusOK, stats)
}

// @Tags hotspot
// @Summary List the history hot regions.
// @Accept json
// @Produce json
// @Success 200 {object} statistics.HistoryHotRegions
// @Router /hotspot/regions/history [post]
func (h *hotStatusHandler) GetHistoryHotRegions(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	historyHotRegionsRequest := &statistics.HistoryHotRegionsRequest{}
	err = json.Unmarshal(data, historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	results, err := h.Handler.GetAllRequestHistroyHotRegion(historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, results)
}
