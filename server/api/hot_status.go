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
	"github.com/tikv/pd/server/cluster"
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

// HistoryHotRegionsRequest wrap request condition from tidb.
// it is request from tidb
type HistoryHotRegionsRequest struct {
	StartTime int64    `json:"start_time,omitempty"`
	EndTime   int64    `json:"end_time,omitempty"`
	RegionIDs []uint64 `json:"region_ids,omitempty"`
	StoreIDs  []uint64 `json:"store_ids,omitempty"`
	PeerIDs   []uint64 `json:"peer_ids,omitempty"`
	//0 means not leader,1 means leader
	Roles          []int64  `json:"is_leader,omitempy"`
	HotRegionTypes []string `json:"hot_region_type,omitempty"`
	LowHotDegree   int64    `json:"low_hot_degree,omitempty"`
	HighHotDegree  int64    `json:"high_hot_degree,omitempty"`
	LowFlowBytes   float64  `json:"low_flow_bytes,omitempty"`
	HighFlowBytes  float64  `json:"high_flow_bytes,omitempty"`
	LowKeyRate     float64  `json:"low_key_rate,omitempty"`
	HighKeyRate    float64  `json:"high_key_rate,omitempty"`
	LowQueryRate   float64  `json:"low_query_rate,omitempty"`
	HighQueryRate  float64  `json:"high_query_rate,omitempty"`
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
	historyHotRegionsRequest := &HistoryHotRegionsRequest{}
	err = json.Unmarshal(data, historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	results, err := GetAllRequestHistroyHotRegion(h.Handler, historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, results)
}

func GetAllRequestHistroyHotRegion(handler *server.Handler, request *HistoryHotRegionsRequest) (*statistics.HistoryHotRegions, error) {
	var hotRegionTypes []string
	if len(request.HotRegionTypes) != 0 {
		hotRegionTypes = request.HotRegionTypes
	} else {
		hotRegionTypes = cluster.HotRegionTypes
	}
	iter := handler.GetHistoryHotRegionIter(hotRegionTypes, request.StartTime, request.EndTime)
	results := make([]*statistics.HistoryHotRegion, 0)

	regionSet, storeSet, peerSet, roleSet :=
		make(map[uint64]bool), make(map[uint64]bool),
		make(map[uint64]bool), make(map[int64]bool)
	for _, id := range request.RegionIDs {
		regionSet[id] = true
	}
	for _, id := range request.StoreIDs {
		storeSet[id] = true
	}
	for _, id := range request.PeerIDs {
		peerSet[id] = true
	}
	for _, id := range request.Roles {
		roleSet[id] = true
	}
	var next *statistics.HistoryHotRegion
	var err error
	for next, err = iter.Next(); next != nil && err == nil; next, err = iter.Next() {
		if len(regionSet) != 0 && !regionSet[next.RegionID] {
			continue
		}
		if len(storeSet) != 0 && !storeSet[next.StoreID] {
			continue
		}
		if len(peerSet) != 0 && !peerSet[next.PeerID] {
			continue
		}
		if len(roleSet) != 0 && !roleSet[next.IsLeader] {
			continue
		}
		if request.HighHotDegree < next.HotDegree || request.LowHotDegree > next.HotDegree {
			continue
		}
		if request.HighFlowBytes < next.FlowBytes || request.LowFlowBytes > next.FlowBytes {
			continue
		}
		if request.HighKeyRate < next.KeyRate || request.LowKeyRate > next.KeyRate {
			continue
		}
		if request.HighQueryRate < next.QueryRate || request.LowQueryRate > next.QueryRate {
			continue
		}
		results = append(results, next)
	}
	return &statistics.HistoryHotRegions{
		HistoryHotRegion: results,
	}, err
}
