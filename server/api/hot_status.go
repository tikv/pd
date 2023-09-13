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

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type hotStatusHandler struct {
	*server.Handler
	rd *render.Render
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
	StartTime      int64    `json:"start_time,omitempty"`
	EndTime        int64    `json:"end_time,omitempty"`
	RegionIDs      []uint64 `json:"region_ids,omitempty"`
	StoreIDs       []uint64 `json:"store_ids,omitempty"`
	PeerIDs        []uint64 `json:"peer_ids,omitempty"`
	IsLearners     []bool   `json:"is_learners,omitempty"`
	IsLeaders      []bool   `json:"is_leaders,omitempty"`
	HotRegionTypes []string `json:"hot_region_type,omitempty"`
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
// @Router   /hotspot/regions/write [get]
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
			h.rd.JSON(w, http.StatusNotFound, errs.ErrStoreNotFoundByID(id).Error())
			return
		}
		ids = append(ids, id)
	}

	h.rd.JSON(w, http.StatusOK, rc.GetHotWriteRegions(ids...))
}

// @Tags     hotspot
// @Summary  List the hot read regions.
// @Produce  json
// @Success  200  {object}  statistics.StoreHotPeersInfos
// @Router   /hotspot/regions/read [get]
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
			h.rd.JSON(w, http.StatusNotFound, errs.ErrStoreNotFoundByID(id).Error())
			return
		}
		ids = append(ids, id)
	}

	h.rd.JSON(w, http.StatusOK, rc.GetHotReadRegions(ids...))
}

// @Tags     hotspot
// @Summary  List the hot stores.
// @Produce  json
// @Success  200  {object}  HotStoreStats
// @Router   /hotspot/stores [get]
func (h *hotStatusHandler) GetHotStores(w http.ResponseWriter, r *http.Request) {
	stats := HotStoreStats{
		BytesWriteStats: make(map[uint64]float64),
		BytesReadStats:  make(map[uint64]float64),
		KeysWriteStats:  make(map[uint64]float64),
		KeysReadStats:   make(map[uint64]float64),
		QueryWriteStats: make(map[uint64]float64),
		QueryReadStats:  make(map[uint64]float64),
	}
	stores, _ := h.GetStores()
	storesLoads := h.GetStoresLoads()
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
	h.rd.JSON(w, http.StatusOK, stats)
}

// @Tags     hotspot
// @Summary  List the hot buckets.
// @Produce  json
// @Success  200  {object}  HotBucketsResponse
// @Router   /hotspot/buckets [get]
func (h *hotStatusHandler) GetHotBuckets(w http.ResponseWriter, r *http.Request) {
	regionIDs := r.URL.Query()["region_id"]
	ids := make([]uint64, len(regionIDs))
	for i, regionID := range regionIDs {
		if id, err := strconv.ParseUint(regionID, 10, 64); err == nil {
			ids[i] = id
		}
	}
	stats := h.Handler.GetHotBuckets(ids...)
	ret := HotBucketsResponse{}
	for regionID, stats := range stats {
		ret[regionID] = make([]*HotBucketsItem, len(stats))
		for i, stat := range stats {
			ret[regionID][i] = convert(stat)
		}
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
	historyHotRegionsRequest := &HistoryHotRegionsRequest{}
	err = json.Unmarshal(data, historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	results, err := getAllRequestHistoryHotRegion(h.Handler, historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, results)
}

func getAllRequestHistoryHotRegion(handler *server.Handler, request *HistoryHotRegionsRequest) (*storage.HistoryHotRegions, error) {
	var hotRegionTypes = storage.HotRegionTypes
	if len(request.HotRegionTypes) != 0 {
		hotRegionTypes = request.HotRegionTypes
	}
	iter := handler.GetHistoryHotRegionIter(hotRegionTypes, request.StartTime, request.EndTime)
	var results []*storage.HistoryHotRegion
	regionSet, storeSet, peerSet, learnerSet, leaderSet :=
		make(map[uint64]bool), make(map[uint64]bool),
		make(map[uint64]bool), make(map[bool]bool), make(map[bool]bool)
	for _, id := range request.RegionIDs {
		regionSet[id] = true
	}
	for _, id := range request.StoreIDs {
		storeSet[id] = true
	}
	for _, id := range request.PeerIDs {
		peerSet[id] = true
	}
	for _, isLearner := range request.IsLearners {
		learnerSet[isLearner] = true
	}
	for _, isLeader := range request.IsLeaders {
		leaderSet[isLeader] = true
	}
	var next *storage.HistoryHotRegion
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
		if !learnerSet[next.IsLearner] {
			continue
		}
		if !leaderSet[next.IsLeader] {
			continue
		}
		results = append(results, next)
	}
	return &storage.HistoryHotRegions{
		HistoryHotRegion: results,
	}, err
}
