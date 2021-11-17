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
	"net/http"
	"strconv"
	"time"

	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/statistics"
	"github.com/unrolled/render"
)

// Trend describes the cluster's schedule trend.
type Trend struct {
	Stores  []trendStore  `json:"stores"`
	History *trendHistory `json:"history"`
}

type trendStore struct {
	ID              uint64             `json:"id"`
	Address         string             `json:"address"`
	StateName       string             `json:"state_name"`
	Capacity        uint64             `json:"capacity"`
	Available       uint64             `json:"available"`
	RegionCount     int                `json:"region_count"`
	LeaderCount     int                `json:"leader_count"`
	StartTS         *time.Time         `json:"start_ts,omitempty"`
	LastHeartbeatTS *time.Time         `json:"last_heartbeat_ts,omitempty"`
	Uptime          *typeutil.Duration `json:"uptime,omitempty"`

	HotWriteFlow        float64   `json:"hot_write_flow"`
	HotWriteRegionFlows []float64 `json:"hot_write_region_flows"`
	HotReadFlow         float64   `json:"hot_read_flow"`
	HotReadRegionFlows  []float64 `json:"hot_read_region_flows"`
}

type trendHistory struct {
	StartTime int64               `json:"start"`
	EndTime   int64               `json:"end"`
	Entries   []trendHistoryEntry `json:"entries"`
}

type trendHistoryEntry struct {
	From  uint64 `json:"from"`
	To    uint64 `json:"to"`
	Kind  string `json:"kind"`
	Count int    `json:"count"`
}

type trendHandler struct {
	*server.Handler
	svr *server.Server
	rd  *render.Render
}

func newTrendHandler(s *server.Server, rd *render.Render) *trendHandler {
	return &trendHandler{
		Handler: s.GetHandler(),
		svr:     s,
		rd:      rd,
	}
}

// @Tags trend
// @Summary Get the growth and changes of data in the most recent period of time.
// @Param from query integer false "From Unix timestamp"
// @Produce json
// @Success 200 {object} Trend
// @Failure 400 {string} string "The request is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /trend [get]
func (h *trendHandler) Handle(w http.ResponseWriter, r *http.Request) {
	var from time.Time
	if fromStr := r.URL.Query()["from"]; len(fromStr) > 0 {
		fromInt, err := strconv.ParseInt(fromStr[0], 10, 64)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		from = time.Unix(fromInt, 0)
	}

	stores, err := h.getTrendStores()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	history, err := h.getTrendHistory(from)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	trend := Trend{
		Stores:  stores,
		History: history,
	}
	h.rd.JSON(w, http.StatusOK, trend)
}

func (h *trendHandler) getTrendStores() ([]trendStore, error) {
	var readStats, writeStats statistics.StoreHotPeersStat
	if hotRead := h.GetHotReadRegions(); hotRead != nil {
		readStats = hotRead.AsLeader
	}
	if hotWrite := h.GetHotWriteRegions(); hotWrite != nil {
		writeStats = hotWrite.AsPeer
	}
	stores, err := h.GetStores()
	if err != nil {
		return nil, err
	}
	trendStores := make([]trendStore, 0, len(stores))
	for _, store := range stores {
		info := newStoreInfo(h.svr.GetScheduleConfig(), store)
		s := trendStore{
			ID:              info.Store.StoreID,
			Address:         info.Store.Address,
			StateName:       info.Store.StateName,
			Capacity:        uint64(info.Status.Capacity),
			Available:       uint64(info.Status.Available),
			RegionCount:     info.Status.RegionCount,
			LeaderCount:     info.Status.LeaderCount,
			StartTS:         info.Status.StartTS,
			LastHeartbeatTS: info.Status.LastHeartbeatTS,
			Uptime:          info.Status.Uptime,
		}
		s.HotReadFlow, s.HotReadRegionFlows = h.getStoreFlow(readStats, store.GetID())
		s.HotWriteFlow, s.HotWriteRegionFlows = h.getStoreFlow(writeStats, store.GetID())
		trendStores = append(trendStores, s)
	}
	return trendStores, nil
}

func (h *trendHandler) getStoreFlow(stats statistics.StoreHotPeersStat, storeID uint64) (storeByteFlow float64, regionByteFlows []float64) {
	if stats == nil {
		return
	}
	if stat, ok := stats[storeID]; ok {
		storeByteFlow = stat.TotalBytesRate
		for _, flow := range stat.Stats {
			regionByteFlows = append(regionByteFlows, flow.ByteRate)
		}
	}
	return
}

func (h *trendHandler) getTrendHistory(start time.Time) (*trendHistory, error) {
	operatorHistory, err := h.GetHistory(start)
	if err != nil {
		return nil, err
	}
	// Use a tmp map to merge same histories together.
	historyMap := make(map[trendHistoryEntry]int)
	for _, entry := range operatorHistory {
		historyMap[trendHistoryEntry{
			From: entry.From,
			To:   entry.To,
			Kind: entry.Kind.String(),
		}]++
	}
	history := make([]trendHistoryEntry, 0, len(historyMap))
	for entry, count := range historyMap {
		entry.Count = count
		history = append(history, entry)
	}
	return &trendHistory{
		StartTime: start.Unix(),
		EndTime:   time.Now().Unix(),
		Entries:   history,
	}, nil
}
