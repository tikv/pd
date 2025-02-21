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

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/server"
)

type statsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStatsHandler(svr *server.Server, rd *render.Render) *statsHandler {
	return &statsHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     stats
// @Summary  Get region statistics of a specified range.
// @Param    start_key  query  string  true   "Start key"
// @Param    end_key    query  string  true   "End key"
// @Param    count      query  bool    false  "Whether only count the number of regions"
// @Produce  json
// @Success  200  {object}  statistics.RegionStats
// @Router   /stats/region [get]
func (h *statsHandler) GetRegionStatus(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	startKey, endKey := r.URL.Query().Get("start_key"), r.URL.Query().Get("end_key")
	var stats *statistics.RegionStats
	if r.URL.Query().Has("count") {
		stats = rc.GetRegionStatsCount([]byte(startKey), []byte(endKey))
	} else {
		stats = rc.GetRegionStatsByRange([]byte(startKey), []byte(endKey), false)
	}
	h.rd.JSON(w, http.StatusOK, stats)
}

func (h *statsHandler) GetRegionDistribution(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	startKey, endKey, engine := r.URL.Query().Get("start_key"), r.URL.Query().Get("end_key"), r.URL.Query().Get("engine")
	stats := rc.GetRegionStatsByRange([]byte(startKey), []byte(endKey), true)
	stores := rc.GetStores()
	switch engine {
	case core.EngineTiKV:
		f := filter.NewEngineFilter("user", filter.NotSpecialEngines)
		stores = filter.SelectSourceStores(stores, []filter.Filter{f}, rc.GetSchedulerConfig(), nil, nil)
	case core.EngineTiFlash:
		f := filter.NewEngineFilter("user", filter.SpecialEngines)
		stores = filter.SelectSourceStores(stores, []filter.Filter{f}, rc.GetSchedulerConfig(), nil, nil)
	default:
	}

	distributions := make([]RegionDistribution, len(stores))
	for _, store := range stores {
		distributions = append(distributions, RegionDistribution{
			StoreID:    store.GetID(),
			EngineType: store.GetLabelValue(core.EngineKey),
		})
	}
	for _, dis := range distributions {
		if _, ok := stats.StoreLeaderKeys[dis.StoreID]; ok {
			dis.RegionLeaderCount = stats.StoreLeaderCount[dis.StoreID]
			dis.RegionPeerCount = stats.StorePeerCount[dis.StoreID]
			dis.ApproximateSize = stats.StorePeerSize[dis.StoreID]
			dis.ApproximateKeys = stats.StorePeerKeys[dis.StoreID]
			dis.RegionWriteKeys = stats.StoreWriteKeys[dis.StoreID]
			dis.RegionWriteBytes = stats.StoreWriteBytes[dis.StoreID]
			dis.RegionLeaderReadBytes = stats.StoreLeaderReadBytes[dis.StoreID]
			dis.RegionLeaderReadKeys = stats.StoreLeaderReadKeys[dis.StoreID]
			dis.RegionPeerReadKeys = stats.StorePeerReadKeys[dis.StoreID]
			dis.RegionPeerReadBytes = stats.StorePeerReadBytes[dis.StoreID]
			dis.RegionPeerReadQuery = stats.StorePeerReadQuery[dis.StoreID]
		}
	}

	if err := h.rd.JSON(w, http.StatusOK, stats); err != nil {
		log.Error("json data error", zap.Error(err))
	}
}

// RegionDistributions is the response for the region distribution request
type RegionDistributions struct {
	RegionDistributions []*RegionDistribution `json:"region_distribution"`
}

// RegionDistribution wraps region distribution info
// it is storage format of region_distribution_storage
type RegionDistribution struct {
	StoreID               uint64 `json:"store_id"`
	EngineType            string `json:"engine_type"`
	RegionLeaderCount     int    `json:"region_leader_count"`
	RegionPeerCount       int    `json:"region_peer_count"`
	ApproximateSize       int64  `json:"approximate_size"`
	ApproximateKeys       int64  `json:"approximate_keys"`
	RegionWriteBytes      uint64 `json:"region_write_bytes"`
	RegionWriteKeys       uint64 `json:"region_write_keys"`
	RegionLeaderReadBytes uint64 `json:"region_leader_read_bytes"`
	RegionLeaderReadKeys  uint64 `json:"region_leader_read_keys"`
	RegionPeerReadBytes   uint64 `json:"region_peer_read_bytes"`
	RegionPeerReadKeys    uint64 `json:"region_peer_read_keys"`
	RegionPeerReadQuery   uint64 `json:"region_peer_read_query"`
}
