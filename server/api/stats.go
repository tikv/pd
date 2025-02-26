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

	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/log"

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

// @Tags	 distributions
// @Summary  Get region stats and hot flow statistics of a specified range.
// @Param    start_key  query  string  true   "Start key"
// @Param    end_key    query  string  true   "End key"
// @Param    engine     query  string  false  "Engine type such as  tikv or tiflash"
// @Produce  json
// @Success  200  {object}  statistics.RegionStats
// @Router   /distributions/region [get]
func (h *statsHandler) GetRegionDistributions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	startKey, endKey, engine := r.URL.Query().Get("start_key"), r.URL.Query().Get("end_key"), r.URL.Query().Get("engine")

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

	storeMap := make(map[uint64]string, len(stores))
	for _, store := range stores {
		storeMap[store.GetID()] = store.GetLabelValue(core.EngineKey)
	}
	opt := statistics.WithStoreMapOption(storeMap)
	stats := rc.GetRegionStatsByRange([]byte(startKey), []byte(endKey), true, opt)

	stats.StoreEngine = storeMap
	if err := h.rd.JSON(w, http.StatusOK, stats); err != nil {
		log.Error("json data error", zap.Error(err))
	}
}
