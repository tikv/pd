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
	"strings"

	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
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

// @Tags stats
// @Summary Get region statistics of a specified range.
// @Param start_key query string true "Start key"
// @Param end_key query string true "End key"
// @Produce json
// @Success 200 {object} statistics.RegionStats
// @Router /stats/region [get]
func (h *statsHandler) GetRegionStatus(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	startKey, endKey := r.URL.Query().Get("start_key"), r.URL.Query().Get("end_key")
	stats := rc.GetRegionStats([]byte(startKey), []byte(endKey))
	h.rd.JSON(w, http.StatusOK, stats)
}

// @Tags stats
// @Summary Get region statistics of a specified range.
// @Param table IDs {string} string true
// @Produce json
// @Success 200 {object} map[int64]int
// @Failure 400 {string} string "Bad format request."
// @Router /stats/regions [post]
func (h *statsHandler) GetRegionStatusWithTables(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)

	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	tableString, ok := input["table-ids"].(string)
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "missing table-ids")
		return
	}

	tableStrings := strings.Fields(tableString)
	var tableIDs []int64

	for _, tableStr := range tableStrings {
		tableID, err := strconv.ParseInt(tableStr, 10, 64)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, "invalid table-ids")
			return
		}
		tableIDs = append(tableIDs, tableID)
	}

	stats := rc.GetRegionStatsWithTables(tableIDs)
	h.rd.JSON(w, http.StatusOK, stats)
}
