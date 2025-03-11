// Copyright 2022 TiKV Project Authors.
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

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
)

type minResolvedTSHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newMinResolvedTSHandler(svr *server.Server, rd *render.Render) *minResolvedTSHandler {
	return &minResolvedTSHandler{
		svr: svr,
		rd:  rd,
	}
}

// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type minResolvedTS struct {
	IsRealTime          bool              `json:"is_real_time,omitempty"`
	MinResolvedTS       uint64            `json:"min_resolved_ts"`
	PersistInterval     typeutil.Duration `json:"persist_interval,omitempty"`
	StoresMinResolvedTS map[uint64]uint64 `json:"stores_min_resolved_ts"`
}

// @Tags     min_store_resolved_ts
// @Summary  Get store-level min resolved ts.
// @Produce      json
// @Success      200    {array}   minResolvedTS
// @Failure  400  {string}  string  "The input is invalid."
// @Failure      500    {string}  string  "PD server failed to proceed the request."
// @Router   /min-resolved-ts/{store_id} [get]
func (h *minResolvedTSHandler) GetStoreMinResolvedTS(w http.ResponseWriter, r *http.Request) {
	c := getCluster(r)
	idStr := mux.Vars(r)["store_id"]
	storeID, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	value := c.GetStoreMinResolvedTS(storeID)
	persistInterval := c.GetPDServerConfig().MinResolvedTSPersistenceInterval
	h.rd.JSON(w, http.StatusOK, minResolvedTS{
		MinResolvedTS:   value,
		PersistInterval: persistInterval,
		IsRealTime:      persistInterval.Duration != 0,
	})
}

// @Tags         min_resolved_ts
// @Summary      Get cluster-level min resolved ts and optionally store-level min resolved ts.
// @Description  Optionally, we support a query parameter `scope`
// to get store-level min resolved ts by specifying a list of store IDs.
//   - When no scope is given, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be nil.
//   - When scope is `cluster`, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be filled.
//   - When scope given a list of stores, min_resolved_ts will be provided for each store
//     and the scope-specific min_resolved_ts will be returned.
//
// @Produce  json
// @Param        scope  query     string  false  "Scope of the min resolved ts: comma-separated list of store IDs (e.g., '1,2,3')."  default(cluster)
// @Success  200  {array}   minResolvedTS
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router       /min-resolved-ts [get]
func (h *minResolvedTSHandler) GetMinResolvedTS(w http.ResponseWriter, r *http.Request) {
	c := getCluster(r)
	scopeMinResolvedTS := c.GetMinResolvedTS()
	persistInterval := c.GetPDServerConfig().MinResolvedTSPersistenceInterval

	var storesMinResolvedTS map[uint64]uint64
	if scopeStr := r.URL.Query().Get("scope"); len(scopeStr) > 0 {
		// scope is an optional parameter, it can be `cluster` or specified store IDs.
		// - When no scope is given, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be nil.
		// - When scope is `cluster`, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be filled.
		// - When scope given a list of stores, min_resolved_ts will be provided for each store
		//      and the scope-specific min_resolved_ts will be returned.
		if scopeStr == "cluster" {
			stores := c.GetMetaStores()
			ids := make([]uint64, len(stores))
			for i, store := range stores {
				ids[i] = store.GetId()
			}
			// use cluster-level min_resolved_ts as the scope-specific min_resolved_ts.
			_, storesMinResolvedTS = c.GetMinResolvedTSByStoreIDs(ids)
		} else {
			scopeIDs := strings.Split(scopeStr, ",")
			ids := make([]uint64, len(scopeIDs))
			for i, idStr := range scopeIDs {
				id, err := strconv.ParseUint(idStr, 10, 64)
				if err != nil {
					h.rd.JSON(w, http.StatusBadRequest, err.Error())
					return
				}
				ids[i] = id
			}
			scopeMinResolvedTS, storesMinResolvedTS = c.GetMinResolvedTSByStoreIDs(ids)
		}
	}

	h.rd.JSON(w, http.StatusOK, minResolvedTS{
		MinResolvedTS:       scopeMinResolvedTS,
		PersistInterval:     persistInterval,
		IsRealTime:          persistInterval.Duration != 0,
		StoresMinResolvedTS: storesMinResolvedTS,
	})
}
