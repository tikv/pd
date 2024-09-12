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
	"container/heap"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type regionHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionHandler(svr *server.Server, rd *render.Render) *regionHandler {
	return &regionHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     region
// @Summary  Search for a region by region ID.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {object}  response.RegionInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /region/id/{id} [get]
func (h *regionHandler) GetRegionByID(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)

	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	regionInfo := rc.GetRegion(regionID)
	b, err := response.MarshalRegionInfoJSON(r.Context(), regionInfo)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  Search for a region by a key. GetRegion is named to be consistent with gRPC
// @Param    key  path  string  true  "Region key"
// @Produce  json
// @Success  200  {object}  response.RegionInfo
// @Router   /region/key/{key} [get]
func (h *regionHandler) GetRegion(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	key, err := url.QueryUnescape(vars["key"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	// decode hex if query has params with hex format
	paramsByte := [][]byte{[]byte(key)}
	paramsByte, err = apiutil.ParseHexKeys(r.URL.Query().Get("format"), paramsByte)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	regionInfo := rc.GetRegionByKey(paramsByte[0])
	b, err := response.MarshalRegionInfoJSON(r.Context(), regionInfo)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  Check if regions in the given key ranges are replicated. Returns 'REPLICATED', 'INPROGRESS', or 'PENDING'. 'PENDING' means that there is at least one region pending for scheduling. Similarly, 'INPROGRESS' means there is at least one region in scheduling.
// @Param    startKey  query  string  true  "Regions start key, hex encoded"
// @Param    endKey    query  string  true  "Regions end key, hex encoded"
// @Produce  plain
// @Success  200  {string}  string  "INPROGRESS"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/replicated [get]
func (h *regionsHandler) CheckRegionsReplicated(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rawStartKey := vars["startKey"]
	rawEndKey := vars["endKey"]
	state, err := h.Handler.CheckRegionsReplicated(rawStartKey, rawEndKey)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, state)
}

type regionsHandler struct {
	*server.Handler
	svr *server.Server
	rd  *render.Render
}

func newRegionsHandler(svr *server.Server, rd *render.Render) *regionsHandler {
	return &regionsHandler{
		Handler: svr.GetHandler(),
		svr:     svr,
		rd:      rd,
	}
}

// @Tags     region
// @Summary  List all regions in the cluster.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Router   /regions [get]
func (h *regionsHandler) GetRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	regions := rc.GetRegions()
	b, err := response.MarshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  List regions in a given range [startKey, endKey).
// @Param    key     query  string   true   "Region range start key"
// @Param    endkey  query  string   true   "Region range end key"
// @Param    limit   query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/key [get]
func (h *regionsHandler) ScanRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	query := r.URL.Query()
	paramsByte := [][]byte{[]byte(query.Get("key")), []byte(query.Get("end_key"))}
	paramsByte, err := apiutil.ParseHexKeys(query.Get("format"), paramsByte)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	limit, err := h.AdjustLimit(query.Get("limit"))
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	regions := rc.ScanRegions(paramsByte[0], paramsByte[1], limit)
	b, err := response.MarshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  Get count of regions.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Router   /regions/count [get]
func (h *regionsHandler) GetRegionCount(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	count := rc.GetTotalRegionCount()
	h.rd.JSON(w, http.StatusOK, &response.RegionsInfo{Count: count})
}

// @Tags     region
// @Summary  List all regions of a specific store.
// @Param    id  path  integer  true  "Store Id"
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/store/{id} [get]
func (h *regionsHandler) GetStoreRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	// get type from query
	typ := r.URL.Query().Get("type")
	if len(typ) == 0 {
		typ = string(core.AllInSubTree)
	}

	regions, err := rc.GetStoreRegionsByTypeInSubTree(uint64(id), core.SubTreeRegionType(typ))
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	b, err := response.MarshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  List regions belongs to the given keyspace ID.
// @Param    keyspace_id  query  string   true   "Keyspace ID"
// @Param    limit        query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/keyspace/id/{id} [get]
func (h *regionsHandler) GetKeyspaceRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	keyspaceIDStr := vars["id"]
	if keyspaceIDStr == "" {
		h.rd.JSON(w, http.StatusBadRequest, "keyspace id is empty")
		return
	}

	keyspaceID64, err := strconv.ParseUint(keyspaceIDStr, 10, 32)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	keyspaceID := uint32(keyspaceID64)
	keyspaceManager := h.svr.GetKeyspaceManager()
	if _, err := keyspaceManager.LoadKeyspaceByID(keyspaceID); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	limit, err := h.AdjustLimit(r.URL.Query().Get("limit"))
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	regionBound := keyspace.MakeRegionBound(keyspaceID)
	regions := rc.ScanRegions(regionBound.RawLeftBound, regionBound.RawRightBound, limit)
	if limit <= 0 || limit > len(regions) {
		txnRegion := rc.ScanRegions(regionBound.TxnLeftBound, regionBound.TxnRightBound, limit-len(regions))
		regions = append(regions, txnRegion...)
	}
	b, err := response.MarshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  List all regions that miss peer.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/miss-peer [get]
func (h *regionsHandler) GetMissPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.MissPeer, r)
}

func (h *regionsHandler) getRegionsByType(
	w http.ResponseWriter,
	typ statistics.RegionStatisticType,
	r *http.Request,
) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(typ)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	b, err := response.MarshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  List all regions that has extra peer.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/extra-peer [get]
func (h *regionsHandler) GetExtraPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.ExtraPeer, r)
}

// @Tags     region
// @Summary  List all regions that has pending peer.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/pending-peer [get]
func (h *regionsHandler) GetPendingPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.PendingPeer, r)
}

// @Tags     region
// @Summary  List all regions that has down peer.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/down-peer [get]
func (h *regionsHandler) GetDownPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.DownPeer, r)
}

// @Tags     region
// @Summary  List all regions that has learner peer.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/learner-peer [get]
func (h *regionsHandler) GetLearnerPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.LearnerPeer, r)
}

// @Tags     region
// @Summary  List all regions that has offline peer.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/offline-peer [get]
func (h *regionsHandler) GetOfflinePeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.OfflinePeer, r)
}

// @Tags     region
// @Summary  List all regions that are oversized.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/oversized-region [get]
func (h *regionsHandler) GetOverSizedRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.OversizedRegion, r)
}

// @Tags     region
// @Summary  List all regions that are undersized.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/undersized-region [get]
func (h *regionsHandler) GetUndersizedRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.UndersizedRegion, r)
}

// @Tags     region
// @Summary  List all empty regions.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/empty-region [get]
func (h *regionsHandler) GetEmptyRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.EmptyRegion, r)
}

type histItem struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Count int64 `json:"count"`
}

type histSlice []*histItem

func (hist histSlice) Len() int {
	return len(hist)
}

func (hist histSlice) Swap(i, j int) {
	hist[i], hist[j] = hist[j], hist[i]
}

func (hist histSlice) Less(i, j int) bool {
	return hist[i].Start < hist[j].Start
}

// @Tags     region
// @Summary  Get size of histogram.
// @Param    bound  query  integer  false  "Size bound of region histogram"  minimum(1)
// @Produce  json
// @Success  200  {array}   histItem
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/check/hist-size [get]
func (h *regionsHandler) GetSizeHistogram(w http.ResponseWriter, r *http.Request) {
	bound := minRegionHistogramSize
	bound, err := calBound(bound, r)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	rc := getCluster(r)
	regions := rc.GetRegions()
	histSizes := make([]int64, 0, len(regions))
	for _, region := range regions {
		histSizes = append(histSizes, region.GetApproximateSize())
	}
	histItems := calHist(bound, &histSizes)
	h.rd.JSON(w, http.StatusOK, histItems)
}

// @Tags     region
// @Summary  Get keys of histogram.
// @Param    bound  query  integer  false  "Key bound of region histogram"  minimum(1000)
// @Produce  json
// @Success  200  {array}   histItem
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/check/hist-keys [get]
func (h *regionsHandler) GetKeysHistogram(w http.ResponseWriter, r *http.Request) {
	bound := minRegionHistogramKeys
	bound, err := calBound(bound, r)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	rc := getCluster(r)
	regions := rc.GetRegions()
	histKeys := make([]int64, 0, len(regions))
	for _, region := range regions {
		histKeys = append(histKeys, region.GetApproximateKeys())
	}
	histItems := calHist(bound, &histKeys)
	h.rd.JSON(w, http.StatusOK, histItems)
}

func calBound(bound int, r *http.Request) (int, error) {
	if boundStr := r.URL.Query().Get("bound"); boundStr != "" {
		boundInput, err := strconv.Atoi(boundStr)
		if err != nil {
			return -1, err
		}
		if bound < boundInput {
			bound = boundInput
		}
	}
	return bound, nil
}

func calHist(bound int, list *[]int64) *[]*histItem {
	var histMap = make(map[int64]int)
	for _, item := range *list {
		multiple := item / int64(bound)
		if oldCount, ok := histMap[multiple]; ok {
			histMap[multiple] = oldCount + 1
		} else {
			histMap[multiple] = 1
		}
	}
	histItems := make([]*histItem, 0, len(histMap))
	for multiple, count := range histMap {
		histInfo := &histItem{}
		histInfo.Start = multiple * int64(bound)
		histInfo.End = (multiple+1)*int64(bound) - 1
		histInfo.Count = int64(count)
		histItems = append(histItems, histInfo)
	}
	sort.Sort(histSlice(histItems))
	return &histItems
}

// @Tags     region
// @Summary  List all range holes without any region info.
// @Produce  json
// @Success  200  {object}  [][]string
// @Router   /regions/range-holes [get]
func (h *regionsHandler) GetRangeHoles(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	h.rd.JSON(w, http.StatusOK, rc.GetRangeHoles())
}

// @Tags     region
// @Summary  List sibling regions of a specific region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Router   /regions/sibling/{id} [get]
func (h *regionsHandler) GetRegionSiblings(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	region := rc.GetRegion(uint64(id))
	if region == nil {
		h.rd.JSON(w, http.StatusNotFound, errs.ErrRegionNotFound.FastGenByArgs(uint64(id)).Error())
		return
	}

	left, right := rc.GetAdjacentRegions(region)
	b, err := response.MarshalRegionsInfoJSON(r.Context(), []*core.RegionInfo{left, right})
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

const (
	minRegionHistogramSize = 1
	minRegionHistogramKeys = 1000
)

// @Tags     region
// @Summary  List regions with the highest write flow.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/writeflow [get]
func (h *regionsHandler) GetTopWriteFlowRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() })
}

// @Tags     region
// @Summary  List regions with the highest write flow.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/writequery [get]
func (h *regionsHandler) GetTopWriteQueryRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetWriteQueryNum() < b.GetWriteQueryNum()
	})
}

// @Tags     region
// @Summary  List regions with the highest read flow.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/readflow [get]
func (h *regionsHandler) GetTopReadFlowRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() })
}

// @Tags     region
// @Summary  List regions with the highest write flow.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/readquery [get]
func (h *regionsHandler) GetTopReadQueryRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetReadQueryNum() < b.GetReadQueryNum()
	})
}

// @Tags     region
// @Summary  List regions with the largest conf version.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/confver [get]
func (h *regionsHandler) GetTopConfVerRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
	})
}

// @Tags     region
// @Summary  List regions with the largest version.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/version [get]
func (h *regionsHandler) GetTopVersionRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
	})
}

// @Tags     region
// @Summary  List regions with the largest size.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/size [get]
func (h *regionsHandler) GetTopSizeRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetApproximateSize() < b.GetApproximateSize()
	})
}

// @Tags     region
// @Summary  List regions with the largest keys.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/keys [get]
func (h *regionsHandler) GetTopKeysRegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetApproximateKeys() < b.GetApproximateKeys()
	})
}

// @Tags     region
// @Summary  List regions with the highest CPU usage.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/cpu [get]
func (h *regionsHandler) GetTopCPURegions(w http.ResponseWriter, r *http.Request) {
	h.getTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetCPUUsage() < b.GetCPUUsage()
	})
}

// @Tags     region
// @Summary  Accelerate regions scheduling a in given range, only receive hex format for keys
// @Accept   json
// @Param    body   body   object   true   "json params"
// @Param    limit  query  integer  false  "Limit count"  default(256)
// @Produce  json
// @Success  200  {string}  string  "Accelerate regions scheduling in a given range [startKey, endKey)"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/accelerate-schedule [post]
func (h *regionsHandler) AccelerateRegionsScheduleInRange(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	rawStartKey, ok1 := input["start_key"].(string)
	rawEndKey, ok2 := input["end_key"].(string)
	if !ok1 || !ok2 {
		h.rd.JSON(w, http.StatusBadRequest, "start_key or end_key is not string")
		return
	}

	limit, err := h.AdjustLimit(r.URL.Query().Get("limit"), 256 /*default limit*/)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	err = h.Handler.AccelerateRegionsScheduleInRange(rawStartKey, rawEndKey, limit)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Text(w, http.StatusOK, fmt.Sprintf("Accelerate regions scheduling in a given range [%s,%s)", rawStartKey, rawEndKey))
}

// @Tags     region
// @Summary  Accelerate regions scheduling in given ranges, only receive hex format for keys
// @Accept   json
// @Param    body   body   object   true   "json params"
// @Param    limit  query  integer  false  "Limit count"  default(256)
// @Produce  json
// @Success  200  {string}  string  "Accelerate regions scheduling in given ranges [startKey1, endKey1), [startKey2, endKey2), ..."
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/accelerate-schedule/batch [post]
func (h *regionsHandler) AccelerateRegionsScheduleInRanges(w http.ResponseWriter, r *http.Request) {
	var input []map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	limit, err := h.AdjustLimit(r.URL.Query().Get("limit"), 256 /*default limit*/)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	var msgBuilder strings.Builder
	msgBuilder.Grow(128)
	msgBuilder.WriteString("Accelerate regions scheduling in given ranges: ")
	var startKeys, endKeys [][]byte
	for _, rg := range input {
		startKey, rawStartKey, err := apiutil.ParseKey("start_key", rg)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		endKey, rawEndKey, err := apiutil.ParseKey("end_key", rg)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		startKeys = append(startKeys, startKey)
		endKeys = append(endKeys, endKey)
		msgBuilder.WriteString(fmt.Sprintf("[%s,%s), ", rawStartKey, rawEndKey))
	}
	err = h.Handler.AccelerateRegionsScheduleInRanges(startKeys, endKeys, limit)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Text(w, http.StatusOK, msgBuilder.String())
}

func (h *regionsHandler) getTopNRegions(w http.ResponseWriter, r *http.Request, less func(a, b *core.RegionInfo) bool) {
	rc := getCluster(r)
	limit, err := h.AdjustLimit(r.URL.Query().Get("limit"))
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	regions := TopNRegions(rc.GetRegions(), less, limit)
	b, err := response.MarshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  Scatter regions by given key ranges or regions id distributed by given group with given retry limit
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Scatter regions by given key ranges or regions id distributed by given group with given retry limit"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/scatter [post]
func (h *regionsHandler) ScatterRegions(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	rawStartKey, ok1 := input["start_key"].(string)
	rawEndKey, ok2 := input["end_key"].(string)
	group, _ := input["group"].(string)
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
	}

	opsCount, failures, err := func() (int, map[uint64]error, error) {
		if ok1 && ok2 {
			return h.ScatterRegionsByRange(rawStartKey, rawEndKey, group, retryLimit)
		}
		ids, ok := typeutil.JSONToUint64Slice(input["regions_id"])
		if !ok {
			return 0, nil, errors.New("regions_id is invalid")
		}
		return h.ScatterRegionsByID(ids, group, retryLimit)
	}()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	s := h.BuildScatterRegionsResp(opsCount, failures)
	h.rd.JSON(w, http.StatusOK, &s)
}

// @Tags     region
// @Summary  Split regions with given split keys
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Split regions with given split keys"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/split [post]
func (h *regionsHandler) SplitRegions(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	s, ok := input["split_keys"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "split_keys should be provided.")
		return
	}
	rawSplitKeys := s.([]any)
	if len(rawSplitKeys) < 1 {
		h.rd.JSON(w, http.StatusBadRequest, "empty split keys.")
		return
	}
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
	}
	s, err := h.Handler.SplitRegions(r.Context(), rawSplitKeys, retryLimit)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, &s)
}

// RegionHeap implements heap.Interface, used for selecting top n regions.
type RegionHeap struct {
	regions []*core.RegionInfo
	less    func(a, b *core.RegionInfo) bool
}

func (h *RegionHeap) Len() int           { return len(h.regions) }
func (h *RegionHeap) Less(i, j int) bool { return h.less(h.regions[i], h.regions[j]) }
func (h *RegionHeap) Swap(i, j int)      { h.regions[i], h.regions[j] = h.regions[j], h.regions[i] }

// Push pushes an element x onto the heap.
func (h *RegionHeap) Push(x any) {
	h.regions = append(h.regions, x.(*core.RegionInfo))
}

// Pop removes the minimum element (according to Less) from the heap and returns
// it.
func (h *RegionHeap) Pop() any {
	pos := len(h.regions) - 1
	x := h.regions[pos]
	h.regions = h.regions[:pos]
	return x
}

// Min returns the minimum region from the heap.
func (h *RegionHeap) Min() *core.RegionInfo {
	if h.Len() == 0 {
		return nil
	}
	return h.regions[0]
}

// TopNRegions returns top n regions according to the given rule.
func TopNRegions(regions []*core.RegionInfo, less func(a, b *core.RegionInfo) bool, n int) []*core.RegionInfo {
	if n <= 0 {
		return nil
	}

	hp := &RegionHeap{
		regions: make([]*core.RegionInfo, 0, n),
		less:    less,
	}
	for _, r := range regions {
		if hp.Len() < n {
			heap.Push(hp, r)
			continue
		}
		if less(hp.Min(), r) {
			heap.Pop(hp)
			heap.Push(hp, r)
		}
	}

	res := make([]*core.RegionInfo, hp.Len())
	for i := hp.Len() - 1; i >= 0; i-- {
		res[i] = heap.Pop(hp).(*core.RegionInfo)
	}
	return res
}
