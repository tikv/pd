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
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"container/heap"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/statistics"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

// RegionInfo records detail region info for api usage.
type RegionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []*metapb.Peer      `json:"peers,omitempty"`

	Leader          *metapb.Peer      `json:"leader,omitempty"`
	DownPeers       []*pdpb.PeerStats `json:"down_peers,omitempty"`
	PendingPeers    []*metapb.Peer    `json:"pending_peers,omitempty"`
	WrittenBytes    uint64            `json:"written_bytes"`
	ReadBytes       uint64            `json:"read_bytes"`
	WrittenKeys     uint64            `json:"written_keys"`
	ReadKeys        uint64            `json:"read_keys"`
	ApproximateSize int64             `json:"approximate_size"`
	ApproximateKeys int64             `json:"approximate_keys"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// ReplicationStatus represents the replication mode status of the region.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID uint64 `json:"state_id"`
}

func fromPBReplicationStatus(s *replication_modepb.RegionReplicationStatus) *ReplicationStatus {
	if s == nil {
		return nil
	}
	return &ReplicationStatus{
		State:   replication_modepb.RegionReplicationState_name[int32(s.GetState())],
		StateID: s.GetStateId(),
	}
}

// NewRegionInfo create a new api RegionInfo.
func NewRegionInfo(r *core.RegionInfo) *RegionInfo {
	return InitRegion(r, &RegionInfo{})
}

// InitRegion init a new api RegionInfo from the core.RegionInfo.
func InitRegion(r *core.RegionInfo, s *RegionInfo) *RegionInfo {
	if r == nil {
		return nil
	}

	s.ID = r.GetID()
	s.StartKey = core.HexRegionKeyStr(r.GetStartKey())
	s.EndKey = core.HexRegionKeyStr(r.GetEndKey())
	s.RegionEpoch = r.GetRegionEpoch()
	s.Peers = r.GetPeers()
	s.Leader = r.GetLeader()
	s.DownPeers = r.GetDownPeers()
	s.PendingPeers = r.GetPendingPeers()
	s.WrittenBytes = r.GetBytesWritten()
	s.WrittenKeys = r.GetKeysWritten()
	s.ReadBytes = r.GetBytesRead()
	s.ReadKeys = r.GetKeysRead()
	s.ApproximateSize = r.GetApproximateSize()
	s.ApproximateKeys = r.GetApproximateKeys()
	s.ReplicationStatus = fromPBReplicationStatus(r.GetReplicationStatus())

	return s
}

// RegionsInfo contains some regions with the detailed region info.
type RegionsInfo struct {
	Count   int           `json:"count"`
	Regions []*RegionInfo `json:"regions"`
}

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

// @Tags region
// @Summary Search for a region by region ID.
// @Param id path integer true "Region Id"
// @Produce json
// @Success 200 {object} RegionInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /region/id/{id} [get]
func (h *regionHandler) GetRegionByID(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())

	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	regionInfo := rc.GetRegion(regionID)
	h.rd.JSON(w, http.StatusOK, NewRegionInfo(regionInfo))
}

// @Tags region
// @Summary Search for a region by a key.
// @Param key path string true "Region key"
// @Produce json
// @Success 200 {object} RegionInfo
// @Router /region/key/{key} [get]
func (h *regionHandler) GetRegionByKey(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	vars := mux.Vars(r)
	key := vars["key"]
	key, err := url.QueryUnescape(key)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	regionInfo := rc.GetRegionByKey([]byte(key))
	h.rd.JSON(w, http.StatusOK, NewRegionInfo(regionInfo))
}

type regionsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionsHandler(svr *server.Server, rd *render.Render) *regionsHandler {
	return &regionsHandler{
		svr: svr,
		rd:  rd,
	}
}

func convertToAPIRegions(regions []*core.RegionInfo) *RegionsInfo {
	regionInfos := make([]RegionInfo, len(regions))
	regionInfosRefs := make([]*RegionInfo, len(regions))

	for i := 0; i < len(regions); i++ {
		regionInfosRefs[i] = &regionInfos[i]
	}
	for i, r := range regions {
		regionInfosRefs[i] = InitRegion(r, regionInfosRefs[i])
	}
	return &RegionsInfo{
		Count:   len(regions),
		Regions: regionInfosRefs,
	}
}

// @Tags region
// @Summary List all regions in the cluster.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Router /regions [get]
func (h *regionsHandler) GetAll(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	regions := rc.GetRegions()
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List regions start from a key.
// @Param key query string true "Region key"
// @Param limit query integer false "Limit count" default(16)
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/key [get]
func (h *regionsHandler) ScanRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	startKey := r.URL.Query().Get("key")

	limit := defaultRegionLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit > maxRegionLimit {
		limit = maxRegionLimit
	}
	regions := rc.ScanRegions([]byte(startKey), nil, limit)
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary Get count of regions.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Router /regions/count [get]
func (h *regionsHandler) GetRegionCount(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	count := rc.GetRegionCount()
	h.rd.JSON(w, http.StatusOK, &RegionsInfo{Count: count})
}

// @Tags region
// @Summary List all regions of a specific store.
// @Param id path integer true "Store Id"
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/store/{id} [get]
func (h *regionsHandler) GetStoreRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	regions := rc.GetStoreRegions(uint64(id))
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List all regions that miss peer.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /regions/check/miss-peer [get]
func (h *regionsHandler) GetMissPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(statistics.MissPeer)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List all regions that has extra peer.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /regions/check/extra-peer [get]
func (h *regionsHandler) GetExtraPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(statistics.ExtraPeer)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List all regions that has pending peer.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /regions/check/pending-peer [get]
func (h *regionsHandler) GetPendingPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(statistics.PendingPeer)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List all regions that has down peer.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /regions/check/down-peer [get]
func (h *regionsHandler) GetDownPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(statistics.DownPeer)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List all regions that has learner peer.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /regions/check/learner-peer [get]
func (h *regionsHandler) GetLearnerPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(statistics.LearnerPeer)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List all regions that has offline peer.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /regions/check/offline-peer [get]
func (h *regionsHandler) GetOfflinePeer(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(statistics.OfflinePeer)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary List all empty regions.
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /regions/check/empty-region [get]
func (h *regionsHandler) GetEmptyRegion(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetRegionsByType(statistics.EmptyRegion)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
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

// @Tags region
// @Summary Get size of histogram.
// @Param bound query integer false "Size bound of region histogram" minimum(1)
// @Produce json
// @Success 200 {array} histItem
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/check/hist-size [get]
func (h *regionsHandler) GetSizeHistogram(w http.ResponseWriter, r *http.Request) {
	bound := minRegionHistogramSize
	bound, err := calBound(bound, r)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	rc := getCluster(r.Context())
	regions := rc.GetRegions()
	histSizes := make([]int64, 0, len(regions))
	for _, region := range regions {
		histSizes = append(histSizes, region.GetApproximateSize())
	}
	histItems := calHist(bound, &histSizes)
	h.rd.JSON(w, http.StatusOK, histItems)
}

// @Tags region
// @Summary Get keys of histogram.
// @Param bound query integer false "Key bound of region histogram" minimum(1000)
// @Produce json
// @Success 200 {array} histItem
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/check/hist-keys [get]
func (h *regionsHandler) GetKeysHistogram(w http.ResponseWriter, r *http.Request) {
	bound := minRegionHistogramKeys
	bound, err := calBound(bound, r)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	rc := getCluster(r.Context())
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

// @Tags region
// @Summary List sibling regions of a specific region.
// @Param id path integer true "Region Id"
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Failure 404 {string} string "The region does not exist."
// @Router /regions/sibling/{id} [get]
func (h *regionsHandler) GetRegionSiblings(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	region := rc.GetRegion(uint64(id))
	if region == nil {
		h.rd.JSON(w, http.StatusNotFound, server.ErrRegionNotFound(uint64(id)).Error())
		return
	}

	left, right := rc.GetAdjacentRegions(region)
	regionsInfo := convertToAPIRegions([]*core.RegionInfo{left, right})
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

const (
	defaultRegionLimit     = 16
	maxRegionLimit         = 10240
	minRegionHistogramSize = 1
	minRegionHistogramKeys = 1000
)

// @Tags region
// @Summary List regions with the highest write flow.
// @Param limit query integer false "Limit count" default(16)
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/writeflow [get]
func (h *regionsHandler) GetTopWriteFlow(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() })
}

// @Tags region
// @Summary List regions with the highest read flow.
// @Param limit query integer false "Limit count" default(16)
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/readflow [get]
func (h *regionsHandler) GetTopReadFlow(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() })
}

// @Tags region
// @Summary List regions with the largest conf version.
// @Param limit query integer false "Limit count" default(16)
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/confver [get]
func (h *regionsHandler) GetTopConfVer(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
	})
}

// @Tags region
// @Summary List regions with the largest version.
// @Param limit query integer false "Limit count" default(16)
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/version [get]
func (h *regionsHandler) GetTopVersion(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
	})
}

// @Tags region
// @Summary List regions with the largest size.
// @Param limit query integer false "Limit count" default(16)
// @Produce json
// @Success 200 {object} RegionsInfo
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/size [get]
func (h *regionsHandler) GetTopSize(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetApproximateSize() < b.GetApproximateSize()
	})
}

// @Tags region
// @Summary Accelerate regions scheduling a in given range, only receive hex format for keys
// @Accept json
// @Param body body object true "json params"
// @Param limit query integer false "Limit count" default(256)
// @Produce json
// @Success 200 {string} string "Accelerate regions scheduling in a given range[startKey,endKey)"
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/accelerate-schedule [post]
func (h *regionsHandler) AccelerateRegionsScheduleInRange(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	startKey, rawStartKey, err := parseKey("start_key", input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	endKey, rawEndKey, err := parseKey("end_key", input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	limit := 256
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit > maxRegionLimit {
		limit = maxRegionLimit
	}

	regions := rc.ScanRegions(startKey, endKey, limit)
	if len(regions) > 0 {
		regionsIDList := make([]uint64, 0, len(regions))
		for _, region := range regions {
			regionsIDList = append(regionsIDList, region.GetID())
		}
		rc.AddSuspectRegions(regionsIDList...)
	}
	h.rd.Text(w, http.StatusOK, fmt.Sprintf("Accelerate regions scheduling in a given range [%s,%s)", rawStartKey, rawEndKey))
}

func (h *regionsHandler) GetTopNRegions(w http.ResponseWriter, r *http.Request, less func(a, b *core.RegionInfo) bool) {
	rc := getCluster(r.Context())
	limit := defaultRegionLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit > maxRegionLimit {
		limit = maxRegionLimit
	}
	regions := TopNRegions(rc.GetRegions(), less, limit)
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// @Tags region
// @Summary Scatter regions by given key ranges or regions id distributed by given group with given retry limit
// @Accept json
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "Scatter regions by given key ranges or regions id distributed by given group with given retry limit"
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/scatter [post]
func (h *regionsHandler) ScatterRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	_, ok1 := input["start_key"].(string)
	_, ok2 := input["end_key"].(string)
	group, ok := input["group"].(string)
	if !ok {
		group = ""
	}
	retryLimit, ok := input["retry_limit"].(int)
	if !ok {
		retryLimit = 5
	}
	var ops []*operator.Operator
	var failures map[uint64]error
	var err error
	if ok1 && ok2 {
		startKey, _, err := parseKey("start_key", input)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		endKey, _, err := parseKey("end_key", input)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		ops, failures, err = rc.GetRegionScatter().ScatterRegionsByRange(startKey, endKey, group, retryLimit)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		regionsID := input["regions_id"].([]uint64)
		ops, failures, err = rc.GetRegionScatter().ScatterRegionsByID(regionsID, group, retryLimit)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	// If there existed any operator failed to be added into Operator Controller, add its regions into unProcessedRegions
	for _, op := range ops {
		if ok := rc.GetOperatorController().AddOperator(op); !ok {
			failures[op.RegionID()] = fmt.Errorf("region %v failed to add operator", op.RegionID())
		}
	}
	percentage := 100
	if len(failures) > 0 {
		percentage = 100 - 100*len(failures)/(len(ops)+len(failures))
		log.Debug("scatter regions", zap.Errors("failures", func() []error {
			r := make([]error, 0, len(failures))
			for _, err := range failures {
				r = append(r, err)
			}
			return r
		}()))
	}
	s := struct {
		ProcessedPercentage int `json:"processed-percentage"`
	}{
		ProcessedPercentage: percentage,
	}
	h.rd.JSON(w, http.StatusOK, &s)
}

// @Tags region
// @Summary Split regions with given split keys
// @Accept json
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "Split regions with given split keys"
// @Failure 400 {string} string "The input is invalid."
// @Router /regions/split [post]
func (h *regionsHandler) SplitRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	rawSplitKeys, ok := input["split_keys"].([]interface{})
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "split_keys should be provided.")
		return
	}
	if len(rawSplitKeys) < 1 {
		h.rd.JSON(w, http.StatusBadRequest, "empty split keys.")
		return
	}
	retryLimit, ok := input["retry_limit"].(int)
	if !ok {
		retryLimit = 5
	}
	splitKeys := make([][]byte, 0, len(rawSplitKeys))
	for _, rawKey := range rawSplitKeys {
		key, err := hex.DecodeString(rawKey.(string))
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		splitKeys = append(splitKeys, key)
	}
	s := struct {
		ProcessedPercentage int      `json:"processed-percentage"`
		NewRegionsID        []uint64 `json:"regions-id"`
	}{}
	percentage, newRegionsID := rc.GetRegionSplitter().SplitRegions(r.Context(), splitKeys, retryLimit)
	s.ProcessedPercentage = percentage
	s.NewRegionsID = newRegionsID
	failpoint.Inject("splitResponses", func(val failpoint.Value) {
		rawID, ok := val.(int)
		if ok {
			s.ProcessedPercentage = 100
			s.NewRegionsID = []uint64{uint64(rawID)}
		}
	})
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
func (h *RegionHeap) Push(x interface{}) {
	h.regions = append(h.regions, x.(*core.RegionInfo))
}

// Pop removes the minimum element (according to Less) from the heap and returns
// it.
func (h *RegionHeap) Pop() interface{} {
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
