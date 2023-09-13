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
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	jwriter "github.com/mailru/easyjson/jwriter"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

// MetaPeer is api compatible with *metapb.Peer.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaPeer struct {
	*metapb.Peer
	// RoleName is `Role.String()`.
	// Since Role is serialized as int by json by default,
	// introducing it will make the output of pd-ctl easier to identify Role.
	RoleName string `json:"role_name"`
	// IsLearner is `Role == "Learner"`.
	// Since IsLearner was changed to Role in kvproto in 5.0, this field was introduced to ensure api compatibility.
	IsLearner bool `json:"is_learner,omitempty"`
}

func (m *MetaPeer) setDefaultIfNil() {
	if m.Peer == nil {
		m.Peer = &metapb.Peer{
			Id:        m.GetId(),
			StoreId:   m.GetStoreId(),
			Role:      m.GetRole(),
			IsWitness: m.GetIsWitness(),
		}
	}
}

// PDPeerStats is api compatible with *pdpb.PeerStats.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type PDPeerStats struct {
	*pdpb.PeerStats
	Peer MetaPeer `json:"peer"`
}

func (s *PDPeerStats) setDefaultIfNil() {
	if s.PeerStats == nil {
		s.PeerStats = &pdpb.PeerStats{
			Peer:        s.GetPeer(),
			DownSeconds: s.GetDownSeconds(),
		}
	}
	s.Peer.setDefaultIfNil()
}

func fromPeer(peer *metapb.Peer) MetaPeer {
	if peer == nil {
		return MetaPeer{}
	}
	return MetaPeer{
		Peer:      peer,
		RoleName:  peer.GetRole().String(),
		IsLearner: core.IsLearner(peer),
	}
}

func fromPeerSlice(peers []*metapb.Peer) []MetaPeer {
	if peers == nil {
		return nil
	}
	slice := make([]MetaPeer, len(peers))
	for i, peer := range peers {
		slice[i] = fromPeer(peer)
	}
	return slice
}

func fromPeerStats(peer *pdpb.PeerStats) PDPeerStats {
	return PDPeerStats{
		PeerStats: peer,
		Peer:      fromPeer(peer.Peer),
	}
}

func fromPeerStatsSlice(peers []*pdpb.PeerStats) []PDPeerStats {
	if peers == nil {
		return nil
	}
	slice := make([]PDPeerStats, len(peers))
	for i, peer := range peers {
		slice[i] = fromPeerStats(peer)
	}
	return slice
}

// RegionInfo records detail region info for api usage.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// easyjson:json
type RegionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []MetaPeer          `json:"peers,omitempty"`

	Leader          MetaPeer      `json:"leader,omitempty"`
	DownPeers       []PDPeerStats `json:"down_peers,omitempty"`
	PendingPeers    []MetaPeer    `json:"pending_peers,omitempty"`
	CPUUsage        uint64        `json:"cpu_usage"`
	WrittenBytes    uint64        `json:"written_bytes"`
	ReadBytes       uint64        `json:"read_bytes"`
	WrittenKeys     uint64        `json:"written_keys"`
	ReadKeys        uint64        `json:"read_keys"`
	ApproximateSize int64         `json:"approximate_size"`
	ApproximateKeys int64         `json:"approximate_keys"`
	Buckets         []string      `json:"buckets,omitempty"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// ReplicationStatus represents the replication mode status of the region.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID uint64 `json:"state_id"`
}

func fromPBReplicationStatus(s *replication_modepb.RegionReplicationStatus) *ReplicationStatus {
	if s == nil {
		return nil
	}
	return &ReplicationStatus{
		State:   s.GetState().String(),
		StateID: s.GetStateId(),
	}
}

// NewAPIRegionInfo create a new API RegionInfo.
func NewAPIRegionInfo(r *core.RegionInfo) *RegionInfo {
	return InitRegion(r, &RegionInfo{})
}

// InitRegion init a new API RegionInfo from the core.RegionInfo.
func InitRegion(r *core.RegionInfo, s *RegionInfo) *RegionInfo {
	if r == nil {
		return nil
	}

	s.ID = r.GetID()
	s.StartKey = core.HexRegionKeyStr(r.GetStartKey())
	s.EndKey = core.HexRegionKeyStr(r.GetEndKey())
	s.RegionEpoch = r.GetRegionEpoch()
	s.Peers = fromPeerSlice(r.GetPeers())
	s.Leader = fromPeer(r.GetLeader())
	s.DownPeers = fromPeerStatsSlice(r.GetDownPeers())
	s.PendingPeers = fromPeerSlice(r.GetPendingPeers())
	s.CPUUsage = r.GetCPUUsage()
	s.WrittenBytes = r.GetBytesWritten()
	s.WrittenKeys = r.GetKeysWritten()
	s.ReadBytes = r.GetBytesRead()
	s.ReadKeys = r.GetKeysRead()
	s.ApproximateSize = r.GetApproximateSize()
	s.ApproximateKeys = r.GetApproximateKeys()
	s.ReplicationStatus = fromPBReplicationStatus(r.GetReplicationStatus())
	s.Buckets = nil

	keys := r.GetBuckets().GetKeys()
	if len(keys) > 0 {
		s.Buckets = make([]string, len(keys))
		for i, key := range keys {
			s.Buckets[i] = core.HexRegionKeyStr(key)
		}
	}
	return s
}

// Adjust is only used in testing, in order to compare the data from json deserialization.
func (r *RegionInfo) Adjust() {
	for _, peer := range r.DownPeers {
		// Since api.PDPeerStats uses the api.MetaPeer type variable Peer to overwrite PeerStats.Peer,
		// it needs to be restored after deserialization to be completely consistent with the original.
		peer.PeerStats.Peer = peer.Peer.Peer
	}
}

// RegionsInfo contains some regions with the detailed region info.
type RegionsInfo struct {
	Count   int          `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// Adjust is only used in testing, in order to compare the data from json deserialization.
func (s *RegionsInfo) Adjust() {
	for _, r := range s.Regions {
		r.Adjust()
	}
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

// @Tags     region
// @Summary  Search for a region by region ID.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {object}  RegionInfo
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
	h.rd.JSON(w, http.StatusOK, NewAPIRegionInfo(regionInfo))
}

// @Tags     region
// @Summary  Search for a region by a key. GetRegion is named to be consistent with gRPC
// @Param    key  path  string  true  "Region key"
// @Produce  json
// @Success  200  {object}  RegionInfo
// @Router   /region/key/{key} [get]
func (h *regionHandler) GetRegion(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	key := vars["key"]
	key, err := url.QueryUnescape(key)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	regionInfo := rc.GetRegionByKey([]byte(key))
	h.rd.JSON(w, http.StatusOK, NewAPIRegionInfo(regionInfo))
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
	rc := getCluster(r)

	vars := mux.Vars(r)
	startKeyHex := vars["startKey"]
	startKey, err := hex.DecodeString(startKeyHex)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	endKeyHex := vars["endKey"]
	endKey, err := hex.DecodeString(endKeyHex)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	regions := rc.ScanRegions(startKey, endKey, -1)
	state := "REPLICATED"
	for _, region := range regions {
		if !filter.IsRegionReplicated(rc, region) {
			state = "INPROGRESS"
			if rc.GetCoordinator().IsPendingRegion(region.GetID()) {
				state = "PENDING"
				break
			}
		}
	}
	failpoint.Inject("mockPending", func(val failpoint.Value) {
		aok, ok := val.(bool)
		if ok && aok {
			state = "PENDING"
		}
	})
	h.rd.JSON(w, http.StatusOK, state)
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

// marshalRegionsInfoJSON marshals regions to bytes in `RegionsInfo`'s JSON format.
// It is used to reduce the cost of JSON serialization.
func marshalRegionsInfoJSON(ctx context.Context, regions []*core.RegionInfo) ([]byte, error) {
	out := &jwriter.Writer{}
	out.RawByte('{')

	out.RawString("\"count\":")
	out.Int(len(regions))

	out.RawString(",\"regions\":")
	out.RawByte('[')
	region := &RegionInfo{}
	for i, r := range regions {
		select {
		case <-ctx.Done():
			// Return early, avoid the unnecessary computation.
			// See more details in https://github.com/tikv/pd/issues/6835
			return nil, ctx.Err()
		default:
		}
		if i > 0 {
			out.RawByte(',')
		}
		InitRegion(r, region)
		// EasyJSON will not check anonymous struct pointer field and will panic if the field is nil.
		// So we need to set the field to default value explicitly when the anonymous struct pointer is nil.
		region.Leader.setDefaultIfNil()
		for i := range region.Peers {
			region.Peers[i].setDefaultIfNil()
		}
		for i := range region.PendingPeers {
			region.PendingPeers[i].setDefaultIfNil()
		}
		for i := range region.DownPeers {
			region.DownPeers[i].setDefaultIfNil()
		}
		region.MarshalEasyJSON(out)
	}
	out.RawByte(']')

	out.RawByte('}')
	return out.Buffer.BuildBytes(), out.Error
}

// @Tags     region
// @Summary  List all regions in the cluster.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Router   /regions [get]
func (h *regionsHandler) GetRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	regions := rc.GetRegions()
	b, err := marshalRegionsInfoJSON(r.Context(), regions)
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
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/key [get]
func (h *regionsHandler) ScanRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	startKey := r.URL.Query().Get("key")
	endKey := r.URL.Query().Get("end_key")

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
	regions := rc.ScanRegions([]byte(startKey), []byte(endKey), limit)
	b, err := marshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  Get count of regions.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Router   /regions/count [get]
func (h *regionsHandler) GetRegionCount(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	count := rc.GetTotalRegionCount()
	h.rd.JSON(w, http.StatusOK, &RegionsInfo{Count: count})
}

// @Tags     region
// @Summary  List all regions of a specific store.
// @Param    id  path  integer  true  "Store Id"
// @Produce  json
// @Success  200  {object}  RegionsInfo
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
	regions := rc.GetStoreRegions(uint64(id))
	b, err := marshalRegionsInfoJSON(r.Context(), regions)
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
// @Success  200  {object}  RegionsInfo
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

	limit := defaultRegionLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit > maxRegionLimit {
		limit = maxRegionLimit
	}
	regionBound := keyspace.MakeRegionBound(keyspaceID)
	regions := rc.ScanRegions(regionBound.RawLeftBound, regionBound.RawRightBound, limit)
	if limit <= 0 || limit > len(regions) {
		txnRegion := rc.ScanRegions(regionBound.TxnLeftBound, regionBound.TxnRightBound, limit-len(regions))
		regions = append(regions, txnRegion...)
	}
	b, err := marshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  List all regions that miss peer.
// @Produce  json
// @Success  200  {object}  RegionsInfo
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
	b, err := marshalRegionsInfoJSON(r.Context(), regions)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

// @Tags     region
// @Summary  List all regions that has extra peer.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/extra-peer [get]
func (h *regionsHandler) GetExtraPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.ExtraPeer, r)
}

// @Tags     region
// @Summary  List all regions that has pending peer.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/pending-peer [get]
func (h *regionsHandler) GetPendingPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.PendingPeer, r)
}

// @Tags     region
// @Summary  List all regions that has down peer.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/down-peer [get]
func (h *regionsHandler) GetDownPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.DownPeer, r)
}

// @Tags     region
// @Summary  List all regions that has learner peer.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/learner-peer [get]
func (h *regionsHandler) GetLearnerPeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.LearnerPeer, r)
}

// @Tags     region
// @Summary  List all regions that has offline peer.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/offline-peer [get]
func (h *regionsHandler) GetOfflinePeerRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.OfflinePeer, r)
}

// @Tags     region
// @Summary  List all regions that are oversized.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/oversized-region [get]
func (h *regionsHandler) GetOverSizedRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.OversizedRegion, r)
}

// @Tags     region
// @Summary  List all regions that are undersized.
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/check/undersized-region [get]
func (h *regionsHandler) GetUndersizedRegions(w http.ResponseWriter, r *http.Request) {
	h.getRegionsByType(w, statistics.UndersizedRegion, r)
}

// @Tags     region
// @Summary  List all empty regions.
// @Produce  json
// @Success  200  {object}  RegionsInfo
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
// @Summary  List all range holes whitout any region info.
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
// @Success  200  {object}  RegionsInfo
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
		h.rd.JSON(w, http.StatusNotFound, errs.ErrRegionNotFound(uint64(id)).Error())
		return
	}

	left, right := rc.GetAdjacentRegions(region)
	b, err := marshalRegionsInfoJSON(r.Context(), []*core.RegionInfo{left, right})
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Data(w, http.StatusOK, b)
}

const (
	defaultRegionLimit     = 16
	maxRegionLimit         = 10240
	minRegionHistogramSize = 1
	minRegionHistogramKeys = 1000
)

// @Tags     region
// @Summary  List regions with the highest write flow.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/writeflow [get]
func (h *regionsHandler) GetTopWriteFlowRegions(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() })
}

// @Tags     region
// @Summary  List regions with the highest read flow.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/readflow [get]
func (h *regionsHandler) GetTopReadFlowRegions(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() })
}

// @Tags     region
// @Summary  List regions with the largest conf version.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/confver [get]
func (h *regionsHandler) GetTopConfVerRegions(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
	})
}

// @Tags     region
// @Summary  List regions with the largest version.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/version [get]
func (h *regionsHandler) GetTopVersionRegions(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
	})
}

// @Tags     region
// @Summary  List regions with the largest size.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/size [get]
func (h *regionsHandler) GetTopSizeRegions(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetApproximateSize() < b.GetApproximateSize()
	})
}

// @Tags     region
// @Summary  List regions with the largest keys.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/keys [get]
func (h *regionsHandler) GetTopKeysRegions(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetApproximateKeys() < b.GetApproximateKeys()
	})
}

// @Tags     region
// @Summary  List regions with the highest CPU usage.
// @Param    limit  query  integer  false  "Limit count"  default(16)
// @Produce  json
// @Success  200  {object}  RegionsInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/cpu [get]
func (h *regionsHandler) GetTopCPURegions(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
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
	rc := getCluster(r)
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	startKey, rawStartKey, err := apiutil.ParseKey("start_key", input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	endKey, rawEndKey, err := apiutil.ParseKey("end_key", input)
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
	rc := getCluster(r)
	var input []map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
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
	var msgBuilder strings.Builder
	msgBuilder.Grow(128)
	msgBuilder.WriteString("Accelerate regions scheduling in given ranges: ")
	var regions []*core.RegionInfo
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
		regions = append(regions, rc.ScanRegions(startKey, endKey, limit)...)
		msgBuilder.WriteString(fmt.Sprintf("[%s,%s), ", rawStartKey, rawEndKey))
	}
	if len(regions) > 0 {
		regionsIDList := make([]uint64, 0, len(regions))
		for _, region := range regions {
			regionsIDList = append(regionsIDList, region.GetID())
		}
		rc.AddSuspectRegions(regionsIDList...)
	}
	h.rd.Text(w, http.StatusOK, msgBuilder.String())
}

func (h *regionsHandler) GetTopNRegions(w http.ResponseWriter, r *http.Request, less func(a, b *core.RegionInfo) bool) {
	rc := getCluster(r)
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
	b, err := marshalRegionsInfoJSON(r.Context(), regions)
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
	rc := getCluster(r)
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
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
	}
	opsCount := 0
	var failures map[uint64]error
	var err error
	if ok1 && ok2 {
		startKey, _, err := apiutil.ParseKey("start_key", input)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		endKey, _, err := apiutil.ParseKey("end_key", input)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		opsCount, failures, err = rc.GetRegionScatterer().ScatterRegionsByRange(startKey, endKey, group, retryLimit)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		ids, ok := typeutil.JSONToUint64Slice(input["regions_id"])
		if !ok {
			h.rd.JSON(w, http.StatusBadRequest, "regions_id is invalid")
			return
		}
		opsCount, failures, err = rc.GetRegionScatterer().ScatterRegionsByID(ids, group, retryLimit, false)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	// If there existed any operator failed to be added into Operator Controller, add its regions into unProcessedRegions
	percentage := 100
	if len(failures) > 0 {
		percentage = 100 - 100*len(failures)/(opsCount+len(failures))
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

// @Tags     region
// @Summary  Split regions with given split keys
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Split regions with given split keys"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/split [post]
func (h *regionsHandler) SplitRegions(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
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
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
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
