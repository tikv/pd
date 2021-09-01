package api

import (
	"encoding/json"
	"net/http"

	mapset "github.com/deckarep/golang-set"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/statistics"
	"github.com/unrolled/render"
)

type hotHistoryStatusHandler struct {
	*server.Handler
	rd *render.Render
}

func newHotHistoryStatusHandler(handler *server.Handler, rd *render.Render) *hotHistoryStatusHandler {
	return &hotHistoryStatusHandler{
		Handler: handler,
		rd:      rd,
	}
}

// @Tags hotspot
// @Summary List the history hot regions.
// @Produce json
// @Success 200 {object} statistics.HistoryHotRegions
// @Router /hotspot/history-hot-regions [post]
func (h *hotHistoryStatusHandler) GetHotHistoryRegions(w http.ResponseWriter, r *http.Request) {
	len := r.ContentLength
	body := make([]byte, len)
	r.Body.Read(body)
	historyHotRegionsRequest := &statistics.HistoryHotRegionsRequest{}
	json.Unmarshal(body, historyHotRegionsRequest)
	results, err := h.Handler.GetAllRequestHistroyHotRegion(historyHotRegionsRequest)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, results)
}

func getAllRequestHistroyHotRegion(request *statistics.HistoryHotRegionsRequest, storage *cluster.HotRegionStorage) (*statistics.HistoryHotRegions, error) {
	iter := storage.NewIterator(request.StartTime, request.EndTime)
	results := make([]*statistics.HistoryHotRegion, 0)
	var regionSet, storeSet, peerSet, typeSet mapset.Set
	if len(request.RegionID) != 0 {
		regionSet = mapset.NewSet(request.RegionID)
	}
	if len(request.StoreID) != 0 {
		storeSet = mapset.NewSet(request.StoreID)
	}
	if len(request.PeerID) != 0 {
		peerSet = mapset.NewSet(request.PeerID)
	}
	if len(request.HotRegionTypes) != 0 {
		typeSet = mapset.NewSet(request.HotRegionTypes)
	}
	var next *statistics.HistoryHotRegion
	var err error
	for next, err = iter.Next(); next != nil && err != nil; next, err = iter.Next() {
		if regionSet != nil && !regionSet.Contains(next.RegionID) {
			continue
		}
		if storeSet != nil && !storeSet.Contains(next.StoreID) {
			continue
		}
		if peerSet != nil && !peerSet.Contains(next.StoreID) {
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
		if typeSet != nil && !typeSet.Contains(next.HotRegionType) {
			continue
		}
		results = append(results, next)
	}
	return &statistics.HistoryHotRegions{
		HistoryHotRegion: results,
	}, err
}
