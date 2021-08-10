package api

import (
	"encoding/json"
	"net/http"

	"github.com/tikv/pd/server"
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