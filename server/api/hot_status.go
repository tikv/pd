package api

import (
	"net/http"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type hotStatusHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newHotStatusHandler(svr *server.Server, rd *render.Render) *hotStatusHandler {
	return &hotStatusHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *hotStatusHandler) GetHot(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetHotRegions())
}

func (h *hotStatusHandler) GetHotStores(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetHotStores())
}
