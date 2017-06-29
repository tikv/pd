package api

import (
	"net/http"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type statusHandler struct {
	rd *render.Render
}

type status struct {
	BuildTS string `json:"build_ts"`
	GitHash string `json:"git_hash"`
}

func newStatusHandler(rd *render.Render) *statusHandler {
	return &statusHandler{
		rd: rd,
	}
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	version := status{
		BuildTS: server.PDBuildTS,
		GitHash: server.PDGitHash,
	}

	h.rd.JSON(w, http.StatusOK, version)
}
