package api

import (
	"net/http"

	"github.com/tikv/pd/server/versioninfo"
	"github.com/unrolled/render"
)

type version struct {
	Version string `json:"version"`
}

type versionHandler struct {
	rd *render.Render
}

func newVersionHandler(rd *render.Render) *versionHandler {
	return &versionHandler{
		rd: rd,
	}
}

// @Summary Get the version of PD server.
// @Produce json
// @Success 200 {object} version
// @Router /version [get]
func (h *versionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	version := &version{
		Version: versioninfo.PDReleaseVersion,
	}
	h.rd.JSON(w, http.StatusOK, version)
}
