package api

import (
	"net/http"
)

type version struct {
	Version string `json:"version"`
}

func getVersion(w http.ResponseWriter, r *http.Request) {
	version := &version{
		Version: "1.0.0",
	}
	rd.JSON(w, http.StatusOK, version)
}
