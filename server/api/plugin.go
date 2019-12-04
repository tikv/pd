package api

import (
	"net/http"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type pluginHandler struct {
	*server.Handler
	rd *render.Render
}

func newPluginHandler(handler *server.Handler, rd *render.Render) *pluginHandler {
	return &pluginHandler{
		Handler: handler,
		rd:      rd,
	}
}

func (h *pluginHandler) LoadPlugin(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	var err error
	err = h.PluginLoad(input["plugin-path"].(string))
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *pluginHandler) UpdatePlugin(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	var err error
	err = h.PluginUpdate(input["plugin-path"].(string))
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *pluginHandler) UnloadPlugin(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	var err error
	err = h.PluginUnload(input["plugin-path"].(string))
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
