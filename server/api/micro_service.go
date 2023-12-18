package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

type microServiceHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newMicroServiceHandlerHandler(svr *server.Server, rd *render.Render) *microServiceHandler {
	return &microServiceHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     members
// @Summary  Get all members of the cluster for the specified service.
// @Produce  json
// @Success  200  {object}  []string
// @Router   /ms/members/{service} [get]
func (h *microServiceHandler) GetMembers(w http.ResponseWriter, r *http.Request) {
	if !h.svr.IsAPIServiceMode() {
		h.rd.JSON(w, http.StatusServiceUnavailable, "not support micro service")
		return
	}

	if service := mux.Vars(r)["service"]; len(service) > 0 {
		resps, err := discovery.GetMembers(service, h.svr.GetClient())
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if resps == nil {
			h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("no members for %s", service))
			return
		}

		var addrs []string
		for _, resp := range resps.Responses {
			for _, keyValue := range resp.GetResponseRange().GetKvs() {
				var entry discovery.ServiceRegistryEntry
				if err = entry.Deserialize(keyValue.Value); err != nil {
					log.Info("deserialize failed", zap.String("key", string(keyValue.Key)), zap.Error(err))
				}
				addrs = append(addrs, entry.ServiceAddr)
			}
		}
		h.rd.JSON(w, http.StatusOK, addrs)
		return
	}

	h.rd.JSON(w, http.StatusInternalServerError, "please specify service")
}

// @Tags     Primary
// @Summary  Get the primary of the cluster for the specified service.
// @Produce  json
// @Success  200  {object}  pdpb.Member
// @Router   /ms/primary/{service} [get]
func (h *microServiceHandler) GetPrimary(w http.ResponseWriter, r *http.Request) {
	if !h.svr.IsAPIServiceMode() {
		h.rd.JSON(w, http.StatusServiceUnavailable, "not support micro service")
		return
	}
	if service := mux.Vars(r)["service"]; len(service) > 0 {
		primary, _, err := discovery.GetMCSPrimary(service, h.svr.GetClient(), r.URL.Query().Get("keyspace_id"))
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if primary == nil {
			h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("no primary for %s", service))
			return
		}
		h.rd.JSON(w, http.StatusOK, primary)
		return
	}

	h.rd.JSON(w, http.StatusInternalServerError, "please specify service")
}
