// Copyright 2020 TiKV Project Authors.
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
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type serviceGCSafepointHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newServiceGCSafepointHandler(svr *server.Server, rd *render.Render) *serviceGCSafepointHandler {
	return &serviceGCSafepointHandler{
		svr: svr,
		rd:  rd,
	}
}

// ListServiceGCSafepoint is the response for list service GC safepoint.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// This type is in sync with `pd/client/http/types.go`.
type ListServiceGCSafepoint struct {
	ServiceGCSafepoints   []*endpoint.ServiceSafePoint `json:"service_gc_safe_points"`
	MinServiceGcSafepoint uint64                       `json:"min_service_gc_safe_point,omitempty"`
	GCSafePoint           uint64                       `json:"gc_safe_point"`
}

// @Tags     service_gc_safepoint
// @Summary  Get all service GC safepoint.
// @Produce  json
// @Success  200  {array}   ListServiceGCSafepoint
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /gc/safepoint [get]
func (h *serviceGCSafepointHandler) GetGCSafePoint(w http.ResponseWriter, _ *http.Request) {
	storage := h.svr.GetStorage()
	gcSafepoint, err := storage.LoadGCSafePoint()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ssps, err := storage.LoadAllServiceGCSafePoints()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	var minSSp *endpoint.ServiceSafePoint
	for _, ssp := range ssps {
		if (minSSp == nil || minSSp.SafePoint > ssp.SafePoint) &&
			ssp.ExpiredAt > time.Now().Unix() {
			minSSp = ssp
		}
	}
	minServiceGcSafepoint := uint64(0)
	if minSSp != nil {
		minServiceGcSafepoint = minSSp.SafePoint
	}
	list := ListServiceGCSafepoint{
		GCSafePoint:           gcSafepoint,
		ServiceGCSafepoints:   ssps,
		MinServiceGcSafepoint: minServiceGcSafepoint,
	}
	h.rd.JSON(w, http.StatusOK, list)
}

// @Tags     service_gc_safepoint
// @Summary  Delete a service GC safepoint.
// @Param    service_id  path  string  true  "Service ID"
// @Produce  json
// @Success  200  {string}  string  "Delete service GC safepoint successfully."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /gc/safepoint/{service_id} [delete]
// @Tags     rule
func (h *serviceGCSafepointHandler) DeleteGCSafePoint(w http.ResponseWriter, r *http.Request) {
	storage := h.svr.GetStorage()
	serviceID := mux.Vars(r)["service_id"]
	err := storage.RemoveServiceGCSafePoint(serviceID)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Delete service GC safepoint successfully.")
}
