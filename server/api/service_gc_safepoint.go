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
	"math"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server"
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

// GetGCSafePoint gets the service GC safepoint.
// @Tags     service_gc_safepoint
// @Summary  Get all service GC safepoint.
// @Produce  json
// @Success  200  {array}   ListServiceGCSafepoint
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /gc/safepoint [get]
func (h *serviceGCSafepointHandler) GetGCSafePoint(w http.ResponseWriter, _ *http.Request) {
	gcStateManager := h.svr.GetGCStateManager()
	gcState, err := gcStateManager.GetGCState(constant.NullKeyspaceID)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	ssps := make([]*endpoint.ServiceSafePoint, 0, len(gcState.GCBarriers))
	var gcWorkerSsp *endpoint.ServiceSafePoint
	for _, barrier := range gcState.GCBarriers {
		equivalentSsp := barrier.ToServiceSafePoint(constant.NullKeyspaceID)
		ssps = append(ssps, equivalentSsp)
		if equivalentSsp.ServiceID == keypath.GCWorkerServiceSafePointID {
			gcWorkerSsp = equivalentSsp
		}
	}
	if gcWorkerSsp == nil {
		// Generate a pseudo service safe point for GC worker. While GC worker's service safe point won't exist after
		// the new GC states API is adopted, some existing tests relies on the fact to pass: the minimal service safe
		// point indicates the lower bound of the timestamp that is safe to read. Now, this value becomes the txn safe
		// point. We generate a pseudo service safe point with the value of txn safe point to provide the compatibility.
		gcWorkerSsp = &endpoint.ServiceSafePoint{
			ServiceID:  keypath.GCWorkerServiceSafePointID,
			ExpiredAt:  math.MaxInt64,
			SafePoint:  gcState.TxnSafePoint,
			KeyspaceID: constant.NullKeyspaceID,
		}
		ssps = append(ssps, gcWorkerSsp)
	}

	// GC worker should always be the minimum one in most cases. However, this can be violated if the cluster is just
	// upgraded from a old version where GC states API is not yet ready.
	minSSp := gcWorkerSsp
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
		GCSafePoint:           gcState.GCSafePoint,
		ServiceGCSafepoints:   ssps,
		MinServiceGcSafepoint: minServiceGcSafepoint,
	}
	h.rd.JSON(w, http.StatusOK, list)
}

// DeleteGCSafePoint deletes a service GC safepoint.
// @Tags     service_gc_safepoint
// @Summary  Delete a service GC safepoint.
// @Param    service_id  path  string  true  "Service ID"
// @Produce  json
// @Success  200  {string}  string  "Delete service GC safepoint successfully."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /gc/safepoint/{service_id} [delete]
// @Tags     rule
func (h *serviceGCSafepointHandler) DeleteGCSafePoint(w http.ResponseWriter, r *http.Request) {
	// Directly write to the storage and bypassing the existing constraint checks.
	// It's risky to do this, but when this HTTP API is used, it usually means that we are already taking risks.
	provider := h.svr.GetStorage().GetGCStateProvider()
	serviceID := mux.Vars(r)["service_id"]
	err := provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		// As GC barriers and service safe points shares the same data, deleting GC barriers acts the same as deleting
		// service safe points.
		err := wb.DeleteGCBarrier(constant.NullKeyspaceID, serviceID)
		return err
	})
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Delete service GC safepoint successfully.")
}
