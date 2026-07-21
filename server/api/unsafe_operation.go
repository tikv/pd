// Copyright 2021 TiKV Project Authors.
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
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/unrolled/render"

	perrors "github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/unsaferecovery"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
)

var maxPlanExecutionTimeoutSeconds = float64(time.Duration(math.MaxInt64/2) / time.Second)

type unsafeOperationHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newUnsafeOperationHandler(svr *server.Server, rd *render.Render) *unsafeOperationHandler {
	return &unsafeOperationHandler{
		svr: svr,
		rd:  rd,
	}
}

// RemoveFailedStores removes failed stores unsafely.
//
//	@Tags		unsafe
//	@Summary	Remove failed stores unsafely.
//	@Accept		json
//	@Param		body	body	object	true	"json params"
//	@Produce	json
//
// Success 200 {string} string "Request has been accepted."
// Failure 400 {string} string "The input is invalid."
// Failure 500 {string} string "PD server failed to proceed the request."
//
//	@Router		/admin/unsafe/remove-failed-stores [post]
func (h *unsafeOperationHandler) RemoveFailedStores(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	stores := make(map[uint64]struct{})
	autoDetect, exists := input["auto-detect"].(bool)
	if !exists || !autoDetect {
		storeSlice, ok := typeutil.JSONToUint64Slice(input["stores"])
		if !ok {
			h.rd.JSON(w, http.StatusBadRequest, "Store ids are invalid")
			return
		}
		for _, store := range storeSlice {
			stores[store] = struct{}{}
		}
	}

	timeout, err := parseTimeout(input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	planExecutionTimeout, err := parsePlanExecutionTimeout(input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	disableParanoidCheck, err := parseDisableParanoidCheck(input)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := rc.GetUnsafeRecoveryController().RemoveFailedStoresWithOptions(stores, timeout, autoDetect, unsaferecovery.RemoveFailedStoresOptions{
		PlanExecutionTimeout: planExecutionTimeout,
		DisableParanoidCheck: disableParanoidCheck,
	}); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Request has been accepted.")
}

// AbortFailedStoresRemoval aborts the current failed stores removal.
//
//	@Tags		unsafe
//	@Summary	Abort the current failed stores removal.
//	@Produce	json
//
// Success 200 {string} string "Request has been accepted."
// Failure 400 {string} string "There is no ongoing failed stores removal."
// Failure 500 {string} string "PD server failed to proceed the request."
//
//	@Router		/admin/unsafe/remove-failed-stores/abort [post]
func (h *unsafeOperationHandler) AbortFailedStoresRemoval(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	if err := rc.GetUnsafeRecoveryController().AbortFailedStoresRemoval(); err != nil {
		if perrors.ErrorEqual(err, errs.ErrUnsafeRecoveryInvalidInput.FastGenByArgs("no ongoing unsafe recovery")) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Request has been accepted.")
}

func parseTimeout(input map[string]any) (uint64, error) {
	raw, exists := input["timeout"]
	if !exists {
		return 600, nil
	}
	rawTimeout, ok := raw.(float64)
	if !ok || rawTimeout <= 0 || rawTimeout != math.Trunc(rawTimeout) || rawTimeout > maxPlanExecutionTimeoutSeconds {
		return 0, errors.New("timeout is invalid")
	}
	return uint64(rawTimeout), nil
}

func parsePlanExecutionTimeout(input map[string]any) (time.Duration, error) {
	raw, exists, err := getAliasedOption(input, "plan-execution-timeout", "plan_execution_timeout")
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	rawTimeout, ok := raw.(float64)
	if !ok || rawTimeout <= 0 || rawTimeout != math.Trunc(rawTimeout) || rawTimeout > maxPlanExecutionTimeoutSeconds {
		return 0, errors.New("plan-execution-timeout is invalid")
	}
	return time.Duration(int64(rawTimeout)) * time.Second, nil
}

func parseDisableParanoidCheck(input map[string]any) (bool, error) {
	raw, exists, err := getAliasedOption(input, "disable-paranoid-check", "disable_paranoid_check")
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	rawDisableParanoidCheck, ok := raw.(bool)
	if !ok {
		return false, errors.New("disable-paranoid-check is invalid")
	}
	return rawDisableParanoidCheck, nil
}

func getAliasedOption(input map[string]any, keys ...string) (any, bool, error) {
	var (
		raw   any
		exist bool
	)
	for _, key := range keys {
		value, ok := input[key]
		if !ok {
			continue
		}
		if exist {
			return nil, false, errors.New(keys[0] + " is specified multiple times")
		}
		raw = value
		exist = true
	}
	return raw, exist, nil
}

// GetFailedStoresRemovalStatus gets the current status of failed stores removal.
//
//	@Tags		unsafe
//	@Summary	Show the current status of failed stores removal.
//	@Produce	json
//
// Success 200 {object} []StageOutput
//
//	@Router		/admin/unsafe/remove-failed-stores/show [get]
func (h *unsafeOperationHandler) GetFailedStoresRemovalStatus(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	h.rd.JSON(w, http.StatusOK, rc.GetUnsafeRecoveryController().Show())
}
