// Copyright 2022 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/reflectutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"

	"github.com/unrolled/render"
)

type selfProtectionConfHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newSelfProtectionConfHandler(svr *server.Server, rd *render.Render) *selfProtectionConfHandler {
	return &selfProtectionConfHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags self_protection
// @Summary Get Self Protection config.
// @Produce json
// @Success 200 {object} config.Config
// @Router /self-proteciton [get]
func (h *selfProtectionConfHandler) GetSelfProtectionConfig(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetSelfProtectionConfig())
}

// @Tags self_protection
// @Summary Update some self-proteciton config items.
// @Accept json
// @Param body body object false "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /self-proteciton [post]
func (h *selfProtectionConfHandler) SetSelfProtectionConfig(w http.ResponseWriter, r *http.Request) {
	cfg := h.svr.GetSelfProtectionConfig()
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	conf := make(map[string]interface{})
	if err := json.Unmarshal(data, &conf); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	for k, v := range conf {
		if s := strings.Split(k, "."); len(s) > 1 {
			if err := h.updateSelfProtectionConfig(cfg, k, v); err != nil {
				h.rd.JSON(w, http.StatusBadRequest, err.Error())
				return
			}
			continue
		}
		key := reflectutil.FindJSONFullTagByChildTag(reflect.TypeOf(config.SelfProtectionConfig{}), k)
		if key == "" {
			h.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("config item %s not found", k))
			return
		}
		if err := h.updateSelfProtectionConfig(cfg, key, v); err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	h.rd.JSON(w, http.StatusOK, "The self-protection config is updated.")
}

func (h *selfProtectionConfHandler) updateSelfProtectionConfig(cfg *config.SelfProtectionConfig, key string, value interface{}) error {
	kp := strings.Split(key, ".")
	switch kp[0] {
	case "audit":
		return h.updateAudit(cfg, kp[len(kp)-1], value)
	}
	return errors.Errorf("config prefix %s not found", kp[0])
}

func (h *selfProtectionConfHandler) updateAudit(config *config.SelfProtectionConfig, key string, value interface{}) error {
	data, err := json.Marshal(map[string]interface{}{key: value})
	if err != nil {
		return err
	}

	updated, found, err := mergeConfig(&config.AuditConfig, data)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = h.svr.SetAuditConfig(config.AuditConfig)
	}
	return err
}
