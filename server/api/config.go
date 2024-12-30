// Copyright 2016 TiKV Project Authors.
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
	"strconv"
	"strings"
	"time"

	"github.com/unrolled/render"

	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

// This line is to ensure the package `sc` could always be imported so that
// the swagger could generate the right definitions for the config structs.
var _ *sc.ScheduleConfig = nil

type confHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newConfHandler(svr *server.Server, rd *render.Render) *confHandler {
	return &confHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     config
// @Summary  Get full config.
// @Produce  json
// @Success  200  {object}  config.Config
// @Router   /config [get]
func (h *confHandler) GetConfig(w http.ResponseWriter, r *http.Request) {
	cfg := h.svr.GetConfig()
	if h.svr.IsServiceIndependent(constant.SchedulingServiceName) &&
		r.Header.Get(apiutil.XForbiddenForwardToMicroServiceHeader) != "true" {
		schedulingServerConfig, err := h.getSchedulingServerConfig()
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		cfg.Schedule = schedulingServerConfig.Schedule
		cfg.Replication = schedulingServerConfig.Replication
	} else {
		cfg.Schedule.MaxMergeRegionKeys = cfg.Schedule.GetMaxMergeRegionKeys()
	}
	h.rd.JSON(w, http.StatusOK, cfg)
}

// @Tags     config
// @Summary  Get default config.
// @Produce  json
// @Success  200  {object}  config.Config
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/default [get]
func (h *confHandler) GetDefaultConfig(w http.ResponseWriter, _ *http.Request) {
	config := config.NewConfig()
	err := config.Adjust(nil, false)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}

	h.rd.JSON(w, http.StatusOK, config)
}

// FIXME: details of input json body params
// @Tags     config
// @Summary  Update a config item.
// @Accept   json
// @Param    ttlSecond  query  integer  false  "ttl param is only for BR and lightning now. Don't use it."
// @Param    body       body   object   false  "json params"
// @Produce  json
// @Success  200  {string}  string  "The config is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config [post]
func (h *confHandler) SetConfig(w http.ResponseWriter, r *http.Request) {
	cfg := h.svr.GetConfig()
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	conf := make(map[string]any)
	if err := json.Unmarshal(data, &conf); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	if ttlString := r.URL.Query().Get("ttlSecond"); ttlString != "" {
		ttlSec, err := strconv.Atoi(ttlString)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		// if ttlSecond defined, we will apply if to temp configuration.
		err = h.svr.SaveTTLConfig(conf, time.Duration(ttlSec)*time.Second)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if ttlSec == 0 {
			h.rd.JSON(w, http.StatusOK, "The ttl config is deleted.")
		} else {
			h.rd.JSON(w, http.StatusOK, "The ttl config is updated.")
		}
		return
	}

	for k, v := range conf {
		if s := strings.Split(k, "."); len(s) > 1 {
			if err := h.updateConfig(cfg, k, v); err != nil {
				h.rd.JSON(w, http.StatusBadRequest, err.Error())
				return
			}
			continue
		}
		key := reflectutil.FindJSONFullTagByChildTag(reflect.TypeOf(config.Config{}), k)
		if key == "" {
			h.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("config item %s not found", k))
			return
		}
		if err := h.updateConfig(cfg, key, v); err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	h.rd.JSON(w, http.StatusOK, "The config is updated.")
}

func (h *confHandler) updateConfig(cfg *config.Config, key string, value any) error {
	kp := strings.Split(key, ".")
	switch kp[0] {
	case "schedule":
		if h.svr.IsTTLConfigExist(key) {
			return errors.Errorf("need to clean up TTL first for %s", key)
		}
		return h.updateSchedule(cfg, kp[len(kp)-1], value)
	case "replication":
		return h.updateReplication(cfg, kp[len(kp)-1], value)
	case "replication-mode":
		if len(kp) < 2 {
			return errors.Errorf("cannot update config prefix %s", kp[0])
		}
		return h.updateReplicationModeConfig(cfg, kp[1:], value)
	case "pd-server":
		return h.updatePDServerConfig(cfg, kp[len(kp)-1], value)
	case "log":
		return h.updateLogLevel(kp, value)
	case "cluster-version":
		return h.updateClusterVersion(value)
	case "label-property": // TODO: support changing label-property
	case "keyspace":
		return h.updateKeyspaceConfig(cfg, kp[len(kp)-1], value)
	case "micro-service":
		return h.updateMicroServiceConfig(cfg, kp[len(kp)-1], value)
	}
	return errors.Errorf("config prefix %s not found", kp[0])
}

func (h *confHandler) updateKeyspaceConfig(config *config.Config, key string, value any) error {
	updated, found, err := jsonutil.AddKeyValue(&config.Keyspace, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = h.svr.SetKeyspaceConfig(config.Keyspace)
	}
	return err
}

func (h *confHandler) updateMicroServiceConfig(config *config.Config, key string, value any) error {
	updated, found, err := jsonutil.AddKeyValue(&config.MicroService, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = h.svr.SetMicroServiceConfig(config.MicroService)
	}
	return err
}

func (h *confHandler) updateSchedule(config *config.Config, key string, value any) error {
	updated, found, err := jsonutil.AddKeyValue(&config.Schedule, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = h.svr.SetScheduleConfig(config.Schedule)
	}
	return err
}

func (h *confHandler) updateReplication(config *config.Config, key string, value any) error {
	updated, found, err := jsonutil.AddKeyValue(&config.Replication, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = h.svr.SetReplicationConfig(config.Replication)
	}
	return err
}

func (h *confHandler) updateReplicationModeConfig(config *config.Config, key []string, value any) error {
	cfg := make(map[string]any)
	cfg = getConfigMap(cfg, key, value)
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	updated, found, err := jsonutil.MergeJSONObject(&config.ReplicationMode, data)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = h.svr.SetReplicationModeConfig(config.ReplicationMode)
	}
	return err
}

func (h *confHandler) updatePDServerConfig(config *config.Config, key string, value any) error {
	updated, found, err := jsonutil.AddKeyValue(&config.PDServerCfg, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = h.svr.SetPDServerConfig(config.PDServerCfg)
	}
	return err
}

func (h *confHandler) updateLogLevel(kp []string, value any) error {
	if len(kp) != 2 || kp[1] != "level" {
		return errors.Errorf("only support changing log level")
	}
	if level, ok := value.(string); ok {
		err := h.svr.SetLogLevel(level)
		if err != nil {
			return err
		}
		log.SetLevel(logutil.StringToZapLogLevel(level))
		return nil
	}
	return errors.Errorf("input value %v is illegal", value)
}

func (h *confHandler) updateClusterVersion(value any) error {
	if version, ok := value.(string); ok {
		err := h.svr.SetClusterVersion(version)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.Errorf("input value %v is illegal", value)
}

func getConfigMap(cfg map[string]any, key []string, value any) map[string]any {
	if len(key) == 1 {
		cfg[key[0]] = value
		return cfg
	}

	subConfig := make(map[string]any)
	cfg[key[0]] = getConfigMap(subConfig, key[1:], value)
	return cfg
}

// @Tags     config
// @Summary  Get schedule config.
// @Produce  json
// @Success  200  {object}  sc.ScheduleConfig
// @Router   /config/schedule [get]
func (h *confHandler) GetScheduleConfig(w http.ResponseWriter, r *http.Request) {
	if h.svr.IsServiceIndependent(constant.SchedulingServiceName) &&
		r.Header.Get(apiutil.XForbiddenForwardToMicroServiceHeader) != "true" {
		cfg, err := h.getSchedulingServerConfig()
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, cfg.Schedule)
		return
	}
	cfg := h.svr.GetScheduleConfig()
	cfg.MaxMergeRegionKeys = cfg.GetMaxMergeRegionKeys()
	h.rd.JSON(w, http.StatusOK, cfg)
}

// @Tags     config
// @Summary  Update a schedule config item.
// @Accept   json
// @Param    body  body  object  string  "json params"
// @Produce  json
// @Success  200  {string}  string  "The config is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Failure  503  {string}  string  "PD server has no leader."
// @Router   /config/schedule [post]
func (h *confHandler) SetScheduleConfig(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	conf := make(map[string]any)
	if err := json.Unmarshal(data, &conf); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	for k := range conf {
		key := fmt.Sprintf("schedule.%s", k)
		if h.svr.IsTTLConfigExist(key) {
			h.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("need to clean up TTL first for %s", key))
			return
		}
	}

	config := h.svr.GetScheduleConfig()
	err = json.Unmarshal(data, &config)
	if err != nil {
		var errCode errcode.ErrorCode
		err = apiutil.TagJSONError(err)
		if jsonErr, ok := errors.Cause(err).(apiutil.JSONError); ok {
			errCode = errcode.NewInvalidInputErr(jsonErr.Err)
		} else {
			errCode = errcode.NewInternalErr(err)
		}
		apiutil.ErrorResp(h.rd, w, errCode)
		return
	}

	if err := h.svr.SetScheduleConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "The config is updated.")
}

// @Tags     config
// @Summary  Get replication config.
// @Produce  json
// @Success  200  {object}  sc.ReplicationConfig
// @Router   /config/replicate [get]
func (h *confHandler) GetReplicationConfig(w http.ResponseWriter, r *http.Request) {
	if h.svr.IsServiceIndependent(constant.SchedulingServiceName) &&
		r.Header.Get(apiutil.XForbiddenForwardToMicroServiceHeader) != "true" {
		cfg, err := h.getSchedulingServerConfig()
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, cfg.Replication)
		return
	}
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationConfig())
}

// @Tags     config
// @Summary  Update a replication config item.
// @Accept   json
// @Param    body  body  object  string  "json params"
// @Produce  json
// @Success  200  {string}  string  "The config is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Failure  503  {string}  string  "PD server has no leader."
// @Router   /config/replicate [post]
func (h *confHandler) SetReplicationConfig(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetReplicationConfig()
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetReplicationConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "The config is updated.")
}

// @Tags     config
// @Summary  Get label property config.
// @Produce  json
// @Success  200  {object}  config.LabelPropertyConfig
// @Router   /config/label-property [get]
func (h *confHandler) GetLabelPropertyConfig(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLabelProperty())
}

// @Tags     config
// @Summary  Update label property config item.
// @Accept   json
// @Param    body  body  object  string  "json params"
// @Produce  json
// @Success  200  {string}  string  "The config is updated."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Failure  503  {string}  string  "PD server has no leader."
// @Router   /config/label-property [post]
func (h *confHandler) SetLabelPropertyConfig(w http.ResponseWriter, r *http.Request) {
	input := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	var err error
	switch input["action"] {
	case "set":
		err = h.svr.SetLabelProperty(input["type"], input["label-key"], input["label-value"])
	case "delete":
		err = h.svr.DeleteLabelProperty(input["type"], input["label-key"], input["label-value"])
	default:
		err = errors.Errorf("unknown action %v", input["action"])
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "The config is updated.")
}

// @Tags     config
// @Summary  Get cluster version.
// @Produce  json
// @Success  200  {object}  semver.Version
// @Router   /config/cluster-version [get]
func (h *confHandler) GetClusterVersion(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetClusterVersion())
}

// @Tags     config
// @Summary  Update cluster version.
// @Accept   json
// @Param    body  body  object  string  "json params"
// @Produce  json
// @Success  200  {string}  string  "The cluster version is updated."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Failure  503  {string}  string  "PD server has no leader."
// @Router   /config/cluster-version [post]
func (h *confHandler) SetClusterVersion(w http.ResponseWriter, r *http.Request) {
	input := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	version, ok := input["cluster-version"]
	if !ok {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errors.New("not set cluster-version")))
		return
	}

	err := h.svr.SetClusterVersion(version)
	if err != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInternalErr(err))
		return
	}
	h.rd.JSON(w, http.StatusOK, "The cluster version is updated.")
}

// @Tags     config
// @Summary  Get replication mode config.
// @Produce  json
// @Success  200  {object}  config.ReplicationModeConfig
// @Router   /config/replication-mode [get]
func (h *confHandler) GetReplicationModeConfig(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationModeConfig())
}

// @Tags     config
// @Summary  Set replication mode config.
// @Accept   json
// @Param    body  body  object  string  "json params"
// @Produce  json
// @Success  200  {string}  string  "The replication mode config is updated."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/replication-mode [post]
func (h *confHandler) SetReplicationModeConfig(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetReplicationModeConfig()
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetReplicationModeConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "The replication mode config is updated.")
}

// @Tags     config
// @Summary  Get PD server config.
// @Produce  json
// @Success  200  {object}  config.PDServerConfig
// @Router   /config/pd-server [get]
func (h *confHandler) GetPDServerConfig(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetPDServerConfig())
}

func (h *confHandler) getSchedulingServerConfig() (*config.Config, error) {
	addr, ok := h.svr.GetServicePrimaryAddr(h.svr.Context(), constant.SchedulingServiceName)
	if !ok {
		return nil, errs.ErrNotFoundSchedulingPrimary.FastGenByArgs()
	}
	url := fmt.Sprintf("%s/scheduling/api/v1/config", addr)
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := h.svr.GetHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errs.ErrSchedulingServer.FastGenByArgs(resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var schedulingServerConfig config.Config
	err = json.Unmarshal(b, &schedulingServerConfig)
	if err != nil {
		return nil, err
	}
	return &schedulingServerConfig, nil
}
