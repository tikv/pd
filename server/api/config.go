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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/unrolled/render"
)

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

// @Tags config
// @Summary Get full config.
// @Produce json
// @Success 200 {object} config.Config
// @Router /config [get]
func (h *confHandler) Get(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetConfig())
}

// @Tags config
// @Summary Get default config.
// @Produce json
// @Success 200 {object} config.Config
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config/default [get]
func (h *confHandler) GetDefault(w http.ResponseWriter, r *http.Request) {
	config := config.NewConfig()
	err := config.Adjust(nil, false)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}

	h.rd.JSON(w, http.StatusOK, config)
}

// FIXME: details of input json body params
// @Tags config
// @Summary Update a config item.
// @Accept json
// @Param ttlSecond query integer false "ttl". ttl param is only for BR and lightning now. Don't use it.
// @Param body body object false "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config [post]
func (h *confHandler) Post(w http.ResponseWriter, r *http.Request) {
	cfg := h.svr.GetConfig()
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

	var ttls int
	if ttlSec := r.URL.Query().Get("ttlSecond"); ttlSec != "" {
		var err error
		ttls, err = strconv.Atoi(ttlSec)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	// if ttlSecond defined, we will apply if to temp configuration.
	if ttls > 0 {
		err := h.svr.SaveTTLConfig(conf, time.Duration(ttls)*time.Second)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, "The config is updated.")
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
		key := findTag(reflect.TypeOf(config.Config{}), k)
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

func (h *confHandler) updateConfig(cfg *config.Config, key string, value interface{}) error {
	kp := strings.Split(key, ".")
	switch kp[0] {
	case "schedule":
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
	}
	return errors.Errorf("config prefix %s not found", kp[0])
}

// If we have both "a.c" and "b.c" config items, for a given c, it's hard for us to decide which config item it represents.
// We'd better to naming a config item without duplication.
func findTag(t reflect.Type, tag string) string {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		column := field.Tag.Get("json")
		c := strings.Split(column, ",")
		if c[0] == tag {
			return c[0]
		}

		if field.Type.Kind() == reflect.Struct {
			path := findTag(field.Type, tag)
			if path == "" {
				continue
			}
			return field.Tag.Get("json") + "." + path
		}
	}
	return ""
}

func (h *confHandler) updateSchedule(config *config.Config, key string, value interface{}) error {
	data, err := json.Marshal(map[string]interface{}{key: value})
	if err != nil {
		return err
	}

	updated, found, err := h.mergeConfig(&config.Schedule, data)
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

func (h *confHandler) updateReplication(config *config.Config, key string, value interface{}) error {
	data, err := json.Marshal(map[string]interface{}{key: value})
	if err != nil {
		return err
	}

	updated, found, err := h.mergeConfig(&config.Replication, data)
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

func (h *confHandler) updateReplicationModeConfig(config *config.Config, key []string, value interface{}) error {
	cfg := make(map[string]interface{})
	cfg = getConfigMap(cfg, key, value)
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	updated, found, err := h.mergeConfig(&config.ReplicationMode, data)
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

func (h *confHandler) updatePDServerConfig(config *config.Config, key string, value interface{}) error {
	data, err := json.Marshal(map[string]interface{}{key: value})
	if err != nil {
		return err
	}

	updated, found, err := h.mergeConfig(&config.PDServerCfg, data)
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

func (h *confHandler) updateLogLevel(kp []string, value interface{}) error {
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

func (h *confHandler) updateClusterVersion(value interface{}) error {
	if version, ok := value.(string); ok {
		err := h.svr.SetClusterVersion(version)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.Errorf("input value %v is illegal", value)
}

func getConfigMap(cfg map[string]interface{}, key []string, value interface{}) map[string]interface{} {
	if len(key) == 1 {
		cfg[key[0]] = value
		return cfg
	}

	subConfig := make(map[string]interface{})
	cfg[key[0]] = getConfigMap(subConfig, key[1:], value)
	return cfg
}

func (h *confHandler) mergeConfig(v interface{}, data []byte) (updated bool, found bool, err error) {
	old, _ := json.Marshal(v)
	if err := json.Unmarshal(data, v); err != nil {
		return false, false, err
	}
	new, _ := json.Marshal(v)
	if !bytes.Equal(old, new) {
		return true, true, nil
	}
	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		return false, false, err
	}
	t := reflect.TypeOf(v).Elem()
	for i := 0; i < t.NumField(); i++ {
		jsonTag := t.Field(i).Tag.Get("json")
		if i := strings.Index(jsonTag, ","); i != -1 { // trim 'foobar,string' to 'foobar'
			jsonTag = jsonTag[:i]
		}
		if _, ok := m[jsonTag]; ok {
			return false, true, nil
		}
	}
	return false, false, nil
}

// @Tags config
// @Summary Get schedule config.
// @Produce json
// @Success 200 {object} config.ScheduleConfig
// @Router /config/schedule [get]
func (h *confHandler) GetSchedule(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetScheduleConfig())
}

// @Tags config
// @Summary Update a schedule config item.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/schedule [post]
func (h *confHandler) SetSchedule(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetScheduleConfig()
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetScheduleConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "The config is updated.")
}

// @Tags config
// @Summary Get replication config.
// @Produce json
// @Success 200 {object} config.ReplicationConfig
// @Router /config/replicate [get]
func (h *confHandler) GetReplication(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationConfig())
}

// @Tags config
// @Summary Update a replication config item.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/replicate [post]
func (h *confHandler) SetReplication(w http.ResponseWriter, r *http.Request) {
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

// @Tags config
// @Summary Get label property config.
// @Produce json
// @Success 200 {object} config.LabelPropertyConfig
// @Failure 400 {string} string "The input is invalid."
// @Router /config/label-property [get]
func (h *confHandler) GetLabelProperty(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLabelProperty())
}

// @Tags config
// @Summary Update label property config item.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/label-property [post]
func (h *confHandler) SetLabelProperty(w http.ResponseWriter, r *http.Request) {
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

// @Tags config
// @Summary Get cluster version.
// @Produce json
// @Success 200 {object} semver.Version
// @Router /config/cluster-version [get]
func (h *confHandler) GetClusterVersion(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetClusterVersion())
}

// @Tags config
// @Summary Update cluster version.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The cluster version is updated."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/cluster-version [post]
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

// @Tags config
// @Summary Get replication mode config.
// @Produce json
// @Success 200 {object} config.ReplicationModeConfig
// @Router /config/replication-mode [get]
func (h *confHandler) GetReplicationMode(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationModeConfig())
}

// @Tags config
// @Summary Set replication mode config.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The replication mode config is updated."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config/replication-mode [post]
func (h *confHandler) SetReplicationMode(w http.ResponseWriter, r *http.Request) {
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

// @Tags config
// @Summary Get PD server config.
// @Produce json
// @Success 200 {object} config.PDServerConfig
// @Router /config/pd-server [get]
func (h *confHandler) GetPDServer(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetPDServerConfig())
}
