// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errcode"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/apiutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
	"go.uber.org/zap"
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

func (h *confHandler) Get(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetConfig())
}

func (h *confHandler) GetDefault(w http.ResponseWriter, r *http.Request) {
	config := config.NewConfig()
	err := config.Adjust(nil)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}

	h.rd.JSON(w, http.StatusOK, config)
}

func (h *confHandler) Post(w http.ResponseWriter, r *http.Request) {
	if h.svr.GetConfig().EnableDynamicConfig {
		cm := h.svr.GetConfigManager()
		m := make(map[string]interface{})
		json.NewDecoder(r.Body).Decode(&m)
		entries, err := transToEntries(m)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		client := h.svr.GetConfigClient()
		if client == nil {
			h.rd.JSON(w, http.StatusServiceUnavailable, "no leader")
			return
		}
		err = redirectUpdateReq(h.svr.Context(), client, cm, entries)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}
	config := h.svr.GetConfig()
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	found1, err := h.updateSchedule(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	found2, err := h.updateReplication(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	found3, err := h.updatePDServerConfig(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found1 && !found2 && !found3 {
		h.rd.JSON(w, http.StatusBadRequest, "config item not found")
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) updateSchedule(data []byte, config *config.Config) (bool, error) {
	updated, found, err := h.mergeConfig(&config.Schedule, data)
	if err != nil {
		return false, err
	}
	if updated {
		err = h.svr.SetScheduleConfig(config.Schedule)
	}
	return found, err
}

func (h *confHandler) updateReplication(data []byte, config *config.Config) (bool, error) {
	updated, found, err := h.mergeConfig(&config.Replication, data)
	if err != nil {
		return false, err
	}
	if updated {
		err = h.svr.SetReplicationConfig(config.Replication)
	}
	return found, err
}

func (h *confHandler) updatePDServerConfig(data []byte, config *config.Config) (bool, error) {
	updated, found, err := h.mergeConfig(&config.PDServerCfg, data)
	if err != nil {
		return false, err
	}
	if updated {
		err = h.svr.SetPDServerConfig(config.PDServerCfg)
	}
	return found, err
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

func (h *confHandler) GetSchedule(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetScheduleConfig())
}

func (h *confHandler) SetSchedule(w http.ResponseWriter, r *http.Request) {
	if h.svr.GetConfig().EnableDynamicConfig {
		cm := h.svr.GetConfigManager()
		m := make(map[string]interface{})
		json.NewDecoder(r.Body).Decode(&m)
		entries, err := transToEntries(m)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		client := h.svr.GetConfigClient()
		if client == nil {
			h.rd.JSON(w, http.StatusServiceUnavailable, "no leader")
			return
		}
		err = redirectUpdateReq(h.svr.Context(), client, cm, entries)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}
	config := h.svr.GetScheduleConfig()
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetScheduleConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetReplication(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationConfig())
}

func (h *confHandler) SetReplication(w http.ResponseWriter, r *http.Request) {
	if h.svr.GetConfig().EnableDynamicConfig {
		cm := h.svr.GetConfigManager()
		m := make(map[string]interface{})
		json.NewDecoder(r.Body).Decode(&m)
		entries, err := transToEntries(m)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		client := h.svr.GetConfigClient()
		if client == nil {
			h.rd.JSON(w, http.StatusServiceUnavailable, "no leader")
			return
		}

		err = redirectUpdateReq(h.svr.Context(), client, cm, entries)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}
	config := h.svr.GetReplicationConfig()
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetReplicationConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetLabelProperty(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLabelProperty())
}

func (h *confHandler) SetLabelProperty(w http.ResponseWriter, r *http.Request) {
	input := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	if h.svr.GetConfig().EnableDynamicConfig {
		cm := h.svr.GetConfigManager()
		typ := input["type"]
		labelKey, labelValue := input["label-key"], input["label-value"]
		cfg := h.svr.GetScheduleOption().LoadLabelPropertyConfig().Clone()
		switch input["action"] {
		case "set":
			for _, l := range cfg[typ] {
				if l.Key == labelKey && l.Value == labelValue {
					return
				}
			}
			cfg[typ] = append(cfg[typ], config.StoreLabel{Key: labelKey, Value: labelValue})
		case "delete":
			oldLabels := cfg[typ]
			cfg[typ] = []config.StoreLabel{}
			for _, l := range oldLabels {
				if l.Key == labelKey && l.Value == labelValue {
					continue
				}
				cfg[typ] = append(cfg[typ], l)
			}
			if len(cfg[typ]) == 0 {
				delete(cfg, typ)
			}
		default:
			err := errors.Errorf("unknown action %v", input["action"])
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		var buf bytes.Buffer
		if err := toml.NewEncoder(&buf).Encode(cfg); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		entries := []*entry{{key: "label-property", value: buf.String()}}
		client := h.svr.GetConfigClient()
		if client == nil {
			h.rd.JSON(w, http.StatusServiceUnavailable, "no leader")
			return
		}
		err := redirectUpdateReq(h.svr.Context(), client, cm, entries)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, nil)
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
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetClusterVersion(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetClusterVersion())
}

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

	if h.svr.GetConfig().EnableDynamicConfig {
		kind := &configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: server.Component}}}
		v := &configpb.Version{Global: h.svr.GetConfigManager().GlobalCfgs[server.Component].GetVersion()}
		entry := &configpb.ConfigEntry{Name: "cluster-version", Value: version}
		client := h.svr.GetConfigClient()
		if client == nil {
			h.rd.JSON(w, http.StatusServiceUnavailable, "no leader")
		}
		_, _, err := h.svr.GetConfigClient().Update(h.svr.Context(), v, kind, []*configpb.ConfigEntry{entry})
		if err != nil {
			log.Error("update cluster version meet error", zap.Error(err))
		}
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	err := h.svr.SetClusterVersion(version)
	if err != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInternalErr(err))
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
