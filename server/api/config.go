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
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/unrolled/render"

	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	rm "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

// This line is to ensure the package `sc` could always be imported so that
// the swagger could generate the right definitions for the config structs.
var (
	_                                  *sc.ScheduleConfig = nil
	resourceManagerControllerConfigURL                    = "/resource-manager/api/v1/config/controller"
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

// GetConfig gets the full config.
//
//	@Tags		config
//	@Summary	Get full config.
//	@Produce	json
//	@Success	200	{object}	config.Config
//	@Router		/config [get]
func (h *confHandler) GetConfig(w http.ResponseWriter, r *http.Request) {
	if !h.svr.GetMember().IsServing() {
		localCfg := h.svr.GetConfig()
		leaderCfg, err := h.getLeaderConfig()
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		mergedCfg := localCfg
		mergedCfg.Replication = leaderCfg.Replication
		mergedCfg.Schedule = leaderCfg.Schedule
		h.rd.JSON(w, http.StatusOK, mergedCfg)
		return
	}
	cfg := h.svr.GetConfig()
	if h.svr.IsServiceIndependent(constant.SchedulingServiceName) &&
		r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) != "true" {
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

// GetDefaultConfig gets the default config.
//
//	@Tags		config
//	@Summary	Get default config.
//	@Produce	json
//	@Success	200	{object}	config.Config
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/config/default [get]
func (h *confHandler) GetDefaultConfig(w http.ResponseWriter, _ *http.Request) {
	config := config.NewConfig()
	err := config.Adjust(nil, false)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}

	h.rd.JSON(w, http.StatusOK, config)
}

// SetConfig sets the config.
// FIXME: details of input json body params
//
//	@Tags		config
//	@Summary	Update a config item.
//	@Accept		json
//	@Param		ttlSecond	query	integer	false	"ttl param is only for BR and lightning now. Don't use it."
//	@Param		body		body	object	false	"json params"
//	@Produce	json
//	@Success	200	{string}	string	"The config is updated."
//	@Failure	400	{string}	string	"The input is invalid."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/config [post]
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

	oldKeyspace := h.svr.GetPersistOptions().GetKeyspaceConfig()
	cfg.Keyspace = *oldKeyspace.Clone()
	updates, err := h.prepareConfigUpdates(cfg, conf)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	for _, prefix := range slices.Sorted(maps.Keys(updates)) {
		if err := h.updateConfig(cfg, prefix, updates[prefix], oldKeyspace); err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	h.rd.JSON(w, http.StatusOK, "The config is updated.")
}

// prepareConfigUpdates checks the entire request and its final field combinations
// before any setters run. Separate persistence and service writes are not a transaction.
func (h *confHandler) prepareConfigUpdates(cfg *config.Config, conf map[string]any) (map[string]map[string]any, error) {
	oldFallback := cfg.Microservice.EnableResourceManagerFallback
	targets := map[string]any{
		"schedule": &cfg.Schedule, "replication": &cfg.Replication,
		"pd-server": &cfg.PDServerCfg, "keyspace": &cfg.Keyspace,
		"micro-service": &cfg.Microservice,
	}
	updates := make(map[string]map[string]any)
	for _, k := range slices.Sorted(maps.Keys(conf)) {
		value, key := conf[k], k
		if !strings.Contains(k, ".") {
			key = reflectutil.FindJSONFullTagByChildTag(reflect.TypeFor[config.Config](), k)
		}
		if key == "" {
			return nil, errors.Errorf("config item %s not found", k)
		}
		path := strings.Split(key, ".")
		prefix, name := path[0], path[len(path)-1]
		patch := updates[prefix]
		if patch == nil {
			patch = make(map[string]any)
		}
		target, grouped := targets[prefix]
		if grouped {
			if prefix == "schedule" {
				if h.svr.IsTTLConfigExist(key) {
					return nil, errors.Errorf("need to clean up TTL first for %s", key)
				}
			} else {
				// Preserve encoding/json's case-insensitive matching in legacy setters.
				for field := range reflect.TypeOf(target).Elem().Fields() {
					tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")
					if strings.EqualFold(tag, name) {
						name = tag
						break
					}
				}
			}
			if !reflectutil.FindSameFieldByJSON(target, map[string]any{name: value}) {
				return nil, errors.Errorf("config item %s not found", name)
			}
			if _, exists := patch[name]; exists && prefix == "schedule" {
				return nil, errors.Errorf("config item %s specified more than once", name)
			}
			// Merge aliases in sorted key order, preserving disjoint object fields.
			decodeTarget := target
			if prefix == "schedule" {
				decodeTarget = reflect.New(reflect.TypeOf(target).Elem()).Interface()
			}
			updated, _, err := jsonutil.AddKeyValue(decodeTarget, name, value)
			if err != nil {
				return nil, err
			}
			if !updated && prefix != "schedule" {
				continue
			}
		} else {
			switch prefix {
			case "replication-mode":
				if len(path) < 2 {
					return nil, errors.Errorf("cannot update config prefix %s", prefix)
				}
				data, err := json.Marshal(getConfigMap(make(map[string]any), path[1:], value))
				if err != nil {
					return nil, err
				}
				updated, found, err := jsonutil.MergeJSONObject(&cfg.ReplicationMode, data)
				if err != nil {
					return nil, err
				}
				if !found {
					return nil, errors.Errorf("config item %s not found", name)
				}
				if !updated {
					continue
				}
			case "log":
				if len(path) != 2 || name != "level" {
					return nil, errors.New("only support changing log level")
				}
				level, ok := value.(string)
				if !ok {
					return nil, errors.Errorf("input value %v is illegal", value)
				}
				if !logutil.IsLevelLegal(level) {
					return nil, errors.Errorf("log level %s is illegal", level)
				}
			case "cluster-version":
				version, ok := value.(string)
				if !ok {
					return nil, errors.Errorf("input value %v is illegal", value)
				}
				if _, err := versioninfo.ParseVersion(version); err != nil {
					return nil, err
				}
				name = prefix
			case "controller":
				field := reflectutil.FindJSONFullTagByChildTag(reflect.TypeFor[rm.ControllerConfig](), name)
				if field == "" {
					return nil, errors.Errorf("controller config item %s not found", name)
				}
				if field != name {
					return nil, errors.Errorf("can't update controller child item %s, only top-level items are allowed", name)
				}
				// Decode into a fresh value to avoid changing live nested pointers.
				var controller rm.ControllerConfig
				if _, _, err := jsonutil.AddKeyValue(&controller, name, value); err != nil {
					return nil, err
				}
				// Forward each alias separately so partial controller objects also merge.
				name = k
			default:
				return nil, errors.Errorf("config prefix %s not found", prefix)
			}
		}
		patch[name] = value
		updates[prefix] = patch
	}
	for _, prefix := range slices.Sorted(maps.Keys(updates)) {
		if prefix == "schedule" {
			data, err := json.Marshal(updates[prefix])
			if err != nil {
				return nil, err
			}
			if _, _, err := jsonutil.MergeJSONObject(&cfg.Schedule, data); err != nil {
				return nil, err
			}
		}
		var err error
		switch prefix {
		case "schedule":
			err = cfg.Schedule.Validate()
			if err == nil {
				err = cfg.Schedule.Deprecated()
			}
		case "replication":
			err = h.svr.ValidateReplicationConfig(cfg.Replication)
		case "replication-mode":
			if config.NormalizeReplicationMode(cfg.ReplicationMode.ReplicationMode) == "" {
				err = errors.Errorf("invalid replication mode: %v", cfg.ReplicationMode.ReplicationMode)
			}
		case "pd-server":
			err = h.svr.ValidatePDServerConfig(&cfg.PDServerCfg)
		case "keyspace":
			err = cfg.Keyspace.Validate()
			if _, ok := updates[prefix]["meta-service-groups"]; ok && err == nil {
				err = config.AdjustMetaServiceGroups(cfg.Keyspace.MetaServiceGroups)
			}
		case "micro-service":
			if cfg.Microservice.EnableResourceManagerFallback != oldFallback {
				err = errors.New("cannot update config item enable-resource-manager-fallback dynamically")
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return updates, nil
}

// updateConfig applies one group that prepareConfigUpdates has validated.
func (h *confHandler) updateConfig(cfg *config.Config, prefix string, patch map[string]any, oldKeyspace *config.KeyspaceConfig) error {
	switch prefix {
	case "schedule":
		data, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		// Patch the latest schedule configuration under its persistence lock.
		return h.svr.PatchScheduleConfig(data)
	case "replication":
		return h.svr.SetReplicationConfig(cfg.Replication)
	case "replication-mode":
		return h.svr.SetReplicationModeConfig(cfg.ReplicationMode)
	case "pd-server":
		return h.svr.SetPDServerConfig(cfg.PDServerCfg)
	case "keyspace":
		if _, ok := patch["meta-service-groups"]; ok {
			return h.updateMetaServiceGroups(oldKeyspace, &cfg.Keyspace)
		}
		return h.svr.SetKeyspaceConfig(oldKeyspace, &cfg.Keyspace)
	case "micro-service":
		return h.svr.SetMicroserviceConfig(cfg.Microservice)
	case "log":
		return h.updateLogLevel(patch["level"].(string))
	case "cluster-version":
		return h.svr.SetClusterVersion(patch[prefix].(string))
	case "controller":
		for _, name := range slices.Sorted(maps.Keys(patch)) {
			if err := h.updateControllerConfig(name[strings.LastIndex(name, ".")+1:], patch[name]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *confHandler) updateMetaServiceGroups(oldCfg, newCfg *config.KeyspaceConfig) error {
	manager := h.svr.GetMetaServiceGroupManager()
	if manager == nil {
		return errors.New("meta-service groups manager is not initialized")
	}
	// Use newCfg.MetaServiceGroups directly (not a GetMetaServiceGroups copy) so
	// the map persisted via newCfg and the map passed to the manager have the
	// same normalized contents. The manager takes ownership of its own copy.
	newGroups := newCfg.MetaServiceGroups
	// Normalize (trim/dedup) before computing deletedGroups. Otherwise a
	// whitespace-padded ID (e.g. " g ") for an existing group g would not match
	// the already-normalized key in oldCfg, so g would be wrongly classified as a
	// deletion and rejected by UpdateGroupsSafely when it still has keyspaces.
	if err := config.AdjustMetaServiceGroups(newGroups); err != nil {
		return err
	}
	deletedGroups := make([]string, 0)
	for id := range oldCfg.GetMetaServiceGroups() {
		if _, ok := newGroups[id]; !ok {
			deletedGroups = append(deletedGroups, id)
		}
	}
	return manager.UpdateGroupsSafely(h.svr.Context(), newGroups, deletedGroups, func() error {
		return h.svr.SetKeyspaceConfigWithoutKeyspaceManagerUpdate(oldCfg, newCfg)
	}, func() {
		h.svr.UpdateKeyspaceConfig(newCfg)
	})
}

func (h *confHandler) updateLogLevel(level string) error {
	if err := h.svr.SetLogLevel(level); err != nil {
		return err
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))
	return nil
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

// GetScheduleConfig gets the schedule config.
//
//	@Tags		config
//	@Summary	Get schedule config.
//	@Produce	json
//	@Success	200	{object}	sc.ScheduleConfig
//	@Router		/config/schedule [get]
func (h *confHandler) GetScheduleConfig(w http.ResponseWriter, r *http.Request) {
	if h.svr.IsServiceIndependent(constant.SchedulingServiceName) &&
		r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) != "true" {
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

// SetScheduleConfig sets the schedule config.
//
//	@Tags		config
//	@Summary	Update a schedule config item.
//	@Accept		json
//	@Param		body	body	object	string	"json params"
//	@Produce	json
//	@Success	200	{string}	string	"The config is updated."
//	@Failure	400	{string}	string	"The input is invalid."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Failure	503	{string}	string	"PD server has no leader."
//	@Router		/config/schedule [post]
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

	if err := h.svr.PatchScheduleConfig(data); err != nil {
		var errCode errcode.ErrorCode
		taggedErr := apiutil.TagJSONError(err)
		if jsonErr, ok := errors.Cause(taggedErr).(apiutil.JSONError); ok {
			errCode = errcode.NewInvalidInputErr(jsonErr.Err)
			apiutil.ErrorResp(h.rd, w, errCode)
			return
		}
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "The config is updated.")
}

// GetReplicationConfig gets the replication config.
//
//	@Tags		config
//	@Summary	Get replication config.
//	@Produce	json
//	@Success	200	{object}	sc.ReplicationConfig
//	@Router		/config/replicate [get]
func (h *confHandler) GetReplicationConfig(w http.ResponseWriter, r *http.Request) {
	failpoint.Inject("getReplicationConfigFailed", func(v failpoint.Value) {
		code := v.(int)
		h.rd.JSON(w, code, "get config failed")
	})
	if h.svr.IsServiceIndependent(constant.SchedulingServiceName) &&
		r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) != "true" {
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

// SetReplicationConfig sets the replication config.
//
//	@Tags		config
//	@Summary	Update a replication config item.
//	@Accept		json
//	@Param		body	body	object	string	"json params"
//	@Produce	json
//	@Success	200	{string}	string	"The config is updated."
//	@Failure	400	{string}	string	"The input is invalid."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Failure	503	{string}	string	"PD server has no leader."
//	@Router		/config/replicate [post]
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

// GetLabelPropertyConfig gets the label property config.
//
//	@Tags		config
//	@Summary	Get label property config.
//	@Produce	json
//	@Success	200	{object}	config.LabelPropertyConfig
//	@Router		/config/label-property [get]
func (h *confHandler) GetLabelPropertyConfig(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLabelProperty())
}

// SetLabelPropertyConfig sets the label property config.
//
//	@Tags		config
//	@Summary	Update label property config item.
//	@Accept		json
//	@Param		body	body	object	string	"json params"
//	@Produce	json
//	@Success	200	{string}	string	"The config is updated."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Failure	503	{string}	string	"PD server has no leader."
//	@Router		/config/label-property [post]
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

// GetClusterVersion gets the cluster version.
//
//	@Tags		config
//	@Summary	Get cluster version.
//	@Produce	json
//	@Success	200	{object}	semver.Version
//	@Router		/config/cluster-version [get]
func (h *confHandler) GetClusterVersion(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetClusterVersion())
}

// SetClusterVersion sets the cluster version.
//
//	@Tags		config
//	@Summary	Update cluster version.
//	@Accept		json
//	@Param		body	body	object	string	"json params"
//	@Produce	json
//	@Success	200	{string}	string	"The cluster version is updated."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Failure	503	{string}	string	"PD server has no leader."
//	@Router		/config/cluster-version [post]
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

// GetReplicationModeConfig gets the replication mode config.
//
//	@Tags		config
//	@Summary	Get replication mode config.
//	@Produce	json
//	@Success	200	{object}	config.ReplicationModeConfig
//	@Router		/config/replication-mode [get]
func (h *confHandler) GetReplicationModeConfig(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationModeConfig())
}

// SetReplicationModeConfig sets the replication mode config.
//
//	@Tags		config
//	@Summary	Set replication mode config.
//	@Accept		json
//	@Param		body	body	object	string	"json params"
//	@Produce	json
//	@Success	200	{string}	string	"The replication mode config is updated."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/config/replication-mode [post]
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

// GetPDServerConfig gets the PD server config.
//
//	@Tags		config
//	@Summary	Get PD server config.
//	@Produce	json
//	@Success	200	{object}	config.PDServerConfig
//	@Router		/config/pd-server [get]
func (h *confHandler) GetPDServerConfig(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetPDServerConfig())
}

func (h *confHandler) getLeaderConfig() (*config.Config, error) {
	addrs := h.svr.GetMember().GetServingUrls()
	if len(addrs) == 0 {
		return nil, errs.ErrLeaderNil.FastGenByArgs()
	}
	addr := addrs[0]
	url := fmt.Sprintf("%s/pd/api/v1/config", addr)
	resp, err := apiutil.GetJSONWithoutBody(h.svr.GetHTTPClient(), url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errs.ErrSendRequest.FastGenByArgs(resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return unmarshalRemoteConfig(b)
}

func (h *confHandler) getSchedulingServerConfig() (*config.Config, error) {
	addr, ok := h.svr.GetServicePrimaryAddr(h.svr.Context(), constant.SchedulingServiceName)
	if !ok {
		return nil, errs.ErrNotFoundSchedulingPrimary.FastGenByArgs()
	}
	url := fmt.Sprintf("%s/scheduling/api/v1/config", addr)
	resp, err := apiutil.GetJSONWithoutBody(h.svr.GetHTTPClient(), url)
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
	return unmarshalRemoteConfig(b)
}

func unmarshalRemoteConfig(data []byte) (*config.Config, error) {
	var cfg config.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	var fields struct {
		Schedule json.RawMessage `json:"schedule"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	if len(fields.Schedule) != 0 {
		if err := cfg.Schedule.MigrateDeprecatedFlagsFromJSON(fields.Schedule); err != nil {
			return nil, err
		}
	}
	return &cfg, nil
}

func (h *confHandler) updateControllerConfig(key string, value any) error {
	addrs := h.svr.GetMember().GetServingUrls()
	if len(addrs) == 0 {
		return errs.ErrLeaderNil.FastGenByArgs()
	}
	addr := addrs[0]
	url := fmt.Sprintf("%s%s", addr, resourceManagerControllerConfigURL)

	body := map[string]any{
		key: value,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := apiutil.PostJSON(h.svr.GetHTTPClient(), url, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("failed to update controller config: %s", string(b))
	}

	return nil
}
