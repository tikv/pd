// Copyright 2026 TiKV Project Authors.
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
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/pingcap/errors"

	rm "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
)

// prepareConfigUpdates checks the entire request without publishing changes.
// Setters still own persistence and runtime side effects; preparing a batch does
// not turn writes to different storage keys or services into a transaction.
func (h *confHandler) prepareConfigUpdates(cfg *config.Config, conf map[string]any) ([]func() error, error) {
	patches := make(map[string]map[string]any)
	updates := make(map[string]func() error)
	oldKeyspace := h.svr.GetPersistOptions().GetKeyspaceConfig()
	cfg.Keyspace = *oldKeyspace.Clone()
	oldFallback := cfg.Microservice.EnableResourceManagerFallback
	for _, k := range slices.Sorted(maps.Keys(conf)) {
		value := conf[k]
		key := k
		if !strings.Contains(k, ".") {
			key = reflectutil.FindJSONFullTagByChildTag(reflect.TypeFor[config.Config](), k)
		}
		if key == "" {
			return nil, errors.Errorf("config item %s not found", k)
		}
		path := strings.Split(key, ".")
		prefix, name := path[0], path[len(path)-1]
		var target any
		switch prefix {
		case "schedule":
			if h.svr.IsTTLConfigExist(key) {
				return nil, errors.Errorf("need to clean up TTL first for %s", key)
			}
			target = &cfg.Schedule
		case "replication":
			target = &cfg.Replication
		case "replication-mode":
			if len(path) < 2 {
				return nil, errors.Errorf("cannot update config prefix %s", prefix)
			}
			// Merge nested fields into the candidate without calling its setter.
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
			if updated {
				updates[prefix] = func() error { return h.svr.SetReplicationModeConfig(cfg.ReplicationMode) }
			}
			continue
		case "pd-server":
			target = &cfg.PDServerCfg
		case "keyspace":
			target = &cfg.Keyspace
		case "micro-service":
			target = &cfg.Microservice
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
			updates[prefix] = func() error { return h.updateLogLevel(path, value) }
			continue
		case "cluster-version":
			version, ok := value.(string)
			if !ok {
				return nil, errors.Errorf("input value %v is illegal", value)
			}
			if _, err := versioninfo.ParseVersion(version); err != nil {
				return nil, err
			}
			updates[prefix] = func() error { return h.updateClusterVersion(value) }
			continue
		case "controller":
			field := reflectutil.FindJSONFullTagByChildTag(reflect.TypeFor[rm.ControllerConfig](), name)
			if field == "" {
				return nil, errors.Errorf("controller config item %s not found", name)
			}
			if field != name {
				return nil, errors.Errorf("can't update controller child item %s, only top-level items are allowed", name)
			}
			// Decode into a fresh value so nested pointers in the live controller
			// configuration cannot be changed during validation.
			var controller rm.ControllerConfig
			if _, _, err := jsonutil.AddKeyValue(&controller, name, value); err != nil {
				return nil, err
			}
			updates[key] = func() error { return h.updateControllerConfig(name, value) }
			continue
		default:
			return nil, errors.Errorf("config prefix %s not found", prefix)
		}
		found := reflectutil.FindSameFieldByJSON(target, map[string]any{name: value})
		if !found && prefix != "schedule" {
			// The other legacy setters decode with encoding/json, which also
			// accepts case-insensitive field names.
			for field := range reflect.TypeOf(target).Elem().Fields() {
				tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")
				if strings.EqualFold(tag, name) {
					name, found = tag, true
					break
				}
			}
		}
		if !found {
			return nil, errors.Errorf("config item %s not found", name)
		}
		// Check each supplied value before aliases are combined, so a valid
		// spelling cannot hide an invalid value supplied through another one.
		data, err := json.Marshal(map[string]any{name: value})
		if err != nil {
			return nil, err
		}
		probe := reflect.New(reflect.TypeOf(target).Elem()).Interface()
		if err := json.Unmarshal(data, probe); err != nil {
			return nil, err
		}
		patch := patches[prefix]
		if patch == nil {
			patch = make(map[string]any)
			patches[prefix] = patch
		}
		if _, exists := patch[name]; exists && prefix == "schedule" {
			return nil, errors.Errorf("config item %s specified more than once", name)
		}
		patch[name] = value
	}
	for _, prefix := range slices.Sorted(maps.Keys(patches)) {
		patch := patches[prefix]
		data, err := json.Marshal(patch)
		if err != nil {
			return nil, err
		}
		var target any
		var validate, apply func() error
		switch prefix {
		case "schedule":
			target = &cfg.Schedule
			validate = func() error {
				if err := cfg.Schedule.Validate(); err != nil {
					return err
				}
				return cfg.Schedule.Deprecated()
			}
			// Rebase the patch on the latest schedule config under its existing
			// persistence lock rather than publishing this request's snapshot.
			apply = func() error { return h.svr.PatchScheduleConfig(data) }
		case "replication":
			target = &cfg.Replication
			validate = func() error { return h.svr.ValidateReplicationConfig(cfg.Replication) }
			apply = func() error { return h.svr.SetReplicationConfig(cfg.Replication) }
		case "pd-server":
			target = &cfg.PDServerCfg
			validate = func() error { return h.svr.ValidatePDServerConfig(&cfg.PDServerCfg) }
			apply = func() error { return h.svr.SetPDServerConfig(cfg.PDServerCfg) }
		case "keyspace":
			target, validate = &cfg.Keyspace, cfg.Keyspace.Validate
			apply = func() error {
				if _, ok := patch["meta-service-groups"]; ok {
					return h.updateMetaServiceGroups(oldKeyspace, &cfg.Keyspace)
				}
				return h.svr.SetKeyspaceConfig(oldKeyspace, &cfg.Keyspace)
			}
		case "micro-service":
			target = &cfg.Microservice
			validate = func() error {
				if cfg.Microservice.EnableResourceManagerFallback != oldFallback {
					return errors.New("cannot update config item enable-resource-manager-fallback dynamically")
				}
				return nil
			}
			apply = func() error { return h.svr.SetMicroserviceConfig(cfg.Microservice) }
		}
		updated, _, err := jsonutil.MergeJSONObject(target, data)
		if err != nil {
			return nil, err
		}
		if !updated && prefix != "schedule" {
			continue
		}
		if err := validate(); err != nil {
			return nil, err
		}
		if prefix == "keyspace" {
			if _, ok := patch["meta-service-groups"]; ok {
				if err := config.AdjustMetaServiceGroups(cfg.Keyspace.MetaServiceGroups); err != nil {
					return nil, err
				}
			}
		}
		updates[prefix] = apply
	}
	if _, ok := updates["replication-mode"]; ok && config.NormalizeReplicationMode(cfg.ReplicationMode.ReplicationMode) == "" {
		return nil, errors.Errorf("invalid replication mode: %v", cfg.ReplicationMode.ReplicationMode)
	}
	result := make([]func() error, 0, len(updates))
	for _, key := range slices.Sorted(maps.Keys(updates)) {
		result = append(result, updates[key])
	}
	return result, nil
}
