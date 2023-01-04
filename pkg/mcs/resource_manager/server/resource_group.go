// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package server provides a set of struct definitions for the resource group, can be imported.
package server

import (
	"encoding/json"
	"path"
	"sync"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/storage"
	"go.uber.org/zap"
)

const (
	// ResourceGroupPathPrefix is the prefix of the resource group path to store group settings.
	GroupSettingsPathPrefix = "/settings"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	sync.RWMutex
	Name string         `json:"name"`
	Mode rmpb.GroupMode `json:"mode"`
	// RU settings
	RUSettings *RequestUnitSettings `json:"r_u_settings,omitempty"`
	// Native resource settings
	ResourceSettings *NativeResourceSettings `json:"resource_settings,omitempty"`
}

// RequestUnitSettings is the definition of the RU settings.
type RequestUnitSettings struct {
	RRU GroupTokenBucket `json:"rru,omitempty"`
	WRU GroupTokenBucket `json:"wru,omitempty"`
}

// NativeResourceSettings is the definition of the native resource settings.
type NativeResourceSettings struct {
	CPU              GroupTokenBucket `json:"cpu,omitempty"`
	IOReadBandwidth  GroupTokenBucket `json:"io_read_bandwidth,omitempty"`
	IOWriteBandwidth GroupTokenBucket `json:"io_write_bandwidth,omitempty"`
}

func (rg *ResourceGroup) String() string {
	res, err := json.Marshal(rg)
	if err != nil {
		log.Error("marshal resource group failed", zap.Error(err))
		return ""
	}
	return string(res)
}

// Copy copies the resource group.
func (rg *ResourceGroup) Copy() *ResourceGroup {
	// TODO: use a better way to copy
	rg.RLock()
	defer rg.RUnlock()
	res, err := json.Marshal(rg)
	if err != nil {
		panic(err)
	}
	var newRG ResourceGroup
	err = json.Unmarshal(res, &newRG)
	if err != nil {
		panic(err)
	}
	return &newRG
}

// CheckAndInit checks the validity of the resource group and initializes the default values if not setting.
// Only used to initialize the resource group when creating.
func (rg *ResourceGroup) CheckAndInit() error {
	if len(rg.Name) == 0 || len(rg.Name) > 32 {
		return errors.New("invalid resource group name, the length should be in [1,32]")
	}
	if rg.Mode != rmpb.GroupMode_RUMode && rg.Mode != rmpb.GroupMode_NativeMode {
		return errors.New("invalid resource group mode")
	}
	if rg.Mode == rmpb.GroupMode_RUMode {
		if rg.RUSettings == nil {
			rg.RUSettings = &RequestUnitSettings{}
		}
		if rg.ResourceSettings != nil {
			return errors.New("invalid resource group settings, RU mode should not set resource settings")
		}
	}
	if rg.Mode == rmpb.GroupMode_NativeMode {
		if rg.ResourceSettings == nil {
			rg.ResourceSettings = &NativeResourceSettings{}
		}
		if rg.RUSettings != nil {
			return errors.New("invalid resource group settings, native mode should not set RU settings")
		}
	}
	return nil
}

// PatchSettings patches the resource group settings.
// Only used to patch the resource group when updating.
// Note: the tokens is the delta value to patch.
func (rg *ResourceGroup) PatchSettings(metaGroup *rmpb.ResourceGroup) error {
	rg.Lock()
	defer rg.Unlock()
	if metaGroup.GetMode() != rg.Mode {
		return errors.New("only support reconfigure in same mode, maybe you should delete and create a new one")
	}
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if metaGroup.GetRUSettings() == nil {
			return errors.New("invalid resource group settings, RU mode should set RU settings")
		}
		rg.RUSettings.RRU.patch(metaGroup.GetRUSettings().GetRRU())
		rg.RUSettings.WRU.patch(metaGroup.GetRUSettings().GetWRU())
	case rmpb.GroupMode_NativeMode:
		if metaGroup.GetResourceSettings() == nil {
			return errors.New("invalid resource group settings, native mode should set resource settings")
		}
		rg.ResourceSettings.CPU.patch(metaGroup.GetResourceSettings().GetCpu())
		rg.ResourceSettings.IOReadBandwidth.patch(metaGroup.GetResourceSettings().GetIoRead())
		rg.ResourceSettings.IOWriteBandwidth.patch(metaGroup.GetResourceSettings().GetIoWrite())
	}
	log.Info("patch resource group settings", zap.String("name", rg.Name), zap.String("settings", rg.String()))
	return nil
}

// FromProtoResourceGroup converts a rmpb.ResourceGroup to a ResourceGroup.
func FromProtoResourceGroup(group *rmpb.ResourceGroup) *ResourceGroup {
	var (
		resourceSettings *NativeResourceSettings
		ruSettings       *RequestUnitSettings
	)

	rg := &ResourceGroup{
		Name: group.Name,
		Mode: group.Mode,
	}
	switch group.GetMode() {
	case rmpb.GroupMode_RUMode:
		if settings := group.GetRUSettings(); settings != nil {
			ruSettings = &RequestUnitSettings{
				RRU: GroupTokenBucket{
					TokenBucket: settings.GetRRU(),
				},
				WRU: GroupTokenBucket{
					TokenBucket: settings.GetWRU(),
				},
			}
			rg.RUSettings = ruSettings
		}
	case rmpb.GroupMode_NativeMode:
		if settings := group.GetResourceSettings(); settings != nil {
			resourceSettings = &NativeResourceSettings{
				CPU: GroupTokenBucket{
					TokenBucket: settings.GetCpu(),
				},
				IOReadBandwidth: GroupTokenBucket{
					TokenBucket: settings.GetIoRead(),
				},
				IOWriteBandwidth: GroupTokenBucket{
					TokenBucket: settings.GetIoWrite(),
				},
			}
			rg.ResourceSettings = resourceSettings
		}
	}
	return rg
}

// IntoProtoResourceGroup converts a ResourceGroup to a rmpb.ResourceGroup.
func (rg *ResourceGroup) IntoProtoResourceGroup() *rmpb.ResourceGroup {
	rg.RLock()
	defer rg.RUnlock()
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		group := &rmpb.ResourceGroup{
			Name: rg.Name,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RRU: rg.RUSettings.RRU.TokenBucket,
				WRU: rg.RUSettings.WRU.TokenBucket,
			},
		}
		return group
	case rmpb.GroupMode_NativeMode: // Native mode
		group := &rmpb.ResourceGroup{
			Name: rg.Name,
			Mode: rmpb.GroupMode_NativeMode,
			ResourceSettings: &rmpb.GroupResourceSettings{
				Cpu:     rg.ResourceSettings.CPU.TokenBucket,
				IoRead:  rg.ResourceSettings.IOReadBandwidth.TokenBucket,
				IoWrite: rg.ResourceSettings.IOWriteBandwidth.TokenBucket,
			},
		}
		return group
	}
	return nil
}

// persistSettings persists the resource group settings.
// TODO: persist the state of the group separately.
func (rg *ResourceGroup) persistSettings(storage storage.Storage) error {
	rg.RLock()
	defer rg.RUnlock()
	metaGroup := rg.IntoProtoResourceGroup()
	return storage.SaveResourceGroup(path.Join(GroupSettingsPathPrefix, rg.Name), metaGroup)
}
