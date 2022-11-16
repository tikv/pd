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
	"fmt"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
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

// CheckAndInit checks the validity of the resource group and initializes the default values if not setting.
func (rg *ResourceGroup) CheckAndInit() error {
	res, _ := json.Marshal(rg)
	fmt.Println("CheckAndInit", string(res))
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

// FromProtoResourceGroup converts a rmpb.ResourceGroup to a ResourceGroup.
func FromProtoResourceGroup(group *rmpb.ResourceGroup) *ResourceGroup {
	var (
		resourceSettings *NativeResourceSettings
		ruSettings       *RequestUnitSettings
	)

	if settings := group.GetSettings().GetResourceSettings(); settings != nil {
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
	}

	if settings := group.GetSettings().GetRUSettings(); settings != nil {
		ruSettings = &RequestUnitSettings{
			RRU: GroupTokenBucket{
				TokenBucket: settings.GetRRU(),
			},
			WRU: GroupTokenBucket{
				TokenBucket: settings.GetWRU(),
			},
		}
	}

	rg := &ResourceGroup{
		Name:             group.ResourceGroupName,
		Mode:             group.Settings.Mode,
		RUSettings:       ruSettings,
		ResourceSettings: resourceSettings,
	}
	return rg
}

// IntoProtoResourceGroup converts a ResourceGroup to a rmpb.ResourceGroup.
func (rg *ResourceGroup) IntoProtoResourceGroup() *rmpb.ResourceGroup {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		group := &rmpb.ResourceGroup{
			ResourceGroupName: rg.Name,
			Settings: &rmpb.GroupSettings{
				Mode: rmpb.GroupMode_RUMode,
				RUSettings: &rmpb.GroupRequestUnitSettings{
					RRU: rg.RUSettings.RRU.GetTokenBucket(),
					WRU: rg.RUSettings.WRU.GetTokenBucket(),
				},
			},
		}
		return group
	case rmpb.GroupMode_NativeMode: // Native mode
		group := &rmpb.ResourceGroup{
			ResourceGroupName: rg.Name,
			Settings: &rmpb.GroupSettings{
				Mode: rmpb.GroupMode_NativeMode,
				ResourceSettings: &rmpb.GroupResourceSettings{
					Cpu:     rg.ResourceSettings.CPU.GetTokenBucket(),
					IoRead:  rg.ResourceSettings.IOReadBandwidth.GetTokenBucket(),
					IoWrite: rg.ResourceSettings.IOWriteBandwidth.GetTokenBucket(),
				},
			},
		}
		return group
	}
	return nil
}
