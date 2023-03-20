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
	"sync"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"go.uber.org/zap"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	sync.RWMutex
	Name string         `json:"name"`
	Mode rmpb.GroupMode `json:"mode"`
	// RU settings
	RUSettings *RequestUnitSettings `json:"r_u_settings,omitempty"`
	// raw resource settings
	RawResourceSettings *RawResourceSettings `json:"raw_resource_settings,omitempty"`
	Priority            uint32               `json:"priority"`
}

// RequestUnitSettings is the definition of the RU settings.
type RequestUnitSettings struct {
	RU GroupTokenBucket `json:"r_u,omitempty"`
}

// NewRequestUnitSettings creates a new RequestUnitSettings with the given token bucket.
func NewRequestUnitSettings(tokenBucket *rmpb.TokenBucket) *RequestUnitSettings {
	return &RequestUnitSettings{
		RU: NewGroupTokenBucket(tokenBucket),
	}
}

// RawResourceSettings is the definition of the native resource settings.
type RawResourceSettings struct {
	CPU              GroupTokenBucket `json:"cpu,omitempty"`
	IOReadBandwidth  GroupTokenBucket `json:"io_read_bandwidth,omitempty"`
	IOWriteBandwidth GroupTokenBucket `json:"io_write_bandwidth,omitempty"`
}

// NewRawResourceSettings creates a new RawResourceSettings with the given token buckets.
func NewRawResourceSettings(cpu, ioRead, ioWrite *rmpb.TokenBucket) *RawResourceSettings {
	return &RawResourceSettings{
		CPU:              NewGroupTokenBucket(cpu),
		IOReadBandwidth:  NewGroupTokenBucket(ioRead),
		IOWriteBandwidth: NewGroupTokenBucket(ioWrite),
	}
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
	if rg.Priority > 16 {
		return errors.New("invalid resource group priority, the value should be in [0,16]")
	}
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if rg.RUSettings == nil {
			rg.RUSettings = NewRequestUnitSettings(nil)
		}
		if rg.RawResourceSettings != nil {
			return errors.New("invalid resource group settings, RU mode should not set raw resource settings")
		}
	case rmpb.GroupMode_RawMode:
		if rg.RawResourceSettings == nil {
			rg.RawResourceSettings = NewRawResourceSettings(nil, nil, nil)
		}
		if rg.RUSettings != nil {
			return errors.New("invalid resource group settings, raw mode should not set RU settings")
		}
	default:
		return errors.New("invalid resource group mode")
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
	if metaGroup.GetPriority() > 16 {
		return errors.New("invalid resource group priority, the value should be in [0,16]")
	}
	rg.Priority = metaGroup.Priority
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if metaGroup.GetRUSettings() == nil {
			return errors.New("invalid resource group settings, RU mode should set RU settings")
		}
		rg.RUSettings.RU.patch(metaGroup.GetRUSettings().GetRU())
	case rmpb.GroupMode_RawMode:
		if metaGroup.GetRawResourceSettings() == nil {
			return errors.New("invalid resource group settings, raw mode should set resource settings")
		}
		rg.RawResourceSettings.CPU.patch(metaGroup.GetRawResourceSettings().GetCpu())
		rg.RawResourceSettings.IOReadBandwidth.patch(metaGroup.GetRawResourceSettings().GetIoRead())
		rg.RawResourceSettings.IOWriteBandwidth.patch(metaGroup.GetRawResourceSettings().GetIoWrite())
	}
	log.Info("patch resource group settings", zap.String("name", rg.Name), zap.String("settings", rg.String()))
	return nil
}

// FromProtoResourceGroup converts a rmpb.ResourceGroup to a ResourceGroup.
func FromProtoResourceGroup(group *rmpb.ResourceGroup) *ResourceGroup {
	rg := &ResourceGroup{
		Name:     group.Name,
		Mode:     group.Mode,
		Priority: group.Priority,
	}
	switch group.GetMode() {
	case rmpb.GroupMode_RUMode:
		if settings := group.GetRUSettings(); settings != nil {
			rg.RUSettings = NewRequestUnitSettings(settings.GetRU())
		}
	case rmpb.GroupMode_RawMode:
		if settings := group.GetRawResourceSettings(); settings != nil {
			rg.RawResourceSettings = NewRawResourceSettings(
				settings.GetCpu(),
				settings.GetIoRead(),
				settings.GetIoWrite(),
			)
		}
	}
	return rg
}

// RequestRU requests the RU of the resource group.
func (rg *ResourceGroup) RequestRU(
	now time.Time,
	neededTokens float64,
	targetPeriodMs uint64,
) *rmpb.GrantedRUTokenBucket {
	rg.Lock()
	defer rg.Unlock()
	if rg.RUSettings == nil || rg.RUSettings.RU.Settings == nil {
		return nil
	}
	tb, trickleTimeMs := rg.RUSettings.RU.request(now, neededTokens, targetPeriodMs)
	return &rmpb.GrantedRUTokenBucket{GrantedTokens: tb, TrickleTimeMs: trickleTimeMs}
}

// IntoProtoResourceGroup converts a ResourceGroup to a rmpb.ResourceGroup.
func (rg *ResourceGroup) IntoProtoResourceGroup() *rmpb.ResourceGroup {
	rg.RLock()
	defer rg.RUnlock()
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		group := &rmpb.ResourceGroup{
			Name:     rg.Name,
			Mode:     rmpb.GroupMode_RUMode,
			Priority: rg.Priority,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: rg.RUSettings.RU.GetTokenBucket(),
			},
		}
		return group
	case rmpb.GroupMode_RawMode: // Raw mode
		group := &rmpb.ResourceGroup{
			Name:     rg.Name,
			Mode:     rmpb.GroupMode_RawMode,
			Priority: rg.Priority,
			RawResourceSettings: &rmpb.GroupRawResourceSettings{
				Cpu:     rg.RawResourceSettings.CPU.GetTokenBucket(),
				IoRead:  rg.RawResourceSettings.IOReadBandwidth.GetTokenBucket(),
				IoWrite: rg.RawResourceSettings.IOWriteBandwidth.GetTokenBucket(),
			},
		}
		return group
	}
	return nil
}

// persistSettings persists the resource group settings.
// TODO: persist the state of the group separately.
func (rg *ResourceGroup) persistSettings(storage endpoint.ResourceGroupStorage) error {
	metaGroup := rg.IntoProtoResourceGroup()
	return storage.SaveResourceGroupSetting(rg.Name, metaGroup)
}

// GroupStates is the tokens set of a resource group.
type GroupStates struct {
	// RU tokens
	RU *GroupTokenBucketState `json:"r_u,omitempty"`
	// raw resource tokens
	CPU     *GroupTokenBucketState `json:"cpu,omitempty"`
	IORead  *GroupTokenBucketState `json:"io_read,omitempty"`
	IOWrite *GroupTokenBucketState `json:"io_write,omitempty"`
}

// GetGroupStates get the token set of ResourceGroup.
func (rg *ResourceGroup) GetGroupStates() *GroupStates {
	rg.RLock()
	defer rg.RUnlock()
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		tokens := &GroupStates{
			RU: rg.RUSettings.RU.GroupTokenBucketState.Clone(),
		}
		return tokens
	case rmpb.GroupMode_RawMode: // Raw mode
		tokens := &GroupStates{
			CPU:     rg.RawResourceSettings.CPU.GroupTokenBucketState.Clone(),
			IORead:  rg.RawResourceSettings.IOReadBandwidth.GroupTokenBucketState.Clone(),
			IOWrite: rg.RawResourceSettings.IOWriteBandwidth.GroupTokenBucketState.Clone(),
		}
		return tokens
	}
	return nil
}

// SetStatesIntoResourceGroup updates the state of resource group.
func (rg *ResourceGroup) SetStatesIntoResourceGroup(states *GroupStates) {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if state := states.RU; state != nil {
			rg.RUSettings.RU.GroupTokenBucketState = *state
		}
	case rmpb.GroupMode_RawMode:
		if state := states.CPU; state != nil {
			rg.RawResourceSettings.CPU.GroupTokenBucketState = *state
		}
		if state := states.IORead; state != nil {
			rg.RawResourceSettings.IOReadBandwidth.GroupTokenBucketState = *state
		}
		if state := states.IOWrite; state != nil {
			rg.RawResourceSettings.IOWriteBandwidth.GroupTokenBucketState = *state
		}
	}
}

// persistStates persists the resource group tokens.
func (rg *ResourceGroup) persistStates(storage endpoint.ResourceGroupStorage) error {
	states := rg.GetGroupStates()
	return storage.SaveResourceGroupStates(rg.Name, states)
}
