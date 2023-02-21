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
	"context"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"go.uber.org/zap"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	Name string         `json:"name"`
	Mode rmpb.GroupMode `json:"mode"`
	// RU settings
	RUSettings *RequestUnitSettings `json:"r_u_settings,omitempty"`
}

// RequestUnitSettings is the definition of the RU settings.
type RequestUnitSettings struct {
	RU *GroupTokenBucket `json:"ru,omitempty"`
}

// NewRequestUnitSettings creates a new RequestUnitSettings with the given token bucket.
func NewRequestUnitSettings(ctx context.Context, tokenBucket *rmpb.TokenBucket) *RequestUnitSettings {
	return &RequestUnitSettings{
		RU: newGroupTokenBucket(ctx, tokenBucket),
	}
}

// Copy copies the resource group.
func (rg *ResourceGroup) Copy() *ResourceGroup {
	return &ResourceGroup{
		Name: rg.Name,
		Mode: rg.Mode,
		RUSettings: &RequestUnitSettings{
			RU: rg.RUSettings.RU.Copy(),
		},
	}
}

// patchSettings patches the resource group settings.
// Only used to patch the resource group when updating.
// Note: the tokens is the delta value to patch.
func (rg *ResourceGroup) patchSettings(metaGroup *rmpb.ResourceGroup) error {
	if metaGroup.GetMode() != rg.Mode {
		return errors.New("only support reconfigure in same mode, maybe you should delete and create a new one")
	}
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		settings := metaGroup.GetRUSettings().GetRU()
		if settings == nil {
			return errors.New("invalid resource group settings, RU mode should set RU settings")
		}
		rg.RUSettings.RU.patch(settings)
		log.Info("patch resource group ru settings", zap.String("name", rg.Name), zap.Any("settings", settings))
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
	return nil
}

// fromProtoResourceGroup converts a rmpb.ResourceGroup to a ResourceGroup.
func fromProtoResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) *ResourceGroup {
	rg := &ResourceGroup{
		Name: group.Name,
		Mode: group.Mode,
	}
	switch group.GetMode() {
	case rmpb.GroupMode_RUMode:
		rg.RUSettings = NewRequestUnitSettings(ctx, group.GetRUSettings().GetRU())
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
	return rg
}

// RequestRU requests the RU of the resource group.
func (rg *ResourceGroup) RequestRU(
	now time.Time,
	neededTokens float64,
	targetPeriodMs, clientUniqueID uint64,
) *rmpb.GrantedRUTokenBucket {
	return rg.RUSettings.RU.request(now, neededTokens, targetPeriodMs, clientUniqueID)
}

// intoProtoResourceGroup converts a ResourceGroup to a rmpb.ResourceGroup.
func (rg *ResourceGroup) intoProtoResourceGroup() *rmpb.ResourceGroup {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		tokenBucket := &rmpb.TokenBucket{}
		if rg.RUSettings != nil && rg.RUSettings.RU != nil {
			tokenBucket.Settings = rg.RUSettings.RU.Settings
			tokenBucket.Tokens = rg.RUSettings.RU.Tokens
		}
		group := &rmpb.ResourceGroup{
			Name: rg.Name,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: tokenBucket,
			},
		}
		return group
	case rmpb.GroupMode_RawMode: // Raw mode
		panic("no implementation")
	}
	return nil
}

// persistSettings persists the resource group settings.
// TODO: persist the state of the group separately.
func (rg *ResourceGroup) persistSettings(storage endpoint.ResourceGroupStorage) error {
	metaGroup := rg.intoProtoResourceGroup()
	return storage.SaveResourceGroupSetting(rg.Name, metaGroup)
}

// GroupStates is the tokens set of a resource group.
type GroupStates struct {
	// RU tokens
	RU *GroupTokenBucketState `json:"r_u,omitempty"`
}

// GetGroupStates get the token set of ResourceGroup.
func (rg *ResourceGroup) GetGroupStates() *GroupStates {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		tokens := &GroupStates{
			RU: rg.RUSettings.RU.Copy().GroupTokenBucketState,
		}
		return tokens
	case rmpb.GroupMode_RawMode: // Raw mode
		panic("no implementation")
	}
	return nil
}

// SetStatesIntoResourceGroup updates the state of resource group.
func (rg *ResourceGroup) SetStatesIntoResourceGroup(states *GroupStates) {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if state := states.RU; state != nil {
			rg.RUSettings.RU.setState(states.RU)
		}
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
}

// persistStates persists the resource group tokens.
func (rg *ResourceGroup) persistStates(storage endpoint.ResourceGroupStorage) error {
	states := rg.GetGroupStates()
	return storage.SaveResourceGroupStates(rg.Name, states)
}
