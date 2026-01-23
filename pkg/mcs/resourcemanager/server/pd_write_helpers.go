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

package server

import (
	"errors"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

type ResourceGroupWriteStorage interface {
	endpoint.ResourceGroupStorage
	kv.Base
}

// ValidateResourceGroupForWrite validates the basic resource group fields for write operations.
func ValidateResourceGroupForWrite(group *rmpb.ResourceGroup) error {
	if group == nil {
		return errs.ErrInvalidGroup
	}
	if len(group.GetName()) == 0 || len(group.GetName()) > maxGroupNameLength {
		return errs.ErrInvalidGroup
	}
	if group.GetPriority() > maxPriority {
		return errs.ErrInvalidGroup
	}
	return nil
}

// EnsureDefaultResourceGroupExists makes sure the default resource group exists in storage.
//
// This mirrors the behavior of the resource-manager service when creating the first group
// for a keyspace (it initializes the default group as well).
func EnsureDefaultResourceGroupExists(storage ResourceGroupWriteStorage, keyspaceID uint32) error {
	if storage == nil {
		return errors.New("storage is nil")
	}
	// Fast-path: already exists.
	raw, err := storage.Load(keypath.KeyspaceResourceGroupSettingPath(keyspaceID, DefaultResourceGroupName))
	if err != nil {
		return err
	}
	if raw != "" {
		return nil
	}

	group := &rmpb.ResourceGroup{
		Name:     DefaultResourceGroupName,
		Mode:     rmpb.GroupMode_RUMode,
		Priority: middlePriority,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   uint64(UnlimitedRate),
					BurstLimit: UnlimitedBurstLimit,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: keyspaceID},
	}
	return PersistResourceGroupSettingsAndStates(storage, keyspaceID, group)
}

// PersistResourceGroupSettingsAndStates persists both the settings and initial states for a group.
func PersistResourceGroupSettingsAndStates(storage endpoint.ResourceGroupStorage, keyspaceID uint32, group *rmpb.ResourceGroup) error {
	if err := ValidateResourceGroupForWrite(group); err != nil {
		return err
	}
	if err := storage.SaveResourceGroupSetting(keyspaceID, group.GetName(), group); err != nil {
		return err
	}
	states := FromProtoResourceGroup(group).GetGroupStates()
	return storage.SaveResourceGroupStates(keyspaceID, group.GetName(), states)
}
