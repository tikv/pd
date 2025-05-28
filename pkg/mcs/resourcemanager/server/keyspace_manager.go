// Copyright 2025 TiKV Project Authors.
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
	"encoding/json"
	"math"
	"sort"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	defaultConsumptionChanSize = 1024
	maxGroupNameLength         = 32
	middlePriority             = 8
	maxPriority                = 16
	unlimitedRate              = math.MaxInt32
	unlimitedBurstLimit        = -1
	// DefaultResourceGroupName is the reserved default resource group name within each keyspace.
	DefaultResourceGroupName = "default"
)

// consumptionItem is used to send the consumption info to the background metrics flusher.
type consumptionItem struct {
	keyspaceID        uint32
	resourceGroupName string
	*rmpb.Consumption
	isBackground bool
	isTiFlash    bool
}

type keyspaceResourceGroupManager struct {
	syncutil.RWMutex
	groups map[string]*ResourceGroup

	keyspaceID uint32
	storage    endpoint.ResourceGroupStorage
}

func newKeyspaceResourceGroupManager(keyspaceID uint32, storage endpoint.ResourceGroupStorage) *keyspaceResourceGroupManager {
	return &keyspaceResourceGroupManager{
		groups:     make(map[string]*ResourceGroup),
		keyspaceID: keyspaceID,
		storage:    storage,
	}
}

func (krgm *keyspaceResourceGroupManager) addResourceGroupFromRaw(name string, rawValue string) error {
	group := &rmpb.ResourceGroup{}
	if err := proto.Unmarshal([]byte(rawValue), group); err != nil {
		log.Error("failed to parse the keyspace resource group meta info",
			zap.Uint32("keyspace-id", krgm.keyspaceID), zap.String("name", name), zap.String("raw-value", rawValue), zap.Error(err))
		return err
	}
	krgm.Lock()
	krgm.groups[group.Name] = FromProtoResourceGroup(group)
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) setRawStatesIntoResourceGroup(name string, rawValue string) error {
	tokens := &GroupStates{}
	if err := json.Unmarshal([]byte(rawValue), tokens); err != nil {
		log.Error("failed to parse the keyspace resource group state",
			zap.Uint32("keyspace-id", krgm.keyspaceID), zap.String("name", name), zap.String("raw-value", rawValue), zap.Error(err))
		return err
	}
	krgm.Lock()
	if group, ok := krgm.groups[name]; ok {
		group.SetStatesIntoResourceGroup(tokens)
	}
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) initDefaultResourceGroup() {
	krgm.RLock()
	if _, ok := krgm.groups[DefaultResourceGroupName]; ok {
		krgm.RUnlock()
		return
	}
	krgm.RUnlock()
	defaultGroup := &ResourceGroup{
		Name: DefaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &RequestUnitSettings{
			RU: &GroupTokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   unlimitedRate,
					BurstLimit: unlimitedBurstLimit,
				},
			},
		},
		Priority: middlePriority,
	}
	if err := krgm.addResourceGroup(defaultGroup.IntoProtoResourceGroup()); err != nil {
		log.Warn("init default group failed", zap.Uint32("keyspace-id", krgm.keyspaceID), zap.Error(err))
	}
}

func (krgm *keyspaceResourceGroupManager) addResourceGroup(grouppb *rmpb.ResourceGroup) error {
	if len(grouppb.Name) == 0 || len(grouppb.Name) > maxGroupNameLength {
		return errs.ErrInvalidGroup
	}
	// Check the Priority.
	if grouppb.GetPriority() > maxPriority {
		return errs.ErrInvalidGroup
	}
	group := FromProtoResourceGroup(grouppb)
	krgm.Lock()
	defer krgm.Unlock()
	if err := group.persistSettings(krgm.keyspaceID, krgm.storage); err != nil {
		return err
	}
	if err := group.persistStates(krgm.keyspaceID, krgm.storage); err != nil {
		return err
	}
	krgm.groups[group.Name] = group
	return nil
}

func (krgm *keyspaceResourceGroupManager) modifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errs.ErrInvalidGroup
	}
	krgm.RLock()
	curGroup, ok := krgm.groups[group.Name]
	krgm.RUnlock()
	if !ok {
		return errs.ErrResourceGroupNotExists.FastGenByArgs(group.Name)
	}

	err := curGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	return curGroup.persistSettings(krgm.keyspaceID, krgm.storage)
}

func (krgm *keyspaceResourceGroupManager) deleteResourceGroup(name string) error {
	if name == DefaultResourceGroupName {
		return errs.ErrDeleteReservedGroup
	}
	if err := krgm.storage.DeleteResourceGroupSetting(krgm.keyspaceID, name); err != nil {
		return err
	}
	krgm.Lock()
	delete(krgm.groups, name)
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) getResourceGroup(name string, withStats bool) *ResourceGroup {
	krgm.RLock()
	defer krgm.RUnlock()
	if group, ok := krgm.groups[name]; ok {
		return group.Clone(withStats)
	}
	return nil
}

func (krgm *keyspaceResourceGroupManager) getMutableResourceGroup(name string) *ResourceGroup {
	krgm.Lock()
	defer krgm.Unlock()
	return krgm.groups[name]
}

func (krgm *keyspaceResourceGroupManager) getResourceGroupList(withStats, includeDefault bool) []*ResourceGroup {
	krgm.RLock()
	res := make([]*ResourceGroup, 0, len(krgm.groups))
	for _, group := range krgm.groups {
		if !includeDefault && group.Name == DefaultResourceGroupName {
			continue
		}
		res = append(res, group.Clone(withStats))
	}
	krgm.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}

func (krgm *keyspaceResourceGroupManager) getResourceGroupNames(includeDefault bool) []string {
	krgm.RLock()
	defer krgm.RUnlock()
	res := make([]string, 0, len(krgm.groups))
	for name := range krgm.groups {
		if !includeDefault && name == DefaultResourceGroupName {
			continue
		}
		res = append(res, name)
	}
	return res
}

func (krgm *keyspaceResourceGroupManager) persistResourceGroupRunningState() {
	krgm.RLock()
	keys := make([]string, 0, len(krgm.groups))
	for k := range krgm.groups {
		keys = append(keys, k)
	}
	krgm.RUnlock()
	for idx := range keys {
		krgm.RLock()
		group, ok := krgm.groups[keys[idx]]
		if ok {
			if err := group.persistStates(krgm.keyspaceID, krgm.storage); err != nil {
				log.Error("persist keyspace resource group state failed",
					zap.Uint32("keyspace-id", krgm.keyspaceID),
					zap.String("group-name", group.Name),
					zap.Int("index", idx),
					zap.Error(err))
			}
		}
		krgm.RUnlock()
	}
}
