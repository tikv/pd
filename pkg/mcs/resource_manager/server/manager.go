// Copyright 2022 TiKV Project Authors.
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
	"path"
	"sort"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/storage"
	"go.uber.org/zap"
)

// Manager is the manager of resource group.
type Manager struct {
	sync.RWMutex
	groups  map[string]*ResourceGroup
	storage func() storage.Storage
}

// NewManager returns a new Manager.
func NewManager(srv *server.Server) *Manager {
	m := &Manager{
		groups:  make(map[string]*ResourceGroup),
		storage: srv.GetStorage,
	}
	srv.AddStartCallback(m.Init)
	return m
}

// Init initializes the resource group manager.
func (m *Manager) Init() {
	handler := func(k, v string) {
		group := &rmpb.ResourceGroup{}
		if err := proto.Unmarshal([]byte(v), group); err != nil {
			log.Error("err", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		m.groups[group.Name] = FromProtoResourceGroup(group)
	}
	m.storage().LoadResourceGroups(groupSettingsPathPrefix, handler)
}

// AddResourceGroup puts a resource group.
func (m *Manager) AddResourceGroup(group *ResourceGroup) error {
	m.RLock()
	_, ok := m.groups[group.Name]
	m.RUnlock()
	if ok {
		return errors.New("this group already exists")
	}
	err := group.CheckAndInit()
	if err != nil {
		return err
	}
	m.Lock()
	if err := group.persistSettings(m.storage()); err != nil {
		return err
	}
	m.groups[group.Name] = group
	m.Unlock()
	return nil
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errors.New("invalid group name")
	}
	m.Lock()
	defer m.Unlock()
	curGroup, ok := m.groups[group.Name]
	if !ok {
		return errors.New("not exists the group")
	}
	newGroup := curGroup.Copy()
	err := newGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	if err := newGroup.persistSettings(m.storage()); err != nil {
		return err
	}
	m.groups[group.Name] = newGroup
	return nil
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(name string) error {
	prefix := path.Join(groupSettingsPathPrefix, name)
	if err := m.storage().DeleteResourceGroup(prefix); err != nil {
		return err
	}
	m.Lock()
	delete(m.groups, name)
	m.Unlock()
	return nil
}

// GetResourceGroup returns a copy of a resource group.
func (m *Manager) GetResourceGroup(name string) *ResourceGroup {
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group.Copy()
	}
	return nil
}

// GetMutableResourceGroup returns a mutable resource group.
func (m *Manager) GetMutableResourceGroup(name string) *ResourceGroup {
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns copies of resource group list.
func (m *Manager) GetResourceGroupList() []*ResourceGroup {
	m.RLock()
	res := make([]*ResourceGroup, 0, len(m.groups))
	for _, group := range m.groups {
		res = append(res, group.Copy())
	}
	m.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}
