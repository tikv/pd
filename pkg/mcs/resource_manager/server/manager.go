// Copyright 2020 TiKV Project Authors.
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
	"sort"
	"sync"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
)

// Manager is the manager of resource group.
type Manager struct {
	sync.RWMutex
	groups  map[string]*ResourceGroup
	storage func() storage.Storage
	// TODO: dispatch resource group to storage node
	getStores func() ([]*core.StoreInfo, error)
}

// NewManager returns a new Manager.
func NewManager(srv *server.Server) *Manager {
	getStores := func() ([]*core.StoreInfo, error) {
		rc := srv.GetRaftCluster()
		if rc == nil {
			return nil, errors.New("RaftCluster is nil")
		}
		return rc.GetStores(), nil
	}

	m := &Manager{
		groups:    make(map[string]*ResourceGroup),
		storage:   srv.GetStorage,
		getStores: getStores,
	}
	return m
}

// Init initializes the resource group manager.
func (m *Manager) Init() {
	handler := func(k, v string) {
		var group ResourceGroup
		if err := json.Unmarshal([]byte(v), &group); err != nil {
			panic(err)
		}
		m.groups[group.Name] = &group
	}
	m.storage().LoadResourceGroups(handler)
}

// PutResourceGroup puts a resource group.
func (m *Manager) PutResourceGroup(group *ResourceGroup) error {
	if err := m.storage().SaveResourceGroup(group.Name, group); err != nil {
		return err
	}
	m.Lock()
	m.groups[group.Name] = group
	m.Unlock()
	return nil
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(name string) error {
	if err := m.storage().DeleteResourceGroup(name); err != nil {
		return err
	}
	m.Lock()
	delete(m.groups, name)
	m.Unlock()
	return nil
}

// GetResourceGroup returns a resource group.
func (m *Manager) GetResourceGroup(name string) *ResourceGroup {
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns a resource group list.
func (m *Manager) GetResourceGroupList() []*ResourceGroup {
	res := make([]*ResourceGroup, 0)
	m.RLock()
	for _, group := range m.groups {
		res = append(res, group)
	}
	m.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}
