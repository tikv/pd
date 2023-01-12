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
	"context"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/storage"
	"go.uber.org/zap"
)

const (
	defaultConsumptionChanSize = 1024
	metricsFlushInterval       = time.Minute
	metricsGCInterval          = time.Hour
)

// Manager is the manager of resource group.
type Manager struct {
	sync.RWMutex
	groups  map[string]*ResourceGroup
	storage func() storage.Storage
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan struct {
		resourceGroupName string
		*rmpb.Consumption
	}
	// metricsMap is used to store the metrics of each resource group.
	// It will be updated and persisted by the (*Manager).backgroundMetricsFlush.
	metricsMap map[string]*Metrics
}

// NewManager returns a new Manager.
func NewManager(srv *server.Server) *Manager {
	m := &Manager{
		groups:  make(map[string]*ResourceGroup),
		storage: srv.GetStorage,
		consumptionDispatcher: make(chan struct {
			resourceGroupName string
			*rmpb.Consumption
		}, defaultConsumptionChanSize),
		metricsMap: make(map[string]*Metrics),
	}
	srv.AddStartCallback(m.Init)
	ctx := srv.Context()
	go m.backgroundMetricsFlush(ctx)
	go m.backgroundMetricsGC(ctx)
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
	m.storage().LoadResourceGroupSettings(handler)
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
	if err := m.storage().DeleteResourceGroupSetting(name); err != nil {
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

// Update and flush the metrics info periodically.
func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	ticker := time.NewTicker(metricsFlushInterval)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return
	case consumption := <-m.consumptionDispatcher:
		// Aggregate the consumption info to the metrics.
		if metrics := m.getMetrics(consumption.resourceGroupName); metrics != nil {
			metrics.Update(consumption.Consumption)
		}
	// Flush the metrics info to the storage.
	case <-ticker.C:
		// TODO: persist the metrics info into the storage.
		m.resetAllMetrics()
	}
}

// GC the metrics info periodically.
func (m *Manager) backgroundMetricsGC(ctx context.Context) {
	ticker := time.NewTicker(metricsGCInterval)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return
	// GC the outdated metrics info in storage.
	case <-ticker.C:
		// TODO: delete the outdated metrics info from the storage.
	}
}

func (m *Manager) getMetrics(name string) *Metrics {
	m.RLock()
	defer m.RUnlock()
	return m.metricsMap[name]
}

func (m *Manager) resetAllMetrics() {
	m.Lock()
	defer m.Unlock()
	for _, metrics := range m.metricsMap {
		metrics.Reset()
	}
}
