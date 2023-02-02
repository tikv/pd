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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
)

const defaultConsumptionChanSize = 1024

// Manager is the manager of resource group.
type Manager struct {
	sync.RWMutex
	groups  map[string]*ResourceGroup
	storage endpoint.ResourceGroupStorage
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan struct {
		resourceGroupName string
		*rmpb.Consumption
	}
}

// NewManager returns a new Manager.
func NewManager(srv *server.Server) *Manager {
	m := &Manager{
		groups: make(map[string]*ResourceGroup),
		consumptionDispatcher: make(chan struct {
			resourceGroupName string
			*rmpb.Consumption
		}, defaultConsumptionChanSize),
	}
	// Initialize the manager storage after the server is started.
	srv.AddStartCallback(func() {
		m.storage = endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(srv.GetClient(), "resource_group"),
			nil,
		)
		log.Info("resource group manager storage has been initialized")
	})
	// Initialize the manager after the leader is elected.
	srv.AddLeaderCallback(m.Init)
	return m
}

// Init initializes the resource group manager.
func (m *Manager) Init(ctx context.Context) {
	// Reset the resource groups first.
	m.groups = make(map[string]*ResourceGroup)
	m.storage.LoadResourceGroupSettings(func(k, v string) {
		group := &rmpb.ResourceGroup{}
		if err := proto.Unmarshal([]byte(v), group); err != nil {
			log.Error("err", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		m.groups[group.Name] = FromProtoResourceGroup(group)
	})
	// Start the background metrics flusher.
	go m.backgroundMetricsFlush(ctx)
	log.Info("resource group manager has been initialized")
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
	if err := group.persistSettings(m.storage); err != nil {
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
	if err := newGroup.persistSettings(m.storage); err != nil {
		return err
	}
	m.groups[group.Name] = newGroup
	return nil
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(name string) error {
	if err := m.storage.DeleteResourceGroupSetting(name); err != nil {
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

// Receive the consumption and flush it to the metrics.
func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case consumptionInfo := <-m.consumptionDispatcher:
			consumption := consumptionInfo.Consumption
			if consumption == nil {
				continue
			}
			var (
				name                     = consumptionInfo.resourceGroupName
				rruMetrics               = readRequestUnitCost.WithLabelValues(name)
				wruMetrics               = writeRequestUnitCost.WithLabelValues(name)
				readByteMetrics          = readByteCost.WithLabelValues(name)
				writeByteMetrics         = writeByteCost.WithLabelValues(name)
				kvCPUMetrics             = kvCPUCost.WithLabelValues(name)
				sqlCPUMetrics            = sqlCPUCost.WithLabelValues(name)
				readRequestCountMetrics  = requestCount.WithLabelValues(name, readTypeLabel)
				writeRequestCountMetrics = requestCount.WithLabelValues(name, writeTypeLabel)
			)
			// RU info.
			if consumption.RRU != 0 {
				rruMetrics.Observe(consumption.RRU)
			}
			if consumption.WRU != 0 {
				wruMetrics.Observe(consumption.WRU)
			}
			// Byte info.
			if consumption.ReadBytes != 0 {
				readByteMetrics.Observe(consumption.ReadBytes)
			}
			if consumption.WriteBytes != 0 {
				writeByteMetrics.Observe(consumption.WriteBytes)
			}
			// CPU time info.
			if consumption.TotalCpuTimeMs > 0 {
				if consumption.SqlLayerCpuTimeMs > 0 {
					sqlCPUMetrics.Observe(consumption.SqlLayerCpuTimeMs)
				}
				kvCPUMetrics.Observe(consumption.TotalCpuTimeMs - consumption.SqlLayerCpuTimeMs)
			}
			// RPC count info.
			if consumption.KvReadRpcCount != 0 {
				readRequestCountMetrics.Add(consumption.KvReadRpcCount)
			}
			if consumption.KvWriteRpcCount != 0 {
				writeRequestCountMetrics.Add(consumption.KvWriteRpcCount)
			}
		}
	}
}
