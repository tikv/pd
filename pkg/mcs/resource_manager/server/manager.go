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
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"go.uber.org/zap"
)

const defaultConsumptionChanSize = 1024

const (
	metricsCleanupInterval = time.Minute
	metricsCleanupTimeout  = 20 * time.Minute
)

// Manager is the manager of resource group.
type Manager struct {
	sync.RWMutex
	srv     bs.Server
	groups  map[string]*ResourceGroup
	storage endpoint.ResourceGroupStorage
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan struct {
		resourceGroupName string
		*rmpb.Consumption
	}
	// record update time of each resource group
	comsumptionRecord map[string]time.Time
}

// NewManager returns a new Manager.
func NewManager(srv bs.Server) *Manager {
	m := &Manager{
		groups: make(map[string]*ResourceGroup),
		consumptionDispatcher: make(chan struct {
			resourceGroupName string
			*rmpb.Consumption
		}, defaultConsumptionChanSize),
		comsumptionRecord: make(map[string]time.Time),
	}
	// The first initialization after the server is started.
	srv.AddStartCallback(func() {
		log.Info("resource group manager starts to initialize", zap.String("name", srv.Name()))
		m.storage = endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(srv.GetClient(), "resource_group"),
			nil,
		)
		m.srv = srv
	})
	// The second initialization after becoming serving.
	srv.AddServiceReadyCallback(m.Init)
	return m
}

// GetBasicServer returns the basic server.
func (m *Manager) GetBasicServer() bs.Server {
	return m.srv
}

// Init initializes the resource group manager.
func (m *Manager) Init(ctx context.Context) {
	// Reset the resource groups first.
	m.groups = make(map[string]*ResourceGroup)
	handler := func(k, v string) {
		group := &rmpb.ResourceGroup{}
		if err := proto.Unmarshal([]byte(v), group); err != nil {
			log.Error("err", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		m.groups[group.Name] = FromProtoResourceGroup(group)
	}
	m.storage.LoadResourceGroupSettings(handler)
	tokenHandler := func(k, v string) {
		tokens := &GroupStates{}
		if err := json.Unmarshal([]byte(v), tokens); err != nil {
			log.Error("err", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		if group, ok := m.groups[k]; ok {
			group.SetStatesIntoResourceGroup(tokens)
		}
	}
	m.storage.LoadResourceGroupStates(tokenHandler)
	// Start the background metrics flusher.
	go m.backgroundMetricsFlush(ctx)
	go m.persistLoop(ctx)
	log.Info("resource group manager finishes initialization")
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
	defer m.Unlock()
	if err := group.persistSettings(m.storage); err != nil {
		return err
	}
	if err := group.persistStates(m.storage); err != nil {
		return err
	}
	m.groups[group.Name] = group
	return nil
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errors.New("invalid group name")
	}
	m.Lock()
	curGroup, ok := m.groups[group.Name]
	m.Unlock()
	if !ok {
		return errors.New("not exists the group")
	}

	err := curGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	return curGroup.persistSettings(m.storage)
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

func (m *Manager) persistLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	failpoint.Inject("fastPersist", func() {
		ticker.Stop()
		ticker = time.NewTicker(100 * time.Millisecond)
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.persistResourceGroupRunningState()
		}
	}
}

func (m *Manager) persistResourceGroupRunningState() {
	m.RLock()
	keys := make([]string, 0, len(m.groups))
	for k := range m.groups {
		keys = append(keys, k)
	}
	m.RUnlock()
	for idx := 0; idx < len(keys); idx++ {
		m.RLock()
		group, ok := m.groups[keys[idx]]
		m.RUnlock()
		if ok {
			group.persistStates(m.storage)
		}
	}
}

// Receive the consumption and flush it to the metrics.
func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	ticker := time.NewTicker(metricsCleanupInterval)
	defer ticker.Stop()
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

			m.comsumptionRecord[name] = time.Now()

		case <-ticker.C:
			// Clean up the metrics that have not been updated for a long time.
			for name, lastTime := range m.comsumptionRecord {
				if time.Since(lastTime) > metricsCleanupTimeout {
					readRequestUnitCost.DeleteLabelValues(name)
					writeRequestUnitCost.DeleteLabelValues(name)
					readByteCost.DeleteLabelValues(name)
					writeByteCost.DeleteLabelValues(name)
					kvCPUCost.DeleteLabelValues(name)
					sqlCPUCost.DeleteLabelValues(name)
					requestCount.DeleteLabelValues(name, readTypeLabel)
					requestCount.DeleteLabelValues(name, writeTypeLabel)
					delete(m.comsumptionRecord, name)
				}
			}
		}
	}
}
