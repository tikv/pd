// Copyright 2024 TiKV Project Authors.
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

package cgroup

import (
	"context"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/shirou/gopsutil/v3/mem"
	bs "github.com/tikv/pd/pkg/basicserver"
	"go.uber.org/zap"
)

const (
	refreshInterval = 10 * time.Second
)

// Monitor is used to monitor the cgroup.
type Monitor struct {
	started         bool
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	cfgMaxProcs     int
	lastMaxProcs    int
	lastMemoryLimit uint64
}

// StartMonitor uses to start the cgroup monitoring.
// WARN: this function is not thread-safe.
func (m *Monitor) StartMonitor(ctx context.Context) {
	if m.started {
		return
	}
	m.started = true
	if runtime.GOOS != "linux" {
		return
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(1)
	go m.refreshCgroupLoop()
	log.Info("cgroup monitor started")
}

// StopMonitor uses to stop the cgroup monitoring.
// WARN: this function is not thread-safe.
func (m *Monitor) StopMonitor() {
	if !m.started {
		return
	}
	if runtime.GOOS != "linux" {
		return
	}
	m.started = false
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	log.Info("cgroup monitor stopped")
}

func (m *Monitor) refreshCgroupLoop() {
	ticker := time.NewTicker(refreshInterval)
	defer func() {
		if r := recover(); r != nil {
			log.Error("[pd] panic in the recoverable goroutine",
				zap.String("func-info", "refreshCgroupLoop"),
				zap.Reflect("r", r),
				zap.Stack("stack"))
		}
		m.wg.Done()
		ticker.Stop()
	}()

	err := m.refreshCgroupCPU()
	if err != nil {
		log.Warn("failed to get cgroup memory limit", zap.Error(err))
	}
	err = m.refreshCgroupMemory()
	if err != nil {
		log.Warn("failed to get cgroup memory limit", zap.Error(err))
	}
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			err = m.refreshCgroupCPU()
			if err != nil {
				log.Debug("failed to get cgroup cpu quota", zap.Error(err))
			}
			err = m.refreshCgroupMemory()
			if err != nil {
				log.Debug("failed to get cgroup memory limit", zap.Error(err))
			}
		}
	}
}

func (m *Monitor) refreshCgroupCPU() error {
	// Get the number of CPUs.
	quota := runtime.NumCPU()

	// Get CPU quota from cgroup.
	cpuPeriod, cpuQuota, err := GetCPUPeriodAndQuota()
	if err != nil {
		return err
	}
	if cpuPeriod > 0 && cpuQuota > 0 {
		ratio := float64(cpuQuota) / float64(cpuPeriod)
		if ratio < float64(quota) {
			quota = int(math.Ceil(ratio))
		}
	}

	if quota != m.lastMaxProcs {
		log.Info("set the maxprocs", zap.Int("quota", quota))
		bs.ServerMaxProcsGauge.Set(float64(quota))
		m.lastMaxProcs = quota
	} else if m.lastMaxProcs == 0 {
		log.Info("set the maxprocs", zap.Int("maxprocs", m.cfgMaxProcs))
		bs.ServerMaxProcsGauge.Set(float64(m.cfgMaxProcs))
		m.lastMaxProcs = m.cfgMaxProcs
	}
	return nil
}

func (m *Monitor) refreshCgroupMemory() error {
	memLimit, err := GetMemoryLimit()
	if err != nil {
		return err
	}
	vmem, err := mem.VirtualMemory()
	if err != nil {
		return err
	}
	if memLimit > vmem.Total {
		memLimit = vmem.Total
	}
	if memLimit != m.lastMemoryLimit {
		log.Info("set the memory limit", zap.Uint64("mem-limit", memLimit))
		bs.ServerMemoryLimit.Set(float64(memLimit))
		m.lastMemoryLimit = memLimit
	}
	return nil
}
