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

package server

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/shirou/gopsutil/v3/mem"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/cgroup"
	"go.uber.org/zap"
)

const (
	refreshInterval = 10 * time.Second
)

type cgroupMonitor struct {
	started         bool
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	cfgMaxProcs     int
	lastMaxProcs    int
	lastMemoryLimit uint64
}

// StartCgroupMonitor uses to start the cgroup monitoring.
// WARN: this function is not thread-safe.
func (cgmon *cgroupMonitor) startCgroupMonitor() {
	if cgmon.started {
		return
	}
	cgmon.started = true
	// Get configured maxprocs.
	cgmon.cfgMaxProcs = runtime.GOMAXPROCS(0)
	cgmon.ctx, cgmon.cancel = context.WithCancel(context.Background())
	cgmon.wg.Add(1)
	go cgmon.refreshCgroupLoop()
	log.Info("cgroup monitor started")
}

// StopCgroupMonitor uses to stop the cgroup monitoring.
// WARN: this function is not thread-safe.
func (cgmon *cgroupMonitor) stopCgroupMonitor() {
	if !cgmon.started {
		return
	}
	cgmon.started = false
	if cgmon.cancel != nil {
		cgmon.cancel()
	}
	cgmon.wg.Wait()
	log.Info("cgroup monitor stopped")
}

func (cgmon *cgroupMonitor) refreshCgroupLoop() {
	ticker := time.NewTicker(refreshInterval)
	defer func() {
		if r := recover(); r != nil {
			log.Error("[pd] panic in the recoverable goroutine",
				zap.String("funcInfo", "refreshCgroupLoop"),
				zap.Reflect("r", r),
				zap.Stack("stack"))
		}
		cgmon.wg.Done()
		ticker.Stop()
	}()

	cgmon.refreshCgroupCPU()
	cgmon.refreshCgroupMemory()
	for {
		select {
		case <-cgmon.ctx.Done():
			return
		case <-ticker.C:
			cgmon.refreshCgroupCPU()
			cgmon.refreshCgroupMemory()
		}
	}
}

func (cgmon *cgroupMonitor) refreshCgroupCPU() {
	// Get the number of CPUs.
	quota := runtime.NumCPU()

	// Get CPU quota from cgroup.
	cpuPeriod, cpuQuota, err := cgroup.GetCPUPeriodAndQuota()
	if err != nil {
		log.Warn("failed to get cgroup cpu quota", zap.Error(err))
		return
	}
	if cpuPeriod > 0 && cpuQuota > 0 {
		ratio := float64(cpuQuota) / float64(cpuPeriod)
		if ratio < float64(quota) {
			quota = int(math.Ceil(ratio))
		}
	}

	if quota != cgmon.lastMaxProcs && quota < cgmon.cfgMaxProcs {
		runtime.GOMAXPROCS(quota)
		log.Info(fmt.Sprintf("maxprocs set to %v", quota))
		bs.ServerMaxProcsGauge.Set(float64(quota))
		cgmon.lastMaxProcs = quota
	} else if cgmon.lastMaxProcs == 0 {
		log.Info(fmt.Sprintf("maxprocs set to %v", cgmon.cfgMaxProcs))
		bs.ServerMaxProcsGauge.Set(float64(cgmon.cfgMaxProcs))
		cgmon.lastMaxProcs = cgmon.cfgMaxProcs
	}
}

func (cgmon *cgroupMonitor) refreshCgroupMemory() {
	memLimit, err := cgroup.GetMemoryLimit()
	if err != nil {
		log.Warn("failed to get cgroup memory limit", zap.Error(err))
		return
	}
	vmem, err := mem.VirtualMemory()
	if err != nil {
		log.Warn("failed to get system memory size", zap.Error(err))
		return
	}
	if memLimit > vmem.Total {
		memLimit = vmem.Total
	}
	if memLimit != cgmon.lastMemoryLimit {
		log.Info(fmt.Sprintf("memory limit set to %v", memLimit))
		bs.ServerMemoryLimit.Set(float64(memLimit))
		cgmon.lastMemoryLimit = memLimit
	}
}
