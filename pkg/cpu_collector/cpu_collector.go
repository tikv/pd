// Copyright 2021 TiKV Project Authors.
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

package collector

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/tikv/pd/pkg/errs"
	"go.uber.org/zap"
)

// CPUCollector is a daemon to collect the CPU usage of a PD server within a certain interval.
type CPUCollector struct {
	// Make sure every collector only has one background goroutine to keep collecting and updating the usage.
	once     sync.Once
	interval time.Duration
	usage    atomic.Value
}

// NewCPUCollector returns a new CPUCollector which will collect CPU usage info at the given interval.
// `(*CPUCollector).Start()` should be called later to start a daemon to do the collecting work.
func NewCPUCollector(interval time.Duration) *CPUCollector {
	return &CPUCollector{
		interval: interval,
	}
}

// Start will run a daemon to do the collecting work. This method is singleton.
func (collector *CPUCollector) Start(ctx context.Context) {
	collector.once.Do(func() {
		go func() {
			// Clear the usage when exit.
			defer collector.usage.Store(0.0)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				usagePercent, err := cpu.PercentWithContext(ctx, collector.interval, false)
				if err != nil {
					log.Error("get cpu usage percent meets error",
						zap.Duration("interval", collector.interval), errs.ZapError(err))
					return
				}
				if len(usagePercent) <= 0 {
					log.Warn("get empty cpu usage percent result",
						zap.Duration("interval", collector.interval))
					continue
				}
				collector.usage.Store(usagePercent[0])
			}
		}()
	})
}

// GetCPUUsage returns the latest CPU usage info collected in the past set interval.
func (collector *CPUCollector) GetCPUUsage() float64 {
	usageInterface := collector.usage.Load()
	if usageInterface == nil {
		return 0.0
	}
	return usageInterface.(float64)
}
