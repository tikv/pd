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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/tikv/pd/pkg/errs"
	"go.uber.org/zap"
)

// CPUCollector is a daemon to collect the CPU usage of a PD server within a certain interval.
type CPUCollector struct {
	// Make sure every collector only has one background goroutine to keep collecting and updating the CPU times info.
	once              sync.Once
	process           *process.Process
	interval          time.Duration
	totalCPUTimes     atomic.Value /* float64 */
	totalCPUTimesRate atomic.Value /* float64 */
	lastUpdateTime    atomic.Value /* time.Time */
}

// NewCPUCollector returns a new CPUCollector which will collect CPU usage info at the given interval.
// `(*CPUCollector).Start()` should be called later to start a daemon to do the collecting work.
func NewCPUCollector(interval time.Duration) *CPUCollector {
	collector := &CPUCollector{
		interval: interval,
	}
	collector.totalCPUTimes.Store(0.0)
	collector.totalCPUTimesRate.Store(0.0)
	collector.lastUpdateTime.Store(time.Now())
	return collector
}

// Start will run a daemon to do the collecting work. This method is singleton.
func (collector *CPUCollector) Start(ctx context.Context) {
	collector.once.Do(func() {
		go func() {
			var err error
			if collector.process == nil {
				pid := os.Getpid()
				collector.process, err = process.NewProcessWithContext(ctx, int32(pid))
				if err != nil {
					log.Error("create process instance meets error", zap.Int("pid", pid), errs.ZapError(err))
				}
			}
			// Clear the info after existing.
			defer func() {
				collector.totalCPUTimes.Store(0.0)
				collector.totalCPUTimesRate.Store(0.0)
				collector.lastUpdateTime.Store(time.Now())
			}()
			intervalTicker := time.NewTicker(collector.interval)
			defer intervalTicker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-intervalTicker.C:
				}
				// Time units of the utime and stime later are in seconds.
				timeDelta := float64(time.Since(collector.lastUpdateTime.Load().(time.Time)).Milliseconds()) / 1000
				cpuTimes, err := collector.process.TimesWithContext(ctx)
				if err != nil {
					log.Error("get cpu times meets error",
						zap.Int32("pid", collector.process.Pid),
						zap.Duration("interval", collector.interval),
						errs.ZapError(err))
					return
				}
				newTotalCPUTimes := cpuTimes.User + cpuTimes.System
				oldTotalCPUTimes := collector.totalCPUTimes.Load().(float64)
				collector.totalCPUTimes.Store(newTotalCPUTimes)
				collector.totalCPUTimesRate.Store((newTotalCPUTimes - oldTotalCPUTimes) / timeDelta)
				collector.lastUpdateTime.Store(time.Now())
			}
		}()
	})
}

// GetCPUUsage returns the latest CPU usage info collected in the past set interval.
func (collector *CPUCollector) GetCPUUsage() float64 {
	return collector.totalCPUTimesRate.Load().(float64)
}
