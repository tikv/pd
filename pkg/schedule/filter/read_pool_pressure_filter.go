// Copyright 2026 TiKV Project Authors.
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

package filter

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/plan"
)

const (
	storeCPUUsagePerCore      = 100.0
	readPoolPressureThreshold = 0.70
)

type readPoolPressureFilter struct {
	scope               string
	readCPUByStore      map[uint64]float64
	readPoolThreadCount uint64
}

// NewReadPoolPressureFilter creates a target-store filter for read-pool pressure.
func NewReadPoolPressureFilter(scope string, readCPUByStore map[uint64]float64, readPoolThreadCount uint64) Filter {
	return &readPoolPressureFilter{
		scope:               scope,
		readCPUByStore:      readCPUByStore,
		readPoolThreadCount: readPoolThreadCount,
	}
}

// Scope returns the filter scope.
func (f *readPoolPressureFilter) Scope() string {
	return f.scope
}

// Type returns the filter type.
func (*readPoolPressureFilter) Type() filterType {
	return readPoolPressure
}

// Source allows all source stores.
func (*readPoolPressureFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target filters stores whose read CPU is close to the unified read-pool limit.
func (f *readPoolPressureFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store == nil {
		return statusOK
	}
	readCPU := f.readCPUByStore[store.GetID()]
	if readCPU == 0 || f.readPoolThreadCount == 0 {
		// The guard depends on TiKV StoreConfig sync for
		// readpool.unified.max-thread-count. Before that sync succeeds, keep the
		// previous scheduling behavior instead of filtering by an unknown limit.
		return statusOK
	}
	if readCPU >= float64(f.readPoolThreadCount)*storeCPUUsagePerCore*readPoolPressureThreshold {
		return statusStoreBusy
	}
	return statusOK
}
