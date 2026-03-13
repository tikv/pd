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

package statistics

import (
	"strings"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	// unifiedReadPoolThreadPrefix matches TiKV's unified-read thread prefix:
	// https://github.com/tikv/tikv/blob/master/components/tikv_util/src/thread_name_prefix.rs#L60
	unifiedReadPoolThreadPrefix = "unified-read"
)

func sumCPUUsageByPrefix(cpuUsages []*pdpb.RecordPair, prefix string) uint64 {
	var total uint64
	for _, usage := range cpuUsages {
		if strings.HasPrefix(usage.GetKey(), prefix) {
			total += usage.GetValue()
		}
	}
	return total
}

// storeUnifiedReadCPUUsage returns the store-level unified-read CPU usage.
func storeUnifiedReadCPUUsage(cpuUsages []*pdpb.RecordPair) uint64 {
	return sumCPUUsageByPrefix(cpuUsages, unifiedReadPoolThreadPrefix)
}

// StoreReadCPUUsage returns the store-level read CPU usage derived from unified-read threads.
func StoreReadCPUUsage(cpuUsages []*pdpb.RecordPair) float64 {
	return float64(storeUnifiedReadCPUUsage(cpuUsages))
}

// RegionReadCPUUsage returns the region-level read CPU usage based on unified-read CPU.
// If cpu_stats is missing, return 0.
func RegionReadCPUUsage(peerStat *pdpb.PeerStat) float64 {
	if peerStat == nil {
		return 0
	}
	cpuStats := peerStat.GetCpuStats()
	if cpuStats == nil {
		return 0
	}
	return float64(cpuStats.GetUnifiedRead())
}

// RegionWriteCPUUsage returns the region-level write CPU usage based on scheduler CPU.
// If cpu_stats is missing, return 0.
func RegionWriteCPUUsage(peerStat *pdpb.PeerStat) float64 {
	if peerStat == nil {
		return 0
	}
	cpuStats := peerStat.GetCpuStats()
	if cpuStats == nil {
		return 0
	}
	return float64(cpuStats.GetScheduler())
}
