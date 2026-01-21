// Copyright 2025 TiKV Project Authors.
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
	grpcServerThreadPrefix      = "grpc-server"
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

// StoreUnifiedReadCPUUsage returns the store-level unified-read CPU usage.
func StoreUnifiedReadCPUUsage(cpuUsages []*pdpb.RecordPair) uint64 {
	return sumCPUUsageByPrefix(cpuUsages, unifiedReadPoolThreadPrefix)
}

// StoreReadCPUUsage returns the store-level read CPU usage derived from unified-read and gRPC threads.
func StoreReadCPUUsage(cpuUsages []*pdpb.RecordPair, readQuery, totalQuery uint64) float64 {
	unifiedReadCPU := float64(StoreUnifiedReadCPUUsage(cpuUsages))
	if totalQuery == 0 || readQuery == 0 {
		return unifiedReadCPU
	}
	grpcCPU := float64(StoreGRPCCPUUsage(cpuUsages))
	return unifiedReadCPU + grpcCPU*float64(readQuery)/float64(totalQuery)
}

// StoreGRPCCPUUsage returns the store-level gRPC CPU usage derived from gRPC server threads.
func StoreGRPCCPUUsage(cpuUsages []*pdpb.RecordPair) uint64 {
	return sumCPUUsageByPrefix(cpuUsages, grpcServerThreadPrefix)
}
