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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

func TestStoreReadCPUUsage(t *testing.T) {
	re := require.New(t)
	cpuUsages := []*pdpb.RecordPair{
		{Key: "unified-read-0", Value: 80},
		{Key: "grpc-server-0", Value: 20},
		{Key: "other", Value: 30},
	}
	re.Equal(80.0, StoreReadCPUUsage(cpuUsages))
}

func TestRegionReadCPUUsage(t *testing.T) {
	re := require.New(t)
	peerStat := &pdpb.PeerStat{}
	re.Equal(0.0, RegionReadCPUUsage(peerStat))

	cpuStats := &pdpb.CPUStats{}
	cpuStats.UnifiedRead = 80
	peerStat.CpuStats = cpuStats
	re.Equal(80.0, RegionReadCPUUsage(peerStat))
}

func TestRegionWriteCPUUsage(t *testing.T) {
	re := require.New(t)
	peerStat := &pdpb.PeerStat{}
	re.Equal(0.0, RegionWriteCPUUsage(peerStat))

	cpuStats := &pdpb.CPUStats{}
	cpuStats.Scheduler = 66
	peerStat.CpuStats = cpuStats
	re.Equal(66.0, RegionWriteCPUUsage(peerStat))
}
