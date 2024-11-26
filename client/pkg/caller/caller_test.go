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
<<<<<<<< HEAD:pkg/statistics/cpu_test.go
	cpuUsages := []*pdpb.RecordPair{
		{Key: "unified-read-0", Value: 80},
		{Key: "grpc-server-0", Value: 20},
		{Key: "other", Value: 30},
	}
	re.Equal(80.0, StoreReadCPUUsage(cpuUsages))
========

	re.Equal(Component("github.com/tikv/pd/client/pkg/caller"), GetComponent(0))
	re.Equal(Component("testing"), GetComponent(1))
	re.Equal(Component("runtime"), GetComponent(2))
	re.Equal(Component("unknown"), GetComponent(3))
>>>>>>>> da0000a9eb (*: move some common libraries to the client/pkg directory (#8858)):client/pkg/caller/caller_test.go
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
