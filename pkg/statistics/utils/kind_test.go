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

package utils

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
)

func TestGetLoads(t *testing.T) {
	re := require.New(t)
	queryStats := &pdpb.QueryStats{
		Get:                    5,
		Coprocessor:            6,
		Scan:                   7,
		Put:                    8,
		Delete:                 9,
		DeleteRange:            10,
		AcquirePessimisticLock: 11,
		Rollback:               12,
		Prewrite:               13,
		Commit:                 14,
	}
	regionA := core.NewRegionInfo(&metapb.Region{Id: 100, Peers: []*metapb.Peer{}}, nil,
		core.SetReadBytes(1),
		core.SetReadKeys(2),
		core.SetWrittenBytes(3),
		core.SetWrittenKeys(4),
		core.SetQueryStats(queryStats))
	loads := regionA.GetLoads()
	re.Len(loads, int(RegionStatCount))
	re.Equal(float64(regionA.GetBytesRead()), loads[RegionReadBytes])
	re.Equal(float64(regionA.GetKeysRead()), loads[RegionReadKeys])
	re.Equal(float64(regionA.GetReadQueryNum()), loads[RegionReadQueryNum])
	readQuery := float64(queryStats.Coprocessor + queryStats.Get + queryStats.Scan)
	re.Equal(float64(regionA.GetReadQueryNum()), readQuery)
	re.Equal(float64(regionA.GetBytesWritten()), loads[RegionWriteBytes])
	re.Equal(float64(regionA.GetKeysWritten()), loads[RegionWriteKeys])
	re.Equal(float64(regionA.GetWriteQueryNum()), loads[RegionWriteQueryNum])
	writeQuery := float64(queryStats.Put + queryStats.Delete + queryStats.DeleteRange + queryStats.AcquirePessimisticLock + queryStats.Rollback + queryStats.Prewrite + queryStats.Commit)
	re.Equal(float64(regionA.GetWriteQueryNum()), writeQuery)

	loads = regionA.GetWriteLoads()
	re.Len(loads, int(RegionStatCount))
	re.Equal(0.0, loads[RegionReadBytes])
	re.Equal(0.0, loads[RegionReadKeys])
	re.Equal(0.0, loads[RegionReadQueryNum])
	re.Equal(float64(regionA.GetBytesWritten()), loads[RegionWriteBytes])
	re.Equal(float64(regionA.GetKeysWritten()), loads[RegionWriteKeys])
	re.Equal(float64(regionA.GetWriteQueryNum()), loads[RegionWriteQueryNum])
}
