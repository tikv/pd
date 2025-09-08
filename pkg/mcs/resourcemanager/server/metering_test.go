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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const (
	testKeyspaceName  = "test"
	testKeyspaceName1 = "keyspace-1"
	testKeyspaceName2 = "keyspace-2"
)

func TestRUCollectorCollectSingleKeyspace(t *testing.T) {
	re := require.New(t)
	collector := newRUCollector()

	tidbConsumption := &consumptionItem{
		keyspaceName: testKeyspaceName,
		Consumption: &rmpb.Consumption{
			RRU:                      100.0,
			WRU:                      50.0,
			ReadCrossAzTrafficBytes:  1024,
			WriteCrossAzTrafficBytes: 2048,
		},
		isBackground: false,
		isTiFlash:    false,
	}
	collector.Collect(tidbConsumption)

	tiflashConsumption := tidbConsumption
	tiflashConsumption.isTiFlash = true
	collector.Collect(tiflashConsumption)

	records := collector.Flush()
	re.Len(records, 1)
	re.Empty(collector.keyspaceRUMetering)
	record := records[0]
	re.Equal(ruMeteringVersion, record[meteringDataVersionField])
	re.Equal(testKeyspaceName, record[meteringDataClusterIDField])
	re.Equal(sourceName, record[meteringDataSourceNameField])
	re.Equal(newMeteringRUValue(150.0), record[meteringDataOltpRUField])
	re.Equal(newMeteringRUValue(150.0), record[meteringDataOlapRUField])
	re.Equal(newMeteringBytesValue(6144), record[meteringDataCrossAZTrafficBytesField])
}

func TestRUCollectorCollectMultipleKeyspaces(t *testing.T) {
	re := require.New(t)
	collector := newRUCollector()

	consumption1 := &consumptionItem{
		keyspaceName: testKeyspaceName1,
		Consumption: &rmpb.Consumption{
			RRU:                      50.0,
			WRU:                      30.0,
			ReadCrossAzTrafficBytes:  100,
			WriteCrossAzTrafficBytes: 200,
		},
		isBackground: false,
		isTiFlash:    false,
	}

	consumption2 := &consumptionItem{
		keyspaceName: testKeyspaceName2,
		Consumption: &rmpb.Consumption{
			RRU:                      75.0,
			WRU:                      25.0,
			ReadCrossAzTrafficBytes:  300,
			WriteCrossAzTrafficBytes: 400,
		},
		isBackground: false,
		isTiFlash:    true,
	}

	collector.Collect(consumption1)
	collector.Collect(consumption2)

	records := collector.Flush()
	re.Len(records, 2)
	re.Empty(collector.keyspaceRUMetering)

	for _, record := range records {
		keyspaceName := record[meteringDataClusterIDField]
		switch keyspaceName {
		case testKeyspaceName1:
			re.Equal(testKeyspaceName1, keyspaceName)
			re.Equal(sourceName, record[meteringDataSourceNameField])
			re.Equal(newMeteringRUValue(80.0), record[meteringDataOltpRUField])
			re.Equal(newMeteringRUValue(0.0), record[meteringDataOlapRUField])
			re.Equal(newMeteringBytesValue(300), record[meteringDataCrossAZTrafficBytesField])
		case testKeyspaceName2:
			re.Equal(testKeyspaceName2, keyspaceName)
			re.Equal(sourceName, record[meteringDataSourceNameField])
			re.Equal(newMeteringRUValue(0.0), record[meteringDataOltpRUField])
			re.Equal(newMeteringRUValue(100.0), record[meteringDataOlapRUField])
			re.Equal(newMeteringBytesValue(700), record[meteringDataCrossAZTrafficBytesField])
		default:
			re.Fail("unexpected keyspace", keyspaceName)
		}
	}
}
