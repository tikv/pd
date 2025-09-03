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
	"github.com/pingcap/metering_sdk/common"
)

func TestRUCollectorCollectSingleKeyspace(t *testing.T) {
	re := require.New(t)
	collector := newRUCollector()

	tidbConsumption := &consumptionItem{
		keyspaceName: "test",
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
	record := records[0]
	re.Equal(ruMeteringVersion, record["version"])
	re.Equal("test", record["cluster_id"])
	re.Equal(sourceName, record["source_name"])
	re.Equal(common.MeteringValue{Value: uint64(150.0), Unit: "RU"}, record["oltp_ru"])
	re.Equal(common.MeteringValue{Value: uint64(150.0), Unit: "RU"}, record["olap_ru"])
	re.Equal(common.MeteringValue{Value: uint64(6144), Unit: "Bytes"}, record["cross_az_traffic_bytes"])
}

func TestRUCollectorCollectMultipleKeyspaces(t *testing.T) {
	re := require.New(t)
	collector := newRUCollector()

	consumption1 := &consumptionItem{
		keyspaceName: "keyspace-1",
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
		keyspaceName: "keyspace-2",
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

	for _, record := range records {
		keyspaceName := record["cluster_id"]
		switch keyspaceName {
		case "keyspace-1":
			re.Equal("keyspace-1", keyspaceName)
			re.Equal(sourceName, record["source_name"])
			re.Equal(common.MeteringValue{Value: uint64(80.0), Unit: "RU"}, record["oltp_ru"])
			re.Equal(common.MeteringValue{Value: uint64(0.0), Unit: "RU"}, record["olap_ru"])
			re.Equal(common.MeteringValue{Value: uint64(300), Unit: "Bytes"}, record["cross_az_traffic_bytes"])
		case "keyspace-2":
			re.Equal("keyspace-2", keyspaceName)
			re.Equal(sourceName, record["source_name"])
			re.Equal(common.MeteringValue{Value: uint64(0.0), Unit: "RU"}, record["oltp_ru"])
			re.Equal(common.MeteringValue{Value: uint64(100.0), Unit: "RU"}, record["olap_ru"])
			re.Equal(common.MeteringValue{Value: uint64(700), Unit: "Bytes"}, record["cross_az_traffic_bytes"])
		default:
			re.Fail("unexpected keyspace", keyspaceName)
		}
	}
}
