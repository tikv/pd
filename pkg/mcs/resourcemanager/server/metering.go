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
	"sync"

	"github.com/pingcap/metering_sdk/common"

	"github.com/tikv/pd/pkg/metering"
)

const (
	resourceManagerCategory = "resource-manager"
	ruMeteringVersion       = "1"
	sourceName              = "pd"

	meteringUnitRU    = "RU"
	meteringUnitBytes = "Bytes"

	meteringDataVersionField             = "version"
	meteringDataClusterIDField           = "cluster_id"
	meteringDataSourceNameField          = "source_name"
	meteringDataOltpRUField              = "oltp_ru"
	meteringDataOlapRUField              = "olap_ru"
	meteringDataCrossAZTrafficBytesField = "cross_az_traffic_bytes"
)

type ruMetering struct {
	// TODO: distinguish the DML and DDL RU consumption from the OLTP RU.
	oltpRU              float64
	olapRU              float64
	crossAZTrafficBytes uint64
}

func (rm *ruMetering) add(consumption *consumptionItem) {
	ru := consumption.RRU + consumption.WRU
	if consumption.isTiFlash {
		rm.olapRU += ru
	} else {
		rm.oltpRU += ru
	}
	rm.crossAZTrafficBytes += consumption.ReadCrossAzTrafficBytes + consumption.WriteCrossAzTrafficBytes
}

func (rm *ruMetering) oltpMeteringValue() common.MeteringValue {
	return newMeteringRUValue(rm.oltpRU)
}

func (rm *ruMetering) olapMeteringValue() common.MeteringValue {
	return newMeteringRUValue(rm.olapRU)
}

func newMeteringRUValue(value float64) common.MeteringValue {
	return common.MeteringValue{Value: uint64(value), Unit: meteringUnitRU}
}

func (rm *ruMetering) crossAZTrafficBytesMeteringValue() common.MeteringValue {
	return newMeteringBytesValue(rm.crossAZTrafficBytes)
}

func newMeteringBytesValue(value uint64) common.MeteringValue {
	return common.MeteringValue{Value: value, Unit: meteringUnitBytes}
}

var _ metering.Collector = (*ruCollector)(nil)

type ruCollector struct {
	sync.RWMutex
	// KeyspaceName -> RU metering data
	keyspaceRUMetering map[string]*ruMetering
}

func newRUCollector() *ruCollector {
	return &ruCollector{
		keyspaceRUMetering: make(map[string]*ruMetering),
	}
}

// Category returns the category of the collector.
func (*ruCollector) Category() string { return resourceManagerCategory }

// Collect collects the RU metering data.
func (c *ruCollector) Collect(data any) {
	c.Lock()
	defer c.Unlock()
	consumption := data.(*consumptionItem)
	rm, ok := c.keyspaceRUMetering[consumption.keyspaceName]
	if !ok {
		rm = &ruMetering{}
		c.keyspaceRUMetering[consumption.keyspaceName] = rm
	}
	rm.add(consumption)
}

// Aggregate aggregates the RU metering data.
func (c *ruCollector) Aggregate() []map[string]any {
	c.Lock()
	keyspaceRUMetering := c.keyspaceRUMetering
	c.keyspaceRUMetering = make(map[string]*ruMetering)
	c.Unlock()
	records := make([]map[string]any, 0, len(keyspaceRUMetering))
	for keyspaceName, ruMetering := range keyspaceRUMetering {
		// Convert the ruMetering to the map[string]any.
		records = append(records, map[string]any{
			meteringDataVersionField:             ruMeteringVersion,
			meteringDataClusterIDField:           keyspaceName, // keyspaceName is the logical cluster ID in the metering data.
			meteringDataSourceNameField:          sourceName,
			meteringDataOltpRUField:              ruMetering.oltpMeteringValue(),
			meteringDataOlapRUField:              ruMetering.olapMeteringValue(),
			meteringDataCrossAZTrafficBytesField: ruMetering.crossAZTrafficBytesMeteringValue(),
		})
	}
	return records
}
