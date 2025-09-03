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
)

var _ metering.Collector = (*ruCollector)(nil)

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
	return common.MeteringValue{Value: uint64(rm.oltpRU), Unit: "RU"}
}

func (rm *ruMetering) olapMeteringValue() common.MeteringValue {
	return common.MeteringValue{Value: uint64(rm.olapRU), Unit: "RU"}
}

func (rm *ruMetering) crossAZTrafficBytesMeteringValue() common.MeteringValue {
	return common.MeteringValue{Value: rm.crossAZTrafficBytes, Unit: "Bytes"}
}

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

// Flush flushes the RU metering data.
func (c *ruCollector) Flush() []map[string]any {
	// Read the data first.
	c.RLock()
	records := make([]map[string]any, 0, len(c.keyspaceRUMetering))
	for keyspaceName, ruMetering := range c.keyspaceRUMetering {
		// Convert the ruMetering to the map[string]any.
		records = append(records, map[string]any{
			"version":                ruMeteringVersion,
			"cluster_id":             keyspaceName, // keyspaceName is the logical cluster ID in the metering data.
			"source_name":            sourceName,
			"oltp_ru":                ruMetering.oltpMeteringValue(),
			"olap_ru":                ruMetering.olapMeteringValue(),
			"cross_az_traffic_bytes": ruMetering.crossAZTrafficBytesMeteringValue(),
		})
	}
	c.RUnlock()
	// Clear the data after reading.
	c.Lock()
	c.keyspaceRUMetering = make(map[string]*ruMetering)
	c.Unlock()
	return records
}
