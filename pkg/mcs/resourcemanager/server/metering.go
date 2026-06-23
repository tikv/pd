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
	// ResourceManagerCategory is the category of the resource manager.
	ResourceManagerCategory = "resource-manager"
	ruMeteringVersion       = "1"

	meteringDataOLTPRUField              = "oltp_ru"
	meteringDataOLAPRUField              = "olap_ru"
	meteringDataWriteBytesField          = "write_bytes"
	meteringDataCrossAZTrafficBytesField = "cross_az_traffic_bytes"
	meteringDataTiDBRUV2Field            = "tidb_ru_v2"
	meteringDataTiKVRUV2Field            = "tikv_ru_v2"
	meteringDataTiFlashRUV2Field         = "tiflash_ru_v2"
)

type ruMetering struct {
	oltpRU              float64
	olapRU              float64
	writeBytes          uint64
	crossAZTrafficBytes uint64
	tidbRUV2            float64
	tikvRUV2            float64
	tiflashRUV2         float64
}

// RUCarry tracks negative RU that cannot be emitted to metering until
// later positive usage offsets it.
type RUCarry struct {
	oltpRU float64
	olapRU float64
}

func applyRUCarry(value float64, carry *float64) float64 {
	value += *carry
	if value < 0 {
		*carry = value
		return 0
	}
	*carry = 0
	return value
}

func (c *RUCarry) empty() bool {
	return c.oltpRU == 0 && c.olapRU == 0
}

func (rm *ruMetering) add(consumption *consumptionItem) {
	// Keep the legacy oltp/olap buckets unchanged for compatibility, and
	// expose the finer-grained experimental RUv2 breakdown separately.
	ru := consumption.RRU + consumption.WRU
	if consumption.isTiFlash {
		rm.olapRU += ru
	} else {
		rm.oltpRU += ru
	}
	rm.writeBytes += uint64(consumption.WriteBytes)
	rm.crossAZTrafficBytes += consumption.ReadCrossAzTrafficBytes + consumption.WriteCrossAzTrafficBytes
	rm.tidbRUV2 += consumption.TidbRUV2
	rm.tikvRUV2 += consumption.TikvRUV2
	rm.tiflashRUV2 += consumption.TiflashRUV2
}

func (rm *ruMetering) oltpMeteringValue() common.MeteringValue {
	return metering.NewRUValue(rm.oltpRU)
}

func (rm *ruMetering) olapMeteringValue() common.MeteringValue {
	return metering.NewRUValue(rm.olapRU)
}

func (rm *ruMetering) writeBytesMeteringValue() common.MeteringValue {
	return metering.NewBytesValue(rm.writeBytes)
}

func (rm *ruMetering) crossAZTrafficBytesMeteringValue() common.MeteringValue {
	return metering.NewBytesValue(rm.crossAZTrafficBytes)
}

func (rm *ruMetering) tidbRUV2MeteringValue() common.MeteringValue {
	return metering.NewRUValue(rm.tidbRUV2)
}

func (rm *ruMetering) tikvRUV2MeteringValue() common.MeteringValue {
	return metering.NewRUValue(rm.tikvRUV2)
}

func (rm *ruMetering) tiflashRUV2MeteringValue() common.MeteringValue {
	return metering.NewRUValue(rm.tiflashRUV2)
}

var _ metering.Collector = (*ruCollector)(nil)

type ruCollector struct {
	sync.RWMutex
	// KeyspaceName -> RU metering data
	keyspaceRUMetering map[string]*ruMetering
	// KeyspaceName -> pending negative metering RU
	keyspaceRUCarry map[string]*RUCarry
}

func newRUCollector() *ruCollector {
	return &ruCollector{
		keyspaceRUMetering: make(map[string]*ruMetering),
		keyspaceRUCarry:    make(map[string]*RUCarry),
	}
}

func (c *ruCollector) remove(keyspaceName string) {
	c.Lock()
	defer c.Unlock()
	delete(c.keyspaceRUMetering, keyspaceName)
	delete(c.keyspaceRUCarry, keyspaceName)
}

// Category returns the category of the collector.
func (*ruCollector) Category() string { return ResourceManagerCategory }

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
	if len(keyspaceRUMetering) == 0 {
		c.Unlock()
		return nil
	}
	for keyspaceName, ruMetering := range keyspaceRUMetering {
		carry := c.keyspaceRUCarry[keyspaceName]
		if carry == nil {
			carry = &RUCarry{}
		}
		ruMetering.oltpRU = applyRUCarry(ruMetering.oltpRU, &carry.oltpRU)
		ruMetering.olapRU = applyRUCarry(ruMetering.olapRU, &carry.olapRU)
		if carry.empty() {
			delete(c.keyspaceRUCarry, keyspaceName)
		} else {
			c.keyspaceRUCarry[keyspaceName] = carry
		}
	}
	c.Unlock()
	records := make([]map[string]any, 0, len(keyspaceRUMetering))
	for keyspaceName, ruMetering := range keyspaceRUMetering {
		// Convert the ruMetering to the map[string]any.
		records = append(records, map[string]any{
			metering.DataVersionField:            ruMeteringVersion,
			metering.DataClusterIDField:          keyspaceName, // keyspaceName is the logical cluster ID in the metering data.
			metering.DataSourceNameField:         metering.SourceNamePD,
			meteringDataOLTPRUField:              ruMetering.oltpMeteringValue(),
			meteringDataOLAPRUField:              ruMetering.olapMeteringValue(),
			meteringDataWriteBytesField:          ruMetering.writeBytesMeteringValue(),
			meteringDataCrossAZTrafficBytesField: ruMetering.crossAZTrafficBytesMeteringValue(),
			meteringDataTiDBRUV2Field:            ruMetering.tidbRUV2MeteringValue(),
			meteringDataTiKVRUV2Field:            ruMetering.tikvRUV2MeteringValue(),
			meteringDataTiFlashRUV2Field:         ruMetering.tiflashRUV2MeteringValue(),
		})
	}
	return records
}
