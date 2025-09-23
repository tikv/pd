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

package cluster

import (
	"sync"

	"github.com/docker/go-units"

	"github.com/pingcap/metering_sdk/common"

	"github.com/tikv/pd/pkg/metering"
)

const (
	storageSizeCollectorCategory = "storage-size"
	storageSizeMeteringVersion   = "1"

	meteringDataRowBasedStorageSizeField    = "row_based_storage_size"
	meteringDataColumnBasedStorageSizeField = "column_based_storage_size"
)

var _ metering.Collector = (*storageSizeCollector)(nil)

type storageSizeInfo struct {
	keyspaceName string
	// Both storage size are in MiB.
	rowBasedStorageSize    uint64
	columnBasedStorageSize uint64
}

func (s *storageSizeInfo) rowBasedStorageSizeMeteringValue() common.MeteringValue {
	return metering.NewBytesValue(s.rowBasedStorageSize * units.MiB)
}

func (s *storageSizeInfo) columnBasedStorageSizeMeteringValue() common.MeteringValue {
	return metering.NewBytesValue(s.columnBasedStorageSize * units.MiB)
}

type storageSizeCollector struct {
	sync.RWMutex
	// KeyspaceName -> storageSizeInfo
	keyspaceStorageSize map[string]*storageSizeInfo
}

func newStorageSizeCollector() *storageSizeCollector {
	return &storageSizeCollector{
		keyspaceStorageSize: make(map[string]*storageSizeInfo),
	}
}

// Category returns the category of the collector.
func (*storageSizeCollector) Category() string { return storageSizeCollectorCategory }

// Collect collects the row-based and column-based storage size data.
func (c *storageSizeCollector) Collect(data any) {
	c.Lock()
	defer c.Unlock()
	info := data.([]*storageSizeInfo)
	for _, info := range info {
		c.keyspaceStorageSize[info.keyspaceName] = info
	}
}

// Aggregate aggregates the row-based and column-based storage size data.
func (c *storageSizeCollector) Aggregate() []map[string]any {
	c.Lock()
	keyspaceStorageSize := c.keyspaceStorageSize
	c.keyspaceStorageSize = make(map[string]*storageSizeInfo)
	c.Unlock()
	records := make([]map[string]any, 0, len(keyspaceStorageSize))
	for keyspaceName, storageSizeInfo := range keyspaceStorageSize {
		// Convert the storageSizeInfo to the map[string]any.
		records = append(records, map[string]any{
			metering.DataVersionField:               storageSizeMeteringVersion,
			metering.DataClusterIDField:             keyspaceName, // keyspaceName is the logical cluster ID in the metering data.
			metering.DataSourceNameField:            metering.SourceNamePD,
			meteringDataRowBasedStorageSizeField:    storageSizeInfo.rowBasedStorageSizeMeteringValue(),
			meteringDataColumnBasedStorageSizeField: storageSizeInfo.columnBasedStorageSizeMeteringValue(),
		})
	}
	return records
}
