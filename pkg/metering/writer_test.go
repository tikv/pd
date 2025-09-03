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

package metering

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/metering_sdk/storage"

	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type mockCollector struct {
	sync.RWMutex
	category string
	data     []any
	flushed  []map[string]any
}

func newMockCollector(category string) *mockCollector {
	return &mockCollector{
		category: category,
		data:     make([]any, 0),
		flushed:  make([]map[string]any, 0),
	}
}

// Category implements the Collector interface.
func (mc *mockCollector) Category() string {
	return mc.category
}

// Collect implements the Collector interface.
func (mc *mockCollector) Collect(data any) {
	mc.Lock()
	defer mc.Unlock()
	mc.data = append(mc.data, data)
}

// Flush implements the Collector interface.
func (mc *mockCollector) Flush() []map[string]any {
	mc.Lock()
	defer mc.Unlock()

	records := make([]map[string]any, len(mc.data))
	for i, d := range mc.data {
		records[i] = map[string]any{"data": d}
	}

	mc.flushed = append(mc.flushed, records...)
	mc.data = make([]any, 0)

	return records
}

func (mc *mockCollector) getFlushedData() []map[string]any {
	mc.RLock()
	defer mc.RUnlock()
	return mc.flushed
}

func (mc *mockCollector) getCollectedData() []any {
	mc.RLock()
	defer mc.RUnlock()
	return mc.data
}

func TestConfigAdjust(t *testing.T) {
	re := require.New(t)

	// Test config without Type field - should set default to S3.
	config := &Config{
		Bucket: "test-bucket",
		Prefix: "test-prefix",
		Region: "us-west-2",
	}

	err := config.adjust()
	re.NoError(err)
	re.Equal(storage.ProviderTypeS3, config.Type)
}

func TestRegisterCollector(t *testing.T) {
	re := require.New(t)

	// Create a mock writer.
	ctx, cancel := context.WithCancel(context.Background())
	writer := &Writer{
		id:         "testid",
		ctx:        ctx,
		cancel:     cancel,
		collectors: make(map[string]Collector),
	}
	defer writer.Stop()

	// Test registering a collector.
	collector1 := newMockCollector("category1")
	writer.RegisterCollector(collector1)

	collectors := writer.getCollectors()
	re.Len(collectors, 1)
	re.Contains(collectors, "category1")
	re.Equal(collector1, collectors["category1"])

	// Test registering another collector.
	collector2 := newMockCollector("category2")
	writer.RegisterCollector(collector2)

	collectors = writer.getCollectors()
	re.Len(collectors, 2)
	re.Contains(collectors, "category1")
	re.Contains(collectors, "category2")

	// Test overwriting existing collector.
	collector1New := newMockCollector("category1")
	writer.RegisterCollector(collector1New)

	collectors = writer.getCollectors()
	re.Len(collectors, 2)
	re.Equal("category1", collectors["category1"].Category())
	// Verify it's a different instance by checking if it's not the original.
	re.NotContains(collector1, collectors["category1"])
}

func TestFlushMeteringDataWithCollectors(t *testing.T) {
	re := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	writer := &Writer{
		id:         "testid",
		ctx:        ctx,
		cancel:     cancel,
		collectors: make(map[string]Collector),
		inner:      nil,
	}
	defer writer.Stop()

	// Noop if no registered collectors.
	writer.flushMeteringData()

	// Add collectors with data.
	collector1 := newMockCollector("category1")
	collector1.Collect("data1")
	collector1.Collect("data2")

	collector2 := newMockCollector("category2")
	collector2.Collect("data3")

	collector3 := newMockCollector("category3")
	// collector3 has no data.

	writer.RegisterCollector(collector1)
	writer.RegisterCollector(collector2)
	writer.RegisterCollector(collector3)

	// Verify data before flush.
	re.Len(collector1.getCollectedData(), 2)
	re.Len(collector2.getCollectedData(), 1)
	re.Empty(collector3.getCollectedData())

	// Call flush.
	writer.flushMeteringData()

	// Verify collectors were flushed (data cleared).
	re.Empty(collector1.getCollectedData())
	re.Empty(collector2.getCollectedData())
	re.Empty(collector3.getCollectedData())

	// Verify flushed data was generated.
	re.Len(collector1.getFlushedData(), 2)
	re.Len(collector2.getFlushedData(), 1)
	re.Empty(collector3.getFlushedData())
}
