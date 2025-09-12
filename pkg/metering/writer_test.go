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
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/pingcap/metering_sdk/config"
	meteringreader "github.com/pingcap/metering_sdk/reader/metering"
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

// Aggregate implements the Collector interface.
func (mc *mockCollector) Aggregate() []map[string]any {
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

func TestConfigValidate(t *testing.T) {
	re := require.New(t)

	// Test config without Type field - should set default to S3.
	c := &config.MeteringConfig{
		Region: "us-west-2",
		Bucket: "test-bucket",
	}

	err := validateMeteringConfig(c)
	re.Error(err)

	// Test config without Bucket field - should return error.
	c = &config.MeteringConfig{
		Type:   storage.ProviderTypeS3,
		Region: "us-west-2",
	}
	err = validateMeteringConfig(c)
	re.Error(err)
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
	re.NotSame(collector1, collectors["category1"])
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

	ts := time.Now().Unix()
	// Noop if no registered collectors.
	writer.flushMeteringData(ctx, ts)

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
	writer.flushMeteringData(ctx, ts)

	// Verify collectors were flushed (data cleared).
	re.Empty(collector1.getCollectedData())
	re.Empty(collector2.getCollectedData())
	re.Empty(collector3.getCollectedData())

	// Verify flushed data was generated.
	re.Len(collector1.getFlushedData(), 2)
	re.Len(collector2.getFlushedData(), 1)
	re.Empty(collector3.getFlushedData())
}

func newLocalWriter(ctx context.Context, re *require.Assertions, dir string) (*Writer, *meteringreader.MeteringReader) {
	c := config.NewMeteringConfig().WithLocalFS(dir)
	writer, err := NewWriter(ctx, c, "testlocalwriter")
	re.NoError(err)

	provider, err := storage.NewObjectStorageProvider(c.ToProviderConfig())
	re.NoError(err)
	meteringConfig := config.DefaultConfig().WithLogger(zap.L())
	reader := meteringreader.NewMeteringReader(provider, meteringConfig)
	return writer, reader
}

func readMeteringData(
	ctx context.Context, re *require.Assertions,
	reader *meteringreader.MeteringReader,
	category string, ts int64,
) []map[string]any {
	_, err := reader.ListFilesByTimestamp(ctx, ts)
	re.NoError(err)

	categories, err := reader.GetCategories(ctx, ts)
	re.NoError(err)

	var data []map[string]any
	for _, c := range categories {
		if c != category {
			continue
		}
		categoryFiles, err := reader.GetFilesByCategory(ctx, ts, c)
		re.NoError(err)
		if len(categoryFiles) == 0 {
			return nil
		}
		data = make([]map[string]any, 0, len(categoryFiles))
		for _, filePath := range categoryFiles {
			meteringData, err := reader.ReadFile(ctx, filePath)
			re.NoError(err)
			data = append(data, meteringData.Data...)
		}
	}
	return data
}

func TestMeteringDataReadWriteSingleCategory(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create writer and reader using helper function.
	writer, reader := newLocalWriter(ctx, re, t.TempDir())
	defer writer.Stop()

	// Create and register a collector.
	category := "test-category"
	collector := newMockCollector(category)
	writer.RegisterCollector(collector)

	// Add some test data.
	testData := []any{
		map[string]any{"id": 1, "value": "test1", "timestamp": time.Now().Unix()},
		map[string]any{"id": 2, "value": "test2", "timestamp": time.Now().Unix()},
		map[string]any{"id": 3, "value": "test3", "timestamp": time.Now().Unix()},
	}
	for _, data := range testData {
		collector.Collect(data)
	}

	// Flush the data.
	ts := time.Now().Truncate(time.Minute).Unix()
	writer.flushMeteringData(ctx, ts)

	// Read the data back using helper function.
	readData := readMeteringData(ctx, re, reader, category, ts)
	re.NotNil(readData)
	re.Len(readData, 3)

	// Verify the data content.
	for i, record := range readData {
		data := record["data"].(map[string]any)
		originalData := testData[i].(map[string]any)

		// Compare each field individually to handle type conversion issues.
		re.Equal(originalData["id"], int(data["id"].(float64)))
		re.Equal(originalData["value"], data["value"])
		re.Equal(originalData["timestamp"], int64(data["timestamp"].(float64)))
	}
}

func TestMeteringDataReadWriteMultipleCategories(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create writer and reader using helper function.
	writer, reader := newLocalWriter(ctx, re, t.TempDir())
	defer writer.Stop()

	// Create and register multiple collectors.
	categories := []string{"category-0", "category-1", "category-2"}
	collectors := make([]*mockCollector, 0, len(categories))
	for _, category := range categories {
		collector := newMockCollector(category)
		writer.RegisterCollector(collector)
		collectors = append(collectors, collector)
	}

	// Add different test data to each collector.
	collectors[0].Collect(map[string]any{"type": "type1", "count": 10})
	collectors[0].Collect(map[string]any{"type": "type1", "count": 20})
	collectors[1].Collect(map[string]any{"metric": "cpu", "value": 75.5})
	collectors[1].Collect(map[string]any{"metric": "memory", "value": 85.2})
	collectors[1].Collect(map[string]any{"metric": "disk", "value": 45.8})
	collectors[2].Collect(map[string]any{"event": "login", "user": "user1"})

	// Flush the data.
	ts := time.Now().Truncate(time.Minute).Unix()
	writer.flushMeteringData(ctx, ts)

	// Read data for each category.
	data0 := readMeteringData(ctx, re, reader, categories[0], ts)
	data1 := readMeteringData(ctx, re, reader, categories[1], ts)
	data2 := readMeteringData(ctx, re, reader, categories[2], ts)

	// Verify category-0 data.
	re.NotNil(data0)
	re.Len(data0, 2)
	data0_0 := data0[0]["data"].(map[string]any)
	re.Equal("type1", data0_0["type"])
	re.Equal(10, int(data0_0["count"].(float64)))
	data0_1 := data0[1]["data"].(map[string]any)
	re.Equal("type1", data0_1["type"])
	re.Equal(20, int(data0_1["count"].(float64)))

	// Verify category-2 data.
	re.NotNil(data1)
	re.Len(data1, 3)
	data1_0 := data1[0]["data"].(map[string]any)
	re.Equal("cpu", data1_0["metric"])
	re.Equal(75.5, data1_0["value"])
	data1_1 := data1[1]["data"].(map[string]any)
	re.Equal("memory", data1_1["metric"])
	re.Equal(85.2, data1_1["value"])
	data1_2 := data1[2]["data"].(map[string]any)
	re.Equal("disk", data1_2["metric"])
	re.Equal(45.8, data1_2["value"])

	// Verify category-3 data.
	re.NotNil(data2)
	re.Len(data2, 1)
	data2_0 := data2[0]["data"].(map[string]any)
	re.Equal("login", data2_0["event"])
	re.Equal("user1", data2_0["user"])
}

func TestMeteringDataReadWriteEmptyData(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create writer and reader using helper function.
	writer, reader := newLocalWriter(ctx, re, t.TempDir())
	defer writer.Stop()

	// Create and register a collector but don't add any data.
	emptyCategory := "empty-category"
	collector := newMockCollector(emptyCategory)
	writer.RegisterCollector(collector)

	// Flush without any data.
	ts := time.Now().Truncate(time.Minute).Unix()
	writer.flushMeteringData(ctx, ts)

	// Try to read data - should return nil for empty category.
	readData := readMeteringData(ctx, re, reader, emptyCategory, ts)
	re.Nil(readData)

	// Try to read data for non-existent category.
	nonExistentData := readMeteringData(ctx, re, reader, "non-existent", ts)
	re.Nil(nonExistentData)
}
