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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"

	"github.com/tikv/pd/pkg/utils/logutil"
)

// meteringFlushInterval is the interval to flush the metering to the underlying storage.
const meteringFlushInterval = time.Minute

// Collector collects events into records with caller-defined fields.
type Collector interface {
	Category() string
	Collect(data any)
	Flush() []map[string]any
}

// Writer is used to as a delegate to collect and flush the metering data to the underlying storage.
type Writer struct {
	sync.RWMutex
	id       string
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	provider storage.ObjectStorageProvider
	inner    *meteringwriter.MeteringWriter
	// collectors holds category -> collector, allowing callers to customize record fields per category.
	collectors map[string]Collector
}

// NewWriter creates a new metering writer to collect and report the metering data to the underlying storage.
func NewWriter(ctx context.Context, meteringConfig *Config, id string) (*Writer, error) {
	err := meteringConfig.adjust()
	if err != nil {
		return nil, err
	}

	// Create storage provider
	providerConfig := &storage.ProviderConfig{
		Type:   meteringConfig.Type,
		Bucket: meteringConfig.Bucket,
		Region: meteringConfig.Region,
		Prefix: meteringConfig.Prefix,
	}
	provider, err := storage.NewObjectStorageProvider(providerConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create storage provider for metering")
	}

	ctx, cancel := context.WithCancel(ctx)
	return &Writer{
		id:       id,
		ctx:      ctx,
		cancel:   cancel,
		provider: provider,
		inner: meteringwriter.NewMeteringWriter(
			provider,
			config.DefaultConfig().WithLogger(zap.L()),
		),
		collectors: make(map[string]Collector),
	}, nil
}

// Start starts the metering writer background loop
func (mw *Writer) Start() {
	if mw == nil {
		return
	}
	mw.wg.Add(1)
	go mw.meteringLoop()
	log.Info("metering writer started")
}

// Stop stops the metering writer
func (mw *Writer) Stop() {
	if mw == nil {
		return
	}
	if mw.cancel != nil {
		mw.cancel()
	}
	mw.wg.Wait()
	if mw.inner != nil {
		mw.inner.Close()
	}
	log.Info("metering writer stopped")
}

// RegisterCollector registers a collector for a category. Overwrites existing if any.
func (mw *Writer) RegisterCollector(collector Collector) {
	if mw == nil {
		return
	}
	mw.Lock()
	defer mw.Unlock()
	if mw.collectors == nil {
		mw.collectors = make(map[string]Collector)
	}
	mw.collectors[collector.Category()] = collector
}

// meteringLoop runs the background loop to flush metering data periodically.
func (mw *Writer) meteringLoop() {
	defer logutil.LogPanic()
	defer mw.wg.Done()

	ticker := time.NewTicker(meteringFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mw.ctx.Done():
			log.Info("metering writer loop exits")
			return
		case <-ticker.C:
			mw.flushMeteringData()
		}
	}
}

// flushMeteringData flushes aggregated metering data to the underlying storage
func (mw *Writer) flushMeteringData() {
	collectors := mw.getCollectors()
	if len(collectors) == 0 {
		return
	}

	ts := time.Now().Unix() / 60
	for category, collector := range collectors {
		records := collector.Flush()
		if len(records) == 0 {
			continue
		}
		meteringData := &common.MeteringData{
			SelfID:    mw.id,
			Timestamp: ts,
			Category:  category,
			Data:      records,
		}
		if mw.inner == nil {
			continue
		}
		if err := mw.inner.Write(mw.ctx, meteringData); err != nil {
			log.Error("failed to write metering data to underlying storage",
				zap.String("category", category), zap.Error(err))
			continue
		}
		log.Info("successfully wrote metering data to underlying storage",
			zap.Int64("timestamp", meteringData.Timestamp),
			zap.String("category", category),
			zap.Int("record-count", len(records)))
	}
}

func (mw *Writer) getCollectors() map[string]Collector {
	mw.RLock()
	defer mw.RUnlock()
	collectors := make(map[string]Collector, len(mw.collectors))
	for k, v := range mw.collectors {
		collectors[k] = v
	}
	return collectors
}
