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

//go:build !swagger

// Because the swag command line tool doesn't provide a decent way to exclude specific packages from the documentation generation,
// the build tag above is a workaround to avoid involving unexpected third-party dependencies when building the swagger documentation.

package metering

import (
	"context"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"

	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	flushInterval       = time.Minute
	flushTimeout        = flushInterval / 2
	writeMaxAttempts    = 6
	writeRetryBaseDelay = time.Second
)

// Collector collects events into records with caller-defined fields.
type Collector interface {
	Category() string
	Collect(data any)
	Aggregate() []map[string]any
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
func NewWriter(ctx context.Context, c *config.MeteringConfig, id string) (*Writer, error) {
	err := validateMeteringConfig(c)
	if err != nil {
		return nil, err
	}
	var provider storage.ObjectStorageProvider
	provider, err = storage.NewObjectStorageProvider(c.ToProviderConfig())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create storage provider for metering")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Writer{
		id:       id,
		ctx:      ctx,
		cancel:   cancel,
		provider: provider,
		inner: meteringwriter.NewMeteringWriterFromConfig(
			provider,
			config.DefaultConfig().WithLogger(zap.L()),
			c,
		),
		collectors: make(map[string]Collector),
	}, nil
}

func validateMeteringConfig(c *config.MeteringConfig) error {
	if len(c.Type) == 0 {
		return errors.New("type is required for the metering config")
	}
	switch c.Type {
	case storage.ProviderTypeS3:
		fallthrough
	case storage.ProviderTypeOSS:
		if len(c.Region) == 0 {
			return errors.New("region is required for the metering config")
		}
		if len(c.Bucket) == 0 {
			return errors.New("bucket is required for the metering config")
		}
	case storage.ProviderTypeLocalFS:
		if len(c.LocalFS.BasePath) == 0 {
			return errors.New("base path is required for the metering config")
		}
	default:
		return errors.New("unsupported provider type for the metering config")
	}
	return nil
}

// Start starts the metering writer background loop
func (mw *Writer) Start() {
	if mw == nil {
		return
	}
	mw.wg.Add(1)
	go mw.meteringLoop()
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
	category := collector.Category()
	log.Info("registering metering collector", zap.String("category", category))
	mw.Lock()
	defer mw.Unlock()
	if mw.collectors == nil {
		mw.collectors = make(map[string]Collector)
	}
	mw.collectors[category] = collector
}

// meteringLoop runs the background loop to flush metering data periodically.
func (mw *Writer) meteringLoop() {
	defer logutil.LogPanic()
	defer mw.wg.Done()

	now := time.Now()
	// Truncate to the nearest minute and add the interval to get the next flush time.
	// By default, metering data should be flushed at the minute level with the correct timestamp.
	// To ensure accuracy and avoid unexpected overwrites from overlapping timestamps, we need to round the time accordingly.
	next := now.Truncate(flushInterval).Add(flushInterval)
	log.Info("metering writer loop started",
		zap.String("self-id", mw.id),
		zap.Time("now", now),
		zap.Time("next", next))
	for {
		select {
		case <-mw.ctx.Done():
			ts := next.Unix()
			log.Info("context done received, flushing metering data one last time",
				zap.Int64("timestamp", ts))
			// Due to the context is already done, we need to use a new context to flush the data.
			mw.flushMeteringData(context.Background(), ts)
			log.Info("metering writer loop exits")
			return
		case <-time.After(next.Sub(now)):
			mw.flushMeteringData(mw.ctx, next.Unix())
			now = time.Now()
			next = next.Add(flushInterval)
		}
	}
}

// flushMeteringData flushes aggregated metering data to the underlying storage
func (mw *Writer) flushMeteringData(ctx context.Context, ts int64) {
	collectors := mw.GetCollectors()
	if len(collectors) == 0 {
		return
	}

	for category, collector := range collectors {
		records := collector.Aggregate()
		if len(records) == 0 {
			continue
		}
		// This should never happen in the real world, it's used to avoid panic during testing.
		if mw.inner == nil {
			continue
		}
		meteringData := &common.MeteringData{
			SelfID:    mw.id,
			Timestamp: ts,
			Category:  category,
			Data:      records,
		}
		recordCount := len(records)
		start := time.Now()
		baseLogFields := []zap.Field{
			zap.String("self-id", mw.id),
			zap.Int64("timestamp", ts),
			zap.String("category", category),
			zap.Int("record-count", recordCount),
		}
		var (
			err       error
			attempt   int
			wait      time.Duration
			baseDelay = writeRetryBaseDelay
		)
	retryLoop:
		for {
			attempt++
			writeCtx, cancel := context.WithTimeout(ctx, flushTimeout)
			// Inject mock write error for testing, value is the count to encounter error.
			failpoint.Inject("mockWriteError", func(val failpoint.Value) {
				baseDelay = time.Millisecond
				if val.(int) >= attempt {
					cancel()
				}
			})
			// Ensure the context is valid before writing.
			if writeCtx.Err() != nil {
				err = writeCtx.Err()
			} else {
				// TODO: write with pagination if needed.
				err = mw.inner.Write(writeCtx, meteringData)
			}
			cancel()
			if err == nil {
				break
			}
			log.Warn("failed to write metering data to underlying storage, will retry",
				append(baseLogFields,
					zap.Int("attempt", attempt),
					zap.Int("max-attempts", writeMaxAttempts),
					zap.Duration("last-wait", wait),
					zap.Error(err))...)
			if attempt >= writeMaxAttempts {
				break
			}
			// Check whether the upper context is done.
			if ctx.Err() != nil {
				break
			}
			// Calculate the wait time for the next retry.
			wait = time.Duration(
				math.Min(
					// The wait time is the base delay multiplied by 2^(attempt-1).
					float64(baseDelay<<uint(attempt-1)),
					// The maximum wait time is 10 times the base delay.
					float64(10*baseDelay),
				),
			)
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				err = ctx.Err()
				break retryLoop
			}
		}
		cost := time.Since(start)
		logFields := append(baseLogFields,
			zap.Int("attempts", attempt),
			zap.Duration("cost", cost),
		)
		if err != nil {
			log.Error("failed to write metering data to underlying storage",
				append(logFields, zap.Error(err))...)
		} else {
			log.Info("successfully wrote metering data to underlying storage",
				logFields...)
		}
	}
}

// GetCollectors returns the registered collectors.
func (mw *Writer) GetCollectors() map[string]Collector {
	mw.RLock()
	defer mw.RUnlock()
	collectors := make(map[string]Collector, len(mw.collectors))
	for k, v := range mw.collectors {
		collectors[k] = v
	}
	return collectors
}
