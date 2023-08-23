// Copyright 2023 TiKV Project Authors.
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

package backoff

import (
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Backoffer is a utility for retrying queries.
type Backoffer struct {
	ctx context.Context

	fn         map[string]backoffFn
	maxSleep   int
	totalSleep int

	errors         []error
	configs        []*Config
	backoffSleepMS map[string]int
	backoffTimes   map[string]int
}

// NewBackoffer (Deprecated) creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	return &Backoffer{
		ctx:      ctx,
		maxSleep: maxSleep,
	}
}

// Backoff sleeps a while base on the Config and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *Backoffer) Backoff(cfg *Config, err error) error {
	if span := opentracing.SpanFromContext(b.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("tikv.backoff.%s", cfg), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(b.ctx, span1)
	}
	return b.BackoffWithCfgAndMaxSleep(cfg, -1, err)
}

// BackoffWithCfgAndMaxSleep sleeps a while base on the Config and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithCfgAndMaxSleep(cfg *Config, maxSleepMs int, err error) error {
	select {
	case <-b.ctx.Done():
		return errors.WithStack(err)
	default:
	}
	b.errors = append(b.errors, errors.Errorf("%s at %s", err.Error(), time.Now().Format(time.RFC3339Nano)))
	b.configs = append(b.configs, cfg)

	// Lazy initialize.
	if b.fn == nil {
		b.fn = make(map[string]backoffFn)
	}
	f, ok := b.fn[cfg.name]
	if !ok {
		f = cfg.createBackoffFn()
		b.fn[cfg.name] = f
	}
	realSleep := f(b.ctx, maxSleepMs)

	b.totalSleep += realSleep
	if b.backoffSleepMS == nil {
		b.backoffSleepMS = make(map[string]int)
	}
	b.backoffSleepMS[cfg.name] += realSleep
	if b.backoffTimes == nil {
		b.backoffTimes = make(map[string]int)
	}
	b.backoffTimes[cfg.name]++

	log.Debug("retry later",
		zap.Error(err),
		zap.Int("totalSleep", b.totalSleep),
		zap.Int("maxSleep", b.maxSleep),
		zap.Stringer("type", cfg))
	return nil
}

func (b *Backoffer) String() string {
	if b.totalSleep == 0 {
		return ""
	}
	return fmt.Sprintf(" backoff(%dms %v)", b.totalSleep, b.configs)
}

// GetTotalSleep returns total sleep time.
func (b *Backoffer) GetTotalSleep() int {
	return b.totalSleep
}

// GetCtx returns the bound context.
func (b *Backoffer) GetCtx() context.Context {
	return b.ctx
}

// SetCtx sets the bound context to ctx.
func (b *Backoffer) SetCtx(ctx context.Context) {
	b.ctx = ctx
}

// GetBackoffTimes returns a map contains backoff time count by type.
func (b *Backoffer) GetBackoffTimes() map[string]int {
	return b.backoffTimes
}

// GetBackoffTime returns backoff time count by specific type.
func (b *Backoffer) GetBackoffTime(s string) int {
	return b.backoffTimes[s]
}

// GetTotalBackoffTimes returns the total backoff times of the backoffer.
func (b *Backoffer) GetTotalBackoffTimes() int {
	total := 0
	for _, t := range b.backoffTimes {
		total += t
	}
	return total
}

// GetBackoffSleepMS returns a map contains backoff sleep time by type.
func (b *Backoffer) GetBackoffSleepMS() map[string]int {
	return b.backoffSleepMS
}

// ErrorsNum returns the number of errors.
func (b *Backoffer) ErrorsNum() int {
	return len(b.errors)
}

// Reset resets the sleep state of the backoffer, so that following backoff
// can sleep shorter. The reason why we don't create a new backoffer is that
// backoffer is similar to context, and it records some metrics that we
// want to record for an entire process which is composed of serveral stages.
func (b *Backoffer) Reset() {
	b.fn = nil
	b.totalSleep = 0
}

// ResetMaxSleep resets the sleep state and max sleep limit of the backoffer.
// It's used when switches to the next stage of the process.
func (b *Backoffer) ResetMaxSleep(maxSleep int) {
	b.Reset()
	b.maxSleep = maxSleep
}
