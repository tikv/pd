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

package retry

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
)

// Option is used to customize the backoffer.
type Option func(*Backoffer)

// withMinLogInterval sets the minimum log interval for retrying.
// Because the retry interval may be not the factor of log interval, so this is the minimum interval.
func withMinLogInterval(interval time.Duration) Option {
	return func(bo *Backoffer) {
		bo.logInterval = interval
	}
}

// Backoffer is a backoff policy for retrying operations.
type Backoffer struct {
	// base defines the initial time interval to wait before each retry.
	base time.Duration
	// max defines the max time interval to wait before each retry.
	max time.Duration
	// total defines the max total time duration cost in retrying. If it's 0, it means infinite retry until success.
	total time.Duration
	// retryableChecker is used to check if the error is retryable.
	// If it's not set, it will always retry unconditionally no matter what the error is.
	retryableChecker func(err error) bool
	// logInterval defines the log interval for retrying.
	logInterval time.Duration
	// nextLogTime is used to record the next log time.
	nextLogTime time.Duration

	attempt      int
	next         time.Duration
	currentTotal time.Duration
}

// Exec is a helper function to exec backoff.
func (bo *Backoffer) Exec(
	ctx context.Context,
	fn func() error,
) error {
	defer bo.resetBackoff()
	var (
		err   error
		after *time.Timer
	)
	fnName := getFunctionName(fn)
	for {
		err = fn()
		bo.attempt++
		if err == nil || !bo.isRetryable(err) {
			break
		}
		currentInterval := bo.nextInterval()
		bo.nextLogTime += currentInterval
		if bo.logInterval > 0 && bo.nextLogTime >= bo.logInterval {
			bo.nextLogTime %= bo.logInterval
			log.Warn("[pd.backoffer] exec fn failed and retrying",
				zap.String("fn-name", fnName), zap.Int("retry-time", bo.attempt), zap.Error(err))
		}
		if after == nil {
			after = time.NewTimer(currentInterval)
		} else {
			after.Reset(currentInterval)
		}
		select {
		case <-ctx.Done():
			after.Stop()
			return errors.Trace(ctx.Err())
		case <-after.C:
			failpoint.Inject("backOffExecute", func() {
				testBackOffExecuteFlag.Store(true)
			})
		}
		after.Stop()
		// If the current total time exceeds the maximum total time, return the last error.
		if bo.total > 0 {
			bo.currentTotal += currentInterval
			if bo.currentTotal >= bo.total {
				break
			}
		}
	}
	return err
}

// InitialBackoffer make the initial state for retrying.
//   - `base` defines the initial time interval to wait before each retry.
//   - `max` defines the max time interval to wait before each retry.
//   - `total` defines the max total time duration cost in retrying. If it's 0, it means infinite retry until success.
func InitialBackoffer(base, max, total time.Duration, opts ...Option) *Backoffer {
	// Make sure the base is less than or equal to the max.
	if base > max {
		base = max
	}
	// Make sure the total is not less than the base.
	if total > 0 && total < base {
		total = base
	}
	bo := &Backoffer{
		base:         base,
		max:          max,
		total:        total,
		next:         base,
		currentTotal: 0,
		attempt:      0,
	}
	for _, opt := range opts {
		opt(bo)
	}
	return bo
}

// SetRetryableChecker sets the retryable checker, `overwrite` flag is used to indicate whether to overwrite the existing checker.
func (bo *Backoffer) SetRetryableChecker(checker func(err error) bool, overwrite bool) {
	if !overwrite && bo.retryableChecker != nil {
		return
	}
	bo.retryableChecker = checker
}

func (bo *Backoffer) isRetryable(err error) bool {
	if bo.retryableChecker == nil {
		return true
	}
	return bo.retryableChecker(err)
}

// nextInterval for now use the `exponentialInterval`.
func (bo *Backoffer) nextInterval() time.Duration {
	return bo.exponentialInterval()
}

// exponentialInterval returns the exponential backoff duration.
func (bo *Backoffer) exponentialInterval() time.Duration {
	backoffInterval := bo.next
	// Make sure the total backoff time is less than the total.
	if bo.total > 0 && bo.currentTotal+backoffInterval > bo.total {
		backoffInterval = bo.total - bo.currentTotal
	}
	bo.next *= 2
	// Make sure the next backoff time is less than the max.
	if bo.next > bo.max {
		bo.next = bo.max
	}
	return backoffInterval
}

// resetBackoff resets the backoff to initial state.
func (bo *Backoffer) resetBackoff() {
	bo.next = bo.base
	bo.currentTotal = 0
	bo.attempt = 0
	bo.nextLogTime = 0
}

// Only used for test.
var testBackOffExecuteFlag atomic.Bool

// TestBackOffExecute Only used for test.
func TestBackOffExecute() bool {
	return testBackOffExecuteFlag.Load()
}

func getFunctionName(f any) string {
	strs := strings.Split(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), ".")
	return strings.Split(strs[len(strs)-1], "-")[0]
}

// Define context key type
type boCtxKey struct{}

// Key used to store backoffer
var backofferKey = boCtxKey{}

// FromContext retrieves the backoffer from the context
func FromContext(ctx context.Context) *Backoffer {
	if ctx == nil {
		return nil
	}
	if bo, ok := ctx.Value(backofferKey).(*Backoffer); ok {
		return bo
	}
	return nil
}

// WithBackoffer stores the backoffer into a new context
func WithBackoffer(ctx context.Context, bo *Backoffer) context.Context {
	return context.WithValue(ctx, backofferKey, bo)
}
