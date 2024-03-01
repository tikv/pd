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
	"time"

	"github.com/pingcap/failpoint"
)

// Backoffer is a backoff policy for retrying operations.
type Backoffer struct {
	// base defines the initial time interval to wait before each retry.
	base time.Duration
	// max defines the max time interval to wait before each retry.
	max time.Duration
	// total defines the max total time duration cost in retrying. If it's 0, it means infinite retry until success.
	total time.Duration
	// retryableChecker is used to check if the error is retryable.
	// By default, all errors are retryable.
	retryableChecker func(err error) bool

	next         time.Duration
	currentTotal time.Duration
}

// Exec is a helper function to exec backoff.
func (bo *Backoffer) Exec(
	ctx context.Context,
	fn func() error,
) error {
	defer bo.resetBackoff()
	return bo.ExecWithoutReset(ctx, fn)
}

// ExecWithoutReset is a helper function to exec backoff and not reset backoffer.
func (bo *Backoffer) ExecWithoutReset(
	ctx context.Context,
	fn func() error,
) error {
	var (
		err   error
		after *time.Timer
	)
	for {
		err = fn()
		if !bo.isRetryable(err) {
			break
		}
		if bo.wait(ctx, after) {
			break
		}
	}
	return err
}

func (bo *Backoffer) WaitAndExecWithoutReset(
	ctx context.Context,
	fn func() error,
) bool {
	var (
		err   error
		after *time.Timer
	)
	for {
		if bo.wait(ctx, after) {
			return false
		}
		err = fn()
		if !bo.isRetryable(err) {
			break
		}
	}
	return err == nil
}

func (bo *Backoffer) wait(
	ctx context.Context,
	after *time.Timer,
) bool {
	currentInterval := bo.nextInterval()
	if after == nil {
		after = time.NewTimer(currentInterval)
	} else {
		after.Reset(currentInterval)
	}

	select {
	case <-ctx.Done():
		after.Stop()
		return true
	case <-after.C:
		failpoint.Inject("backOffExecute", func() {
			testBackOffExecuteFlag = true
		})
	}
	after.Stop()
	// If the current total time exceeds the maximum total time, return the last error.
	if bo.total > 0 {
		bo.currentTotal += currentInterval
		if bo.currentTotal >= bo.total {
			return true
		}
	}
	return false
}

// InitialBackoffer make the initial state for retrying.
//   - `base` defines the initial time interval to wait before each retry.
//   - `max` defines the max time interval to wait before each retry.
//   - `total` defines the max total time duration cost in retrying. If it's 0, it means infinite retry until success.
func InitialBackoffer(base, max, total time.Duration) *Backoffer {
	// Make sure the base is less than or equal to the max.
	if base > max {
		base = max
	}
	// Make sure the total is not less than the base.
	if total > 0 && total < base {
		total = base
	}
	return &Backoffer{
		base:  base,
		max:   max,
		total: total,
		retryableChecker: func(err error) bool {
			return err != nil
		},
		next:         base,
		currentTotal: 0,
	}
}

// SetRetryableChecker sets the retryable checker.
func (bo *Backoffer) SetRetryableChecker(checker func(err error) bool) {
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
}

// Clone clones the backoff for `base`, `max`, `total` and `retryableChecker`.
func (bo *Backoffer) Clone() *Backoffer {
	return &Backoffer{
		base:             bo.base,
		max:              bo.max,
		total:            bo.total,
		retryableChecker: bo.retryableChecker,
	}
}

// Only used for test.
var testBackOffExecuteFlag = false

// TestBackOffExecute Only used for test.
func TestBackOffExecute() bool {
	return testBackOffExecuteFlag
}
