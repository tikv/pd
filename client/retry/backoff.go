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

// BackOffer is a backoff policy for retrying operations.
type BackOffer struct {
	max  time.Duration
	next time.Duration
	base time.Duration
}

// Exec is a helper function to exec backoff.
func (bo *BackOffer) Exec(
	ctx context.Context,
	fn func() error,
) error {
	if err := fn(); err != nil {
		after := time.NewTimer(bo.nextInterval())
		defer after.Stop()
		select {
		case <-ctx.Done():
		case <-after.C:
			failpoint.Inject("backOffExecute", func() {
				testBackOffExecuteFlag = true
			})
		}
		return err
	}
	// reset backoff when fn() succeed.
	bo.resetBackoff()
	return nil
}

// InitialBackOffer make the initial state for retrying.
func InitialBackOffer(base, max time.Duration) BackOffer {
	return BackOffer{
		max:  max,
		base: base,
		next: base,
	}
}

// nextInterval for now use the `exponentialInterval`.
func (bo *BackOffer) nextInterval() time.Duration {
	return bo.exponentialInterval()
}

// exponentialInterval returns the exponential backoff duration.
func (bo *BackOffer) exponentialInterval() time.Duration {
	backoffInterval := bo.next
	bo.next *= 2
	if bo.next > bo.max {
		bo.next = bo.max
	}
	return backoffInterval
}

// resetBackoff resets the backoff to initial state.
func (bo *BackOffer) resetBackoff() {
	bo.next = bo.base
}

// Only used for test.
var testBackOffExecuteFlag = false

// TestBackOffExecute Only used for test.
func TestBackOffExecute() bool {
	return testBackOffExecuteFlag
}
