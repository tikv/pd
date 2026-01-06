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

package retry

import (
	"context"
	"time"

	"github.com/pingcap/errors"
)

var (
	// QueryRetryMaxTimes is the max retry times for querying microservice.
	queryRetryMaxTimes = 10
	// queryRetryInterval is the retry interval for querying microservice.
	queryRetryInterval = 500 * time.Millisecond
)

// WithConfig retries the given function with a fixed interval.
func WithConfig(
	ctx context.Context, f func() error,
) error {
	return Retry(ctx, queryRetryMaxTimes, queryRetryInterval, f)
}

// Retry retries the given function with a fixed interval.
func Retry(
	ctx context.Context, maxTimes int, interval time.Duration, f func() error,
) error {
	var err error
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range maxTimes {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
		}
	}
	return errors.WithStack(err)
}
