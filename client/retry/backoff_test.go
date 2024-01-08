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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoffer(t *testing.T) {
	re := require.New(t)

	baseBackoff := 100 * time.Millisecond
	maxBackoff := time.Second
	totalBackoff := time.Second

	backoff := InitialBackoffer(baseBackoff, maxBackoff, totalBackoff)
	re.Equal(backoff.nextInterval(), baseBackoff)
	re.Equal(backoff.nextInterval(), 2*baseBackoff)

	for i := 0; i < 10; i++ {
		re.LessOrEqual(backoff.nextInterval(), maxBackoff)
	}
	re.Equal(backoff.nextInterval(), maxBackoff)

	// Reset backoff
	backoff.resetBackoff()
	var (
		start       time.Time
		execCount   int
		err         error
		expectedErr = errors.New("test")
	)
	err = backoff.Exec(context.Background(), func() error {
		execCount++
		if start.IsZero() {
			start = time.Now()
		}
		return expectedErr
	})
	total := time.Since(start)
	re.ErrorIs(err, expectedErr)
	re.Equal(4, execCount)
	re.InDelta(totalBackoff, total, float64(maxBackoff/2))
}
