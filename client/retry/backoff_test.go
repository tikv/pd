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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestExponentialBackoff(t *testing.T) {
	re := require.New(t)

	baseBackoff := 100 * time.Millisecond
	maxBackoff := 1 * time.Second

	backoff := InitialBackOffer(baseBackoff, maxBackoff)
	re.Equal(backoff.nextInterval(), baseBackoff)
	re.Equal(backoff.nextInterval(), 2*baseBackoff)

	for i := 0; i < 10; i++ {
		re.LessOrEqual(backoff.nextInterval(), maxBackoff)
	}
	re.Equal(backoff.nextInterval(), maxBackoff)

	// Reset backoff
	backoff.resetBackoff()
	err := backoff.Exec(context.Background(), func() error {
		return errors.New("test")
	})
	re.Error(err)
}
