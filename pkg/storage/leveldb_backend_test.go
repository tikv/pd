// Copyright 2024 TiKV Project Authors.
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

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestLevelDBBackend(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend, err := newLevelDBBackend(ctx, t.TempDir(), nil)
	re.NoError(err)
	re.NotNil(backend)
	key, value := "k1", "v1"
	// Save without flush.
	err = backend.SaveIntoBatch(key, []byte(value))
	re.NoError(err)
	val, err := backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	// Flush and load.
	err = backend.Flush()
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Equal(value, val)
	// Delete and load.
	err = backend.Remove(key)
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	// Save twice without flush.
	err = backend.SaveIntoBatch(key, []byte(value))
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	value = "v2"
	err = backend.SaveIntoBatch(key, []byte(value))
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	// Delete before flush.
	err = backend.Remove(key)
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	// Flush and load.
	err = backend.Flush()
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Equal(value, val)
	// Delete and load.
	err = backend.Remove(key)
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	// Test the background flush.
	backend.flushRate = defaultDirtyFlushTick
	err = backend.SaveIntoBatch(key, []byte(value))
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	testutil.Eventually(re, func() bool {
		val, err = backend.Load(key)
		re.NoError(err)
		return value == val
	}, testutil.WithWaitFor(defaultDirtyFlushTick*5), testutil.WithTickInterval(defaultDirtyFlushTick/2))
	err = backend.Remove(key)
	re.NoError(err)
	val, err = backend.Load(key)
	re.NoError(err)
	re.Empty(val)
	backend.flushRate = defaultFlushRate
	// Test the flush when the cache is full.
	backend.flushRate = time.Minute
	for i := range backend.batchSize {
		key, value = fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		err = backend.SaveIntoBatch(key, []byte(value))
		re.NoError(err)
		if i < backend.batchSize-1 {
			// The cache is not full yet.
			val, err = backend.Load(key)
			re.NoError(err)
			re.Empty(val)
		} else {
			// The cache is full, and the flush is triggered.
			val, err = backend.Load(key)
			re.NoError(err)
			re.Equal(value, val)
		}
	}
	backend.flushRate = defaultFlushRate
	// Close the backend.
	err = backend.Close()
	re.NoError(err)
}
