// Copyright 2026 TiKV Project Authors.
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

package keyspace

import (
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceInfoMetrics(t *testing.T) {
	resetKeyspaceInfoMetrics()
	t.Cleanup(resetKeyspaceInfoMetrics)

	SetKeyspaceInfoMetrics(42, "test-name")
	require.Equal(t, float64(1), testutil.ToFloat64(keyspaceInfo.WithLabelValues("42", "test-name")))

	SetKeyspaceInfoMetrics(42, "test-name")
	require.Equal(t, 1, testutil.CollectAndCount(keyspaceInfo))
	require.Equal(t, float64(1), testutil.ToFloat64(keyspaceInfo.WithLabelValues("42", "test-name")))

	SetKeyspaceInfoMetrics(42, "new-name")
	require.Equal(t, 1, testutil.CollectAndCount(keyspaceInfo))
	require.Equal(t, float64(1), testutil.ToFloat64(keyspaceInfo.WithLabelValues("42", "new-name")))

	deleteKeyspaceInfoMetrics(42)
	require.Equal(t, 0, testutil.CollectAndCount(keyspaceInfo))
}

func TestKeyspaceInfoMetricsConcurrentUpdates(t *testing.T) {
	resetKeyspaceInfoMetrics()
	t.Cleanup(resetKeyspaceInfoMetrics)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%3 == 0 {
				deleteKeyspaceInfoMetrics(42)
				return
			}
			SetKeyspaceInfoMetrics(42, strconv.Itoa(i%2))
		}()
	}
	wg.Wait()

	SetKeyspaceInfoMetrics(42, "final-name")
	require.Equal(t, 1, testutil.CollectAndCount(keyspaceInfo))
	require.Equal(t, float64(1), testutil.ToFloat64(keyspaceInfo.WithLabelValues("42", "final-name")))
}
