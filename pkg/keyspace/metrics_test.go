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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceInfoMetrics(t *testing.T) {
	ResetKeyspaceInfoMetrics()
	t.Cleanup(ResetKeyspaceInfoMetrics)

	SetKeyspaceInfoMetrics(42, "old-name")
	require.Equal(t, float64(1), testutil.ToFloat64(keyspaceInfo.WithLabelValues("42", "old-name")))

	SetKeyspaceInfoMetrics(42, "new-name")
	require.Equal(t, 1, testutil.CollectAndCount(keyspaceInfo))
	require.Equal(t, float64(1), testutil.ToFloat64(keyspaceInfo.WithLabelValues(
		strconv.FormatUint(42, 10), "new-name")))

	DeleteKeyspaceInfoMetrics(42)
	require.Equal(t, 0, testutil.CollectAndCount(keyspaceInfo))
}
