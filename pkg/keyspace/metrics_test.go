// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

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
