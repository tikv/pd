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

package operator

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestDeleteStoreMetricsClearsStoreLimitCostCounter(t *testing.T) {
	re := require.New(t)
	defer storeLimitCostCounter.Reset()

	storeLimitCostCounter.WithLabelValues("1", "add-peer").Add(1)
	DeleteStoreMetrics("1")
	// DeletePartialMatch returns how many series it found and removed, so a
	// zero return here proves DeleteStoreMetrics already deleted it -- unlike
	// checking WithLabelValues' value, which would recreate a fresh
	// (zero-valued) series regardless of whether the old one was cleaned up.
	re.Zero(storeLimitCostCounter.DeletePartialMatch(prometheus.Labels{"store": "1"}))
}

func TestResetOperatorMetricsClearsStoreLimitCostCounter(t *testing.T) {
	re := require.New(t)
	defer storeLimitCostCounter.Reset()

	storeLimitCostCounter.WithLabelValues("1", "add-peer").Add(1)
	ResetOperatorMetrics()
	re.Zero(storeLimitCostCounter.DeletePartialMatch(prometheus.Labels{"store": "1"}))
}
