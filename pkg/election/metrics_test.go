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

package election

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestLeaseMetricsObserveRemainingTTLUsesGauge(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	defer localTTLRemaining.DeleteLabelValues(purpose)

	metrics := newLeaseMetrics(purpose)
	metrics.observeRemainingTTL(3 * time.Second)
	metrics.observeRemainingTTL(1500 * time.Millisecond)

	metric := collectLocalTTLRemainingMetric(t, purpose)
	re.NotNil(metric.Gauge)
	re.Nil(metric.Histogram)
	re.Equal(1.5, metric.GetGauge().GetValue())
}

func TestLeaseMetricsObserveRemainingTTLClampsNegativeGaugeValue(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	defer localTTLRemaining.DeleteLabelValues(purpose)

	metrics := newLeaseMetrics(purpose)
	metrics.observeRemainingTTL(-time.Second)

	metric := collectLocalTTLRemainingMetric(t, purpose)
	re.NotNil(metric.Gauge)
	re.Equal(0.0, metric.GetGauge().GetValue())
}

func TestLeaseMetricsObserveKeepAliveRequestDuration(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	defer keepAliveRequestDuration.DeleteLabelValues(purpose, metricsResultSuccess)
	defer keepAliveRequestDuration.DeleteLabelValues(purpose, metricsResultError)
	defer keepAliveRequestDuration.DeleteLabelValues(purpose, metricsResultCanceled)

	metrics := newLeaseMetrics(purpose)
	metrics.observeKeepAliveRequestDurationMetrics(1500*time.Millisecond, nil)
	metrics.observeKeepAliveRequestDurationMetrics(2500*time.Millisecond, stderrors.New("unavailable"))
	metrics.observeKeepAliveRequestDurationMetrics(500*time.Millisecond, context.Canceled)

	successMetric := collectMetricByLabels(t, keepAliveRequestDuration, map[string]string{
		metricsLabelPurpose: purpose,
		metricsLabelResult:  metricsResultSuccess,
	})
	re.NotNil(successMetric.Histogram)
	re.Equal(uint64(1), successMetric.GetHistogram().GetSampleCount())
	re.Equal(1.5, successMetric.GetHistogram().GetSampleSum())

	errorMetric := collectMetricByLabels(t, keepAliveRequestDuration, map[string]string{
		metricsLabelPurpose: purpose,
		metricsLabelResult:  metricsResultError,
	})
	re.NotNil(errorMetric.Histogram)
	re.Equal(uint64(1), errorMetric.GetHistogram().GetSampleCount())
	re.Equal(2.5, errorMetric.GetHistogram().GetSampleSum())

	canceledMetric := collectMetricByLabels(t, keepAliveRequestDuration, map[string]string{
		metricsLabelPurpose: purpose,
		metricsLabelResult:  metricsResultCanceled,
	})
	re.NotNil(canceledMetric.Histogram)
	re.Equal(uint64(1), canceledMetric.GetHistogram().GetSampleCount())
	re.Equal(0.5, canceledMetric.GetHistogram().GetSampleSum())
}

func collectLocalTTLRemainingMetric(t *testing.T, purpose string) *dto.Metric {
	t.Helper()
	return collectMetricByLabels(t, localTTLRemaining, map[string]string{metricsLabelPurpose: purpose})
}

func collectMetricByLabels(t *testing.T, collector prometheus.Collector, expectedLabels map[string]string) *dto.Metric {
	t.Helper()
	re := require.New(t)
	metric := findMetricByLabels(t, collector, expectedLabels)
	re.NotNil(metric, "metric not found: %v", expectedLabels)
	return metric
}

func findMetricByLabels(t *testing.T, collector prometheus.Collector, expectedLabels map[string]string) *dto.Metric {
	t.Helper()
	re := require.New(t)
	ch := make(chan prometheus.Metric, 1000)
	collector.Collect(ch)
	close(ch)

	for metric := range ch {
		dtoMetric := &dto.Metric{}
		re.NoError(metric.Write(dtoMetric))
		matchedLabels := 0
		for _, label := range dtoMetric.GetLabel() {
			if expectedValue, ok := expectedLabels[label.GetName()]; ok && label.GetValue() == expectedValue {
				matchedLabels++
			}
		}
		if matchedLabels == len(expectedLabels) {
			return dtoMetric
		}
	}
	return nil
}
