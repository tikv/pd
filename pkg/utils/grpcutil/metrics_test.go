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

package grpcutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

func TestRequestCounterCountsFailureAndUnknownCaller(t *testing.T) {
	re := require.New(t)
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_request_counter_total"},
		[]string{"request", "caller_id", "caller_component", "event"})

	RequestCounter("GetGCSafePoint", nil, &pdpb.Error{
		Type:    pdpb.ErrorType_UNKNOWN,
		Message: "test error",
	}, counter)

	re.Equal(float64(1), gatherCounterValue(re, counter, "GetGCSafePoint", "unknown", "unknown", "failed"))
}

func TestRequestCounterCountsFailureWithEmptyCaller(t *testing.T) {
	re := require.New(t)
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_request_counter_with_empty_caller_total"},
		[]string{"request", "caller_id", "caller_component", "event"})

	RequestCounter("UpdateGCSafePoint", &pdpb.RequestHeader{}, &pdpb.Error{
		Type:    pdpb.ErrorType_UNKNOWN,
		Message: "test error",
	}, counter)

	re.Equal(float64(1), gatherCounterValue(re, counter, "UpdateGCSafePoint", "unknown", "unknown", "failed"))
}

func gatherCounterValue(re *require.Assertions, counter *prometheus.CounterVec, values ...string) float64 {
	registry := prometheus.NewRegistry()
	re.NoError(registry.Register(counter))
	metricFamilies, err := registry.Gather()
	re.NoError(err)
	expectedLabels := map[string]string{
		"request":          values[0],
		"caller_id":        values[1],
		"caller_component": values[2],
		"event":            values[3],
	}
	for _, metricFamily := range metricFamilies {
		for _, metric := range metricFamily.GetMetric() {
			labels := metric.GetLabel()
			if len(labels) != len(values) {
				continue
			}
			matched := true
			for _, label := range labels {
				if expectedLabels[label.GetName()] != label.GetValue() {
					matched = false
					break
				}
			}
			if matched {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}
