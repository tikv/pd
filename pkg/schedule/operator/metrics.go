// Copyright 2018 TiKV Project Authors.
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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tikv/pd/pkg/schedule/types"
)

var (
	operatorStepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "finish_operator_steps_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator step.",
			Buckets:   []float64{0.5, 1, 2, 4, 8, 16, 20, 40, 60, 90, 120, 180, 240, 300, 480, 600, 720, 900, 1200, 1800, 3600},
		}, []string{"type"})

	operatorLimitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "operator_limit",
			Help:      "Counter of operator meeting limit",
		}, []string{"type", "name"})

	storeLimitCostCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "store_limit_cost",
			Help:      "limit rate cost of store.",
		}, []string{"store", "limit_type"})

	// OperatorExceededStoreLimitCounter exposes the counter when operator meet exceeded store limit.
	OperatorExceededStoreLimitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "operator_exceeded_store_limit",
			Help:      "Counter of operator meeting store limit",
		}, []string{"desc"})

	// TODO: pre-allocate gauge metrics
	operatorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "operators_count",
			Help:      "Counter of schedule operators.",
		}, []string{"type", "event"})

	operatorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "finish_operators_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator.",
			Buckets:   []float64{0.5, 1, 2, 4, 8, 16, 20, 40, 60, 90, 120, 180, 240, 300, 480, 600, 720, 900, 1200, 1800, 3600},
		}, []string{"type"})

	operatorSizeHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "operator_region_size",
			Help:      "Bucketed histogram of the operator region size.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1MB~1TB
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(operatorStepDuration)
	prometheus.MustRegister(operatorLimitCounter)
	prometheus.MustRegister(OperatorExceededStoreLimitCounter)
	prometheus.MustRegister(operatorCounter)
	prometheus.MustRegister(operatorDuration)
	prometheus.MustRegister(operatorSizeHist)
	prometheus.MustRegister(storeLimitCostCounter)
}

// IncOperatorLimitCounter increases the counter of operator meeting limit.
func IncOperatorLimitCounter(typ types.CheckerSchedulerType, kind OpKind) {
	operatorLimitCounter.WithLabelValues(typ.String(), kind.String()).Inc()
}
