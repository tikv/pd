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

import "github.com/prometheus/client_golang/prometheus"

var (
	operatorStepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "finish_operator_steps_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator step.",
			Buckets:   []float64{0.5, 1, 2, 4, 8, 16, 20, 40, 60, 90, 120, 180, 240, 300, 480, 600, 720, 900, 1200, 1800, 3600},
		}, []string{"type"})

	// OperatorLimitCounter exposes the counter when meeting limit.
	OperatorLimitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "operator_limit",
			Help:      "Counter of operator meeting limit",
		}, []string{"type", "name"})
)

func init() {
	prometheus.MustRegister(operatorStepDuration)
	prometheus.MustRegister(OperatorLimitCounter)
}
