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

package ratelimit

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	nameStr = "runner_name"
	taskStr = "task_type"
)

var (
	runnerTaskMaxWaitingDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_task_max_waiting_duration_seconds",
			Help:      "The duration of tasks waiting in the runner.",
		}, []string{nameStr})
	runnerPendingTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_pending_tasks",
			Help:      "The number of pending tasks in the runner.",
		}, []string{nameStr, taskStr})
	runnerFailedTasks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_failed_tasks_total",
			Help:      "The number of failed tasks in the runner.",
		}, []string{nameStr, taskStr})
	runnerSucceededTasks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_success_tasks_total",
			Help:      "The number of tasks in the runner.",
		}, []string{nameStr, taskStr})
	runnerTaskExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_task_execution_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished tasks.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{nameStr, taskStr})
)

func init() {
	prometheus.MustRegister(runnerTaskMaxWaitingDuration)
	prometheus.MustRegister(runnerPendingTasks)
	prometheus.MustRegister(runnerFailedTasks)
	prometheus.MustRegister(runnerTaskExecutionDuration)
	prometheus.MustRegister(runnerSucceededTasks)
}
