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

var (
	RunnerTaskMaxWaitingDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_task_max_waiting_duration_seconds",
			Help:      "The duration of tasks waiting in the runner.",
		}, []string{"name"})

	RunnerTaskPendingTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_task_pending_tasks",
			Help:      "The number of pending tasks in the runner.",
		}, []string{"name"})
	RunnerTaskFailedTasks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "ratelimit",
			Name:      "runner_task_failed_tasks_total",
			Help:      "The number of failed tasks in the runner.",
		}, []string{"name"})
)

func init() {
	prometheus.MustRegister(RunnerTaskMaxWaitingDuration)
	prometheus.MustRegister(RunnerTaskPendingTasks)
	prometheus.MustRegister(RunnerTaskFailedTasks)
}
