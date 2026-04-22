// Copyright 2025 TiKV Project Authors.
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

package maintenance

import "github.com/prometheus/client_golang/prometheus"

var (
	maintenanceTaskInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "maintenance",
			Name:      "task_info",
			Help:      "Current maintenance task info in PD",
		}, []string{"task_type", "task_id"})
)

func init() {
	prometheus.MustRegister(maintenanceTaskInfo)
}

// SetMaintenanceTaskInfo sets the maintenance task info metric.
func SetMaintenanceTaskInfo(taskType, taskID string, value float64) {
	maintenanceTaskInfo.WithLabelValues(taskType, taskID).Set(value)
}
