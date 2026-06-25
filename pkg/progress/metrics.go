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

package progress

import "github.com/prometheus/client_golang/prometheus"

var (
	storesProgressGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "progress",
			Help:      "The current progress of corresponding action",
		}, []string{"address", "store", "action"})

	storesSpeedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "speed",
			Help:      "The current speed of corresponding action",
		}, []string{"address", "store", "action"})

	storesETAGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "eta",
			Help:      "The ETA of corresponding action",
		}, []string{"address", "store", "action"})
)

func init() {
	prometheus.MustRegister(storesProgressGauge)
	prometheus.MustRegister(storesSpeedGauge)
	prometheus.MustRegister(storesETAGauge)
}
