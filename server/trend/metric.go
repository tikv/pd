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

package trend

import "github.com/prometheus/client_golang/prometheus"

var (
	leaderTrendAvgRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "trend_avg_rate",
			Help:      "The trend rate of PD server healthy.",
		}, []string{"etcd_server", "name"})
)

func init() {
	prometheus.MustRegister(leaderTrendAvgRate)
}
