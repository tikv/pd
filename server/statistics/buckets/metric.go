// Copyright 2022 TiKV Project Authors.
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

package buckets

import "github.com/prometheus/client_golang/prometheus"

var (
	bucketsHeartbeatIntervalHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "buckets_heartbeat_interval_hist",
			Help:      "Bucketed histogram of the batch size of handled requests.",
			Buckets:   prometheus.LinearBuckets(0, 30, 20),
		})
	bucketsHotDegreeHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "buckets_hot_degree_hist",
			Help:      "The distribution of bucket flow bytes",
			Buckets:   prometheus.LinearBuckets(-100, 10, 20),
		})
)

func init() {
	prometheus.MustRegister(bucketsHeartbeatIntervalHist)
	prometheus.MustRegister(bucketsHotDegreeHist)
}
