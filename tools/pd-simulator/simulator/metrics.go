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

package simulator

import "github.com/prometheus/client_golang/prometheus"

var (
	snapDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv",
			Subsystem: "raftstore",
			Name:      "snapshot_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled snap requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"store", "type"})

	schedulingCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "scheduling_count",
			Help:      "Counter of region scheduling",
		}, []string{"type"})

	snapshotCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "snapshot_count",
			Help:      "Counter of region snapshot",
		}, []string{"store", "type"})
)

func init() {
	prometheus.MustRegister(snapDuration)
	prometheus.MustRegister(schedulingCounter)
	prometheus.MustRegister(snapshotCounter)
}
