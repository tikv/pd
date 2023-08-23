// Copyright 2017 TiKV Project Authors.
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

package schedulers

import "github.com/prometheus/client_golang/prometheus"

var (
	schedulerStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "status",
			Help:      "Status of the scheduler.",
		}, []string{"kind", "type"})

	schedulerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "event_count",
			Help:      "Counter of scheduler events.",
		}, []string{"type", "name"})

	// TODO: pre-allocate gauge metrics
	opInfluenceStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "op_influence",
			Help:      "Store status for schedule",
		}, []string{"scheduler", "store", "type"})

	// TODO: pre-allocate gauge metrics
	tolerantResourceStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "tolerant_resource",
			Help:      "Store status for schedule",
		}, []string{"scheduler"})

	balanceWitnessCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "balance_witness",
			Help:      "Counter of balance witness scheduler.",
		}, []string{"type", "store"})

	// TODO: pre-allocate gauge metrics
	hotSchedulerResultCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_region",
			Help:      "Counter of hot region scheduler.",
		}, []string{"type", "store"})

	balanceDirectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "balance_direction",
			Help:      "Counter of direction of balance related schedulers.",
		}, []string{"type", "source", "target"})

	// TODO: pre-allocate gauge metrics
	hotDirectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_region_direction",
			Help:      "Counter of hot region scheduler.",
		}, []string{"type", "rw", "store", "direction", "dim"})

	hotPendingStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_pending",
			Help:      "Pending influence status in hot region scheduler.",
		}, []string{"type", "source", "target"})

	hotPeerHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_peer",
			Help:      "Bucketed histogram of the scheduling hot peer.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30),
		}, []string{"type", "rw", "dim"})

	storeSlowTrendEvictedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_slow_trend_evicted_status",
			Help:      "Store evited by slow trend status for schedule",
		}, []string{"address", "store"})

	storeSlowTrendActionStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_slow_trend_action_status",
			Help:      "Store trend scheduler calculating actions",
		}, []string{"reason"})

	storeSlowTrendMiscGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_slow_trend_misc",
			Help:      "Store trend internal uncatalogued values",
		}, []string{"type"})

	// HotPendingSum is the sum of pending influence in hot region scheduler.
	HotPendingSum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_pending_sum",
			Help:      "Pending influence sum of store in hot region scheduler.",
		}, []string{"store", "rw", "dim"})
)

func init() {
	prometheus.MustRegister(schedulerStatusGauge)
	prometheus.MustRegister(schedulerCounter)
	prometheus.MustRegister(balanceWitnessCounter)
	prometheus.MustRegister(hotSchedulerResultCounter)
	prometheus.MustRegister(hotDirectionCounter)
	prometheus.MustRegister(balanceDirectionCounter)
	prometheus.MustRegister(opInfluenceStatus)
	prometheus.MustRegister(tolerantResourceStatus)
	prometheus.MustRegister(hotPendingStatus)
	prometheus.MustRegister(hotPeerHist)
	prometheus.MustRegister(storeSlowTrendEvictedStatusGauge)
	prometheus.MustRegister(storeSlowTrendActionStatusGauge)
	prometheus.MustRegister(storeSlowTrendMiscGauge)
	prometheus.MustRegister(HotPendingSum)
}
