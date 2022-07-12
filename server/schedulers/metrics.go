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

var schedulerCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "event_count",
		Help:      "Counter of scheduler events.",
	}, []string{"type", "name"})

var schedulerStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "inner_status",
		Help:      "Inner status of the scheduler.",
	}, []string{"type", "name"})

var opInfluenceStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "op_influence",
		Help:      "Store status for schedule",
	}, []string{"scheduler", "store", "type"})

var tolerantResourceStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "tolerant_resource",
		Help:      "Store status for schedule",
	}, []string{"scheduler", "source", "target"})

var balanceLeaderCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "balance_leader",
		Help:      "Counter of balance leader scheduler.",
	}, []string{"type", "store"})

var balanceWitnessCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "balance_witness",
		Help:      "Counter of balance witness scheduler.",
	}, []string{"type", "store"})

var balanceRegionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "balance_region",
		Help:      "Counter of balance region scheduler.",
	}, []string{"type", "store"})

var hotSchedulerResultCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "hot_region",
		Help:      "Counter of hot region scheduler.",
	}, []string{"type", "store"})

var balanceDirectionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "balance_direction",
		Help:      "Counter of direction of balance related schedulers.",
	}, []string{"type", "source", "target"})

var hotDirectionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "hot_region_direction",
		Help:      "Counter of hot region scheduler.",
	}, []string{"type", "rw", "store", "direction", "dim"})

var scatterRangeLeaderCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "scatter_range_leader",
		Help:      "Counter of scatter range leader scheduler.",
	}, []string{"type", "store"})

var scatterRangeRegionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "scatter_range_region",
		Help:      "Counter of scatter range region scheduler.",
	}, []string{"type", "store"})

var hotPendingStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "hot_pending",
		Help:      "Counter of direction of balance related schedulers.",
	}, []string{"type", "source", "target"})

func init() {
	prometheus.MustRegister(schedulerCounter)
	prometheus.MustRegister(schedulerStatus)
	prometheus.MustRegister(balanceLeaderCounter)
	prometheus.MustRegister(balanceRegionCounter)
	prometheus.MustRegister(hotSchedulerResultCounter)
	prometheus.MustRegister(hotDirectionCounter)
	prometheus.MustRegister(balanceDirectionCounter)
	prometheus.MustRegister(scatterRangeLeaderCounter)
	prometheus.MustRegister(scatterRangeRegionCounter)
	prometheus.MustRegister(opInfluenceStatus)
	prometheus.MustRegister(tolerantResourceStatus)
	prometheus.MustRegister(hotPendingStatus)
}
