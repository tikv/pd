// Copyright 2016 TiKV Project Authors.
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

package tso

import "github.com/prometheus/client_golang/prometheus"

const (
	dcLabel   = "dc"
	typeLabel = "type"
)

var (
	tsoCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "tso",
			Name:      "events",
			Help:      "Counter of tso events",
		}, []string{typeLabel, dcLabel})

	tsoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "tso",
			Help:      "Record of tso metadata.",
		}, []string{typeLabel, dcLabel})

	tsoGap = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "tso_gap_millionseconds",
			Help:      "The minimal (non-zero) TSO gap for each DC.",
		}, []string{dcLabel})

	tsoAllocatorRole = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "tso",
			Name:      "role",
			Help:      "Indicate the PD server role info, whether it's a TSO allocator.",
		}, []string{dcLabel})
)

func init() {
	prometheus.MustRegister(tsoCounter)
	prometheus.MustRegister(tsoGauge)
	prometheus.MustRegister(tsoGap)
	prometheus.MustRegister(tsoAllocatorRole)
}
