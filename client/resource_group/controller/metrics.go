// Copyright 2023 TiKV Project Authors.
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

package controller

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace             = "resource_manager_client"
	requestSubsystem      = "request"
	ruSubsystem           = "resource_unit"
	tokenRequestSubsystem = "token_request"

	resourceGroupNameLabel = "name"
)

var (
	resourceGroupStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "resource_group",
			Name:      "status",
			Help:      "Status of the resource group.",
		}, []string{resourceGroupNameLabel})

	successfulRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: requestSubsystem,
			Name:      "success",
			Buckets:   prometheus.ExponentialBuckets(0.001, 4, 8), // 0.001 ~ 40.96
			Help:      "",
		}, []string{resourceGroupNameLabel})

	failedRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: requestSubsystem,
			Name:      "fail",
			Help:      "",
		}, []string{resourceGroupNameLabel})

	readRequestUnitCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "read_request_unit",
			Help:      "Bucketed histogram of the read request unit cost for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(1, 10, 5), // 1 ~ 100000
		}, []string{resourceGroupNameLabel})

	writeRequestUnitCost = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: ruSubsystem,
			Name:      "write_request_unit",
			Help:      "Bucketed histogram of the write request unit cost for all resource groups.",
			Buckets:   prometheus.ExponentialBuckets(3, 10, 5), // 3 ~ 300000
		}, []string{resourceGroupNameLabel})

	successfulTokenRequestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: tokenRequestSubsystem,
			Name:      "success",
			Help:      "",
		})

	failedTokenRequestCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tokenRequestSubsystem,
			Name:      "fail",
			Help:      "",
		})

	resourceGroupTokenRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tokenRequestSubsystem,
			Name:      "resource_group",
			Help:      "",
		}, []string{resourceGroupNameLabel})
)

func init() {
	prometheus.MustRegister(resourceGroupStatusGauge)
	prometheus.MustRegister(successfulRequestDuration)
	prometheus.MustRegister(failedRequestCounter)
	prometheus.MustRegister(readRequestUnitCost)
	prometheus.MustRegister(writeRequestUnitCost)
	prometheus.MustRegister(successfulTokenRequestDuration)
	prometheus.MustRegister(failedTokenRequestCounter)
	prometheus.MustRegister(resourceGroupTokenRequestCounter)
}
