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
			Buckets:   []float64{.005, .01, .05, .1, .5, 1, 5, 10, 20, 25, 30}, // 0.005 ~ 30
			Help:      "Bucketed histogram of wait duration of successful request.",
		}, []string{resourceGroupNameLabel})

	failedRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: requestSubsystem,
			Name:      "fail",
			Help:      "Counter of failed request.",
		}, []string{resourceGroupNameLabel})

	requestRetryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: requestSubsystem,
			Name:      "retry",
			Help:      "Counter of retry time for request.",
		}, []string{resourceGroupNameLabel})

	tokenRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: tokenRequestSubsystem,
			Name:      "duration",
			Help:      "Bucketed histogram of latency(s) of token request.",
		}, []string{"type"})

	resourceGroupTokenRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: tokenRequestSubsystem,
			Name:      "resource_group",
			Help:      "Counter of token request by every resource group.",
		}, []string{resourceGroupNameLabel})
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	failedTokenRequestDuration     = tokenRequestDuration.WithLabelValues("fail")
	successfulTokenRequestDuration = tokenRequestDuration.WithLabelValues("success")
)

func init() {
	prometheus.MustRegister(resourceGroupStatusGauge)
	prometheus.MustRegister(successfulRequestDuration)
	prometheus.MustRegister(failedRequestCounter)
	prometheus.MustRegister(requestRetryCounter)
	prometheus.MustRegister(tokenRequestDuration)
	prometheus.MustRegister(resourceGroupTokenRequestCounter)
}
