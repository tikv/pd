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

package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace             = "resource_manager_client"
	requestSubsystem      = "request"
	tokenRequestSubsystem = "token_request"

	// TODO: remove old label in 8.x
	resourceGroupNameLabel    = "name"
	newResourceGroupNameLabel = "resource_group"

	errType = "type"
)

var (
	// ResourceGroupStatusGauge comments placeholder
	ResourceGroupStatusGauge *prometheus.GaugeVec
	// SuccessfulRequestDuration comments placeholder
	SuccessfulRequestDuration *prometheus.HistogramVec
	// FailedLimitReserveDuration comments placeholder
	FailedLimitReserveDuration *prometheus.HistogramVec
	// FailedRequestCounter comments placeholder
	FailedRequestCounter *prometheus.CounterVec
	// GroupRunningKVRequestCounter comments placeholder
	GroupRunningKVRequestCounter *prometheus.GaugeVec
	// RequestRetryCounter comments placeholder
	RequestRetryCounter *prometheus.CounterVec
	// TokenRequestDuration comments placeholder
	TokenRequestDuration *prometheus.HistogramVec
	// ResourceGroupTokenRequestCounter comments placeholder
	ResourceGroupTokenRequestCounter *prometheus.CounterVec
	// LowTokenRequestNotifyCounter comments placeholder
	LowTokenRequestNotifyCounter *prometheus.CounterVec
	// TokenConsumedHistogram comments placeholder
	TokenConsumedHistogram *prometheus.HistogramVec
	// FailedTokenRequestDuration comments placeholder, WithLabelValues is a heavy operation, define variable to avoid call it every time.
	FailedTokenRequestDuration prometheus.Observer
	// SuccessfulTokenRequestDuration comments placeholder, WithLabelValues is a heavy operation, define variable to avoid call it every time.
	SuccessfulTokenRequestDuration prometheus.Observer
)

func init() {
	initMetrics(nil)
}

func initMetrics(constLabels prometheus.Labels) {
	ResourceGroupStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "resource_group",
			Name:        "status",
			Help:        "Status of the resource group.",
			ConstLabels: constLabels,
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	SuccessfulRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   requestSubsystem,
			Name:        "success",
			Buckets:     []float64{0.0005, .005, .01, .05, .1, .5, 1, 5, 10, 20, 25, 30, 60, 600, 1800, 3600}, // 0.0005 ~ 1h
			Help:        "Bucketed histogram of wait duration of successful request.",
			ConstLabels: constLabels,
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	FailedLimitReserveDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   requestSubsystem,
			Name:        "limit_reserve_time_failed",
			Buckets:     []float64{0.0005, .01, .05, .1, .5, 1, 5, 10, 20, 25, 30, 60, 600, 1800, 3600, 86400}, // 0.0005 ~ 24h
			Help:        "Bucketed histogram of wait duration of failed request.",
			ConstLabels: constLabels,
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	FailedRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   requestSubsystem,
			Name:        "fail",
			Help:        "Counter of failed request.",
			ConstLabels: constLabels,
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel, errType})

	GroupRunningKVRequestCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   requestSubsystem,
			Name:        "running_kv_request",
			Help:        "Counter of running kv request.",
			ConstLabels: constLabels,
		}, []string{newResourceGroupNameLabel})

	RequestRetryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   requestSubsystem,
			Name:        "retry",
			Help:        "Counter of retry time for request.",
			ConstLabels: constLabels,
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	TokenRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   tokenRequestSubsystem,
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 13), // 1ms ~ 8s
			Name:        "duration",
			Help:        "Bucketed histogram of latency(s) of token request.",
			ConstLabels: constLabels,
		}, []string{"type"})

	ResourceGroupTokenRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   tokenRequestSubsystem,
			Name:        "resource_group",
			Help:        "Counter of token request by every resource group.",
			ConstLabels: constLabels,
		}, []string{resourceGroupNameLabel, newResourceGroupNameLabel})

	LowTokenRequestNotifyCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   tokenRequestSubsystem,
			Name:        "low_token_notified",
			Help:        "Counter of low token request.",
			ConstLabels: constLabels,
		}, []string{newResourceGroupNameLabel})
	TokenConsumedHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   tokenRequestSubsystem,
			Name:        "consume",
			Buckets:     []float64{.5, 1, 2, 5, 10, 15, 20, 40, 64, 128, 256, 512, 1024, 2048}, // 0 ~ 2048
			Help:        "Bucketed histogram of token consume.",
			ConstLabels: constLabels,
		}, []string{newResourceGroupNameLabel})

	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	FailedTokenRequestDuration = TokenRequestDuration.WithLabelValues("fail")
	SuccessfulTokenRequestDuration = TokenRequestDuration.WithLabelValues("success")
}

// InitAndRegisterMetrics initializes and register metrics.
func InitAndRegisterMetrics(constLabels prometheus.Labels) {
	initMetrics(constLabels)
	prometheus.MustRegister(ResourceGroupStatusGauge)
	prometheus.MustRegister(SuccessfulRequestDuration)
	prometheus.MustRegister(FailedRequestCounter)
	prometheus.MustRegister(FailedLimitReserveDuration)
	prometheus.MustRegister(GroupRunningKVRequestCounter)
	prometheus.MustRegister(RequestRetryCounter)
	prometheus.MustRegister(TokenRequestDuration)
	prometheus.MustRegister(ResourceGroupTokenRequestCounter)
	prometheus.MustRegister(LowTokenRequestNotifyCounter)
	prometheus.MustRegister(TokenConsumedHistogram)
}
