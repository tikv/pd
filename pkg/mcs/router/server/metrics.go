package server

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace       = "router"
	serverSubsystem = "server"
)

var (
	queryRegionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "query_region_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of region query requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	regionRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverSubsystem,
			Name:      "region_request_cnt",
			Help:      "Counter of region request.",
		}, []string{"request", "caller_id", "caller_component", "event"})
)

func init() {
	prometheus.MustRegister(regionRequestCounter)
	prometheus.MustRegister(queryRegionDuration)
}
