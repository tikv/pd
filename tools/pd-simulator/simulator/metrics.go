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

	snapRecvCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "snap_recv_count",
			Help:      "Counter of receiving region snapshot",
		}, []string{"store"})

	snapSendCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "snap_send_count",
			Help:      "Counter of sending region snapshot",
		}, []string{"store"})
)

func init() {
	prometheus.MustRegister(snapDuration)
	prometheus.MustRegister(schedulingCounter)
	prometheus.MustRegister(snapRecvCounter)
	prometheus.MustRegister(snapSendCounter)
}
