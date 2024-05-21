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
