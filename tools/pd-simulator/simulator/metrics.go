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
)

func init() {
	prometheus.MustRegister(snapDuration)
}
