package hbstream

import "github.com/prometheus/client_golang/prometheus"

var (
	heartbeatStreamCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "hbstream",
			Name:      "region_message",
			Help:      "Counter of message hbstream sent.",
		}, []string{"address", "store", "type", "status"})
)

func init() {
	prometheus.MustRegister(heartbeatStreamCounter)
}
