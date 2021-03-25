package api

import "github.com/prometheus/client_golang/prometheus"

var (
	storeProgressGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_off_progress",
			Help:      "process after store up or down ",
		}, []string{"address", "store", "type"})
)

func init() {
	prometheus.MustRegister(storeProgressGauge)
}
