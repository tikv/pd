// Copyright 2019 TiKV Project Authors.
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

package statistics

import "github.com/prometheus/client_golang/prometheus"

var (
	hotCacheStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "hotcache",
			Name:      "status",
			Help:      "Status of the hotspot.",
		}, []string{"name", "store", "type"})

	storeStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_status",
			Help:      "Store status for schedule",
		}, []string{"address", "store", "type"})

	regionStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "regions",
			Name:      "status",
			Help:      "Status of the regions.",
		}, []string{"type"})

	clusterStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "status",
			Help:      "Status of the cluster.",
		}, []string{"type"})

	placementStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "placement_status",
			Help:      "Status of the cluster placement.",
		}, []string{"type", "name", "store"})

	configStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "config",
			Name:      "status",
			Help:      "Status of the scheduling configurations.",
		}, []string{"type"})

	// StoreLimitGauge is used to record the current store limit.
	StoreLimitGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "store_limit",
			Help:      "Status of the store limit.",
		}, []string{"store", "type"})

	regionLabelLevelGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "regions",
			Name:      "label_level",
			Help:      "Number of regions in the different label level.",
		}, []string{"type"})
	readByteHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "read_byte_hist",
			Help:      "The distribution of region read bytes",
			Buckets:   prometheus.ExponentialBuckets(1, 8, 12),
		})
	writeByteHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "write_byte_hist",
			Help:      "The distribution of region write bytes",
			Buckets:   prometheus.ExponentialBuckets(1, 8, 12),
		})
	readKeyHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "read_key_hist",
			Help:      "The distribution of region read keys",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})
	writeKeyHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "write_key_hist",
			Help:      "The distribution of region write keys",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})
	readQueryHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "read_query_hist",
			Help:      "The distribution of region read query",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
		})
	writeQueryHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "write_query_hist",
			Help:      "The distribution of region write query",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
		})
	regionHeartbeatIntervalHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "region_heartbeat_interval_hist",
			Help:      "Bucketed histogram of the batch size of handled requests.",
			Buckets:   prometheus.LinearBuckets(0, 30, 20),
		})
	storeHeartbeatIntervalHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_heartbeat_interval_hist",
			Help:      "Bucketed histogram of the batch size of handled requests.",
			Buckets:   prometheus.LinearBuckets(0, 5, 12),
		})

	regionAbnormalPeerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "regions",
			Name:      "abnormal_peer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled success cmds.",
			Buckets:   prometheus.ExponentialBuckets(1, 1.4, 30), // 1s ~ 6.72 hours
		}, []string{"type"})

	hotCacheFlowQueueStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "hotcache",
			Name:      "flow_queue_status",
			Help:      "Status of the hotspot flow queue.",
		}, []string{"type"})

	hotPeerSummary = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_peers_summary",
			Help:      "Hot peers summary for each store",
		}, []string{"type", "store"})
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	regionMissVoterPeerDuration = regionAbnormalPeerDuration.WithLabelValues("miss-voter-peer")
	regionDownPeerDuration      = regionAbnormalPeerDuration.WithLabelValues("down-peer")
)

func init() {
	prometheus.MustRegister(hotCacheStatusGauge)
	prometheus.MustRegister(storeStatusGauge)
	prometheus.MustRegister(regionStatusGauge)
	prometheus.MustRegister(clusterStatusGauge)
	prometheus.MustRegister(placementStatusGauge)
	prometheus.MustRegister(configStatusGauge)
	prometheus.MustRegister(StoreLimitGauge)
	prometheus.MustRegister(regionLabelLevelGauge)
	prometheus.MustRegister(readByteHist)
	prometheus.MustRegister(readKeyHist)
	prometheus.MustRegister(writeKeyHist)
	prometheus.MustRegister(writeByteHist)
	prometheus.MustRegister(regionHeartbeatIntervalHist)
	prometheus.MustRegister(storeHeartbeatIntervalHist)
	prometheus.MustRegister(regionAbnormalPeerDuration)
	prometheus.MustRegister(hotCacheFlowQueueStatusGauge)
	prometheus.MustRegister(hotPeerSummary)
}
