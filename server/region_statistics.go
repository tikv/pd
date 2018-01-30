// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

type regionStatistics struct {
	opt          *scheduleOption
	classifier   namespace.Classifier
	missPeers    map[string]map[uint64]*core.RegionInfo
	morePeers    map[string]map[uint64]*core.RegionInfo
	downPeers    map[string]map[uint64]*core.RegionInfo
	pendingPeers map[string]map[uint64]*core.RegionInfo
}

func newRegionStatistics(opt *scheduleOption, classifier namespace.Classifier) *regionStatistics {
	return &regionStatistics{
		opt:          opt,
		classifier:   classifier,
		missPeers:    make(map[string]map[uint64]*core.RegionInfo),
		morePeers:    make(map[string]map[uint64]*core.RegionInfo),
		downPeers:    make(map[string]map[uint64]*core.RegionInfo),
		pendingPeers: make(map[string]map[uint64]*core.RegionInfo),
	}
}

func putRegion(stats map[string]map[uint64]*core.RegionInfo, namespace string, region *core.RegionInfo) {
	regionID := region.GetId()
	if m, ok := stats[namespace]; !ok {
		mm := make(map[uint64]*core.RegionInfo)
		stats[namespace] = mm
		mm[regionID] = region
	} else {
		m[regionID] = region
	}

}

func putMetrics(stats map[string]map[string]float64, namespace, key string, value float64) {
	if m, ok := stats[namespace]; !ok {
		mm := make(map[string]float64)
		stats[namespace] = mm
		mm[key] = value
	} else {
		m[key] = value
	}
}

func (r *regionStatistics) Observe(region *core.RegionInfo) {
	// Region state.
	namespace := r.classifier.GetRegionNamespace(region)
	if len(region.Peers) < r.opt.GetMaxReplicas(namespace) {
		putRegion(r.missPeers, namespace, region)
	} else if len(region.Peers) > r.opt.GetMaxReplicas(namespace) {
		putRegion(r.morePeers, namespace, region)
	}

	if len(region.DownPeers) > 0 {
		putRegion(r.downPeers, namespace, region)
	}
	if len(region.PendingPeers) > 0 {
		putRegion(r.pendingPeers, namespace, region)
	}
}

func (r *regionStatistics) Collect() {
	metrics := make(map[string]map[string]float64)
	for namespace := range r.missPeers {
		putMetrics(metrics, namespace, "miss_peer_region_count", float64(len(r.missPeers[namespace])))

	}
	for namespace := range r.morePeers {
		putMetrics(metrics, namespace, "more_peer_region_count", float64(len(r.morePeers[namespace])))
	}
	for namespace := range r.downPeers {
		putMetrics(metrics, namespace, "down_peer_region_count", float64(len(r.downPeers[namespace])))
	}
	for namespace := range r.pendingPeers {
		putMetrics(metrics, namespace, "pending_peer_region_count", float64(len(r.pendingPeers[namespace])))
	}
	for namespace, m := range metrics {
		for label, value := range m {
			clusterStatusGauge.WithLabelValues(label, namespace).Set(value)
		}
	}
	r.missPeers = make(map[string]map[uint64]*core.RegionInfo)
	r.morePeers = make(map[string]map[uint64]*core.RegionInfo)
	r.downPeers = make(map[string]map[uint64]*core.RegionInfo)
	r.pendingPeers = make(map[string]map[uint64]*core.RegionInfo)
}
