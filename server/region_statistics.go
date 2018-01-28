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

func (r *regionStatistics) Observe(region *core.RegionInfo) {
	// Region state.
	regionID := region.GetId()
	namespace := r.classifier.GetRegionNamespace(region)
	if len(region.Peers) < r.opt.GetMaxReplicas(namespace) {
		r.missPeers[namespace][regionID] = region
	} else if len(region.Peers) > r.opt.GetMaxReplicas(namespace) {
		r.morePeers[namespace][regionID] = region
	}

	if len(region.DownPeers) > 0 {
		r.downPeers[namespace][regionID] = region
	}
	if len(region.PendingPeers) > 0 {
		r.pendingPeers[namespace][regionID] = region
	}
}

func (r *regionStatistics) Collect() {
	metrics := make(map[string]map[string]float64)
	for namespace := range r.missPeers {
		metrics[namespace]["miss_peer_region_count"] = float64(len(r.missPeers[namespace]))
	}
	for namespace := range r.morePeers {
		metrics[namespace]["more_peer_region_count"] = float64(len(r.morePeers[namespace]))
	}
	for namespace := range r.downPeers {
		metrics[namespace]["down_peer_region_count"] = float64(len(r.downPeers[namespace]))
	}
	for namespace := range r.pendingPeers {
		metrics[namespace]["pending_peer_region_count"] = float64(len(r.pendingPeers[namespace]))
	}
	for namespace, m := range metrics {
		for label, value := range m {
			clusterStatusGauge.WithLabelValues(label, namespace).Set(value)
		}
	}
}
