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
	"fmt"
	"strings"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

type regionStatisticType uint32

const (
	missPeer regionStatisticType = 1 << iota
	morePeer
	downPeer
	pendingPeer
	incorrectNamespace
)

type regionStatistics struct {
	opt        *scheduleOption
	classifier namespace.Classifier
	stats      map[regionStatisticType]map[string]*core.RegionInfo
	index      map[uint64]regionStatisticType
}

func newRegionStatistics(opt *scheduleOption, classifier namespace.Classifier) *regionStatistics {
	r := &regionStatistics{
		opt:        opt,
		classifier: classifier,
		stats:      make(map[regionStatisticType]map[string]*core.RegionInfo),
		index:      make(map[uint64]regionStatisticType),
	}
	r.stats[missPeer] = make(map[string]*core.RegionInfo)
	r.stats[morePeer] = make(map[string]*core.RegionInfo)
	r.stats[downPeer] = make(map[string]*core.RegionInfo)
	r.stats[pendingPeer] = make(map[string]*core.RegionInfo)
	r.stats[incorrectNamespace] = make(map[string]*core.RegionInfo)
	return r
}

func (r *regionStatistics) putRegion(typ regionStatisticType, namespace string, region *core.RegionInfo) {
	regionID := region.GetId()
	key := fmt.Sprintf("%s-%d", namespace, regionID)
	r.stats[typ][key] = region
}

func addMetrics(stats map[string]map[string]float64, key, typ string) {
	k := strings.Split(key, "-")
	namespace := k[0]
	if m, ok := stats[namespace]; !ok {
		mm := make(map[string]float64)
		stats[namespace] = mm
		mm[typ] = 1
	} else {
		m[typ]++
	}
}

func (r *regionStatistics) deleteEntry(deleteIndex regionStatisticType, namespace string, regionID uint64) {
	index := regionStatisticType(1)
	key := fmt.Sprintf("%s-%d", namespace, regionID)
	for deleteIndex > 0 {
		needDelete := deleteIndex & 1
		if needDelete == 1 {
			delete(r.stats[index], key)
		}
		deleteIndex = deleteIndex >> 1
		index = index << 1
	}
}

func (r *regionStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo) {
	// Region state.
	namespace := r.classifier.GetRegionNamespace(region)
	var (
		peerTypeIndex regionStatisticType
		deleteIndex   regionStatisticType
	)
	if len(region.Peers) < r.opt.GetMaxReplicas(namespace) {
		r.putRegion(missPeer, namespace, region)
		peerTypeIndex |= missPeer
	} else if len(region.Peers) > r.opt.GetMaxReplicas(namespace) {
		r.putRegion(morePeer, namespace, region)
		peerTypeIndex |= morePeer
	}
	if len(region.DownPeers) > 0 {
		r.putRegion(downPeer, namespace, region)
		peerTypeIndex |= downPeer
	}
	if len(region.PendingPeers) > 0 {
		r.putRegion(pendingPeer, namespace, region)
		peerTypeIndex |= pendingPeer
	}
	for _, store := range stores {
		ns := r.classifier.GetStoreNamespace(store)
		if ns == namespace {
			continue
		}
		r.putRegion(incorrectNamespace, namespace, region)
		peerTypeIndex |= incorrectNamespace
		break
	}

	if oldIndex, ok := r.index[region.GetId()]; ok {
		deleteIndex = oldIndex &^ peerTypeIndex
	}
	r.deleteEntry(deleteIndex, namespace, region.GetId())
	r.index[region.GetId()] = peerTypeIndex
}

func (r *regionStatistics) Collect() {
	metrics := make(map[string]map[string]float64)
	for key := range r.stats[missPeer] {
		addMetrics(metrics, key, "miss_peer_region_count")
	}
	for key := range r.stats[morePeer] {
		addMetrics(metrics, key, "more_peer_region_count")
	}
	for key := range r.stats[downPeer] {
		addMetrics(metrics, key, "down_peer_region_count")
	}
	for key := range r.stats[pendingPeer] {
		addMetrics(metrics, key, "pending_peer_region_count")
	}
	for key := range r.stats[incorrectNamespace] {
		addMetrics(metrics, key, "incorrect_namespace_region_count")
	}
	for namespace, m := range metrics {
		for label, value := range m {
			clusterStatusGauge.WithLabelValues(label, namespace).Set(value)
		}
	}
}
