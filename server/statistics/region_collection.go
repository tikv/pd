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

package statistics

import (
	"fmt"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

// RegionStatisticType represents the type of the region's status.
type RegionStatisticType uint32

// region status type
const (
	MissPeer RegionStatisticType = 1 << iota
	ExtraPeer
	DownPeer
	PendingPeer
	OfflinePeer
	IncorrectNamespace
	LearnerPeer
)

// RegionStatistics is used to record the status of regions.
type RegionStatistics struct {
	opt        ScheduleOptions
	classifier namespace.Classifier
	stats      map[RegionStatisticType]map[uint64]*core.RegionInfo
	index      map[uint64]RegionStatisticType
}

// NewRegionStatistics creates a new RegionStatistics.
func NewRegionStatistics(opt ScheduleOptions, classifier namespace.Classifier) *RegionStatistics {
	r := &RegionStatistics{
		opt:        opt,
		classifier: classifier,
		stats:      make(map[RegionStatisticType]map[uint64]*core.RegionInfo),
		index:      make(map[uint64]RegionStatisticType),
	}
	r.stats[MissPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[ExtraPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[DownPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[PendingPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[OfflinePeer] = make(map[uint64]*core.RegionInfo)
	r.stats[IncorrectNamespace] = make(map[uint64]*core.RegionInfo)
	r.stats[LearnerPeer] = make(map[uint64]*core.RegionInfo)
	return r
}

// GetRegionStatsByType gets the status of the region by types.
func (r *RegionStatistics) GetRegionStatsByType(typ RegionStatisticType) []*core.RegionInfo {
	res := make([]*core.RegionInfo, 0, len(r.stats[typ]))
	for _, r := range r.stats[typ] {
		res = append(res, r)
	}
	return res
}

func (r *RegionStatistics) deleteEntry(deleteIndex RegionStatisticType, regionID uint64) {
	for typ := RegionStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.stats[typ], regionID)
		}
	}
}

// Observe records the current regions' status.
func (r *RegionStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo) {
	// Region state.
	regionID := region.GetID()
	namespace := r.classifier.GetRegionNamespace(region)
	var (
		peerTypeIndex RegionStatisticType
		deleteIndex   RegionStatisticType
	)
	if len(region.GetPeers()) < r.opt.GetMaxReplicas(namespace) {
		r.stats[MissPeer][regionID] = region
		peerTypeIndex |= MissPeer
	} else if len(region.GetPeers()) > r.opt.GetMaxReplicas(namespace) {
		r.stats[ExtraPeer][regionID] = region
		peerTypeIndex |= ExtraPeer
	}

	if len(region.GetDownPeers()) > 0 {
		r.stats[DownPeer][regionID] = region
		peerTypeIndex |= DownPeer
	}

	if len(region.GetPendingPeers()) > 0 {
		r.stats[PendingPeer][regionID] = region
		peerTypeIndex |= PendingPeer
	}

	if len(region.GetLearners()) > 0 {
		r.stats[LearnerPeer][regionID] = region
		peerTypeIndex |= LearnerPeer
	}

	for _, store := range stores {
		if store.IsOffline() {
			peer := region.GetStorePeer(store.GetID())
			if peer != nil {
				r.stats[OfflinePeer][regionID] = region
				peerTypeIndex |= OfflinePeer
			}
		}
		ns := r.classifier.GetStoreNamespace(store)
		if ns == namespace {
			continue
		}
		r.stats[IncorrectNamespace][regionID] = region
		peerTypeIndex |= IncorrectNamespace
		break
	}

	if oldIndex, ok := r.index[regionID]; ok {
		deleteIndex = oldIndex &^ peerTypeIndex
	}
	r.deleteEntry(deleteIndex, regionID)
	r.index[regionID] = peerTypeIndex
}

// ClearDefunctRegion is used to handle the overlap region.
func (r *RegionStatistics) ClearDefunctRegion(regionID uint64) {
	if oldIndex, ok := r.index[regionID]; ok {
		r.deleteEntry(oldIndex, regionID)
	}
}

// Collect collects the metrics of the regions' status.
func (r *RegionStatistics) Collect() {
	regionStatusGauge.WithLabelValues("miss_peer_region_count").Set(float64(len(r.stats[MissPeer])))
	regionStatusGauge.WithLabelValues("extra_peer_region_count").Set(float64(len(r.stats[ExtraPeer])))
	regionStatusGauge.WithLabelValues("down_peer_region_count").Set(float64(len(r.stats[DownPeer])))
	regionStatusGauge.WithLabelValues("pending_peer_region_count").Set(float64(len(r.stats[PendingPeer])))
	regionStatusGauge.WithLabelValues("offline_peer_region_count").Set(float64(len(r.stats[OfflinePeer])))
	regionStatusGauge.WithLabelValues("incorrect_namespace_region_count").Set(float64(len(r.stats[IncorrectNamespace])))
	regionStatusGauge.WithLabelValues("learner_peer_region_count").Set(float64(len(r.stats[LearnerPeer])))
}

// LabelLevelStatistics is the statistics of the level of labels.
type LabelLevelStatistics struct {
	regionLabelLevelStats map[uint64]int
	labelLevelCounter     map[int]int
}

// NewLabelLevelStatistics creates a new LabelLevelStatistics.
func NewLabelLevelStatistics() *LabelLevelStatistics {
	return &LabelLevelStatistics{
		regionLabelLevelStats: make(map[uint64]int),
		labelLevelCounter:     make(map[int]int),
	}
}

// Observe records the current label status.
func (l *LabelLevelStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo, labels []string) {
	regionID := region.GetID()
	regionLabelLevel := getRegionLabelIsolationLevel(stores, labels)
	if level, ok := l.regionLabelLevelStats[regionID]; ok {
		if level == regionLabelLevel {
			return
		}
		l.labelLevelCounter[level]--
	}
	l.regionLabelLevelStats[regionID] = regionLabelLevel
	l.labelLevelCounter[regionLabelLevel]++
}

// Collect collects the metrics of the label status.
func (l *LabelLevelStatistics) Collect() {
	for level, count := range l.labelLevelCounter {
		typ := fmt.Sprintf("level_%d", level)
		regionLabelLevelGauge.WithLabelValues(typ).Set(float64(count))
	}
}

// ClearDefunctRegion is used to handle the overlap region.
func (l *LabelLevelStatistics) ClearDefunctRegion(regionID uint64) {
	if level, ok := l.regionLabelLevelStats[regionID]; ok {
		l.labelLevelCounter[level]--
		delete(l.regionLabelLevelStats, regionID)
	}
}

func getRegionLabelIsolationLevel(stores []*core.StoreInfo, labels []string) int {
	if len(stores) == 0 || len(labels) == 0 {
		return 0
	}
	queueStores := [][]*core.StoreInfo{stores}
	for level, label := range labels {
		newQueueStores := make([][]*core.StoreInfo, 0, len(stores))
		for _, stores := range queueStores {
			notIsolatedStores := notIsolatedStoresWithLabel(stores, label)
			if len(notIsolatedStores) > 0 {
				newQueueStores = append(newQueueStores, notIsolatedStores...)
			}
		}
		queueStores = newQueueStores
		if len(queueStores) == 0 {
			return level + 1
		}
	}
	return 0
}

func notIsolatedStoresWithLabel(stores []*core.StoreInfo, label string) [][]*core.StoreInfo {
	m := make(map[string][]*core.StoreInfo)
	for _, s := range stores {
		labelValue := s.GetLabelValue(label)
		if labelValue == "" {
			continue
		}
		m[labelValue] = append(m[labelValue], s)
	}
	var res [][]*core.StoreInfo
	for _, stores := range m {
		if len(stores) > 1 {
			res = append(res, stores)
		}
	}
	return res
}
