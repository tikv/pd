// Copyright 2018 TiKV Project Authors.
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

import (
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
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
	LearnerPeer
	EmptyRegion
)

const nonIsolation = "none"

// RegionInfo is used to record the status of region.
type RegionInfo struct {
	*core.RegionInfo
	startMissVoterPeerTS int64
	startDownPeerTS      int64
}

// RegionStatistics is used to record the status of regions.
type RegionStatistics struct {
	opt          *config.PersistOptions
	stats        map[RegionStatisticType]map[uint64]*RegionInfo
	offlineStats map[RegionStatisticType]map[uint64]*core.RegionInfo
	index        map[uint64]RegionStatisticType
	offlineIndex map[uint64]RegionStatisticType
	ruleManager  *placement.RuleManager
}

// NewRegionStatistics creates a new RegionStatistics.
func NewRegionStatistics(opt *config.PersistOptions, ruleManager *placement.RuleManager) *RegionStatistics {
	r := &RegionStatistics{
		opt:          opt,
		stats:        make(map[RegionStatisticType]map[uint64]*RegionInfo),
		offlineStats: make(map[RegionStatisticType]map[uint64]*core.RegionInfo),
		index:        make(map[uint64]RegionStatisticType),
		offlineIndex: make(map[uint64]RegionStatisticType),
	}
	r.stats[MissPeer] = make(map[uint64]*RegionInfo)
	r.stats[ExtraPeer] = make(map[uint64]*RegionInfo)
	r.stats[DownPeer] = make(map[uint64]*RegionInfo)
	r.stats[PendingPeer] = make(map[uint64]*RegionInfo)
	r.stats[LearnerPeer] = make(map[uint64]*RegionInfo)
	r.stats[EmptyRegion] = make(map[uint64]*RegionInfo)

	r.offlineStats[MissPeer] = make(map[uint64]*core.RegionInfo)
	r.offlineStats[ExtraPeer] = make(map[uint64]*core.RegionInfo)
	r.offlineStats[DownPeer] = make(map[uint64]*core.RegionInfo)
	r.offlineStats[PendingPeer] = make(map[uint64]*core.RegionInfo)
	r.offlineStats[LearnerPeer] = make(map[uint64]*core.RegionInfo)
	r.offlineStats[EmptyRegion] = make(map[uint64]*core.RegionInfo)
	r.offlineStats[OfflinePeer] = make(map[uint64]*core.RegionInfo)
	r.ruleManager = ruleManager
	return r
}

// GetRegionStatsByType gets the status of the region by types.
func (r *RegionStatistics) GetRegionStatsByType(typ RegionStatisticType) []*core.RegionInfo {
	res := make([]*core.RegionInfo, 0, len(r.stats[typ]))
	for _, r := range r.stats[typ] {
		res = append(res, r.RegionInfo)
	}
	return res
}

// GetOfflineRegionStatsByType gets the status of the offline region by types.
func (r *RegionStatistics) GetOfflineRegionStatsByType(typ RegionStatisticType) []*core.RegionInfo {
	res := make([]*core.RegionInfo, 0, len(r.stats[typ]))
	for _, r := range r.offlineStats[typ] {
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

func (r *RegionStatistics) deleteOfflineEntry(deleteIndex RegionStatisticType, regionID uint64) {
	for typ := RegionStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.offlineStats[typ], regionID)
		}
	}
}

// Observe records the current regions' status.
func (r *RegionStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo) {
	// Region state.
	regionID := region.GetID()
	var (
		peerTypeIndex        RegionStatisticType
		offlinePeerTypeIndex RegionStatisticType
		deleteIndex          RegionStatisticType
	)
	desiredReplicas := r.opt.GetMaxReplicas()
	desiredVoters := desiredReplicas
	if r.opt.IsPlacementRulesEnabled() {
		if !r.ruleManager.IsInitialized() {
			log.Warn("ruleManager haven't been initialized")
			return
		}
		desiredReplicas = 0
		desiredVoters = 0
		rules := r.ruleManager.GetRulesForApplyRegion(region)
		for _, rule := range rules {
			desiredReplicas += rule.Count
			if rule.Role != placement.Learner {
				desiredVoters += rule.Count
			}
		}
	}

	var isOffline bool

	for _, store := range stores {
		if store.IsOffline() {
			peer := region.GetStorePeer(store.GetID())
			if peer != nil {
				isOffline = true
				break
			}
		}
	}

	conditions := map[RegionStatisticType]bool{
		MissPeer:    len(region.GetPeers()) < desiredReplicas,
		ExtraPeer:   len(region.GetPeers()) > desiredReplicas,
		DownPeer:    len(region.GetDownPeers()) > 0,
		PendingPeer: len(region.GetPendingPeers()) > 0,
		LearnerPeer: len(region.GetLearners()) > 0,
		EmptyRegion: region.GetApproximateSize() <= core.EmptyRegionApproximateSize,
	}

	for typ, c := range conditions {
		if c {
			if isOffline {
				r.offlineStats[typ][regionID] = region
				offlinePeerTypeIndex |= typ
			}
			info := r.stats[typ][regionID]
			if info == nil {
				info = &RegionInfo{
					RegionInfo: region,
				}
			}
			if typ == DownPeer {
				if info.startDownPeerTS != 0 {
					regionDownPeerDuration.Observe(float64(time.Now().Unix() - info.startDownPeerTS))
				} else {
					info.startDownPeerTS = time.Now().Unix()
				}
			} else if typ == MissPeer && len(region.GetVoters()) < desiredVoters {
				if info.startMissVoterPeerTS != 0 {
					regionMissVoterPeerDuration.Observe(float64(time.Now().Unix() - info.startMissVoterPeerTS))
				} else {
					info.startMissVoterPeerTS = time.Now().Unix()
				}
			}

			r.stats[typ][regionID] = info
			peerTypeIndex |= typ
		}
	}

	if isOffline {
		r.offlineStats[OfflinePeer][regionID] = region
		offlinePeerTypeIndex |= OfflinePeer
	}

	if oldIndex, ok := r.offlineIndex[regionID]; ok {
		deleteIndex = oldIndex &^ offlinePeerTypeIndex
	}
	r.deleteOfflineEntry(deleteIndex, regionID)
	r.offlineIndex[regionID] = offlinePeerTypeIndex

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
	if oldIndex, ok := r.offlineIndex[regionID]; ok {
		r.deleteOfflineEntry(oldIndex, regionID)
	}
}

// Collect collects the metrics of the regions' status.
func (r *RegionStatistics) Collect() {
	regionStatusGauge.WithLabelValues("miss-peer-region-count").Set(float64(len(r.stats[MissPeer])))
	regionStatusGauge.WithLabelValues("extra-peer-region-count").Set(float64(len(r.stats[ExtraPeer])))
	regionStatusGauge.WithLabelValues("down-peer-region-count").Set(float64(len(r.stats[DownPeer])))
	regionStatusGauge.WithLabelValues("pending-peer-region-count").Set(float64(len(r.stats[PendingPeer])))
	regionStatusGauge.WithLabelValues("learner-peer-region-count").Set(float64(len(r.stats[LearnerPeer])))
	regionStatusGauge.WithLabelValues("empty-region-count").Set(float64(len(r.stats[EmptyRegion])))

	offlineRegionStatusGauge.WithLabelValues("miss-peer-region-count").Set(float64(len(r.offlineStats[MissPeer])))
	offlineRegionStatusGauge.WithLabelValues("extra-peer-region-count").Set(float64(len(r.offlineStats[ExtraPeer])))
	offlineRegionStatusGauge.WithLabelValues("down-peer-region-count").Set(float64(len(r.offlineStats[DownPeer])))
	offlineRegionStatusGauge.WithLabelValues("pending-peer-region-count").Set(float64(len(r.offlineStats[PendingPeer])))
	offlineRegionStatusGauge.WithLabelValues("learner-peer-region-count").Set(float64(len(r.offlineStats[LearnerPeer])))
	offlineRegionStatusGauge.WithLabelValues("empty-region-count").Set(float64(len(r.offlineStats[EmptyRegion])))
	offlineRegionStatusGauge.WithLabelValues("offline-peer-region-count").Set(float64(len(r.offlineStats[OfflinePeer])))
}

// Reset resets the metrics of the regions' status.
func (r *RegionStatistics) Reset() {
	regionStatusGauge.Reset()
	offlineRegionStatusGauge.Reset()
}

// LabelStatistics is the statistics of the level of labels.
type LabelStatistics struct {
	regionLabelStats map[uint64]string
	labelCounter     map[string]int
}

// NewLabelStatistics creates a new LabelStatistics.
func NewLabelStatistics() *LabelStatistics {
	return &LabelStatistics{
		regionLabelStats: make(map[uint64]string),
		labelCounter:     make(map[string]int),
	}
}

// Observe records the current label status.
func (l *LabelStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo, labels []string) {
	regionID := region.GetID()
	regionIsolation := getRegionLabelIsolation(stores, labels)
	if label, ok := l.regionLabelStats[regionID]; ok {
		if label == regionIsolation {
			return
		}
		l.labelCounter[label]--
	}
	l.regionLabelStats[regionID] = regionIsolation
	l.labelCounter[regionIsolation]++
}

// Collect collects the metrics of the label status.
func (l *LabelStatistics) Collect() {
	for level, count := range l.labelCounter {
		regionLabelLevelGauge.WithLabelValues(level).Set(float64(count))
	}
}

// Reset resets the metrics of the label status.
func (l *LabelStatistics) Reset() {
	regionLabelLevelGauge.Reset()
}

// ClearDefunctRegion is used to handle the overlap region.
func (l *LabelStatistics) ClearDefunctRegion(regionID uint64) {
	if label, ok := l.regionLabelStats[regionID]; ok {
		l.labelCounter[label]--
		delete(l.regionLabelStats, regionID)
	}
}

// GetLabelCounter is only used for tests.
func (l *LabelStatistics) GetLabelCounter() map[string]int {
	return l.labelCounter
}

func getRegionLabelIsolation(stores []*core.StoreInfo, labels []string) string {
	if len(stores) == 0 || len(labels) == 0 {
		return nonIsolation
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
			return labels[level]
		}
	}
	return nonIsolation
}

func notIsolatedStoresWithLabel(stores []*core.StoreInfo, label string) [][]*core.StoreInfo {
	var emptyValueStores []*core.StoreInfo
	valueStoresMap := make(map[string][]*core.StoreInfo)

	for _, s := range stores {
		labelValue := s.GetLabelValue(label)
		if labelValue == "" {
			emptyValueStores = append(emptyValueStores, s)
		} else {
			valueStoresMap[labelValue] = append(valueStoresMap[labelValue], s)
		}
	}

	if len(valueStoresMap) == 0 {
		// Usually it is because all TiKVs lack this label.
		if len(emptyValueStores) > 1 {
			return [][]*core.StoreInfo{emptyValueStores}
		}
		return nil
	}

	var res [][]*core.StoreInfo
	if len(emptyValueStores) == 0 {
		// No TiKV lacks this label.
		for _, stores := range valueStoresMap {
			if len(stores) > 1 {
				res = append(res, stores)
			}
		}
	} else {
		// Usually it is because some TiKVs lack this label.
		// The TiKVs in each label and the TiKVs without label form a group.
		for _, stores := range valueStoresMap {
			stores = append(stores, emptyValueStores...)
			res = append(res, stores)
		}
	}
	return res
}
