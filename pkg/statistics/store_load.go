// Copyright 2021 TiKV Project Authors.
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
	"math"
	"time"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/statistics/utils"
)

// StoreLoadDetail records store load information.
type StoreLoadDetail struct {
	*StoreSummaryInfo
	LoadPred *StoreLoadPred
	HotPeers []*HotPeerStat
}

// ToHotPeersStat abstracts load information to HotPeersStat.
func (li *StoreLoadDetail) ToHotPeersStat() *HotPeersStat {
	storeByteRate, storeKeyRate, storeQueryRate := li.LoadPred.Current.Loads[utils.ByteDim],
		li.LoadPred.Current.Loads[utils.KeyDim], li.LoadPred.Current.Loads[utils.QueryDim]
	if len(li.HotPeers) == 0 {
		return &HotPeersStat{
			StoreByteRate:  storeByteRate,
			StoreKeyRate:   storeKeyRate,
			StoreQueryRate: storeQueryRate,
			TotalBytesRate: 0.0,
			TotalKeysRate:  0.0,
			TotalQueryRate: 0.0,
			Count:          0,
			Stats:          make([]HotPeerStatShow, 0),
		}
	}
	var byteRate, keyRate, queryRate float64
	peers := make([]HotPeerStatShow, 0, len(li.HotPeers))
	for _, peer := range li.HotPeers {
		if peer.HotDegree > 0 {
			peers = append(peers, toHotPeerStatShow(peer))
			byteRate += peer.GetLoad(utils.ByteDim)
			keyRate += peer.GetLoad(utils.KeyDim)
			queryRate += peer.GetLoad(utils.QueryDim)
		}
	}

	return &HotPeersStat{
		TotalBytesRate: byteRate,
		TotalKeysRate:  keyRate,
		TotalQueryRate: queryRate,
		StoreByteRate:  storeByteRate,
		StoreKeyRate:   storeKeyRate,
		StoreQueryRate: storeQueryRate,
		Count:          len(peers),
		Stats:          peers,
	}
}

// IsUniform returns true if the stores are uniform.
func (li *StoreLoadDetail) IsUniform(dim int, threshold float64) bool {
	return li.LoadPred.Stddev.Loads[dim] < threshold
}

func toHotPeerStatShow(p *HotPeerStat) HotPeerStatShow {
	byteRate := p.GetLoad(utils.ByteDim)
	keyRate := p.GetLoad(utils.KeyDim)
	queryRate := p.GetLoad(utils.QueryDim)
	return HotPeerStatShow{
		StoreID:   p.StoreID,
		Stores:    p.GetStores(),
		IsLeader:  p.IsLeader(),
		RegionID:  p.RegionID,
		HotDegree: p.HotDegree,
		ByteRate:  byteRate,
		KeyRate:   keyRate,
		QueryRate: queryRate,
		AntiCount: p.AntiCount,
	}
}

// StoreSummaryInfo records the summary information of store.
type StoreSummaryInfo struct {
	*core.StoreInfo
	isTiFlash  bool
	PendingSum *Influence
}

// Influence records operator influence.
type Influence struct {
	Loads        []float64
	HotPeerCount float64
}

// SummaryStoreInfos return a mapping from store to summary information.
func SummaryStoreInfos(stores []*core.StoreInfo) map[uint64]*StoreSummaryInfo {
	infos := make(map[uint64]*StoreSummaryInfo, len(stores))
	for _, store := range stores {
		info := &StoreSummaryInfo{
			StoreInfo:  store,
			isTiFlash:  store.IsTiFlash(),
			PendingSum: nil,
		}
		infos[store.GetID()] = info
	}
	return infos
}

// AddInfluence adds influence to pending sum.
func (s *StoreSummaryInfo) AddInfluence(infl *Influence, w float64) {
	if infl == nil || w == 0 {
		return
	}
	if s.PendingSum == nil {
		s.PendingSum = &Influence{
			Loads:        make([]float64, len(infl.Loads)),
			HotPeerCount: 0,
		}
	}
	for i, load := range infl.Loads {
		s.PendingSum.Loads[i] += load * w
	}
	s.PendingSum.HotPeerCount += infl.HotPeerCount * w
}

// IsTiFlash returns true if the store is TiFlash.
func (s *StoreSummaryInfo) IsTiFlash() bool {
	return s.isTiFlash
}

// SetEngineAsTiFlash set whether store is TiFlash, it is only used in tests.
func (s *StoreSummaryInfo) SetEngineAsTiFlash() {
	s.isTiFlash = true
}

// Loads is a vector that contains different dimensions of loads.
type Loads [utils.DimLen]float64

// HistoryLoads is a circular buffer of loads. The reason for using
// [utils.DimLen][]float64 instead of [][utils.DimLen]float64 is to make it easier
// for the `checkHistoryLoadsByPriority` function to handle.
type HistoryLoads [utils.DimLen][] /* buffer size */ float64

// StoreKindLoads is a vector that contains different dimensions of store loads.
type StoreKindLoads [utils.StoreLoadCount]float64

// RegionKindLoads is a vector that contains different dimensions of region loads.
type RegionKindLoads [utils.RegionStatCount]float64

// StoreLoad records the current load.
type StoreLoad struct {
	Loads        Loads
	HotPeerCount float64
	HistoryLoads HistoryLoads
}

// ToLoadPred returns the current load and future predictive load.
func (load StoreLoad) ToLoadPred(rwTy utils.RWType, infl *Influence) *StoreLoadPred {
	future := StoreLoad{
		Loads:        load.Loads,
		HotPeerCount: load.HotPeerCount,
	}
	if infl != nil {
		switch rwTy {
		case utils.Read:
			future.Loads[utils.ByteDim] += infl.Loads[utils.RegionReadBytes]
			future.Loads[utils.KeyDim] += infl.Loads[utils.RegionReadKeys]
			future.Loads[utils.QueryDim] += infl.Loads[utils.RegionReadQueryNum]
		case utils.Write:
			future.Loads[utils.ByteDim] += infl.Loads[utils.RegionWriteBytes]
			future.Loads[utils.KeyDim] += infl.Loads[utils.RegionWriteKeys]
			future.Loads[utils.QueryDim] += infl.Loads[utils.RegionWriteQueryNum]
		}
		future.HotPeerCount += infl.HotPeerCount
	}
	return &StoreLoadPred{
		Current: load,
		Future:  future,
	}
}

// StoreLoadPred is a prediction of a store.
type StoreLoadPred struct {
	Current StoreLoad
	Future  StoreLoad
	Expect  StoreLoad
	Stddev  StoreLoad
}

// Min returns the min load between current and future.
func (lp *StoreLoadPred) Min() *StoreLoad {
	return MinLoad(&lp.Current, &lp.Future)
}

// Max returns the max load between current and future.
func (lp *StoreLoadPred) Max() *StoreLoad {
	return MaxLoad(&lp.Current, &lp.Future)
}

// Pending returns the pending load.
func (lp *StoreLoadPred) Pending() *StoreLoad {
	mx, mn := lp.Max(), lp.Min()
	var loads Loads
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &StoreLoad{
		Loads:        loads,
		HotPeerCount: 0,
	}
}

// Diff return the difference between min and max.
func (lp *StoreLoadPred) Diff() *StoreLoad {
	mx, mn := lp.Max(), lp.Min()
	var loads Loads
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &StoreLoad{
		Loads:        loads,
		HotPeerCount: mx.HotPeerCount - mn.HotPeerCount,
	}
}

// MinLoad return the min store load.
func MinLoad(a, b *StoreLoad) *StoreLoad {
	var loads Loads
	for i := range loads {
		loads[i] = math.Min(a.Loads[i], b.Loads[i])
	}
	return &StoreLoad{
		Loads:        loads,
		HotPeerCount: math.Min(a.HotPeerCount, b.HotPeerCount),
	}
}

// MaxLoad return the max store load.
func MaxLoad(a, b *StoreLoad) *StoreLoad {
	var loads Loads
	for i := range loads {
		loads[i] = math.Max(a.Loads[i], b.Loads[i])
	}
	return &StoreLoad{
		Loads:        loads,
		HotPeerCount: math.Max(a.HotPeerCount, b.HotPeerCount),
	}
}

const (
	// DefaultHistorySampleInterval is the sampling interval for history load.
	DefaultHistorySampleInterval = 30 * time.Second
	// DefaultHistorySampleDuration  is the duration for saving history load.
	DefaultHistorySampleDuration = 5 * time.Minute
)

// StoreHistoryLoads records the history load of a store.
type StoreHistoryLoads struct {
	// loads[read/write][leader/follower]-->[store id]-->history load
	loads          [utils.RWTypeLen][constant.ResourceKindLen]map[uint64]*storeHistoryLoad
	sampleInterval time.Duration
	sampleDuration time.Duration
}

// NewStoreHistoryLoads creates a StoreHistoryLoads.
func NewStoreHistoryLoads(sampleDuration time.Duration, sampleInterval time.Duration) *StoreHistoryLoads {
	st := StoreHistoryLoads{
		sampleDuration: sampleDuration,
		sampleInterval: sampleInterval,
	}
	for i := utils.RWType(0); i < utils.RWTypeLen; i++ {
		for j := constant.ResourceKind(0); j < constant.ResourceKindLen; j++ {
			st.loads[i][j] = make(map[uint64]*storeHistoryLoad)
		}
	}
	return &st
}

// Add adds the store load to the history.
func (s *StoreHistoryLoads) Add(storeID uint64, rwTp utils.RWType, kind constant.ResourceKind, pointLoad Loads) {
	load, ok := s.loads[rwTp][kind][storeID]
	if !ok {
		size := int(DefaultHistorySampleDuration / DefaultHistorySampleInterval)
		if s.sampleInterval != 0 {
			size = int(s.sampleDuration / s.sampleInterval)
		}
		if s.sampleDuration == 0 {
			size = 0
		}
		load = newStoreHistoryLoad(size, s.sampleInterval)
		s.loads[rwTp][kind][storeID] = load
	}
	load.add(pointLoad)
}

// Get returns the store loads from the history, not one time point.
// In another word, the result is [dim][time].
func (s *StoreHistoryLoads) Get(storeID uint64, rwTp utils.RWType, kind constant.ResourceKind) HistoryLoads {
	load, ok := s.loads[rwTp][kind][storeID]
	if !ok {
		return HistoryLoads{}
	}
	return load.get()
}

// UpdateConfig updates the sample duration and interval.
func (s *StoreHistoryLoads) UpdateConfig(sampleDuration time.Duration, sampleInterval time.Duration) *StoreHistoryLoads {
	if s.sampleDuration == sampleDuration && s.sampleInterval == sampleInterval {
		return s
	}
	return NewStoreHistoryLoads(sampleDuration, sampleInterval)
}

type storeHistoryLoad struct {
	update time.Time
	// loads is a circular buffer.
	historyloads   HistoryLoads
	size           int
	count          int
	sampleInterval time.Duration
}

func newStoreHistoryLoad(size int, sampleInterval time.Duration) *storeHistoryLoad {
	return &storeHistoryLoad{
		size:           size,
		sampleInterval: sampleInterval,
	}
}

// add adds the store load to the history.
func (s *storeHistoryLoad) add(pointLoad Loads) {
	// reject if the loads length is not equal to the dimension.
	if time.Since(s.update) < s.sampleInterval || s.size == 0 || len(pointLoad) != len(s.historyloads) {
		return
	}
	if s.count == 0 {
		for dim := range s.historyloads {
			s.historyloads[dim] = make([]float64, s.size)
		}
	}
	for dim, v := range pointLoad {
		s.historyloads[dim][s.count%s.size] = v
	}
	s.count++
	s.update = time.Now()
}

func (s *storeHistoryLoad) get() HistoryLoads {
	return s.historyloads
}
