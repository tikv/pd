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

	"github.com/tikv/pd/server/core"
)

// StoreLoadDetail records store load information.
type StoreLoadDetail struct {
	Info     *StoreSummaryInfo
	LoadPred *StoreLoadPred
	HotPeers []*HotPeerStat
}

// GetID return the ID of store.
func (li *StoreLoadDetail) GetID() uint64 {
	return li.Info.Store.GetID()
}

// ToHotPeersStat abstracts load information to HotPeersStat.
func (li *StoreLoadDetail) ToHotPeersStat() *HotPeersStat {
	totalLoads := make([]float64, RegionStatCount)
	if len(li.HotPeers) == 0 {
		return &HotPeersStat{
			TotalLoads:     totalLoads,
			TotalBytesRate: 0.0,
			TotalKeysRate:  0.0,
			TotalQueryRate: 0.0,
			Count:          0,
			Stats:          make([]HotPeerStatShow, 0),
		}
	}
	kind := Write
	if li.HotPeers[0].Kind == Read {
		kind = Read
	}

	peers := make([]HotPeerStatShow, 0, len(li.HotPeers))
	for _, peer := range li.HotPeers {
		if peer.HotDegree > 0 {
			peers = append(peers, toHotPeerStatShow(peer, kind))
			for i := range totalLoads {
				totalLoads[i] += peer.GetLoad(RegionStatKind(i))
			}
		}
	}

	b, k, q := GetRegionStatKind(kind, ByteDim), GetRegionStatKind(kind, KeyDim), GetRegionStatKind(kind, QueryDim)
	byteRate, keyRate, queryRate := totalLoads[b], totalLoads[k], totalLoads[q]
	storeByteRate, storeKeyRate, storeQueryRate := li.LoadPred.Current.Loads[ByteDim],
		li.LoadPred.Current.Loads[KeyDim], li.LoadPred.Current.Loads[QueryDim]

	return &HotPeersStat{
		TotalLoads:     totalLoads,
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

func toHotPeerStatShow(p *HotPeerStat, kind RWType) HotPeerStatShow {
	b, k, q := GetRegionStatKind(kind, ByteDim), GetRegionStatKind(kind, KeyDim), GetRegionStatKind(kind, QueryDim)
	byteRate := p.Loads[b]
	keyRate := p.Loads[k]
	queryRate := p.Loads[q]
	return HotPeerStatShow{
		StoreID:        p.StoreID,
		RegionID:       p.RegionID,
		HotDegree:      p.HotDegree,
		ByteRate:       byteRate,
		KeyRate:        keyRate,
		QueryRate:      queryRate,
		AntiCount:      p.AntiCount,
		LastUpdateTime: p.LastUpdateTime,
	}
}

// GetRegionStatKind gets region statistics kind.
func GetRegionStatKind(rwTy RWType, dim int) RegionStatKind {
	switch {
	case rwTy == Read && dim == ByteDim:
		return RegionReadBytes
	case rwTy == Read && dim == KeyDim:
		return RegionReadKeys
	case rwTy == Write && dim == ByteDim:
		return RegionWriteBytes
	case rwTy == Write && dim == KeyDim:
		return RegionWriteKeys
	case rwTy == Write && dim == QueryDim:
		return RegionWriteQuery
	case rwTy == Read && dim == QueryDim:
		return RegionReadQuery
	}
	return 0
}

// StoreSummaryInfo records the summary information of store.
type StoreSummaryInfo struct {
	Store      *core.StoreInfo
	IsTiFlash  bool
	PendingSum *Influence
}

// Influence records operator influence.
type Influence struct {
	Loads []float64
	Count float64
}

// SummaryStoreInfos return a mapping from store to summary information.
func SummaryStoreInfos(stores []*core.StoreInfo) map[uint64]*StoreSummaryInfo {
	infos := make(map[uint64]*StoreSummaryInfo, len(stores))
	for _, store := range stores {
		info := &StoreSummaryInfo{
			Store:      store,
			IsTiFlash:  core.IsStoreContainLabel(store.GetMeta(), core.EngineKey, core.EngineTiFlash),
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
			Loads: make([]float64, len(infl.Loads)),
			Count: 0,
		}
	}
	for i, load := range infl.Loads {
		s.PendingSum.Loads[i] += load * w
	}
	s.PendingSum.Count += infl.Count * w
}

// GetPendingInfluence returns the current pending influence.
func GetPendingInfluence(stores []*core.StoreInfo) map[uint64]*Influence {
	stInfos := SummaryStoreInfos(stores)
	ret := make(map[uint64]*Influence, len(stInfos))
	for id, info := range stInfos {
		if info.PendingSum != nil {
			ret[id] = info.PendingSum
		}
	}
	return ret
}

// StoreLoad records the current load.
type StoreLoad struct {
	Loads []float64
	Count float64
}

// ToLoadPred returns the current load and future predictive load.
func (load StoreLoad) ToLoadPred(rwTy RWType, infl *Influence) *StoreLoadPred {
	future := StoreLoad{
		Loads: append(load.Loads[:0:0], load.Loads...),
		Count: load.Count,
	}
	if infl != nil {
		switch rwTy {
		case Read:
			future.Loads[ByteDim] += infl.Loads[RegionReadBytes]
			future.Loads[KeyDim] += infl.Loads[RegionReadKeys]
			future.Loads[QueryDim] += infl.Loads[RegionReadQuery]
		case Write:
			future.Loads[ByteDim] += infl.Loads[RegionWriteBytes]
			future.Loads[KeyDim] += infl.Loads[RegionWriteKeys]
			future.Loads[QueryDim] += infl.Loads[RegionWriteQuery]
		}
		future.Count += infl.Count
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
	loads := make([]float64, len(mx.Loads))
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &StoreLoad{
		Loads: loads,
		Count: 0,
	}
}

// Diff return the difference between min and max.
func (lp *StoreLoadPred) Diff() *StoreLoad {
	mx, mn := lp.Max(), lp.Min()
	loads := make([]float64, len(mx.Loads))
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &StoreLoad{
		Loads: loads,
		Count: mx.Count - mn.Count,
	}
}

// MinLoad return the min store load.
func MinLoad(a, b *StoreLoad) *StoreLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Min(a.Loads[i], b.Loads[i])
	}
	return &StoreLoad{
		Loads: loads,
		Count: math.Min(a.Count, b.Count),
	}
}

// MaxLoad return the max store load.
func MaxLoad(a, b *StoreLoad) *StoreLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Max(a.Loads[i], b.Loads[i])
	}
	return &StoreLoad{
		Loads: loads,
		Count: math.Max(a.Count, b.Count),
	}
}
