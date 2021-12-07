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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"math"
	"time"

	"github.com/tikv/pd/pkg/movingaverage"
	"go.uber.org/zap"
)

const (
	byteDim int = iota
	keyDim
	dimLen
)

type dimStat struct {
	typ         int
	Rolling     *movingaverage.TimeMedian  // it's used to statistic hot degree and average speed.
	LastAverage *movingaverage.AvgOverTime // it's used to obtain the average speed in last second as instantaneous speed.
}

func newDimStat(typ int) *dimStat {
	reportInterval := RegionHeartBeatReportInterval * time.Second
	return &dimStat{
		typ:         typ,
		Rolling:     movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, reportInterval),
		LastAverage: movingaverage.NewAvgOverTime(reportInterval),
	}
}

func (d *dimStat) Add(delta float64, interval time.Duration) {
	d.LastAverage.Add(delta, interval)
	d.Rolling.Add(delta, interval)
}

func (d *dimStat) isLastAverageHot(thresholds [dimLen]float64) bool {
	return d.LastAverage.Get() >= thresholds[d.typ]
}

func (d *dimStat) isHot(thresholds [dimLen]float64) bool {
	return d.Rolling.Get() >= thresholds[d.typ]
}

func (d *dimStat) isFull() bool {
	return d.LastAverage.IsFull()
}

func (d *dimStat) clearLastAverage() {
	d.LastAverage.Clear()
}

func (d *dimStat) Get() float64 {
	return d.Rolling.Get()
}

func (d *dimStat) Clone() *dimStat {
	return &dimStat{
		typ:         d.typ,
		Rolling:     d.Rolling.Clone(),
		LastAverage: d.LastAverage.Clone(),
	}
}

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	StoreID  uint64 `json:"store_id"`
	RegionID uint64 `json:"region_id"`

	// HotDegree records the times for the region considered as hot spot during each HandleRegionHeartbeat
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int `json:"anti_count"`

	Kind     FlowKind `json:"-"`
	ByteRate float64  `json:"flow_bytes"`
	KeyRate  float64  `json:"flow_keys"`

	// rolling statistics, recording some recently added records.
	rollingByteRate *dimStat
	rollingKeyRate  *dimStat

	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`

	needDelete             bool
	isLeader               bool
	isNew                  bool
	justTransferLeader     bool
	interval               uint64
	thresholds             [dimLen]float64
	peers                  []uint64
	lastTransferLeaderTime time.Time
}

// ID returns region ID. Implementing TopNItem.
func (stat *HotPeerStat) ID() uint64 {
	return stat.RegionID
}

// Less compares two HotPeerStat.Implementing TopNItem.
func (stat *HotPeerStat) Less(k int, than TopNItem) bool {
	rhs := than.(*HotPeerStat)
	switch k {
	case keyDim:
		return stat.GetKeyRate() < rhs.GetKeyRate()
	case byteDim:
		fallthrough
	default:
		return stat.GetByteRate() < rhs.GetByteRate()
	}
}

// Log is used to output some info
func (stat *HotPeerStat) Log(str string, level func(msg string, fields ...zap.Field)) {
	level(str,
		zap.Uint64("interval", stat.interval),
		zap.Uint64("region-id", stat.RegionID),
		zap.Uint64("store", stat.StoreID),
		zap.Float64("byte-rate", stat.GetByteRate()),
		zap.Float64("byte-rate-instant", stat.ByteRate),
		zap.Float64("byte-rate-threshold", stat.thresholds[byteDim]),
		zap.Float64("key-rate", stat.GetKeyRate()),
		zap.Float64("key-rate-instant", stat.KeyRate),
		zap.Float64("key-rate-threshold", stat.thresholds[keyDim]),
		zap.Int("hot-degree", stat.HotDegree),
		zap.Int("hot-anti-count", stat.AntiCount),
		zap.Bool("just-transfer-leader", stat.justTransferLeader),
		zap.Bool("is-leader", stat.isLeader),
		zap.Bool("need-delete", stat.IsNeedDelete()),
		zap.String("type", stat.Kind.String()),
		zap.Time("last-transfer-leader-time", stat.lastTransferLeaderTime))
}

// IsNeedCoolDownTransferLeader use cooldown time after transfer leader to avoid unnecessary schedule
func (stat *HotPeerStat) IsNeedCoolDownTransferLeader(minHotDegree int) bool {
	return time.Since(stat.lastTransferLeaderTime).Seconds() < float64(minHotDegree*RegionHeartBeatReportInterval)
}

// IsNeedDelete to delete the item in cache.
func (stat *HotPeerStat) IsNeedDelete() bool {
	return stat.needDelete
}

// IsLeader indicates the item belong to the leader.
func (stat *HotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// IsNew indicates the item is first update in the cache of the region.
func (stat *HotPeerStat) IsNew() bool {
	return stat.isNew
}

// GetByteRate returns denoised BytesRate if possible.
func (stat *HotPeerStat) GetByteRate() float64 {
	if stat.rollingByteRate == nil {
		return math.Round(stat.ByteRate)
	}
	return math.Round(stat.rollingByteRate.Get())
}

// GetKeyRate returns denoised KeysRate if possible.
func (stat *HotPeerStat) GetKeyRate() float64 {
	if stat.rollingKeyRate == nil {
		return math.Round(stat.KeyRate)
	}
	return math.Round(stat.rollingKeyRate.Get())
}

// GetThresholds returns thresholds
func (stat *HotPeerStat) GetThresholds() [dimLen]float64 {
	return stat.thresholds
}

// Clone clones the HotPeerStat
func (stat *HotPeerStat) Clone() *HotPeerStat {
	ret := *stat
	ret.ByteRate = stat.GetByteRate()
	ret.rollingByteRate = nil
	ret.KeyRate = stat.GetKeyRate()
	ret.rollingKeyRate = nil
	return &ret
}

func (stat *HotPeerStat) isFullAndHot() bool {
	return (stat.rollingByteRate.isFull() && stat.rollingByteRate.isLastAverageHot(stat.thresholds)) ||
		(stat.rollingKeyRate.isFull() && stat.rollingKeyRate.isLastAverageHot(stat.thresholds))
}

func (stat *HotPeerStat) clearLastAverage() {
	stat.rollingByteRate.clearLastAverage()
	stat.rollingKeyRate.clearLastAverage()
}
