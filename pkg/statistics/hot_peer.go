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

import (
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

type dimStat struct {
	syncutil.RWMutex
	rolling         *movingaverage.TimeMedian // it's used to statistic hot degree and average speed.
	lastIntervalSum int                       // lastIntervalSum and lastDelta are used to calculate the average speed of the last interval.
	lastDelta       float64
}

func newDimStat(reportInterval time.Duration) *dimStat {
	return &dimStat{
		rolling:         movingaverage.NewTimeMedian(utils.DefaultAotSize, rollingWindowsSize, reportInterval),
		lastIntervalSum: 0,
		lastDelta:       0,
	}
}

func (d *dimStat) add(delta float64, interval time.Duration) {
	d.Lock()
	defer d.Unlock()
	d.lastIntervalSum += int(interval.Seconds())
	d.lastDelta += delta
	d.rolling.Add(delta, interval)
}

func (d *dimStat) isLastAverageHot(threshold float64) bool {
	d.RLock()
	defer d.RUnlock()
	return d.lastDelta/float64(d.lastIntervalSum) >= threshold
}

func (d *dimStat) isHot(threshold float64) bool {
	d.RLock()
	defer d.RUnlock()
	return d.rolling.Get() >= threshold
}

func (d *dimStat) isFull(interval time.Duration) bool {
	d.RLock()
	defer d.RUnlock()
	return d.lastIntervalSum >= int(interval.Seconds())
}

func (d *dimStat) clearLastAverage() {
	d.Lock()
	defer d.Unlock()
	d.lastIntervalSum = 0
	d.lastDelta = 0
}

func (d *dimStat) get() float64 {
	d.RLock()
	defer d.RUnlock()
	return d.rolling.Get()
}

func (d *dimStat) clone() *dimStat {
	d.RLock()
	defer d.RUnlock()
	return &dimStat{
		rolling:         d.rolling.Clone(),
		lastIntervalSum: d.lastIntervalSum,
	}
}

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	StoreID  uint64 `json:"store_id"`
	RegionID uint64 `json:"region_id"`
	// HotDegree records the times for the region considered as hot spot during each report.
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove region in cache.
	AntiCount int `json:"anti_count"`
	// Loads contains only Kind-related statistics and is DimLen in length.
	Loads []float64 `json:"loads"`
	// rolling statistics contains denoising data, it's DimLen in length.
	rollingLoads []*dimStat
	// stores contains the all peer's storeID in this region.
	stores []uint64
	// actionType is the action type of the region, add, update or remove.
	actionType utils.ActionType
	// isLeader is true means that the region has a leader on this store.
	isLeader bool
	// lastTransferLeaderTime is used to cool down frequent transfer leader.
	lastTransferLeaderTime time.Time
	// If the peer didn't been send by store heartbeat when it is already stored as hot peer stat,
	// we will handle it as cold peer and mark the inCold flag
	inCold bool
	// If the item in storeA is just inherited from storeB,
	// then other store, such as storeC, will be forbidden to inherit from storeA until the item in storeA is hot.
	allowInherited bool
}

// ID returns region ID. Implementing TopNItem.
func (stat *HotPeerStat) ID() uint64 {
	return stat.RegionID
}

// Less compares two HotPeerStat. Implementing TopNItem.
func (stat *HotPeerStat) Less(dim int, than utils.TopNItem) bool {
	return stat.GetLoad(dim) < than.(*HotPeerStat).GetLoad(dim)
}

// Log is used to output some info
func (stat *HotPeerStat) Log(str string) {
	if log.GetLevel() <= zap.DebugLevel {
		log.Debug(str,
			zap.Uint64("region-id", stat.RegionID),
			zap.Bool("is-leader", stat.isLeader),
			zap.Float64s("loads", stat.GetLoads()),
			zap.Float64s("loads-instant", stat.Loads),
			zap.Int("hot-degree", stat.HotDegree),
			zap.Int("hot-anti-count", stat.AntiCount),
			zap.Duration("sum-interval", stat.getIntervalSum()),
			zap.Bool("allow-inherited", stat.allowInherited),
			zap.String("action-type", stat.actionType.String()),
			zap.Time("last-transfer-leader-time", stat.lastTransferLeaderTime))
	}
}

// IsNeedCoolDownTransferLeader use cooldown time after transfer leader to avoid unnecessary schedule
func (stat *HotPeerStat) IsNeedCoolDownTransferLeader(minHotDegree int, rwTy utils.RWType) bool {
	return time.Since(stat.lastTransferLeaderTime).Seconds() < float64(minHotDegree*rwTy.ReportInterval())
}

// IsLeader indicates the item belong to the leader.
func (stat *HotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// GetActionType returns the item action type.
func (stat *HotPeerStat) GetActionType() utils.ActionType {
	return stat.actionType
}

// GetLoad returns denoising load if possible.
func (stat *HotPeerStat) GetLoad(dim int) float64 {
	if stat.rollingLoads != nil {
		return math.Round(stat.rollingLoads[dim].get())
	}
	return math.Round(stat.Loads[dim])
}

// GetLoads returns denoising loads if possible.
func (stat *HotPeerStat) GetLoads() []float64 {
	if stat.rollingLoads != nil {
		ret := make([]float64, len(stat.rollingLoads))
		for dim := range ret {
			ret[dim] = math.Round(stat.rollingLoads[dim].get())
		}
		return ret
	}
	return stat.Loads
}

// Clone clones the HotPeerStat.
func (stat *HotPeerStat) Clone() *HotPeerStat {
	ret := *stat
	ret.Loads = make([]float64, utils.DimLen)
	for i := range utils.DimLen {
		ret.Loads[i] = stat.GetLoad(i) // replace with denoising loads
	}
	ret.rollingLoads = nil
	return &ret
}

func (stat *HotPeerStat) isHot(thresholds []float64) bool {
	return slice.AnyOf(stat.rollingLoads, func(i int) bool {
		return stat.rollingLoads[i].isLastAverageHot(thresholds[i])
	})
}

func (stat *HotPeerStat) clearLastAverage() {
	for _, l := range stat.rollingLoads {
		l.clearLastAverage()
	}
}

// getIntervalSum returns the sum of all intervals.
// only used for test
func (stat *HotPeerStat) getIntervalSum() time.Duration {
	if len(stat.rollingLoads) == 0 || stat.rollingLoads[0] == nil {
		return 0
	}
	return time.Duration(stat.rollingLoads[0].lastIntervalSum) * time.Second
}

// GetStores returns the stores of all peers in the region.
func (stat *HotPeerStat) GetStores() []uint64 {
	return stat.stores
}
