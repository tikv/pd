// Copyright 2026 TiKV Project Authors.
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

package schedulers

import (
	"bytes"
	"math/rand/v2"
	"time"

	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const hotLeaderCandidateRefreshInterval = time.Second

type hotPeerStatsProvider interface {
	GetHotPeerStatsForStores(rw utils.RWType, storeIDs []uint64) map[uint64][]*statistics.HotPeerStat
}

type hotLeaderStoreCandidates struct {
	read         []uint64
	write        []uint64
	refreshAfter time.Time
}

type hotLeaderCandidates struct {
	stores map[uint64]*hotLeaderStoreCandidates
	now    func() time.Time
}

func newHotLeaderCandidates() *hotLeaderCandidates {
	return &hotLeaderCandidates{
		stores: make(map[uint64]*hotLeaderStoreCandidates),
		now:    time.Now,
	}
}

func (c *hotLeaderCandidates) refresh(provider hotPeerStatsProvider, targetStoreIDs []uint64) {
	now := c.now()
	targetStores := make(map[uint64]struct{}, len(targetStoreIDs))
	dueStoreIDs := make([]uint64, 0, len(targetStoreIDs))
	for _, storeID := range targetStoreIDs {
		if _, ok := targetStores[storeID]; ok {
			continue
		}
		targetStores[storeID] = struct{}{}
		storeCandidates, ok := c.stores[storeID]
		if !ok {
			storeCandidates = &hotLeaderStoreCandidates{}
			c.stores[storeID] = storeCandidates
		}
		if !now.Before(storeCandidates.refreshAfter) {
			dueStoreIDs = append(dueStoreIDs, storeID)
		}
	}
	for storeID := range c.stores {
		if _, ok := targetStores[storeID]; !ok {
			delete(c.stores, storeID)
		}
	}
	if len(dueStoreIDs) == 0 {
		return
	}

	readStats := provider.GetHotPeerStatsForStores(utils.Read, dueStoreIDs)
	writeStats := provider.GetHotPeerStatsForStores(utils.Write, dueStoreIDs)
	for _, storeID := range dueStoreIDs {
		storeCandidates := c.stores[storeID]
		if readStats != nil {
			storeCandidates.read = hotLeaderRegionIDs(readStats[storeID], storeID)
		}
		if writeStats != nil {
			storeCandidates.write = hotLeaderRegionIDs(writeStats[storeID], storeID)
		}
		storeCandidates.refreshAfter = now.Add(hotLeaderCandidateRefreshInterval)
	}

	readCandidateCount := 0
	for _, storeCandidates := range c.stores {
		readCandidateCount += len(storeCandidates.read)
	}
	readRegionIDs := make(map[uint64]struct{}, readCandidateCount)
	for _, storeCandidates := range c.stores {
		for _, regionID := range storeCandidates.read {
			readRegionIDs[regionID] = struct{}{}
		}
	}
	for _, storeCandidates := range c.stores {
		write := storeCandidates.write[:0]
		for _, regionID := range storeCandidates.write {
			if _, ok := readRegionIDs[regionID]; !ok {
				write = append(write, regionID)
			}
		}
		storeCandidates.write = write
	}
}

func hotLeaderRegionIDs(stats []*statistics.HotPeerStat, storeID uint64) []uint64 {
	regionIDs := make([]uint64, 0, len(stats))
	seen := make(map[uint64]struct{}, len(stats))
	for _, stat := range stats {
		if stat == nil || stat.StoreID != storeID || !stat.IsLeader() {
			continue
		}
		if _, ok := seen[stat.RegionID]; ok {
			continue
		}
		seen[stat.RegionID] = struct{}{}
		regionIDs = append(regionIDs, stat.RegionID)
	}
	return regionIDs
}

func (c *hotLeaderCandidates) pop(
	rw utils.RWType,
	targetStoreIDs []uint64,
) (storeID, regionID uint64, ok bool) {
	eligibleStoreIDs := make([]uint64, 0, len(targetStoreIDs))
	seen := make(map[uint64]struct{}, len(targetStoreIDs))
	for _, storeID := range targetStoreIDs {
		if _, ok := seen[storeID]; ok {
			continue
		}
		seen[storeID] = struct{}{}
		storeCandidates := c.stores[storeID]
		if storeCandidates == nil {
			continue
		}
		var regionIDs []uint64
		switch rw {
		case utils.Read:
			regionIDs = storeCandidates.read
		case utils.Write:
			regionIDs = storeCandidates.write
		default:
			return 0, 0, false
		}
		if len(regionIDs) > 0 {
			eligibleStoreIDs = append(eligibleStoreIDs, storeID)
		}
	}
	if len(eligibleStoreIDs) == 0 {
		return 0, 0, false
	}

	storeID = eligibleStoreIDs[rand.IntN(len(eligibleStoreIDs))]
	storeCandidates := c.stores[storeID]
	regionIDs := &storeCandidates.read
	if rw == utils.Write {
		regionIDs = &storeCandidates.write
	}
	index := rand.IntN(len(*regionIDs))
	regionID = (*regionIDs)[index]
	last := len(*regionIDs) - 1
	(*regionIDs)[index] = (*regionIDs)[last]
	*regionIDs = (*regionIDs)[:last]
	return storeID, regionID, true
}

func scheduleEvictHotLeaderBatch(
	name string,
	cluster sche.SchedulerCluster,
	conf evictLeaderStoresConf,
	opController *operator.Controller,
	candidates *hotLeaderCandidates,
) []*operator.Operator {
	storeIDs := conf.getStores()
	candidates.refresh(cluster, storeIDs)
	batchSize := conf.getBatch()
	if batchSize <= 0 {
		return nil
	}

	ops := make([]*operator.Operator, 0, batchSize)
	selected := make(map[uint64]struct{}, batchSize)
	for _, rw := range []utils.RWType{utils.Read, utils.Write} {
		for len(ops) < batchSize {
			storeID, regionID, ok := candidates.pop(rw, storeIDs)
			if !ok {
				break
			}
			op := scheduleEvictHotLeader(
				name, cluster, conf, opController, selected, storeID, regionID,
			)
			if op == nil {
				evictLeaderHotCandidateRejectedCounter.Inc()
				continue
			}
			selected[regionID] = struct{}{}
			if rw == utils.Read {
				op.Counters = append(op.Counters, evictLeaderPickReadHotCounter)
			} else {
				op.Counters = append(op.Counters, evictLeaderPickWriteHotCounter)
			}
			ops = append(ops, op)
		}
	}

	if len(ops) < batchSize {
		evictLeaderHotFallbackCounter.Inc()
		ops = append(ops, scheduleEvictLeaderFallback(
			name, cluster, conf, opController, selected, batchSize-len(ops),
		)...)
	}
	return ops
}

func scheduleEvictHotLeader(
	name string,
	cluster sche.SchedulerCluster,
	conf evictLeaderStoresConf,
	opController *operator.Controller,
	selected map[uint64]struct{},
	storeID uint64,
	regionID uint64,
) *operator.Operator {
	if _, ok := selected[regionID]; ok {
		return nil
	}
	region := cluster.GetRegion(regionID)
	if region == nil ||
		region.GetLeader() == nil ||
		region.GetLeader().GetStoreId() != storeID ||
		!regionIsInKeyRanges(region, conf.getKeyRangesByID(storeID)) ||
		opController.GetOperator(regionID) != nil ||
		len(region.GetPendingPeers()) > 0 ||
		len(region.GetDownPeers()) > 0 {
		return nil
	}
	return createEvictLeaderOperator(name, cluster, region)
}

func regionIsInKeyRanges(region *core.RegionInfo, ranges []keyutil.KeyRange) bool {
	for _, keyRange := range ranges {
		if bytes.Compare(region.GetStartKey(), keyRange.StartKey) >= 0 &&
			(len(keyRange.EndKey) == 0 ||
				(len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), keyRange.EndKey) <= 0)) {
			return true
		}
	}
	return false
}

func scheduleEvictLeaderFallback(
	name string,
	cluster sche.SchedulerCluster,
	conf evictLeaderStoresConf,
	opController *operator.Controller,
	selected map[uint64]struct{},
	limit int,
) []*operator.Operator {
	ops := make([]*operator.Operator, 0, limit)
	for range limit {
		once := scheduleEvictLeaderOnce(name, cluster, conf, selected, opController)
		if len(once) == 0 {
			break
		}
		for _, op := range once {
			if len(ops) >= limit {
				return ops
			}
			regionID := op.RegionID()
			if _, ok := selected[regionID]; ok {
				continue
			}
			selected[regionID] = struct{}{}
			ops = append(ops, op)
		}
	}
	return ops
}
