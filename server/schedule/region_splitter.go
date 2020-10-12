// Copyright 2020 TiKV Project Authors.
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

package schedule

import (
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

// TODO: support initialize splitRegionsHandler
// SplitRegionsHandler used to handle region splitting
type SplitRegionsHandler interface {
	SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error
	WatchRegionsByKeyRange(startKey, endKey []byte, timeout, watchInterval time.Duration) []uint64
}

// RegionSplitter handles split regions
type RegionSplitter struct {
	cluster opt.Cluster
	handler SplitRegionsHandler
}

// NewRegionSplitter return a region splitter
func NewRegionSplitter(cluster opt.Cluster, handler SplitRegionsHandler) *RegionSplitter {
	return &RegionSplitter{
		cluster: cluster,
		handler: handler,
	}
}

// SplitRegions support splitRegions by given split keys.
func (r *RegionSplitter) SplitRegions(splitKeys [][]byte, retryLimit int) (int, []uint64) {
	unprocessedKeys := splitKeys
	newRegions := make(map[uint64]struct{}, len(splitKeys))
	for i := 0; i < retryLimit; i++ {
		unprocessedKeys = r.splitRegions(unprocessedKeys, newRegions)
		if len(unprocessedKeys) < 1 {
			break
		}
		//TODO: sleep for a while
		time.Sleep(500 * time.Millisecond)
	}
	returned := make([]uint64, 0, len(newRegions))
	for regionID := range newRegions {
		returned = append(returned, regionID)
	}
	return 100 - len(unprocessedKeys)*100/len(splitKeys), returned
}

func (r *RegionSplitter) splitRegions(splitKeys [][]byte, newRegions map[uint64]struct{}) [][]byte {
	//TODO: support batch limit
	groupKeys, unProcessedKeys := r.groupKeysByRegion(splitKeys)
	for regionID, keys := range groupKeys {
		region := r.cluster.GetRegion(regionID)
		// TODO: assert region is not nil
		// TODO: assert leader exists
		// TODO: assert region replicated
		// TODO: assert region not hot
		err := r.handler.SplitRegionByKeys(region, keys)
		if err != nil {
			unProcessedKeys = append(unProcessedKeys, keys...)
			continue
		}
		// TODO: use goroutine to run watchRegionsByKeyRange asynchronously
		// TODO: support configure timeout and interval
		splittedRegionsID := r.handler.WatchRegionsByKeyRange(region.GetStartKey(), region.GetEndKey(), time.Minute, 100*time.Millisecond)
		for _, id := range splittedRegionsID {
			newRegions[id] = struct{}{}
		}
	}
	return unProcessedKeys
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
// filter is used to filter some unwanted keys.
func (r *RegionSplitter) groupKeysByRegion(keys [][]byte) (map[uint64][][]byte, [][]byte) {
	unProcessedKeys := make([][]byte, 0, len(keys))
	groupKeys := make(map[uint64][][]byte, len(keys))
	for _, key := range keys {
		region := r.cluster.GetRegionByKey(key)
		if region == nil {
			unProcessedKeys = append(unProcessedKeys, key)
			continue
		}
		group, ok := groupKeys[region.GetID()]
		if !ok {
			groupKeys[region.GetID()] = [][]byte{}
		}
		//TODO: log reduction
		log.Info("found region",
			zap.Uint64("regionID", region.GetID()),
			zap.String("key", string(key[:])))
		groupKeys[region.GetID()] = append(group, key)
	}
	return groupKeys, unProcessedKeys
}
