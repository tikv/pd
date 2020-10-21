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
	"bytes"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

// SplitRegionsHandler used to handle region splitting
// TODO: support initialize splitRegionsHandler
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
	if len(splitKeys) < 1 {
		return 0, nil
	}
	unprocessedKeys := splitKeys
	newRegions := make(map[uint64]struct{}, len(splitKeys))
	for i := 0; i <= retryLimit; i++ {
		unprocessedKeys = r.splitRegionsByKeys(unprocessedKeys, newRegions)
		if len(unprocessedKeys) < 1 {
			break
		}
		// sleep for a while between each retry
		time.Sleep(500 * time.Millisecond)
	}
	returned := make([]uint64, 0, len(newRegions))
	for regionID := range newRegions {
		returned = append(returned, regionID)
	}
	return 100 - len(unprocessedKeys)*100/len(splitKeys), returned
}

func (r *RegionSplitter) splitRegionsByKeys(splitKeys [][]byte, newRegions map[uint64]struct{}) [][]byte {
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
// If any key failed to be found its Region, it will be placed into unProcessed key.
// If the key is exactly the start key of its region, the key would be discarded directly.
func (r *RegionSplitter) groupKeysByRegion(keys [][]byte) (map[uint64][][]byte, [][]byte) {
	unProcessedKeys := make([][]byte, 0, len(keys))
	groupKeys := make(map[uint64][][]byte, len(keys))
	for _, key := range keys {
		region := r.cluster.GetRegionByKey(key)
		if region == nil {
			log.Info("region hollow", logutil.ZapRedactByteString("key", key))
			unProcessedKeys = append(unProcessedKeys, key)
			continue
		}
		// filter start key
		if bytes.Equal(region.GetStartKey(), key) {
			continue
		}
		_, ok := groupKeys[region.GetID()]
		if !ok {
			groupKeys[region.GetID()] = [][]byte{}
		}
		log.Info("found region",
			zap.Uint64("regionID", region.GetID()),
			logutil.ZapRedactByteString("key", key))
		groupKeys[region.GetID()] = append(groupKeys[region.GetID()], key)
	}
	return groupKeys, unProcessedKeys
}
