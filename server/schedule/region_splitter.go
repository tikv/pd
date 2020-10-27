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
	"errors"
	"math"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const (
	splitBatchRegionLimit = 16
)

// SplitRegionsHandler used to handle region splitting
type SplitRegionsHandler interface {
	SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error
	WatchRegionsByKeyRange(startKey, endKey []byte, splitKeys [][]byte,
		timeout, watchInterval time.Duration, response *splitKeyResponse, wg *sync.WaitGroup)
}

// NewSplitRegionsHandler return SplitRegionsHandler
func NewSplitRegionsHandler(cluster opt.Cluster, oc *OperatorController) SplitRegionsHandler {
	return &splitRegionsHandler{
		cluster: cluster,
		oc:      oc,
	}
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
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(i)))*initialSleepDuration))
	}
	returned := make([]uint64, 0, len(newRegions))
	for regionID := range newRegions {
		returned = append(returned, regionID)
	}
	return 100 - len(unprocessedKeys)*100/len(splitKeys), returned
}

func (r *RegionSplitter) splitRegionsByKeys(splitKeys [][]byte, newRegions map[uint64]struct{}) [][]byte {
	validGroups, unProcessedKeys := r.groupKeysByRegion(splitKeys)
	// batch limit
	var batches []batch
	for _, groupKeys := range validGroups {
		batches = appendKeyBatches(batches, groupKeys.region, groupKeys.keys, splitBatchRegionLimit)
	}
	checkRegionGroupKeys := make(map[uint64]regionGroupKeys, len(validGroups))
	for _, batch := range batches {
		err := r.handler.SplitRegionByKeys(batch.region, batch.keys)
		if err != nil {
			unProcessedKeys = append(unProcessedKeys, batch.keys...)
			continue
		}
		_, ok := checkRegionGroupKeys[batch.regionID]
		if !ok {
			checkRegionGroupKeys[batch.regionID] = regionGroupKeys{
				region: batch.region,
				keys:   [][]byte{},
			}
		}
		checkRegionGroupKeys[batch.regionID] = regionGroupKeys{
			region: checkRegionGroupKeys[batch.regionID].region,
			keys:   append(checkRegionGroupKeys[batch.regionID].keys, batch.keys...),
		}
	}
	wg := &sync.WaitGroup{}
	response := newSplitKeyResponse()
	for _, groupKeys := range checkRegionGroupKeys {
		wg.Add(1)
		go r.handler.WatchRegionsByKeyRange(groupKeys.region.GetStartKey(), groupKeys.region.GetEndKey(),
			groupKeys.keys, time.Minute, 100*time.Millisecond, response, wg)
	}
	wg.Wait()
	for newID := range response.getRegionsID() {
		newRegions[newID] = struct{}{}
	}
	return unProcessedKeys
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// If any key failed to be found its Region, it will be placed into unProcessed key.
// If the key is exactly the start key of its region, the key would be discarded directly.
func (r *RegionSplitter) groupKeysByRegion(keys [][]byte) (map[uint64]regionGroupKeys, [][]byte) {
	unProcessedKeys := make([][]byte, 0, len(keys))
	groups := make(map[uint64]regionGroupKeys, len(keys))
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
		// assert region valid
		if !r.checkRegionValid(region) {
			unProcessedKeys = append(unProcessedKeys, key)
			continue
		}
		_, ok := groups[region.GetID()]
		if !ok {
			groups[region.GetID()] = regionGroupKeys{
				region: region,
				keys:   [][]byte{},
			}
		}
		log.Info("found region",
			zap.Uint64("regionID", region.GetID()),
			logutil.ZapRedactByteString("key", key))
		appendKeys := append(groups[region.GetID()].keys, key)
		groups[region.GetID()] = regionGroupKeys{
			region: region,
			keys:   appendKeys,
		}
	}
	return groups, unProcessedKeys
}

func (r *RegionSplitter) checkRegionValid(region *core.RegionInfo) bool {
	if r.cluster.IsRegionHot(region) {
		return false
	}
	if !opt.IsRegionReplicated(r.cluster, region) {
		r.cluster.AddSuspectRegions(region.GetID())
		return false
	}
	if region.GetLeader() == nil {
		return false
	}
	return true
}

type splitRegionsHandler struct {
	cluster opt.Cluster
	oc      *OperatorController
}

func (h *splitRegionsHandler) SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error {
	op := operator.CreateSplitRegionOperator("region-splitter", region, 0, pdpb.CheckPolicy_USEKEY, splitKeys)
	if ok := h.oc.AddOperator(op); !ok {
		log.Warn("add region split operator failed", zap.Uint64("region-id", region.GetID()))
		return errors.New("add region split operator failed")
	}
	return nil
}

func (h *splitRegionsHandler) WatchRegionsByKeyRange(startKey, endKey []byte, splitKeys [][]byte,
	timeout, watchInterval time.Duration, response *splitKeyResponse, wg *sync.WaitGroup) {
	after := time.After(timeout)
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()
	var regionsID map[uint64]struct{}
	// add regionsID into response at last
	defer func() {
		response.addRegionsID(regionsID)
		wg.Done()
	}()
	for {
		select {
		case <-ticker.C:
			regions := h.cluster.ScanRegions(startKey, endKey, -1)
			for _, region := range regions {
				for _, key := range splitKeys {
					if bytes.Equal(key, region.GetStartKey()) {
						regionsID[region.GetID()] = struct{}{}
					}
				}
			}
			if len(regionsID) < len(splitKeys) {
				continue
			}
			return
		case <-after:
			return
		}
	}
}

type batch struct {
	regionID uint64
	keys     [][]byte
	region   *core.RegionInfo
}

func appendKeyBatches(batches []batch, region *core.RegionInfo, groupKeys [][]byte, limit int) []batch {
	var keys [][]byte
	for start, count := 0, 0; start < len(groupKeys); start++ {
		if count > limit {
			batches = append(batches, batch{regionID: region.GetID(), keys: keys})
			keys = make([][]byte, 0, limit)
			count = 0
		}
		keys = append(keys, groupKeys[start])
		count++
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: region.GetID(), keys: keys, region: region})
	}
	return batches
}

type regionGroupKeys struct {
	region *core.RegionInfo
	keys   [][]byte
}

type splitKeyResponse struct {
	mu struct {
		sync.RWMutex
		newRegions map[uint64]struct{}
	}
}

func newSplitKeyResponse() *splitKeyResponse {
	s := &splitKeyResponse{}
	s.mu.newRegions = map[uint64]struct{}{}
	return s
}

func (r *splitKeyResponse) addRegionsID(regionsID map[uint64]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range regionsID {
		r.mu.newRegions[id] = struct{}{}
	}
}

func (r *splitKeyResponse) getRegionsID() map[uint64]struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.newRegions
}
