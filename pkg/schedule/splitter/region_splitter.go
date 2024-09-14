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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package splitter

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

const (
	watchInterval        = 100 * time.Millisecond
	timeout              = time.Minute
	maxSleepDuration     = time.Minute
	initialSleepDuration = 100 * time.Millisecond
)

// SplitRegionsHandler used to handle region splitting
type SplitRegionsHandler interface {
	SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error
	ScanRegionsByKeyRange(groupKeys *regionGroupKeys, results *splitKeyResults)
}

// NewSplitRegionsHandler return SplitRegionsHandler
func NewSplitRegionsHandler(cluster sche.ClusterInformer, oc *operator.Controller) SplitRegionsHandler {
	return &splitRegionsHandler{
		cluster: cluster,
		oc:      oc,
	}
}

// RegionSplitter handles split regions
type RegionSplitter struct {
	cluster           sche.ClusterInformer
	handler           SplitRegionsHandler
	addSuspectRegions func(bool, ...uint64)
}

// NewRegionSplitter return a region splitter
func NewRegionSplitter(cluster sche.ClusterInformer, handler SplitRegionsHandler, addSuspectRegions func(bool, ...uint64)) *RegionSplitter {
	return &RegionSplitter{
		cluster:           cluster,
		handler:           handler,
		addSuspectRegions: addSuspectRegions,
	}
}

// SplitRegions support splitRegions by given split keys.
func (r *RegionSplitter) SplitRegions(ctx context.Context, splitKeys [][]byte, retryLimit int) (int, []uint64) {
	if len(splitKeys) < 1 {
		return 0, nil
	}
	unprocessedKeys := splitKeys
	newRegions := make(map[uint64]struct{}, len(splitKeys))
	for i := 0; i <= retryLimit; i++ {
		unprocessedKeys = r.splitRegionsByKeys(ctx, unprocessedKeys, newRegions)
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

func (r *RegionSplitter) splitRegionsByKeys(parCtx context.Context, splitKeys [][]byte, newRegions map[uint64]struct{}) [][]byte {
	validGroups := r.groupKeysByRegion(splitKeys)
	for key, group := range validGroups {
		err := r.handler.SplitRegionByKeys(group.region, group.keys)
		if err != nil {
			delete(validGroups, key)
			continue
		}
	}
	results := newSplitKeyResults()
	ticker := time.NewTicker(watchInterval)
	ctx, cancel := context.WithTimeout(parCtx, timeout)
	defer func() {
		ticker.Stop()
		cancel()
	}()
outerLoop:
	for {
		select {
		case <-ticker.C:
			for _, groupKeys := range validGroups {
				if groupKeys.finished {
					continue
				}
				r.handler.ScanRegionsByKeyRange(groupKeys, results)
			}
		case <-ctx.Done():
			break outerLoop
		}
		finished := true
		for _, groupKeys := range validGroups {
			if !groupKeys.finished {
				finished = false
			}
		}
		if finished {
			break
		}
	}
	for newID := range results.getSplitRegions() {
		newRegions[newID] = struct{}{}
	}
	return results.getUnProcessedKeys(splitKeys)
}

// groupKeysByRegion separates keys into groups by their belonging Regions.
func (r *RegionSplitter) groupKeysByRegion(keys [][]byte) map[uint64]*regionGroupKeys {
	groups := make(map[uint64]*regionGroupKeys, len(keys))
	for _, key := range keys {
		region := r.cluster.GetRegionByKey(key)
		if region == nil {
			log.Error("region hollow", logutil.ZapRedactByteString("key", key))
			continue
		}
		// assert region valid
		if !r.checkRegionValid(region) {
			continue
		}
		if bytes.Equal(region.GetStartKey(), key) {
			continue
		}
		log.Info("found region",
			zap.Uint64("region-id", region.GetID()),
			logutil.ZapRedactByteString("key", key))
		_, ok := groups[region.GetID()]
		if !ok {
			groups[region.GetID()] = &regionGroupKeys{
				region: region,
				keys: [][]byte{
					key,
				},
			}
		} else {
			groups[region.GetID()].keys = append(groups[region.GetID()].keys, key)
		}
	}
	return groups
}

func (r *RegionSplitter) checkRegionValid(region *core.RegionInfo) bool {
	if !filter.IsRegionReplicated(r.cluster, region) {
		r.addSuspectRegions(false, region.GetID())
		return false
	}
	if region.GetLeader() == nil {
		return false
	}
	return true
}

type splitRegionsHandler struct {
	cluster sche.ClusterInformer
	oc      *operator.Controller
}

// SplitRegionByKeys split region by keys.
func (h *splitRegionsHandler) SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error {
	op, err := operator.CreateSplitRegionOperator("region-splitter", region, 0, pdpb.CheckPolicy_USEKEY, splitKeys)
	if err != nil {
		return err
	}

	if ok := h.oc.AddOperator(op); !ok {
		log.Warn("add region split operator failed", zap.Uint64("region-id", region.GetID()))
		return errors.New("add region split operator failed")
	}
	return nil
}

// ScanRegionsByKeyRange scans regions by key range.
func (h *splitRegionsHandler) ScanRegionsByKeyRange(groupKeys *regionGroupKeys, results *splitKeyResults) {
	splitKeys := groupKeys.keys
	startKey, endKey := groupKeys.region.GetStartKey(), groupKeys.region.GetEndKey()
	createdRegions := make(map[uint64][]byte, len(splitKeys))
	defer func() {
		results.addRegionsID(createdRegions)
	}()
	regions := h.cluster.ScanRegions(startKey, endKey, -1)
	for _, region := range regions {
		for _, splitKey := range splitKeys {
			if bytes.Equal(splitKey, region.GetStartKey()) {
				log.Info("found split region",
					zap.Uint64("region-id", region.GetID()),
					logutil.ZapRedactString("split-key", hex.EncodeToString(splitKey)))
				createdRegions[region.GetID()] = splitKey
			}
		}
	}
	if len(createdRegions) >= len(splitKeys) {
		groupKeys.finished = true
	}
}

type regionGroupKeys struct {
	// finished indicates all the split regions have been found in `region` according to the `keys`
	finished bool
	region   *core.RegionInfo
	keys     [][]byte
}

type splitKeyResults struct {
	// newRegionID -> newRegionID's startKey
	newRegions map[uint64][]byte
}

func newSplitKeyResults() *splitKeyResults {
	s := &splitKeyResults{}
	s.newRegions = make(map[uint64][]byte)
	return s
}

func (r *splitKeyResults) addRegionsID(regionsID map[uint64][]byte) {
	for id, splitKey := range regionsID {
		r.newRegions[id] = splitKey
	}
}

func (r *splitKeyResults) getSplitRegions() map[uint64][]byte {
	return r.newRegions
}

func (r *splitKeyResults) getUnProcessedKeys(splitKeys [][]byte) [][]byte {
	var unProcessedKeys [][]byte
	for _, splitKey := range splitKeys {
		processed := false
		for _, regionStartKey := range r.newRegions {
			if bytes.Equal(splitKey, regionStartKey) {
				processed = true
				break
			}
		}
		if !processed {
			unProcessedKeys = append(unProcessedKeys, splitKey)
		}
	}
	return unProcessedKeys
}
