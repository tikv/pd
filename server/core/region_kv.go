// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// RegionKV is used to save regions.
type RegionKV struct {
	*leveldbKV
	mu           sync.RWMutex
	batchRegions map[string]*metapb.Region
	batchSize    int
	cacheSize    int
	flushRate    time.Duration
	flushTime    time.Time
	ctx          context.Context
	cancel       context.CancelFunc
}

const (
	//DefaultFlushRegionRate is the ttl to sync the regions to kv storage.
	defaultFlushRegionRate = 3 * time.Second
	//DefaultBatchSize is the batch size to save the regions to kv storage.
	defaultBatchSize = 100
)

// NewRegionKV return a kv storage that is used to save regions.
func NewRegionKV(path string) (*RegionKV, error) {
	levelDB, err := newLeveldbKV(path)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	kv := &RegionKV{
		leveldbKV:    levelDB,
		batchSize:    defaultBatchSize,
		flushRate:    defaultFlushRegionRate,
		batchRegions: make(map[string]*metapb.Region, defaultBatchSize),
		flushTime:    time.Now().Add(defaultFlushRegionRate),
		ctx:          ctx,
		cancel:       cancel,
	}
	kv.backgroundFlush()
	return kv, nil
}

func (kv *RegionKV) backgroundFlush() {
	ticker := time.NewTicker(dirtyFlushTick)
	var (
		isFlush bool
		err     error
	)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				kv.mu.RLock()
				isFlush = kv.flushTime.Before(time.Now())
				kv.mu.RUnlock()
				if !isFlush {
					continue
				}
				if err = kv.FlushRegion(); err != nil {
					log.Info("flush regions error: ", err)
				}
			case <-kv.ctx.Done():
				return
			}
		}
	}()
}

// SaveRegion saves one region to KV.
func (kv *RegionKV) SaveRegion(region *metapb.Region) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.cacheSize < kv.batchSize {
		kv.batchRegions[regionPath(region.GetId())] = region
		kv.cacheSize++

		kv.flushTime = time.Now().Add(kv.flushRate)
		return nil
	}
	kv.batchRegions[regionPath(region.GetId())] = region
	err := kv.flush()

	if err != nil {
		return err
	}
	return nil
}

// DeleteRegion deletes one region from KV.
func (kv *RegionKV) DeleteRegion(region *metapb.Region) error {
	return deleteRegion(kv, region)
}

// LoadRegion loads one regoin from KV.
func (kv *RegionKV) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return loadProto(kv, regionPath(regionID), region)
}

// LoadRegions loads all regions from KV to RegionsInfo.
func (kv *RegionKV) LoadRegions(regions *RegionsInfo) error {
	return loadRegions(kv, regions)
}

func deleteRegion(kv KVBase, region *metapb.Region) error {
	return kv.Delete(regionPath(region.GetId()))
}

func loadRegions(kv KVBase, regions *RegionsInfo) error {
	nextID := uint64(0)
	endKey := regionPath(math.MaxUint64)

	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := maxKVRangeLimit
	for {
		key := regionPath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= minKVRangeLimit {
				continue
			}
			return err
		}

		for _, s := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(s)); err != nil {
				return errors.WithStack(err)
			}

			nextID = region.GetId() + 1
			overlaps := regions.SetRegion(NewRegionInfo(region, nil))
			for _, item := range overlaps {
				if err := deleteRegion(kv, item); err != nil {
					return err
				}
			}
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}

// FlushRegion saves the cache region to region kv storage.
func (kv *RegionKV) FlushRegion() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.flush()
}

func (kv *RegionKV) flush() error {
	if err := kv.SaveRegions(kv.batchRegions); err != nil {
		return err
	}
	kv.cacheSize = 0
	kv.batchRegions = make(map[string]*metapb.Region, kv.batchSize)
	return nil
}

// Close closes the kv.
func (kv *RegionKV) Close() error {
	kv.cancel()
	return kv.db.Close()
}
