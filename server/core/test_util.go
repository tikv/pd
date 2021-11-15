// Copyright 2016 TiKV Project Authors.
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

package core

import (
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// SplitRegions split a set of RegionInfo by the middle of regionKey
func SplitRegions(regions []*RegionInfo) []*RegionInfo {
	results := make([]*RegionInfo, 0, len(regions)*2)
	for _, region := range regions {
		start, end := byte(0), byte(math.MaxUint8)
		if len(region.GetStartKey()) > 0 {
			start = region.GetStartKey()[0]
		}
		if len(region.GetEndKey()) > 0 {
			end = region.GetEndKey()[0]
		}
		middle := []byte{start/2 + end/2}
		left := region.Clone()
		left.meta.Id = region.GetID() + uint64(len(regions))
		left.meta.EndKey = middle
		left.meta.RegionEpoch.Version++
		right := region.Clone()
		right.meta.Id = region.GetID() + uint64(len(regions)*2)
		right.meta.StartKey = middle
		right.meta.RegionEpoch.Version++
		results = append(results, left, right)
	}
	return results
}

// MergeRegions merge a set of RegionInfo by regionKey
func MergeRegions(regions []*RegionInfo) []*RegionInfo {
	results := make([]*RegionInfo, 0, len(regions)/2)
	for i := 0; i < len(regions); i += 2 {
		left := regions[i]
		right := regions[i]
		if i+1 < len(regions) {
			right = regions[i+1]
		}
		region := &RegionInfo{meta: &metapb.Region{
			Id:       left.GetID(),
			StartKey: left.GetStartKey(),
			EndKey:   right.GetEndKey(),
			Peers:    left.meta.Peers,
		}}
		if left.GetRegionEpoch().GetVersion() > right.GetRegionEpoch().GetVersion() {
			region.meta.RegionEpoch = left.GetRegionEpoch()
		} else {
			region.meta.RegionEpoch = right.GetRegionEpoch()
		}
		region.meta.RegionEpoch.Version++
		region.leader = left.leader
		results = append(results, region)
	}
	return results
}

// NewTestRegionInfo creates a RegionInfo for test.
func NewTestRegionInfo(start, end []byte) *RegionInfo {
	return &RegionInfo{meta: &metapb.Region{
		StartKey:    start,
		EndKey:      end,
		RegionEpoch: &metapb.RegionEpoch{},
	}}
}

// NewStoreInfoWithAvailable is create with available and capacity
func NewStoreInfoWithAvailable(id, available, capacity uint64, amp float64) *StoreInfo {
	stats := &pdpb.StoreStats{}
	stats.Capacity = capacity
	stats.Available = available
	usedSize := capacity - available
	regionSize := (float64(usedSize) * amp) / mb
	store := NewStoreInfo(
		&metapb.Store{
			Id: id,
		},
		SetStoreStats(stats),
		SetRegionCount(int(regionSize/96)),
		SetRegionSize(int64(regionSize)),
	)
	return store
}

// NewStoreInfoWithLabel is create a store with specified labels.
func NewStoreInfoWithLabel(id uint64, regionCount int, labels map[string]string) *StoreInfo {
	storeLabels := make([]*metapb.StoreLabel, 0, len(labels))
	for k, v := range labels {
		storeLabels = append(storeLabels, &metapb.StoreLabel{
			Key:   k,
			Value: v,
		})
	}
	stats := &pdpb.StoreStats{}
	stats.Capacity = uint64(1024)
	stats.Available = uint64(1024)
	store := NewStoreInfo(
		&metapb.Store{
			Id:     id,
			Labels: storeLabels,
		},
		SetStoreStats(stats),
		SetRegionCount(regionCount),
		SetRegionSize(int64(regionCount)*10),
	)
	return store
}

// NewStoreInfoWithSizeCount is create a store with size and count.
func NewStoreInfoWithSizeCount(id uint64, regionCount, leaderCount int, regionSize, leaderSize int64) *StoreInfo {
	stats := &pdpb.StoreStats{}
	stats.Capacity = uint64(1024)
	stats.Available = uint64(1024)
	store := NewStoreInfo(
		&metapb.Store{
			Id: id,
		},
		SetStoreStats(stats),
		SetRegionCount(regionCount),
		SetRegionSize(regionSize),
		SetLeaderCount(leaderCount),
		SetLeaderSize(leaderSize),
	)
	return store
}

// RandomKindReadQuery returns query stat with random query kind, only used for unit test.
func RandomKindReadQuery(queryRead uint64) *pdpb.QueryStats {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	switch r.Intn(3) {
	case 0:
		return &pdpb.QueryStats{
			Coprocessor: queryRead,
		}
	case 1:
		return &pdpb.QueryStats{
			Scan: queryRead,
		}
	case 2:
		return &pdpb.QueryStats{
			Get: queryRead,
		}
	default:
		return &pdpb.QueryStats{}
	}
}

// RandomKindWriteQuery returns query stat with random query kind, only used for unit test.
func RandomKindWriteQuery(queryWrite uint64) *pdpb.QueryStats {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	switch r.Intn(7) {
	case 0:
		return &pdpb.QueryStats{
			Put: queryWrite,
		}
	case 1:
		return &pdpb.QueryStats{
			Delete: queryWrite,
		}
	case 2:
		return &pdpb.QueryStats{
			DeleteRange: queryWrite,
		}
	case 3:
		return &pdpb.QueryStats{
			AcquirePessimisticLock: queryWrite,
		}
	case 4:
		return &pdpb.QueryStats{
			Rollback: queryWrite,
		}
	case 5:
		return &pdpb.QueryStats{
			Prewrite: queryWrite,
		}
	case 6:
		return &pdpb.QueryStats{
			Commit: queryWrite,
		}
	default:
		return &pdpb.QueryStats{}
	}
}
