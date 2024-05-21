// Copyright 2024 TiKV Project Authors.
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

package heartbeatbench

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

// Stores contains store stats with lock.
type Stores struct {
	stat []atomic.Value
}

func newStores(storeCount int) *Stores {
	return &Stores{
		stat: make([]atomic.Value, storeCount+1),
	}
}

func (s *Stores) heartbeat(ctx context.Context, cli pdpb.PDClient, storeID uint64) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{Header: header(), Stats: s.stat[storeID].Load().(*pdpb.StoreStats)})
}

func (s *Stores) update(rs *Regions) {
	stats := make([]*pdpb.StoreStats, len(s.stat))
	now := uint64(time.Now().Unix())
	for i := range stats {
		stats[i] = &pdpb.StoreStats{
			StoreId:    uint64(i),
			Capacity:   capacity,
			Available:  capacity,
			QueryStats: &pdpb.QueryStats{},
			PeerStats:  make([]*pdpb.PeerStat, 0),
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now - storeReportInterval,
				EndTimestamp:   now,
			},
		}
	}
	var toUpdate []*pdpb.RegionHeartbeatRequest
	updatedRegions := rs.awakenRegions.Load()
	if updatedRegions == nil {
		toUpdate = rs.regions
	} else {
		toUpdate = updatedRegions.([]*pdpb.RegionHeartbeatRequest)
	}
	for _, region := range toUpdate {
		for _, peer := range region.Region.Peers {
			store := stats[peer.StoreId]
			store.UsedSize += region.ApproximateSize
			store.Available -= region.ApproximateSize
			store.RegionCount += 1
		}
		store := stats[region.Leader.StoreId]
		if region.BytesWritten != 0 {
			store.BytesWritten += region.BytesWritten
			store.BytesRead += region.BytesRead
			store.KeysWritten += region.KeysWritten
			store.KeysRead += region.KeysRead
			store.QueryStats.Get += region.QueryStats.Get
			store.QueryStats.Put += region.QueryStats.Put
			store.PeerStats = append(store.PeerStats, &pdpb.PeerStat{
				RegionId:     region.Region.Id,
				ReadKeys:     region.KeysRead,
				ReadBytes:    region.BytesRead,
				WrittenKeys:  region.KeysWritten,
				WrittenBytes: region.BytesWritten,
				QueryStats:   region.QueryStats,
			})
		}
	}
	for i := range stats {
		s.stat[i].Store(stats[i])
	}
}
