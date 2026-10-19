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

package schedule

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestHotRegionsConcurrentWithMetrics(t *testing.T) {
	cluster := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	const storeCount = 16
	for id := uint64(1); id <= storeCount; id++ {
		cluster.AddRegionStore(id, 1)
		cluster.HotStat.GetOrCreateRollingStoreStats(id).Set(&pdpb.StoreStats{
			Interval:     &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10},
			BytesRead:    id * 1000,
			BytesWritten: id * 2000,
		})
	}
	coordinator := NewCoordinator(t.Context(), cluster, nil)
	t.Cleanup(coordinator.Stop)
	t.Cleanup(ResetHotSpotMetrics)

	// Hotspot API calls must keep independent Store snapshots while the
	// periodic metrics collector refreshes its reusable buffer.
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		for range 100 {
			coordinator.CollectHotSpotMetrics()
		}
	})
	for _, rw := range []utils.RWType{utils.Read, utils.Write, utils.Read, utils.Write} {
		wg.Go(func() {
			<-start
			for range 100 {
				infos := coordinator.GetHotRegionsByType(rw)
				assert.Len(t, infos.AsPeer, storeCount)
				for id := uint64(1); id <= storeCount; id++ {
					if stat := infos.AsPeer[id]; assert.NotNil(t, stat) {
						expected := float64(id * 100)
						if rw == utils.Write {
							expected *= 2
						}
						assert.Equal(t, expected, stat.StoreByteRate)
					}
				}
			}
		})
	}
	close(start)
	wg.Wait()
}
