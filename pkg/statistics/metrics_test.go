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

package statistics

import (
	"strconv"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
)

func TestDeleteClusterStatusMetrics(t *testing.T) {
	re := require.New(t)
	store := core.NewStoreInfo(&metapb.Store{Id: 9876543210})
	other := core.NewStoreInfo(&metapb.Store{Id: 9876543211})
	id := strconv.FormatUint(store.GetID(), 10)
	otherID := strconv.FormatUint(other.GetID(), 10)

	clusterStatusGauge.WithLabelValues(clusterStatusStoreTombstoneCount, id).Set(1)
	clusterStatusGauge.WithLabelValues(clusterStatusStorageSize, id).Set(1)
	clusterStatusGauge.WithLabelValues(clusterStatusStoreTombstoneCount, otherID).Set(1)
	DeleteClusterStatusMetrics(store)

	re.False(clusterStatusGauge.DeleteLabelValues(clusterStatusStoreTombstoneCount, id))
	re.False(clusterStatusGauge.DeleteLabelValues(clusterStatusStorageSize, id))
	re.True(clusterStatusGauge.DeleteLabelValues(clusterStatusStoreTombstoneCount, otherID))
}

type storeMetricsTestConfig struct {
	config.ConfProvider
	beforeObserve func()
}

func (c *storeMetricsTestConfig) GetLocationLabels() []string {
	if c.beforeObserve != nil {
		c.beforeObserve()
		c.beforeObserve = nil
	}
	return c.ConfProvider.GetLocationLabels()
}

func TestObserveStoresAfterDeletion(t *testing.T) {
	for _, duringCollection := range []bool{false, true} {
		name := "after collection"
		if duringCollection {
			name = "after snapshot"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			store := core.NewStoreInfo(&metapb.Store{Id: 9876543210, NodeState: metapb.NodeState_Removed})
			other := core.NewStoreInfo(&metapb.Store{Id: 9876543211, NodeState: metapb.NodeState_Removed})
			cluster.PutStore(store)
			cluster.PutStore(other)
			t.Cleanup(func() {
				for _, s := range []*core.StoreInfo{store, other} {
					DeleteClusterStatusMetrics(s)
					ResetStoreStatistics(s.GetAddress(), strconv.FormatUint(s.GetID(), 10))
				}
			})
			opt := &storeMetricsTestConfig{ConfProvider: mockconfig.NewTestOptions()}
			stats := NewStoresStats()
			NewStoreStatisticsMap(opt).ObserveStores(cluster, stats)
			remove := func() {
				cluster.DeleteStore(store)
				DeleteClusterStatusMetrics(store)
			}
			if duringCollection {
				// GetLocationLabels runs inside Observe, after GetStores took its snapshot.
				opt.beforeObserve = remove
			}
			NewStoreStatisticsMap(opt).ObserveStores(cluster, stats)
			if !duringCollection {
				remove()
			}
			re.Nil(cluster.GetStore(store.GetID()))
			id := strconv.FormatUint(store.GetID(), 10)
			otherID := strconv.FormatUint(other.GetID(), 10)
			re.False(clusterStatusGauge.DeleteLabelValues(clusterStatusStoreTombstoneCount, id))
			re.False(clusterStatusGauge.DeleteLabelValues(clusterStatusStorageSize, id))
			// A tombstone that is still in the cluster must retain its metrics.
			re.True(clusterStatusGauge.DeleteLabelValues(clusterStatusStoreTombstoneCount, otherID))
			re.True(clusterStatusGauge.DeleteLabelValues(clusterStatusStorageSize, otherID))
		})
	}
}
