// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	slowStoreEvictedThreshold   = 100
	slowStoreRecoveredThreshold = 0
)

type slowStoreDetector struct {
	cluster      *RaftCluster
	evictedStore *core.StoreInfo
}

func newSlowStoreDetector(cluster *RaftCluster) *slowStoreDetector {
	return &slowStoreDetector{
		cluster: cluster,
	}
}

func (d *slowStoreDetector) detectSlowStore(stores map[uint64]*core.StoreInfo) {
	if d.evictedStore != nil {
		store, ok := stores[d.evictedStore.GetID()]
		if !ok || store.IsTombstone() {
			// Previous slow store had been removed, remove the sheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Stringer("store", store.GetMeta()))
		} else if store.GetSlowScore() == slowStoreRecoveredThreshold {
			log.Info("slow store has been recovered",
				zap.Stringer("store", store.GetMeta()))
			// Recover leader weight and region weight.
			d.cluster.SetStoreWeight(d.evictedStore.GetID(), d.evictedStore.GetLeaderWeight(), d.evictedStore.GetRegionWeight())
		}
	} else {
		slowStores := make([]*core.StoreInfo, 0)
		for _, store := range stores {
			if store.IsTombstone() {
				continue
			}

			if store.IsUp() && store.IsSlow() {
				slowStores = append(slowStores, store)
			}
		}

		if len(slowStores) == 1 {
			// TODO: add evict leader scheduler.
			store := slowStores[0]
			if store.GetSlowScore() == slowStoreEvictedThreshold {
				d.evictedStore = store.ShallowClone()
				d.cluster.SetStoreWeight(store.GetID(), 0, store.GetRegionWeight())
				log.Info("detected slow store, set leader weight to 0",
					zap.Stringer("store", store.GetMeta()))
			}
		} else if len(slowStores) > 1 {
			// TODO: alert user here or another place.
			storeIds := make([]uint64, len(slowStores))
			for _, store := range slowStores {
				storeIds = append(storeIds, store.GetID())
			}
			log.Info("detected slow stores", zap.Reflect("stores", storeIds))
		}
	}
}
