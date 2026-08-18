// Copyright 2023 TiKV Project Authors.
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

package meta

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// Watcher is used to watch the PD for any meta changes.
type Watcher struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	etcdClient   *clientv3.Client
	basicCluster *core.BasicCluster
	storeWatcher *etcdutil.LoopWatcher

	// onStoreTombstoned is late-bound via SetOnStoreTombstoned once the parent
	// Cluster (which owns hotStat and the scheduling-server-only metrics that this
	// package can't import without a cycle) finishes construction. The watcher
	// itself starts watching before that point, so it's read through an atomic
	// pointer to stay safe against a store event racing the setup.
	onStoreTombstoned atomic.Pointer[func(storeID uint64)]
}

// NewWatcher creates a new watcher to watch the meta change from PD.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	basicCluster *core.BasicCluster,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &Watcher{
		ctx:          ctx,
		cancel:       cancel,
		etcdClient:   etcdClient,
		basicCluster: basicCluster,
	}
	err := w.initializeStoreWatcher()
	if err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *Watcher) initializeStoreWatcher() error {
	putFn := func(kv *mvccpb.KeyValue) error {
		store := &metapb.Store{}
		if err := proto.Unmarshal(kv.Value, store); err != nil {
			log.Warn("failed to unmarshal store entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
		log.Debug("update store meta", zap.Stringer("store", store))
		origin := w.basicCluster.GetStore(store.GetId())
		if origin == nil {
			w.basicCluster.PutStore(core.NewStoreInfo(store))
		} else {
			w.basicCluster.PutStore(origin, core.SetStoreMeta(store))
		}

		if store.GetNodeState() == metapb.NodeState_Removed {
			storeIDStr := strconv.FormatUint(store.GetId(), 10)
			statistics.ResetStoreStatistics(storeIDStr)
			filter.DeleteStoreMetrics(storeIDStr)
			hbstream.DeleteStoreMetrics(storeIDStr)
			schedulers.DeleteStoreMetrics(storeIDStr)
			schedule.DeleteStoreMetrics(storeIDStr)
			scatter.DeleteStoreMetrics(storeIDStr)
			if fn := w.onStoreTombstoned.Load(); fn != nil {
				(*fn)(store.GetId())
			}
		}

		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		storeID, err := keypath.ExtractStoreIDFromPath(key)
		if err != nil {
			return err
		}
		origin := w.basicCluster.GetStore(storeID)
		if origin != nil {
			storeIDStr := strconv.FormatUint(storeID, 10)
			statistics.DeleteClusterStatusMetrics(origin)
			statistics.ResetStoreStatistics(storeIDStr)
			filter.DeleteStoreMetrics(storeIDStr)
			hbstream.DeleteStoreMetrics(storeIDStr)
			schedulers.DeleteStoreMetrics(storeIDStr)
			schedule.DeleteStoreMetrics(storeIDStr)
			scatter.DeleteStoreMetrics(storeIDStr)
			if fn := w.onStoreTombstoned.Load(); fn != nil {
				(*fn)(storeID)
			}
			w.basicCluster.DeleteStore(origin)
			log.Info("delete store meta", zap.Uint64("store-id", storeID))
		}
		return nil
	}
	w.storeWatcher = etcdutil.NewLoopWatcher(
		w.ctx, &w.wg,
		w.etcdClient,
		"scheduling-store-watcher",
		// Watch meta store proto
		keypath.StorePathPrefix(),
		func([]*clientv3.Event) error { return nil },
		putFn, deleteFn,
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	w.storeWatcher.StartWatchLoop()
	return w.storeWatcher.WaitLoad()
}

// SetOnStoreTombstoned sets the callback invoked (at least once) when a store
// transitions to tombstone, for cleanup that only the parent Cluster can do
// without an import cycle (removing rolling hot stats, resetting the
// scheduling-server-owned heartbeat metrics). NewWatcher's initial load runs
// before the caller has a chance to install this callback, so a store already
// tombstoned at startup would otherwise never get it invoked; reconcile against
// whatever the watcher has already loaded here to close that gap.
func (w *Watcher) SetOnStoreTombstoned(fn func(storeID uint64)) {
	w.onStoreTombstoned.Store(&fn)
	for _, store := range w.basicCluster.GetStores() {
		if store.IsRemoved() {
			fn(store.GetID())
		}
	}
}

// Close closes the watcher.
func (w *Watcher) Close() {
	w.cancel()
	w.wg.Wait()
}

// GetStoreWatcher returns the store watcher.
func (w *Watcher) GetStoreWatcher() *etcdutil.LoopWatcher {
	return w.storeWatcher
}
