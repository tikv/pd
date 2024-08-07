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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Watcher is used to watch the PD API server for any meta changes.
type Watcher struct {
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	clusterID uint64
	// storePathPrefix is the path of the store in etcd:
	//  - Key: /pd/{cluster_id}/raft/s/
	//  - Value: meta store proto.
	storePathPrefix string

	etcdClient   *clientv3.Client
	basicCluster *core.BasicCluster
	storeWatcher *etcdutil.LoopWatcher
}

// NewWatcher creates a new watcher to watch the meta change from PD API server.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	clusterID uint64,
	basicCluster *core.BasicCluster,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &Watcher{
		ctx:             ctx,
		cancel:          cancel,
		clusterID:       clusterID,
		storePathPrefix: endpoint.StorePathPrefix(clusterID),
		etcdClient:      etcdClient,
		basicCluster:    basicCluster,
	}
	err := w.initializeStoreWatcher()
	if err != nil {
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
			w.basicCluster.PutStore(origin.Clone(core.SetStoreMeta(store)))
		}

		if store.GetNodeState() == metapb.NodeState_Removed {
			statistics.ResetStoreStatistics(store.GetAddress(), strconv.FormatUint(store.GetId(), 10))
			// TODO: remove hot stats
		}

		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		storeID, err := endpoint.ExtractStoreIDFromPath(w.clusterID, key)
		if err != nil {
			return err
		}
		origin := w.basicCluster.GetStore(storeID)
		if origin != nil {
			w.basicCluster.DeleteStore(origin)
			log.Info("delete store meta", zap.Uint64("store-id", storeID))
		}
		return nil
	}
	w.storeWatcher = etcdutil.NewLoopWatcher(
		w.ctx, &w.wg,
		w.etcdClient,
		"scheduling-store-watcher", w.storePathPrefix,
		func([]*clientv3.Event) error { return nil },
		putFn, deleteFn,
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	w.storeWatcher.StartWatchLoop()
	return w.storeWatcher.WaitLoad()
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
