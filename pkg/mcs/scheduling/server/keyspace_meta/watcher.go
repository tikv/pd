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

package keyspace_meta

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// Watcher is used to watch the PD for any keyspace meta changes.
type Watcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// keyspaceMetaPathPrefix:
	//   - Key: /pd/{cluster_id}/keyspaces/meta/{keyspace_id}
	//   - Value: keyspace.KeyspaceMeta
	keyspaceMetaPathPrefix string

	etcdClient *clientv3.Client

	// checkerController is used to add the suspect key ranges to the checker when the rule changed.
	checkerController *checker.Controller
	// keyspaceCache is used to cache keyspace metadata from watch events.
	keyspaceCache *keyspace.Cache

	// watcher is used to watch the keyspace meta changes in etcd.
	watcher *etcdutil.LoopWatcher
}

// NewWatcher creates a new Watcher instance.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	checkerController *checker.Controller,
	keyspaceCache *keyspace.Cache) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                    ctx,
		cancel:                 cancel,
		keyspaceMetaPathPrefix: keypath.KeyspaceMetaPrefix(),
		etcdClient:             etcdClient,
		checkerController:      checkerController,
		keyspaceCache:          keyspaceCache,
	}
	err := rw.initializeKeyspaceMetaWatcher()
	if err != nil {
		rw.Close()
		return nil, err
	}
	return rw, nil
}

func (rw *Watcher) initializeKeyspaceMetaWatcher() error {
	suspectKeyRanges := &keyutil.KeyRanges{}
	preEventsFn := func([]*clientv3.Event) error {
		suspectKeyRanges.Clean()
		return nil
	}
	putFn := func(kv *mvccpb.KeyValue) error {
		log.Info("update keyspace meta", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		keyspaceID, err := rw.extractKeyspaceIDFromMetaKey(string(kv.Key))
		if err != nil {
			return err
		}
		if rw.keyspaceCache != nil {
			meta, err := keyspace.NewKeyspaceMeta(string(kv.Value))
			if err != nil {
				return err
			}
			if meta.Id != keyspaceID {
				return fmt.Errorf("keyspace ID in meta does not match the one in key, meta Id: %d, keyspace ID: %d", meta.Id, keyspaceID)
			}
			rw.keyspaceCache.Save(meta.Id, meta.Name, meta.State)
		}
		bound := keyspace.MakeRegionBound(keyspaceID)
		suspectKeyRanges.Append(bound.RawLeftBound, bound.RawRightBound)
		suspectKeyRanges.Append(bound.TxnLeftBound, bound.TxnRightBound)
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		log.Info("delete keyspace meta", zap.String("key", string(kv.Key)))
		keyspaceID, err := rw.extractKeyspaceIDFromMetaKey(string(kv.Key))
		if err != nil {
			return err
		}
		if rw.keyspaceCache != nil {
			rw.keyspaceCache.DeleteKeyspace(keyspaceID)
		}
		bound := keyspace.MakeRegionBound(keyspaceID)
		suspectKeyRanges.Append(bound.RawLeftBound, bound.RawRightBound)
		suspectKeyRanges.Append(bound.TxnLeftBound, bound.TxnRightBound)
		return nil
	}
	postEventsFn := func([]*clientv3.Event) error {
		if rw.checkerController == nil {
			return errors.New("checkerController is nil, cannot add suspect key ranges")
		}
		for _, kr := range suspectKeyRanges.Ranges() {
			rw.checkerController.AddSuspectKeyRange(kr.StartKey, kr.EndKey)
		}
		return nil
	}
	rw.watcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-keyspace-meta-watcher",
		// To keep the consistency with the previous code, we should trim the suffix `/`.
		rw.keyspaceMetaPathPrefix,
		preEventsFn,
		putFn, deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	rw.watcher.StartWatchLoop()
	return rw.watcher.WaitLoad()
}

func (rw *Watcher) extractKeyspaceIDFromMetaKey(key string) (uint32, error) {
	idStr := strings.TrimPrefix(key, rw.keyspaceMetaPathPrefix)
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse keyspace id from key %q: %w", key, err)
	}
	return uint32(id), nil
}

// Close closes the watcher.
func (rw *Watcher) Close() {
	rw.cancel()
	rw.wg.Wait()
}
