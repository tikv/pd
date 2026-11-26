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
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
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
	keyspaceCache *keyspace.BoundaryCache

	// watcher is used to watch the keyspace meta changes in etcd.
	watcher *etcdutil.LoopWatcher
}

// NewWatcher creates a new Watcher instance.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	checkerController *checker.Controller,
	keyspaceCache *keyspace.BoundaryCache) (*Watcher, error) {
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
	var (
		staged   []*keyspacepb.KeyspaceMeta
		batchErr error
	)
	readEvent := func(kv *mvccpb.KeyValue, deleted bool) (*keyspacepb.KeyspaceMeta, error) {
		keyspaceID, err := rw.extractKeyspaceIDFromMetaKey(string(kv.Key))
		if err != nil {
			return nil, err
		}
		// Out-of-range IDs cannot contribute a boundary. A PD creation
		// attempt can write one before rolling its metadata back.
		if keyspaceID > constant.MaxValidKeyspaceID {
			return nil, nil
		}
		if deleted {
			return &keyspacepb.KeyspaceMeta{
				Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: keyspaceID},
				State:    keyspacepb.KeyspaceState_TOMBSTONE,
			}, nil
		}
		meta, err := keyspace.NewKeyspaceMeta(string(kv.Value))
		if err != nil {
			return nil, err
		}
		keyspaceRef := meta.GetKeyspace()
		_, hasID := keyspaceRef.(*keyspacepb.KeyspaceMeta_Id)
		// Before the oneof migration, proto3 omitted the zero ID from DEFAULT.
		legacyDefault := keyspaceRef == nil && keyspaceID == constant.DefaultKeyspaceID &&
			meta.GetName() == constant.DefaultKeyspaceName
		if (!hasID && !legacyDefault) || meta.GetId() != keyspaceID {
			return nil, fmt.Errorf("keyspace ID in meta does not match path: meta ID %d, path ID %d", meta.GetId(), keyspaceID)
		}
		return meta, nil
	}
	stage := func(kv *mvccpb.KeyValue, deleted bool) error {
		meta, err := readEvent(kv, deleted)
		if err != nil {
			batchErr = err
			return err
		}
		if meta != nil {
			staged = append(staged, meta)
		}
		return nil
	}
	enqueueSuspectRanges := func() {
		for _, meta := range staged {
			bound := keyspace.MakeRegionBound(meta.GetId())
			rw.checkerController.AddSuspectKeyRange(bound.RawLeftBound, bound.RawRightBound)
			rw.checkerController.AddSuspectKeyRange(bound.TxnLeftBound, bound.TxnRightBound)
		}
		staged = nil
	}
	preEventsFn := func([]*clientv3.Event) error {
		staged, batchErr = nil, nil
		return nil
	}
	postEventsFn := func([]*clientv3.Event) error {
		if batchErr != nil {
			rw.keyspaceCache.Invalidate()
			// Atomic load callbacks recover failed batches with the same
			// snapshot retry and backoff used for compaction.
			return batchErr
		}
		if len(staged) != 0 {
			rw.keyspaceCache.Apply(staged)
			enqueueSuspectRanges()
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
		func(kv *mvccpb.KeyValue) error { return stage(kv, false) },
		func(kv *mvccpb.KeyValue) error { return stage(kv, true) },
		postEventsFn,
		true, /* withPrefix */
	)
	rw.watcher.SetReloadOnCompaction()
	rw.watcher.SetAtomicLoadCallbacks()
	rw.watcher.SetLoadCallbacks(
		rw.keyspaceCache.Invalidate,
		func(complete bool) {
			if complete {
				rw.keyspaceCache.Replace(staged)
				enqueueSuspectRanges()
			} else {
				staged = nil
			}
		},
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
