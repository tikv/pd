// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// Watcher is used to watch the affinity group and label rule changes from PD.
// It watches two paths:
// affinityGroupsPrefix:
//   - Key: /pd/{cluster_id}/affinity_groups/{group_id}
//   - Value: affinity.Group
//
// regionLabelPathPrefix:
//   - Key:
//   - Value: labeler.LabelRule
//   - Filtered to only process rules with ID prefix "affinity_group/"
type Watcher struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	etcdClient *clientv3.Client

	// affinityManager is used to manage the affinity groups in the scheduling server.
	affinityManager *affinity.Manager

	groupWatcher *etcdutil.LoopWatcher
	labelWatcher *etcdutil.LoopWatcher
}

// NewWatcher creates a new watcher to watch the affinity changes from PD.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	affinityManager *affinity.Manager,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &Watcher{
		ctx:             ctx,
		cancel:          cancel,
		etcdClient:      etcdClient,
		affinityManager: affinityManager,
	}
	err := w.initializeGroupWatcher()
	if err != nil {
		w.Close()
		return nil, err
	}
	err = w.initializeAffinityLabelWatcher()
	if err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

// initializeGroupWatcher initializes the watcher for affinity group changes.
func (w *Watcher) initializeGroupWatcher() error {
	putFn := func(kv *mvccpb.KeyValue) error {
		log.Info("update affinity group", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		group := &affinity.Group{}
		if err := json.Unmarshal(kv.Value, group); err != nil {
			log.Error("failed to unmarshal affinity group", zap.String("key", string(kv.Key)), zap.Error(err))
			return err
		}
		w.affinityManager.SyncGroupFromEtcd(group)
		return nil
	}

	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete affinity group", zap.String("key", key))
		groupID := strings.TrimPrefix(key, keypath.AffinityGroupsPrefix())
		w.affinityManager.SyncGroupDeleteFromEtcd(groupID)
		return nil
	}

	w.groupWatcher = etcdutil.NewLoopWatcher(
		w.ctx, &w.wg,
		w.etcdClient,
		"scheduling-affinity-group-watcher",
		strings.TrimSuffix(keypath.AffinityGroupsPrefix(), "/"),
		func([]*clientv3.Event) error { return nil },
		putFn, deleteFn,
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	w.groupWatcher.StartWatchLoop()
	return w.groupWatcher.WaitLoad()
}

// initializeAffinityLabelWatcher initializes the watcher for affinity label rule changes.
// It watches the region_label path but only processes rules with affinity group prefix.
func (w *Watcher) initializeAffinityLabelWatcher() error {
	// Note: labelWatcher does not need preEventsFn/postEventsFn locking
	// because the sync methods will handle locking internally
	preEventsFn := func([]*clientv3.Event) error {
		return nil
	}

	putFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("update affinity label rule", zap.String("key", key), zap.String("value", string(kv.Value)))
		rule, err := labeler.NewLabelRuleFromJSON(kv.Value)
		if err != nil {
			log.Error("failed to unmarshal affinity label rule", zap.String("key", key), zap.Error(err))
			return err
		}
		return w.affinityManager.SyncKeyRangesFromEtcd(rule)
	}

	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete affinity label rule", zap.String("key", key))
		ruleID := strings.TrimPrefix(key, keypath.RegionLabelPathPrefix())
		w.affinityManager.SyncKeyRangesDeleteFromEtcd(ruleID)
		return nil
	}

	postEventsFn := func([]*clientv3.Event) error {
		return nil
	}

	w.labelWatcher = etcdutil.NewLoopWatcher(
		w.ctx, &w.wg,
		w.etcdClient,
		"scheduling-affinity-label-watcher",
		strings.TrimSuffix(keypath.RegionLabelKeyPath(affinity.LabelRuleIDPrefix), "/"), // Filter: only process affinity group label rules
		preEventsFn,
		putFn, deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	w.labelWatcher.StartWatchLoop()
	return w.labelWatcher.WaitLoad()
}

// Close closes the watcher.
func (w *Watcher) Close() {
	w.cancel()
	w.wg.Wait()
}
