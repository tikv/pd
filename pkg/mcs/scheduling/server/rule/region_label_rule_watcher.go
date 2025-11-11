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

package rule

import (
	"context"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// RegionLabelRuleWatcher is used to watch the PD for any Region Label Rule changes.
type RegionLabelRuleWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// regionLabelPathPrefix:
	//   - Key: /pd/{cluster_id}/region_label/{rule_id}
	//  - Value: labeler.LabelRule
	regionLabelPathPrefix string

	etcdClient *clientv3.Client

	// regionLabeler is used to manage the region label rules.
	regionLabeler *labeler.RegionLabeler

	labelWatcher *etcdutil.LoopWatcher
}

// NewRegionLabelRuleWatcher creates a new watcher to watch the Region Label Rule change from PD.
func NewRegionLabelRuleWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	regionLabeler *labeler.RegionLabeler,
) (*RegionLabelRuleWatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	rw := &RegionLabelRuleWatcher{
		ctx:                   ctx,
		cancel:                cancel,
		regionLabelPathPrefix: keypath.RegionLabelPathPrefix(),
		etcdClient:            etcdClient,
		regionLabeler:         regionLabeler,
	}
	err := rw.initializeWatcher()
	if err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *RegionLabelRuleWatcher) initializeWatcher() error {
	// TODO: use txn in region labeler.
	preEventsFn := func([]*clientv3.Event) error {
		// It will be locked until the postEventsFn is finished.
		rw.regionLabeler.Lock()
		return nil
	}
	putFn := func(kv *mvccpb.KeyValue) error {
		log.Debug("update region label rule", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		rule, err := labeler.NewLabelRuleFromJSON(kv.Value)
		if err != nil {
			return err
		}
		return rw.regionLabeler.SetLabelRuleLocked(rule)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Debug("delete region label rule", zap.String("key", key))
		return rw.regionLabeler.DeleteLabelRuleLocked(strings.TrimPrefix(key, rw.regionLabelPathPrefix))
	}
	postEventsFn := func([]*clientv3.Event) error {
		defer rw.regionLabeler.Unlock()
		rw.regionLabeler.BuildRangeListLocked()
		return nil
	}
	rw.labelWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-region-label-watcher",
		// To keep the consistency with the previous code, we should trim the suffix `/`.
		strings.TrimSuffix(rw.regionLabelPathPrefix, "/"),
		preEventsFn,
		putFn, deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	rw.labelWatcher.StartWatchLoop()
	return rw.labelWatcher.WaitLoad()
}

// Close closes the watcher.
func (rw *RegionLabelRuleWatcher) Close() {
	rw.cancel()
	rw.wg.Wait()
}
