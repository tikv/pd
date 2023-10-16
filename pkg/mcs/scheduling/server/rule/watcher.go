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
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

// Watcher is used to watch the PD API server for any Placement Rule changes.
type Watcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// rulesPathPrefix:
	//   - Key: /pd/{cluster_id}/rules/{group_id}-{rule_id}
	//   - Value: placement.Rule
	rulesPathPrefix string
	// ruleGroupPathPrefix:
	//   - Key: /pd/{cluster_id}/rule_group/{group_id}
	//   - Value: placement.RuleGroup
	ruleGroupPathPrefix string
	// regionLabelPathPrefix:
	//   - Key: /pd/{cluster_id}/region_label/{rule_id}
	//  - Value: labeler.LabelRule
	regionLabelPathPrefix string

	etcdClient  *clientv3.Client
	ruleStorage endpoint.RuleStorage

	// checkerController is used to add the suspect key ranges to the checker when the rule changed.
	checkerController atomic.Value
	// ruleManager is used to manage the placement rules.
	ruleManager atomic.Value
	// regionLabeler is used to manage the region label rules.
	regionLabeler atomic.Value

	ruleWatcher  *etcdutil.LoopWatcher
	groupWatcher *etcdutil.LoopWatcher
	labelWatcher *etcdutil.LoopWatcher
}

// NewWatcher creates a new watcher to watch the Placement Rule change from PD API server.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	clusterID uint64,
	ruleStorage endpoint.RuleStorage,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                   ctx,
		cancel:                cancel,
		rulesPathPrefix:       endpoint.RulesPathPrefix(clusterID),
		ruleGroupPathPrefix:   endpoint.RuleGroupPathPrefix(clusterID),
		regionLabelPathPrefix: endpoint.RegionLabelPathPrefix(clusterID),
		etcdClient:            etcdClient,
		ruleStorage:           ruleStorage,
	}
	err := rw.initializeRuleWatcher()
	if err != nil {
		return nil, err
	}
	err = rw.initializeGroupWatcher()
	if err != nil {
		return nil, err
	}
	err = rw.initializeRegionLabelWatcher()
	if err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *Watcher) initializeRuleWatcher() error {
	prefixToTrim := rw.rulesPathPrefix + "/"
	putFn := func(kv *mvccpb.KeyValue) error {
		key, value := string(kv.Key), string(kv.Value)
		log.Info("update placement rule", zap.String("key", key), zap.String("value", value))
		rm := rw.getRuleManager()
		// If the rule manager is not set, it means that the cluster is not initialized yet,
		// we should save the rule to the storage directly first.
		if rm == nil {
			// Since the PD API server will validate the rule before saving it to etcd,
			// so we could directly save the string rule in JSON to the storage here.
			return rw.ruleStorage.SaveRuleJSON(
				strings.TrimPrefix(key, prefixToTrim),
				value,
			)
		}
		rule, err := placement.NewRuleFromJSON(kv.Value)
		if err != nil {
			return err
		}
		// Update the suspect key ranges in the checker.
		rw.getCheckerController().AddSuspectKeyRange(rule.StartKey, rule.EndKey)
		if oldRule := rm.GetRule(rule.GroupID, rule.ID); oldRule != nil {
			rw.getCheckerController().AddSuspectKeyRange(oldRule.StartKey, oldRule.EndKey)
		}
		return rm.SetRule(rule)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete placement rule", zap.String("key", key))
		rm := rw.getRuleManager()
		trimmedKey := strings.TrimPrefix(key, prefixToTrim)
		if rm == nil {
			return rw.ruleStorage.DeleteRule(trimmedKey)
		}
		ruleJSON, err := rw.ruleStorage.LoadRule(trimmedKey)
		if err != nil {
			return err
		}
		rule, err := placement.NewRuleFromJSON([]byte(ruleJSON))
		if err != nil {
			return err
		}
		rw.getCheckerController().AddSuspectKeyRange(rule.StartKey, rule.EndKey)
		return rm.DeleteRule(rule.GroupID, rule.ID)
	}
	postEventFn := func() error {
		return nil
	}
	rw.ruleWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-rule-watcher", rw.rulesPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	rw.ruleWatcher.StartWatchLoop()
	return rw.ruleWatcher.WaitLoad()
}

func (rw *Watcher) initializeGroupWatcher() error {
	prefixToTrim := rw.ruleGroupPathPrefix + "/"
	putFn := func(kv *mvccpb.KeyValue) error {
		key, value := string(kv.Key), string(kv.Value)
		log.Info("update placement rule group", zap.String("key", key), zap.String("value", value))
		rm := rw.getRuleManager()
		if rm == nil {
			return rw.ruleStorage.SaveRuleGroupJSON(
				strings.TrimPrefix(key, prefixToTrim),
				value,
			)
		}
		ruleGroup, err := placement.NewRuleGroupFromJSON(kv.Value)
		if err != nil {
			return err
		}
		return rm.SetRuleGroup(ruleGroup)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete placement rule group", zap.String("key", key))
		rm := rw.getRuleManager()
		trimmedKey := strings.TrimPrefix(key, prefixToTrim)
		if rm == nil {
			return rw.ruleStorage.DeleteRuleGroup(trimmedKey)
		}
		return rm.DeleteRuleGroup(trimmedKey)
	}
	// Trigger the rule manager to reload the rule groups.
	postEventFn := func() error {
		return nil
	}
	rw.groupWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-rule-group-watcher", rw.ruleGroupPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	rw.groupWatcher.StartWatchLoop()
	return rw.groupWatcher.WaitLoad()
}

func (rw *Watcher) initializeRegionLabelWatcher() error {
	prefixToTrim := rw.regionLabelPathPrefix + "/"
	putFn := func(kv *mvccpb.KeyValue) error {
		key, value := string(kv.Key), string(kv.Value)
		log.Info("update region label rule", zap.String("key", key), zap.String("value", value))
		rl := rw.getRegionLabeler()
		if rl == nil {
			return rw.ruleStorage.SaveRegionRuleJSON(
				strings.TrimPrefix(key, prefixToTrim),
				value,
			)
		}
		rule, err := labeler.NewLabelRuleFromJSON(kv.Value)
		if err != nil {
			return err
		}
		return rl.SetLabelRule(rule)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete region label rule", zap.String("key", key))
		rl := rw.getRegionLabeler()
		trimmedKey := strings.TrimPrefix(key, prefixToTrim)
		if rl == nil {
			return rw.ruleStorage.DeleteRegionRule(trimmedKey)
		}
		return rl.DeleteLabelRule(trimmedKey)
	}
	postEventFn := func() error {
		return nil
	}
	rw.labelWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-region-label-watcher", rw.regionLabelPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	rw.labelWatcher.StartWatchLoop()
	return rw.labelWatcher.WaitLoad()
}

// Close closes the watcher.
func (rw *Watcher) Close() {
	rw.cancel()
	rw.wg.Wait()
}

// SetClusterComponents sets the cluster components for the watcher.
func (rw *Watcher) SetClusterComponents(
	sc *checker.Controller,
	rm *placement.RuleManager,
	rl *labeler.RegionLabeler,
) {
	rw.checkerController.Store(sc)
	rw.ruleManager.Store(rm)
	rw.regionLabeler.Store(rl)
}

func (rw *Watcher) getCheckerController() *checker.Controller {
	cc := rw.checkerController.Load()
	if cc == nil {
		return nil
	}
	return cc.(*checker.Controller)
}

func (rw *Watcher) getRuleManager() *placement.RuleManager {
	rm := rw.ruleManager.Load()
	if rm == nil {
		return nil
	}
	return rm.(*placement.RuleManager)
}

func (rw *Watcher) getRegionLabeler() *labeler.RegionLabeler {
	rl := rw.regionLabeler.Load()
	if rl == nil {
		return nil
	}
	return rl.(*labeler.RegionLabeler)
}
