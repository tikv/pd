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
	checkerController *checker.Controller
	// ruleManager is used to manage the placement rules.
	ruleManager *placement.RuleManager
	// regionLabeler is used to manage the region label rules.
	regionLabeler *labeler.RegionLabeler

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
	checkerController *checker.Controller,
	ruleManager *placement.RuleManager,
	regionLabeler *labeler.RegionLabeler,
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
		checkerController:     checkerController,
		ruleManager:           ruleManager,
		regionLabeler:         regionLabeler,
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
		log.Info("update placement rule", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		rule, err := placement.NewRuleFromJSON(kv.Value)
		if err != nil {
			return err
		}
		// Update the suspect key ranges in the checker.
		rw.checkerController.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
		if oldRule := rw.ruleManager.GetRule(rule.GroupID, rule.ID); oldRule != nil {
			rw.checkerController.AddSuspectKeyRange(oldRule.StartKey, oldRule.EndKey)
		}
		return rw.ruleManager.SetRule(rule)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete placement rule", zap.String("key", key))
		ruleJSON, err := rw.ruleStorage.LoadRule(strings.TrimPrefix(key, prefixToTrim))
		if err != nil {
			return err
		}
		rule, err := placement.NewRuleFromJSON([]byte(ruleJSON))
		if err != nil {
			return err
		}
		rw.checkerController.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
		return rw.ruleManager.DeleteRule(rule.GroupID, rule.ID)
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
		log.Info("update placement rule group", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		ruleGroup, err := placement.NewRuleGroupFromJSON(kv.Value)
		if err != nil {
			return err
		}
		// Add all rule key ranges within the group to the suspect key ranges.
		for _, rule := range rw.ruleManager.GetRulesByGroup(ruleGroup.ID) {
			rw.checkerController.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
		}
		return rw.ruleManager.SetRuleGroup(ruleGroup)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete placement rule group", zap.String("key", key))
		trimmedKey := strings.TrimPrefix(key, prefixToTrim)
		for _, rule := range rw.ruleManager.GetRulesByGroup(trimmedKey) {
			rw.checkerController.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
		}
		return rw.ruleManager.DeleteRuleGroup(trimmedKey)
	}
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
		log.Info("update region label rule", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		rule, err := labeler.NewLabelRuleFromJSON(kv.Value)
		if err != nil {
			return err
		}
		return rw.regionLabeler.SetLabelRule(rule)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete region label rule", zap.String("key", key))
		return rw.regionLabeler.DeleteLabelRule(strings.TrimPrefix(key, prefixToTrim))
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
