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

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// Watcher is used to watch the PD API server for any Placement Rule changes.
type Watcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// ruleCommonPathPrefix:
	//  - Key: /pd/{cluster_id}/rule
	//  - Value: placement.Rule or placement.RuleGroup
	ruleCommonPathPrefix string
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
	labelWatcher *etcdutil.LoopWatcher

	// patch is used to cache the placement rule changes.
	patch *placement.RuleConfigPatch
}

// NewWatcher creates a new watcher to watch the Placement Rule change from PD API server.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	ruleStorage endpoint.RuleStorage,
	checkerController *checker.Controller,
	ruleManager *placement.RuleManager,
	regionLabeler *labeler.RegionLabeler,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                   ctx,
		cancel:                cancel,
		rulesPathPrefix:       keypath.RulesPathPrefix(),
		ruleCommonPathPrefix:  keypath.RuleCommonPathPrefix(),
		ruleGroupPathPrefix:   keypath.RuleGroupPathPrefix(),
		regionLabelPathPrefix: keypath.RegionLabelPathPrefix(),
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
	err = rw.initializeRegionLabelWatcher()
	if err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *Watcher) initializeRuleWatcher() error {
	var suspectKeyRanges *core.KeyRanges

	preEventsFn := func([]*clientv3.Event) error {
		// It will be locked until the postEventsFn is finished.
		rw.ruleManager.Lock()
		rw.patch = rw.ruleManager.BeginPatch()
		suspectKeyRanges = &core.KeyRanges{}
		return nil
	}

	putFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if strings.HasPrefix(key, rw.rulesPathPrefix) {
			log.Info("update placement rule", zap.String("key", key), zap.String("value", string(kv.Value)))
			rule, err := placement.NewRuleFromJSON(kv.Value)
			if err != nil {
				return err
			}
			// Try to add the rule change to the patch.
			if err := rw.ruleManager.AdjustRule(rule, ""); err != nil {
				return err
			}
			rw.patch.SetRule(rule)
			// Update the suspect key ranges in lock.
			suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			if oldRule := rw.ruleManager.GetRuleLocked(rule.GroupID, rule.ID); oldRule != nil {
				suspectKeyRanges.Append(oldRule.StartKey, oldRule.EndKey)
			}
			return nil
		} else if strings.HasPrefix(key, rw.ruleGroupPathPrefix) {
			log.Info("update placement rule group", zap.String("key", key), zap.String("value", string(kv.Value)))
			ruleGroup, err := placement.NewRuleGroupFromJSON(kv.Value)
			if err != nil {
				return err
			}
			// Try to add the rule group change to the patch.
			rw.patch.SetGroup(ruleGroup)
			// Update the suspect key ranges
			for _, rule := range rw.ruleManager.GetRulesByGroupLocked(ruleGroup.ID) {
				suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			}
			return nil
		}
		log.Warn("unknown key when updating placement rule", zap.String("key", key))
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if strings.HasPrefix(key, rw.rulesPathPrefix) {
			log.Info("delete placement rule", zap.String("key", key))
			ruleJSON, err := rw.ruleStorage.LoadRule(strings.TrimPrefix(key, rw.rulesPathPrefix+"/"))
			if err != nil {
				return err
			}
			rule, err := placement.NewRuleFromJSON([]byte(ruleJSON))
			if err != nil {
				return err
			}
			// Try to add the rule change to the patch.
			rw.patch.DeleteRule(rule.GroupID, rule.ID)
			// Update the suspect key ranges
			suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			return err
		} else if strings.HasPrefix(key, rw.ruleGroupPathPrefix) {
			log.Info("delete placement rule group", zap.String("key", key))
			trimmedKey := strings.TrimPrefix(key, rw.ruleGroupPathPrefix+"/")
			// Try to add the rule group change to the patch.
			rw.patch.DeleteGroup(trimmedKey)
			// Update the suspect key ranges
			for _, rule := range rw.ruleManager.GetRulesByGroupLocked(trimmedKey) {
				suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			}
			return nil
		}
		log.Warn("unknown key when deleting placement rule", zap.String("key", key))
		return nil
	}
	postEventsFn := func([]*clientv3.Event) error {
		defer rw.ruleManager.Unlock()
		if err := rw.ruleManager.TryCommitPatchLocked(rw.patch); err != nil {
			log.Error("failed to commit patch", zap.Error(err))
			return err
		}
		for _, kr := range suspectKeyRanges.Ranges() {
			rw.checkerController.AddSuspectKeyRange(kr.StartKey, kr.EndKey)
		}
		return nil
	}
	rw.ruleWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-rule-watcher", rw.ruleCommonPathPrefix,
		preEventsFn,
		putFn, deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	rw.ruleWatcher.StartWatchLoop()
	return rw.ruleWatcher.WaitLoad()
}

func (rw *Watcher) initializeRegionLabelWatcher() error {
	prefixToTrim := rw.regionLabelPathPrefix + "/"
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
		log.Info("delete region label rule", zap.String("key", key))
		return rw.regionLabeler.DeleteLabelRuleLocked(strings.TrimPrefix(key, prefixToTrim))
	}
	postEventsFn := func([]*clientv3.Event) error {
		defer rw.regionLabeler.Unlock()
		rw.regionLabeler.BuildRangeListLocked()
		return nil
	}
	rw.labelWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-region-label-watcher", rw.regionLabelPathPrefix,
		preEventsFn,
		putFn, deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	rw.labelWatcher.StartWatchLoop()
	return rw.labelWatcher.WaitLoad()
}

// Close closes the watcher.
func (rw *Watcher) Close() {
	rw.cancel()
	rw.wg.Wait()
}
