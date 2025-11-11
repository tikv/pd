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

	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// PlacementRuleWatcher is used to watch the PD for any Placement Rule changes.
type PlacementRuleWatcher struct {
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

	etcdClient *clientv3.Client

	// checkerController is used to add the suspect key ranges to the checker when the rule changed.
	checkerController *checker.Controller
	// ruleManager is used to manage the placement rules.
	ruleManager *placement.RuleManager

	ruleWatcher *etcdutil.LoopWatcher

	// patch is used to cache the placement rule changes.
	patch *placement.RuleConfigPatch
}

// NewPlacementRuleWatcher creates a new watcher to watch the Placement Rule change from PD.
func NewPlacementRuleWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	checkerController *checker.Controller,
	ruleManager *placement.RuleManager,
) (*PlacementRuleWatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	rw := &PlacementRuleWatcher{
		ctx:                 ctx,
		cancel:              cancel,
		rulesPathPrefix:     keypath.RulesPathPrefix(),
		ruleGroupPathPrefix: keypath.RuleGroupPathPrefix(),
		etcdClient:          etcdClient,
		checkerController:   checkerController,
		ruleManager:         ruleManager,
	}
	err := rw.initializeWatcher()
	if err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *PlacementRuleWatcher) initializeWatcher() error {
	var suspectKeyRanges *keyutil.KeyRanges

	preEventsFn := func([]*clientv3.Event) error {
		// It will be locked until the postEventsFn is finished.
		rw.ruleManager.Lock()
		rw.patch = rw.ruleManager.BeginPatch()
		suspectKeyRanges = &keyutil.KeyRanges{}
		return nil
	}

	putFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if strings.HasPrefix(key, rw.rulesPathPrefix) {
			log.Debug("update placement rule", zap.String("key", key), zap.String("value", string(kv.Value)))
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
			log.Debug("update placement rule group", zap.String("key", key), zap.String("value", string(kv.Value)))
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
			log.Debug("delete placement rule", zap.String("key", key))
			// Parse groupID and ruleID from the key.
			ruleKey := strings.TrimPrefix(key, rw.rulesPathPrefix)
			parts := strings.SplitN(ruleKey, "-", 2)
			if len(parts) != 2 {
				log.Error("invalid rule key format, cannot parse groupID and ruleID",
					zap.String("key", key),
					zap.String("ruleKey", ruleKey))
				return nil
			}
			groupID, ruleID := parts[0], parts[1]
			// Get the rule from the manager to get its key range.
			// The manager is already locked in preEventsFn.
			rule := rw.ruleManager.GetRuleLocked(groupID, ruleID)
			if rule == nil {
				// Rule is not in the manager, maybe it was already deleted or
				// this is a stale event.
				log.Warn("rule to be deleted not found in rule manager",
					zap.String("key", key),
					zap.String("groupID", groupID),
					zap.String("ruleID", ruleID))
				return nil
			}
			// Try to add the rule change to the patch.
			rw.patch.DeleteRule(rule.GroupID, rule.ID)
			// Update the suspect key ranges
			suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			return nil
		} else if strings.HasPrefix(key, rw.ruleGroupPathPrefix) {
			log.Debug("delete placement rule group", zap.String("key", key))
			trimmedKey := strings.TrimPrefix(key, rw.ruleGroupPathPrefix)
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
		"scheduling-placement-rule-watcher",
		// Watch placement.Rule or placement.RuleGroup
		keypath.RuleCommonPathPrefix(),
		preEventsFn,
		putFn, deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	rw.ruleWatcher.StartWatchLoop()
	return rw.ruleWatcher.WaitLoad()
}

// Close closes the watcher.
func (rw *PlacementRuleWatcher) Close() {
	rw.cancel()
	rw.wg.Wait()
}
