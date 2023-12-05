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
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
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

	// pendingDeletion is a structure used to track the rules or rule groups that are marked for deletion.
	// If a rule or rule group cannot be deleted immediately due to the absence of rules,
	// it will be held here and removed later when a new rule or rule group put event allows for its deletion.
	pendingDeletion struct {
		syncutil.RWMutex
		// key: path, value: [groupID, ruleID]
		// The map 'kvs' holds the rules or rule groups that are pending deletion.
		// If a rule group needs to be deleted, the ruleID will be an empty string.
		kvs map[string][2]string
	}
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
		ruleCommonPathPrefix:  endpoint.RuleCommonPathPrefix(clusterID),
		ruleGroupPathPrefix:   endpoint.RuleGroupPathPrefix(clusterID),
		regionLabelPathPrefix: endpoint.RegionLabelPathPrefix(clusterID),
		etcdClient:            etcdClient,
		ruleStorage:           ruleStorage,
		checkerController:     checkerController,
		ruleManager:           ruleManager,
		regionLabeler:         regionLabeler,
		pendingDeletion: struct {
			syncutil.RWMutex
			kvs map[string][2]string
		}{
			kvs: make(map[string][2]string),
		},
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
	putFn := func(kv *mvccpb.KeyValue) error {
		err := func() error {
			if strings.HasPrefix(string(kv.Key), rw.rulesPathPrefix) {
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
			} else if strings.HasPrefix(string(kv.Key), rw.ruleGroupPathPrefix) {
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
			} else {
				log.Warn("unknown key when update placement rule", zap.String("key", string(kv.Key)))
				return nil
			}
		}()
		if err == nil && rw.hasPendingDeletion() {
			rw.tryFinishPendingDeletion()
		}
		return err
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		groupID, ruleID, err := func() (string, string, error) {
			if strings.HasPrefix(string(kv.Key), rw.rulesPathPrefix) {
				log.Info("delete placement rule", zap.String("key", key))
				ruleJSON, err := rw.ruleStorage.LoadRule(strings.TrimPrefix(key, rw.rulesPathPrefix+"/"))
				if err != nil {
					return "", "", err
				}
				rule, err := placement.NewRuleFromJSON([]byte(ruleJSON))
				if err != nil {
					return "", "", err
				}
				rw.checkerController.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
				return rule.GroupID, rule.ID, rw.ruleManager.DeleteRule(rule.GroupID, rule.ID)
			} else if strings.HasPrefix(string(kv.Key), rw.ruleGroupPathPrefix) {
				log.Info("delete placement rule group", zap.String("key", key))
				trimmedKey := strings.TrimPrefix(key, rw.ruleGroupPathPrefix+"/")
				for _, rule := range rw.ruleManager.GetRulesByGroup(trimmedKey) {
					rw.checkerController.AddSuspectKeyRange(rule.StartKey, rule.EndKey)
				}
				return trimmedKey, "", rw.ruleManager.DeleteRuleGroup(trimmedKey)
			} else {
				log.Warn("unknown key when delete placement rule", zap.String("key", string(kv.Key)))
				return "", "", nil
			}
		}()
		if err != nil && strings.Contains(err.Error(), "no rule left") && groupID != "" {
			rw.addPendingDeletion(key, groupID, ruleID)
		}
		return nil
	}
	postEventFn := func() error {
		return nil
	}
	rw.ruleWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-rule-watcher", rw.ruleCommonPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	rw.ruleWatcher.StartWatchLoop()
	return rw.ruleWatcher.WaitLoad()
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

func (rw *Watcher) hasPendingDeletion() bool {
	rw.pendingDeletion.RLock()
	defer rw.pendingDeletion.RUnlock()
	return len(rw.pendingDeletion.kvs) > 0
}

func (rw *Watcher) addPendingDeletion(path, groupID, ruleID string) {
	rw.pendingDeletion.Lock()
	defer rw.pendingDeletion.Unlock()
	rw.pendingDeletion.kvs[path] = [2]string{groupID, ruleID}
}

func (rw *Watcher) tryFinishPendingDeletion() {
	rw.pendingDeletion.Lock()
	defer rw.pendingDeletion.Unlock()
	originLen := len(rw.pendingDeletion.kvs)
	for k, v := range rw.pendingDeletion.kvs {
		groupID, ruleID := v[0], v[1]
		var err error
		if ruleID == "" {
			err = rw.ruleManager.DeleteRuleGroup(groupID)
		} else {
			err = rw.ruleManager.DeleteRule(groupID, ruleID)
		}
		if err == nil {
			delete(rw.pendingDeletion.kvs, k)
		}
	}
	// If the length of the map is changed, it means that some rules or rule groups have been deleted.
	// We need to force load the rules and rule groups to make sure sync with etcd.
	if len(rw.pendingDeletion.kvs) != originLen {
		rw.ruleWatcher.ForceLoad()
		log.Info("force load rules", zap.Int("pending deletion", len(rw.pendingDeletion.kvs)), zap.Int("origin", originLen))
	}
}
