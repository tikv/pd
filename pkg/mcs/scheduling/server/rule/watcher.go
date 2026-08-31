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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const (
	// Keep value loading and patch construction bounded independently from the
	// larger keys-only etcd range responses.
	ruleSnapshotLoadBatchSize = int64(10000)

	// Aim for 2-4 MiB keys-only responses. The initial batch matches the lower
	// bound for the typical placement-rule key size and may grow up to 100k.
	ruleSnapshotScanBatchSize        = int64(50000)
	ruleSnapshotScanMaxBatchSize     = int64(100000)
	ruleSnapshotScanMinResponseBytes = 2 * 1024 * 1024
	ruleSnapshotScanMaxResponseBytes = 4 * 1024 * 1024
)

// Watcher is used to watch the PD for any Placement Rule changes.
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
	labelWatcher *etcdutil.LoopWatcher

	// ruleRevision remains a safe applied lower bound after a callback failure.
	// A gap stops live events from advancing it until snapshot reconciliation.
	ruleRevision           int64
	ruleRevisionContinuous bool

	// patch is used to cache the placement rule changes.
	patch *placement.RuleConfigPatch
}

// NewWatcher creates a new watcher to watch the Placement Rule change from PD.
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
		rw.Close()
		return nil, err
	}
	err = rw.initializeRegionLabelWatcher()
	if err != nil {
		rw.Close()
		return nil, err
	}
	return rw, nil
}

func adjustRuleSnapshotScanBatchSize(current, minimum int64, responseBytes int) int64 {
	switch {
	case responseBytes < ruleSnapshotScanMinResponseBytes:
		return min(current*2, ruleSnapshotScanMaxBatchSize)
	case responseBytes > ruleSnapshotScanMaxResponseBytes:
		return max(current/2, minimum)
	default:
		return current
	}
}

func (rw *Watcher) scanRuleSnapshotKeys(
	ctx context.Context,
	prefix string,
	rangeEnds []string,
	revision int64,
	fetchBatchSize int64,
	processBatchSize int,
	handlePage func([]*mvccpb.KeyValue, int64) error,
) (int64, error) {
	startKey := prefix
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	minFetchBatchSize := min(fetchBatchSize, int64(processBatchSize))
	for rangeIndex := 0; rangeIndex <= len(rangeEnds); rangeIndex++ {
		endKey := prefixEnd
		if rangeIndex < len(rangeEnds) {
			endKey = rangeEnds[rangeIndex]
		}
		for {
			opts := []clientv3.OpOption{
				clientv3.WithRange(endKey),
				// etcd ranges are key-ascending by default.
				clientv3.WithLimit(fetchBatchSize + 1),
				clientv3.WithKeysOnly(),
			}
			if revision > 0 {
				opts = append(opts, clientv3.WithRev(revision))
			}
			resp, err := etcdutil.EtcdKVGetWithContext(ctx, rw.etcdClient, startKey, opts...)
			if err != nil {
				return 0, err
			}
			if revision == 0 {
				revision = resp.Header.Revision
			}

			page := resp.Kvs
			if resp.More {
				if len(page) == 0 {
					return 0, errors.New("placement rule snapshot returned an empty page")
				}
				startKey = string(page[len(page)-1].Key)
				page = page[:len(page)-1]
			}
			if len(page) == 0 {
				if err := handlePage(page, revision); err != nil {
					return 0, err
				}
			} else {
				for start := 0; start < len(page); start += processBatchSize {
					end := min(start+processBatchSize, len(page))
					if err := handlePage(page[start:end], revision); err != nil {
						return 0, err
					}
				}
			}
			responseBytes := (*etcdserverpb.RangeResponse)(resp).Size()
			if resp.More || responseBytes > ruleSnapshotScanMaxResponseBytes {
				fetchBatchSize = adjustRuleSnapshotScanBatchSize(
					fetchBatchSize,
					minFetchBatchSize,
					responseBytes,
				)
			}
			if !resp.More {
				break
			}
		}
		startKey = endKey
	}
	return revision, nil
}

func (rw *Watcher) loadRuleSnapshotValues(
	ctx context.Context,
	metadata []*mvccpb.KeyValue,
	revision int64,
) ([]*mvccpb.KeyValue, error) {
	values := make([]*mvccpb.KeyValue, 0, len(metadata))
	for start := 0; start < len(metadata); start += etcdutil.MaxEtcdTxnOps {
		end := min(start+etcdutil.MaxEtcdTxnOps, len(metadata))
		ops := make([]clientv3.Op, 0, end-start)
		for _, item := range metadata[start:end] {
			ops = append(ops, clientv3.OpGet(string(item.Key), clientv3.WithRev(revision)))
		}
		txnCtx, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
		resp, err := rw.etcdClient.Txn(txnCtx).Then(ops...).Commit()
		cancel()
		if err != nil {
			return nil, err
		}
		if len(resp.Responses) != end-start {
			return nil, errors.New("placement rule snapshot returned an incomplete value batch")
		}
		for i, response := range resp.Responses {
			kvs := response.GetResponseRange().Kvs
			meta := metadata[start+i]
			if len(kvs) != 1 || !bytes.Equal(kvs[0].Key, meta.Key) || kvs[0].ModRevision != meta.ModRevision {
				return nil, fmt.Errorf("placement rule snapshot changed at key %q", meta.Key)
			}
			values = append(values, kvs[0])
		}
	}
	return values, nil
}

func (rw *Watcher) reconcileRuleSnapshot(ctx context.Context) (int64, error) {
	if rw.checkerController == nil {
		return 0, errors.New("checker controller is nil")
	}

	rules, groups := rw.ruleManager.GetRuleConfigForReconcile()
	patch := rw.ruleManager.BeginPatch()
	suspectKeyRanges := &keyutil.KeyRanges{}
	changedGroups := make(map[string]struct{})
	changed := false
	groupIndex, ruleIndex := 0, 0

	deleteGroup := func(group *placement.RuleGroup) {
		patch.DeleteGroup(group.ID)
		changedGroups[group.ID] = struct{}{}
		changed = true
	}
	deleteRule := func(rule *placement.Rule) {
		patch.DeleteRule(rule.GroupID, rule.ID)
		suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
		changed = true
	}

	groupPrefix := []byte(rw.ruleGroupPathPrefix)
	snapshotRevision, err := rw.scanRuleSnapshotKeys(
		ctx, rw.ruleGroupPathPrefix, nil, 0, ruleSnapshotScanBatchSize, int(ruleSnapshotLoadBatchSize),
		func(page []*mvccpb.KeyValue, revision int64) error {
			type groupChange struct {
				meta *mvccpb.KeyValue
				old  *placement.RuleGroup
			}
			changes := make([]groupChange, 0)
			metadata := make([]*mvccpb.KeyValue, 0)
			for _, item := range page {
				if !bytes.HasPrefix(item.Key, groupPrefix) {
					return fmt.Errorf("unexpected placement rule group key %q", item.Key)
				}
				key := item.Key[len(groupPrefix):]
				for groupIndex < len(groups) && bytes.Compare([]byte(groups[groupIndex].ID), key) < 0 {
					deleteGroup(groups[groupIndex])
					groupIndex++
				}
				var old *placement.RuleGroup
				if groupIndex < len(groups) && bytes.Equal([]byte(groups[groupIndex].ID), key) {
					old = groups[groupIndex]
					groupIndex++
				}
				if old != nil && item.ModRevision <= rw.ruleRevision {
					continue
				}
				changes = append(changes, groupChange{meta: item, old: old})
				metadata = append(metadata, item)
			}

			values, err := rw.loadRuleSnapshotValues(ctx, metadata, revision)
			if err != nil {
				return err
			}
			for i, item := range values {
				group, err := placement.NewRuleGroupFromJSON(item.Value)
				if err != nil {
					return fmt.Errorf("failed to load placement rule group snapshot at key %q: %w", item.Key, err)
				}
				if !bytes.Equal([]byte(group.ID), item.Key[len(groupPrefix):]) {
					return fmt.Errorf("placement rule group snapshot key does not match payload identity: %q", item.Key)
				}
				old := changes[i].old
				if old != nil && old.ID == group.ID && old.Index == group.Index && old.Override == group.Override {
					continue
				}
				patch.SetGroup(group)
				changedGroups[group.ID] = struct{}{}
				if old != nil {
					changedGroups[old.ID] = struct{}{}
				}
				changed = true
			}
			return nil
		})
	if err != nil {
		return 0, err
	}
	for groupIndex < len(groups) {
		deleteGroup(groups[groupIndex])
		groupIndex++
	}

	rulePrefix := []byte(rw.rulesPathPrefix)
	ruleKeyBuffer := make([]byte, 0, 128)
	compareRuleKey := func(rule *placement.Rule, key []byte) int {
		ruleKeyBuffer = hex.AppendEncode(ruleKeyBuffer[:0], []byte(rule.GroupID))
		ruleKeyBuffer = append(ruleKeyBuffer, '-')
		ruleKeyBuffer = hex.AppendEncode(ruleKeyBuffer, []byte(rule.ID))
		return bytes.Compare(ruleKeyBuffer, key)
	}
	// Bound each etcd range with the already sorted local rule keys. etcd
	// computes the count over the whole requested range even when a limit is set.
	ruleRangeEnds := make([]string, 0, len(rules)/int(ruleSnapshotScanBatchSize))
	for i := int(ruleSnapshotScanBatchSize); i < len(rules); i += int(ruleSnapshotScanBatchSize) {
		ruleRangeEnds = append(ruleRangeEnds, rw.rulesPathPrefix+rules[i].StoreKey())
	}
	_, err = rw.scanRuleSnapshotKeys(
		ctx, rw.rulesPathPrefix, ruleRangeEnds, snapshotRevision,
		ruleSnapshotScanBatchSize, int(ruleSnapshotLoadBatchSize),
		func(page []*mvccpb.KeyValue, revision int64) error {
			type ruleChange struct {
				meta *mvccpb.KeyValue
				old  *placement.Rule
			}
			changes := make([]ruleChange, 0)
			metadata := make([]*mvccpb.KeyValue, 0)
			for _, item := range page {
				if !bytes.HasPrefix(item.Key, rulePrefix) {
					return fmt.Errorf("unexpected placement rule key %q", item.Key)
				}
				key := item.Key[len(rulePrefix):]
				for ruleIndex < len(rules) && compareRuleKey(rules[ruleIndex], key) < 0 {
					deleteRule(rules[ruleIndex])
					ruleIndex++
				}
				var old *placement.Rule
				if ruleIndex < len(rules) && compareRuleKey(rules[ruleIndex], key) == 0 {
					old = rules[ruleIndex]
					ruleIndex++
					if _, ok := changedGroups[old.GroupID]; ok {
						suspectKeyRanges.Append(old.StartKey, old.EndKey)
					}
				}
				if old != nil && item.ModRevision <= rw.ruleRevision {
					continue
				}
				changes = append(changes, ruleChange{meta: item, old: old})
				metadata = append(metadata, item)
			}

			values, err := rw.loadRuleSnapshotValues(ctx, metadata, revision)
			if err != nil {
				return err
			}
			for i, item := range values {
				rule, err := placement.NewRuleFromJSON(item.Value)
				if err != nil {
					return fmt.Errorf("failed to load placement rule snapshot at key %q: %w", item.Key, err)
				}
				if !bytes.Equal([]byte(rule.StoreKey()), item.Key[len(rulePrefix):]) {
					return fmt.Errorf("placement rule snapshot key does not match payload identity: %q", item.Key)
				}
				if err := rw.ruleManager.AdjustRule(rule, ""); err != nil {
					return fmt.Errorf("failed to adjust placement rule snapshot at key %q: %w", item.Key, err)
				}
				patch.SetRule(rule)
				suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
				if old := changes[i].old; old != nil {
					suspectKeyRanges.Append(old.StartKey, old.EndKey)
				}
				changed = true
			}
			return nil
		})
	if err != nil {
		return 0, err
	}
	for ruleIndex < len(rules) {
		deleteRule(rules[ruleIndex])
		ruleIndex++
	}

	// TryCommitPatchLocked rebuilds the rule index, so skip it when the snapshot
	// contains no detected changes.
	if changed {
		rw.ruleManager.Lock()
		err = rw.ruleManager.TryCommitPatchLocked(patch)
		rw.ruleManager.Unlock()
		if err != nil {
			return 0, err
		}
		for _, keyRange := range suspectKeyRanges.Ranges() {
			rw.checkerController.AddSuspectKeyRange(keyRange.StartKey, keyRange.EndKey)
		}
	}
	rw.ruleRevision = snapshotRevision
	rw.ruleRevisionContinuous = true
	return snapshotRevision + 1, nil
}

func (rw *Watcher) initializeRuleWatcher() error {
	var suspectKeyRanges *keyutil.KeyRanges
	var maxLoadedRevision int64
	var applyFailed bool

	preEventsFn := func([]*clientv3.Event) error {
		// It will be locked until the postEventsFn is finished.
		rw.ruleManager.Lock()
		rw.patch = rw.ruleManager.BeginPatch()
		suspectKeyRanges = &keyutil.KeyRanges{}
		maxLoadedRevision = 0
		applyFailed = false
		return nil
	}

	putFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if strings.HasPrefix(key, rw.rulesPathPrefix) {
			log.Debug("update placement rule", zap.String("key", key), zap.String("value", string(kv.Value)))
			rule, err := placement.NewRuleFromJSON(kv.Value)
			if err != nil {
				applyFailed = true
				return err
			}
			// Try to add the rule change to the patch.
			if err := rw.ruleManager.AdjustRule(rule, ""); err != nil {
				applyFailed = true
				return err
			}
			rw.patch.SetRule(rule)
			// Update the suspect key ranges in lock.
			suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			if oldRule := rw.ruleManager.GetRuleLocked(rule.GroupID, rule.ID); oldRule != nil {
				suspectKeyRanges.Append(oldRule.StartKey, oldRule.EndKey)
			}
			maxLoadedRevision = max(maxLoadedRevision, kv.ModRevision)
			return nil
		} else if strings.HasPrefix(key, rw.ruleGroupPathPrefix) {
			log.Debug("update placement rule group", zap.String("key", key), zap.String("value", string(kv.Value)))
			ruleGroup, err := placement.NewRuleGroupFromJSON(kv.Value)
			if err != nil {
				applyFailed = true
				return err
			}
			// Try to add the rule group change to the patch.
			rw.patch.SetGroup(ruleGroup)
			// Update the suspect key ranges
			for _, rule := range rw.ruleManager.GetRulesByGroupLocked(ruleGroup.ID) {
				suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			}
			maxLoadedRevision = max(maxLoadedRevision, kv.ModRevision)
			return nil
		}
		log.Warn("unknown key when updating placement rule", zap.String("key", key))
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if strings.HasPrefix(key, rw.rulesPathPrefix) {
			log.Debug("delete placement rule", zap.String("key", key))
			ruleJSON, err := rw.ruleStorage.LoadRule(strings.TrimPrefix(key, rw.rulesPathPrefix))
			if err != nil {
				applyFailed = true
				return err
			}
			rule, err := placement.NewRuleFromJSON([]byte(ruleJSON))
			if err != nil {
				applyFailed = true
				return err
			}
			// Try to add the rule change to the patch.
			rw.patch.DeleteRule(rule.GroupID, rule.ID)
			// Update the suspect key ranges
			suspectKeyRanges.Append(rule.StartKey, rule.EndKey)
			maxLoadedRevision = max(maxLoadedRevision, kv.ModRevision)
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
			maxLoadedRevision = max(maxLoadedRevision, kv.ModRevision)
			return nil
		}
		log.Warn("unknown key when deleting placement rule", zap.String("key", key))
		return nil
	}
	postEventsFn := func(events []*clientv3.Event) error {
		defer rw.ruleManager.Unlock()
		if applyFailed {
			rw.ruleRevisionContinuous = false
			return errors.New("failed to apply placement rule events")
		}
		// A scheduling server can start before PD has persisted placement rules.
		// Keep the empty local state until the watch receives the first rule.
		if len(events) == 0 && maxLoadedRevision == 0 {
			return nil
		}
		if err := rw.ruleManager.TryCommitPatchLocked(rw.patch); err != nil {
			rw.ruleRevisionContinuous = false
			log.Error("failed to commit patch", zap.Error(err))
			return err
		}
		for _, kr := range suspectKeyRanges.Ranges() {
			rw.checkerController.AddSuspectKeyRange(kr.StartKey, kr.EndKey)
		}
		if len(events) > 0 && rw.ruleRevisionContinuous {
			rw.ruleRevision = max(rw.ruleRevision, maxLoadedRevision)
		}
		return nil
	}
	rw.ruleWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-rule-watcher",
		// Watch placement.Rule or placement.RuleGroup
		keypath.RuleCommonPathPrefix(),
		preEventsFn,
		putFn, deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	rw.ruleWatcher.SetConsistentLoad()
	rw.ruleWatcher.SetInitialLoadSuccessFn(func() {
		if !applyFailed {
			rw.ruleRevision = maxLoadedRevision
			rw.ruleRevisionContinuous = true
		}
	})
	rw.ruleWatcher.SetCompactionReloadFn(rw.reconcileRuleSnapshot)
	rw.ruleWatcher.StartWatchLoop()
	return rw.ruleWatcher.WaitLoad()
}

func (rw *Watcher) initializeRegionLabelWatcher() error {
	suspectKeyRanges := make([]*labeler.KeyRangeRule, 0)
	// TODO: use txn in region labeler.
	preEventsFn := func([]*clientv3.Event) error {
		// It will be locked until the postEventsFn is finished.
		rw.regionLabeler.Lock()
		for i := range suspectKeyRanges {
			suspectKeyRanges[i] = nil // avoid memory leak
		}
		suspectKeyRanges = suspectKeyRanges[:0]
		return nil
	}
	putFn := func(kv *mvccpb.KeyValue) error {
		log.Debug("update region label rule", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		rule, err := labeler.NewLabelRuleFromJSON(kv.Value)
		if err != nil {
			return err
		}
		err = rw.regionLabeler.SetLabelRuleLocked(rule)
		if err == nil {
			krs := rule.GetKeyRanges()
			if krs != nil {
				suspectKeyRanges = append(suspectKeyRanges, krs...)
			}
		}
		return err
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Debug("delete region label rule", zap.String("key", key))
		id := strings.TrimPrefix(key, rw.regionLabelPathPrefix)
		rule := rw.regionLabeler.GetLabelRuleLocked(id)
		err := rw.regionLabeler.DeleteLabelRuleLocked(id)
		if err == nil && rule != nil {
			krs := rule.GetKeyRanges()
			if krs != nil {
				suspectKeyRanges = append(suspectKeyRanges, krs...)
			}
		}
		return err
	}
	postEventsFn := func([]*clientv3.Event) error {
		defer rw.regionLabeler.Unlock()
		if rw.checkerController == nil {
			return errors.New("checker controller is nil")
		}
		for _, kr := range suspectKeyRanges {
			rw.checkerController.AddSuspectKeyRange(kr.StartKey, kr.EndKey)
		}
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
func (rw *Watcher) Close() {
	rw.cancel()
	rw.wg.Wait()
	if rw.checkerController != nil {
		rw.checkerController.ClearSuspectKeyRanges()
	}
}
