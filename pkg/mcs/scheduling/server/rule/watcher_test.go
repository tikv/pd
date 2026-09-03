// Copyright 2024 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

const (
	rulesNum                    = 16384
	ruleSnapshotBenchmarkRules  = 1_000_000
	ruleSnapshotBenchmarkTxnOps = 10_000
)

func TestLoadLargeRules(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, true)
	defer clean()
	runWatcherLoadLabelRule(ctx, re, client)
}

func BenchmarkLoadLargeRules(b *testing.B) {
	re := require.New(b)
	ctx, client, clean := prepare(b, true)
	defer clean()

	b.ResetTimer() // Resets the timer to ignore initialization time in the benchmark

	for range b.N {
		runWatcherLoadLabelRule(ctx, re, client)
	}
}

func runWatcherLoadLabelRule(ctx context.Context, re *require.Assertions, client *clientv3.Client) {
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)
	ctx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                   ctx,
		cancel:                cancel,
		rulesPathPrefix:       keypath.RulesPathPrefix(),
		ruleGroupPathPrefix:   keypath.RuleGroupPathPrefix(),
		regionLabelPathPrefix: keypath.RegionLabelPathPrefix(),
		etcdClient:            client,
		ruleStorage:           storage,
		regionLabeler:         labelerManager,
	}
	err = rw.initializeRegionLabelWatcher()
	re.NoError(err)
	re.Len(labelerManager.GetAllLabelRules(), rulesNum)
	cancel()
}

func prepare(t require.TestingT, loadLabelRules bool) (context.Context, *clientv3.Client, func()) {
	return prepareWithEtcdConfig(t, loadLabelRules, nil)
}

func prepareWithEtcdConfig(
	t require.TestingT,
	loadLabelRules bool,
	configure func(*embed.Config),
) (context.Context, *clientv3.Client, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestEtcdConfig()
	if configure != nil {
		configure(cfg)
	}
	var err error
	cfg.Dir, err = os.MkdirTemp("", "pd_tests")
	re.NoError(err)
	os.RemoveAll(cfg.Dir)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls, etcdutil.TestEtcdClientPurpose, true)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	if loadLabelRules {
		ops := make([]clientv3.Op, 0, etcdutil.MaxEtcdTxnOps)
		for i := 1; i < rulesNum+1; i++ {
			rule := keyspace.MakeTxnLabelRule(uint32(i))
			value, err := json.Marshal(rule)
			re.NoError(err)
			key := keypath.RegionLabelKeyPath(rule.ID)
			ops = append(ops, clientv3.OpPut(key, string(value)))
			if len(ops) == etcdutil.MaxEtcdTxnOps {
				_, err = client.Txn(ctx).Then(ops...).Commit()
				re.NoError(err)
				ops = ops[:0]
			}
		}
		if len(ops) > 0 {
			_, err = client.Txn(ctx).Then(ops...).Commit()
			re.NoError(err)
		}
	}

	return ctx, client, func() {
		cancel()
		client.Close()
		etcd.Close()
		os.RemoveAll(cfg.Dir)
	}
}

// BenchmarkRuleSnapshotKeyScan compares the current paginated scan with and
// without local-rule-derived range bounds against a one-shot keys-only scan.
// Use -benchtime=1x because preparing the one-million-rule snapshot is costly.
func BenchmarkRuleSnapshotKeyScan(b *testing.B) {
	ctx, client, clean := prepareWithEtcdConfig(b, false, func(cfg *embed.Config) {
		cfg.MaxTxnOps = ruleSnapshotBenchmarkTxnOps
		cfg.LogLevel = "error"
	})
	defer clean()

	prefix := keypath.RulesPathPrefix()
	rangeEnds := make([]string, 0, ruleSnapshotBenchmarkRules/int(ruleSnapshotScanBatchSize))
	ops := make([]clientv3.Op, 0, ruleSnapshotBenchmarkTxnOps)
	var snapshotRevision int64
	commit := func() {
		resp, err := client.Txn(ctx).Then(ops...).Commit()
		require.NoError(b, err)
		snapshotRevision = resp.Header.Revision
		ops = ops[:0]
	}
	for i := range ruleSnapshotBenchmarkRules {
		key := fmt.Sprintf("%s67-%016x", prefix, i)
		if i > 0 && i%int(ruleSnapshotScanBatchSize) == 0 {
			rangeEnds = append(rangeEnds, key)
		}
		ops = append(ops, clientv3.OpPut(key, "{}"))
		if len(ops) == ruleSnapshotBenchmarkTxnOps {
			commit()
		}
	}
	if len(ops) > 0 {
		commit()
	}

	rw := &Watcher{etcdClient: client}
	runPaged := func(b *testing.B, rangeEnds []string) {
		b.ReportAllocs()
		for range b.N {
			scanned := 0
			revision, err := rw.scanRuleSnapshotKeys(
				ctx, prefix, rangeEnds, snapshotRevision,
				ruleSnapshotScanBatchSize, int(ruleSnapshotLoadBatchSize),
				func(page []*mvccpb.KeyValue, _ int64) error {
					scanned += len(page)
					return nil
				})
			require.NoError(b, err)
			require.Equal(b, snapshotRevision, revision)
			require.Equal(b, ruleSnapshotBenchmarkRules, scanned)
		}
	}

	b.Run("paged/empty-local", func(b *testing.B) {
		runPaged(b, nil)
	})
	b.Run("paged/matching-local", func(b *testing.B) {
		runPaged(b, rangeEnds)
	})
	b.Run("one-shot", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resp, err := etcdutil.EtcdKVGetWithContext(
				ctx,
				client,
				prefix,
				clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
				clientv3.WithKeysOnly(),
				clientv3.WithRev(snapshotRevision),
			)
			require.NoError(b, err)
			require.Len(b, resp.Kvs, ruleSnapshotBenchmarkRules)
		}
	})
}

func TestAdjustRuleSnapshotScanBatchSize(t *testing.T) {
	re := require.New(t)
	minimum := ruleSnapshotLoadBatchSize
	re.Equal(int64(100000), adjustRuleSnapshotScanBatchSize(50000, minimum, 1024*1024))
	re.Equal(int64(100000), adjustRuleSnapshotScanBatchSize(100000, minimum, 1024*1024))
	re.Equal(int64(50000), adjustRuleSnapshotScanBatchSize(100000, minimum, 5*1024*1024))
	re.Equal(int64(25000), adjustRuleSnapshotScanBatchSize(50000, minimum, 5*1024*1024))
	re.Equal(minimum, adjustRuleSnapshotScanBatchSize(minimum, minimum, 5*1024*1024))
	re.Equal(int64(50000), adjustRuleSnapshotScanBatchSize(50000, minimum, 3*1024*1024))
}

func TestScanRuleSnapshotKeyRanges(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, false)
	defer clean()

	prefix := keypath.RulesPathPrefix()
	keys := []string{
		prefix + "a",
		prefix + "aa",
		prefix + "aaa",
		prefix + "aaaa",
		prefix + "aaaaa",
		prefix + "c",
	}
	ops := make([]clientv3.Op, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, clientv3.OpPut(key, "value"))
	}
	resp, err := client.Txn(ctx).Then(ops...).Commit()
	re.NoError(err)

	rw := &Watcher{etcdClient: client}
	var loaded []string
	callbackCount := 0
	revision, err := rw.scanRuleSnapshotKeys(ctx, prefix, []string{prefix + "b"}, 0, 4, 2,
		func(page []*mvccpb.KeyValue, _ int64) error {
			re.LessOrEqual(len(page), 2)
			for _, item := range page {
				loaded = append(loaded, string(item.Key))
			}
			if callbackCount == 0 {
				_, err := client.Txn(ctx).Then(
					clientv3.OpDelete(prefix+"c"),
					clientv3.OpPut(prefix+"d", "new"),
				).Commit()
				if err != nil {
					return err
				}
			}
			callbackCount++
			return nil
		})
	re.NoError(err)
	re.Equal(resp.Header.Revision, revision)
	re.Equal(keys, loaded)
	re.Greater(callbackCount, 2)
}

func TestRuleWatcherAllowsEmptyInitialSnapshot(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, false)
	defer clean()

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ruleManager := placement.NewRuleManager(ctx, storage, nil, nil)
	re.NoError(ruleManager.Initialize(3, nil, "", true))
	watchCtx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                 watchCtx,
		cancel:              cancel,
		rulesPathPrefix:     keypath.RulesPathPrefix(),
		ruleGroupPathPrefix: keypath.RuleGroupPathPrefix(),
		etcdClient:          client,
		ruleStorage:         storage,
		ruleManager:         ruleManager,
	}
	defer rw.Close()

	re.NoError(rw.initializeRuleWatcher())
	re.Zero(ruleManager.GetRulesCount())
}

func TestReconcileRuleSnapshot(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, false)
	defer clean()

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	conf := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, conf)
	cluster.AddLabelsStore(1, 0, map[string]string{"zone": "z1"})
	ruleManager := placement.NewRuleManager(ctx, storage, cluster, conf)
	re.NoError(ruleManager.Initialize(3, nil, "", false))
	re.NoError(ruleManager.SetRuleGroup(&placement.RuleGroup{ID: "g", Index: 1}))
	re.NoError(ruleManager.SetRules([]*placement.Rule{
		{GroupID: "g", ID: "deleted", Role: placement.Learner, Count: 1},
		{
			GroupID: "g", ID: "unchanged", Role: placement.Learner, Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: placement.In, Values: []string{"z1"}},
			},
		},
	}))

	cluster.RuleManager = ruleManager
	opController := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), nil)
	checkerController := checker.NewController(ctx, cluster, cluster.GetCheckerConfig(), opController)

	ops := make([]clientv3.Op, 0, ruleManager.GetRulesCount()+1)
	for _, rule := range ruleManager.GetAllRules() {
		value, err := json.Marshal(rule)
		re.NoError(err)
		ops = append(ops, clientv3.OpPut(keypath.RuleKeyPath(rule.StoreKey()), string(value)))
	}
	groupValue, err := json.Marshal(ruleManager.GetRuleGroup("g"))
	re.NoError(err)
	ops = append(ops, clientv3.OpPut(keypath.RuleGroupIDPath("g"), string(groupValue)))
	initial, err := client.Txn(ctx).Then(ops...).Commit()
	re.NoError(err)

	unchangedVersion := ruleManager.GetRule("g", "unchanged").Version
	updatedGroup := &placement.RuleGroup{ID: "g", Index: 2}
	updatedGroupValue, err := json.Marshal(updatedGroup)
	re.NoError(err)
	addedGroup := &placement.RuleGroup{ID: "added", Index: 3}
	addedGroupValue, err := json.Marshal(addedGroup)
	re.NoError(err)
	updatedDefault := ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	updatedDefault.Count = 5
	updatedValue, err := json.Marshal(updatedDefault)
	re.NoError(err)
	updated, err := client.Txn(ctx).Then(
		clientv3.OpDelete(keypath.RuleKeyPath((&placement.Rule{GroupID: "g", ID: "deleted"}).StoreKey())),
		clientv3.OpPut(keypath.RuleGroupIDPath(updatedGroup.ID), string(updatedGroupValue)),
		clientv3.OpPut(keypath.RuleGroupIDPath(addedGroup.ID), string(addedGroupValue)),
		clientv3.OpPut(keypath.RuleKeyPath(updatedDefault.StoreKey()), string(updatedValue)),
	).Commit()
	re.NoError(err)

	rw := &Watcher{
		rulesPathPrefix:     keypath.RulesPathPrefix(),
		ruleGroupPathPrefix: keypath.RuleGroupPathPrefix(),
		etcdClient:          client,
		ruleManager:         ruleManager,
		checkerController:   checkerController,
		ruleRevision:        initial.Header.Revision,
	}
	nextRevision, err := rw.reconcileRuleSnapshot(ctx)
	re.NoError(err)
	re.Equal(updated.Header.Revision+1, nextRevision)
	re.Nil(ruleManager.GetRule("g", "deleted"))
	re.Equal(5, ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID).Count)
	re.Equal(unchangedVersion, ruleManager.GetRule("g", "unchanged").Version)
	re.Equal(updatedGroup, ruleManager.GetRuleGroup("g"))
	re.Equal(addedGroup, ruleManager.GetRuleGroup("added"))
	re.Equal(updated.Header.Revision, rw.ruleRevision)

	groupsDeleted, err := client.Txn(ctx).Then(
		clientv3.OpDelete(keypath.RuleGroupIDPath(updatedGroup.ID)),
		clientv3.OpDelete(keypath.RuleGroupIDPath(addedGroup.ID)),
	).Commit()
	re.NoError(err)
	nextRevision, err = rw.reconcileRuleSnapshot(ctx)
	re.NoError(err)
	re.Equal(groupsDeleted.Header.Revision+1, nextRevision)
	re.Zero(ruleManager.GetRuleGroup("g").Index)
	re.Nil(ruleManager.GetRuleGroup("added"))
	re.Equal(groupsDeleted.Header.Revision, rw.ruleRevision)

	updatedRule := ruleManager.GetRule("g", "unchanged")
	updatedRule.LabelConstraints[0].Values = []string{"z2"}
	updatedRuleValue, err := json.Marshal(updatedRule)
	re.NoError(err)
	updatedRuleResp, err := client.Put(ctx, keypath.RuleKeyPath(updatedRule.StoreKey()), string(updatedRuleValue))
	re.NoError(err)

	_, err = rw.reconcileRuleSnapshot(ctx)
	re.ErrorContains(err, "can not match any store")
	re.Equal(groupsDeleted.Header.Revision, rw.ruleRevision)
	re.Equal([]string{"z1"}, ruleManager.GetRule("g", "unchanged").LabelConstraints[0].Values)

	cluster.SetStoreLabel(1, map[string]string{"zone": "z2"})
	nextRevision, err = rw.reconcileRuleSnapshot(ctx)
	re.NoError(err)
	re.Equal(updatedRuleResp.Header.Revision+1, nextRevision)
	re.Equal(updatedRuleResp.Header.Revision, rw.ruleRevision)
	re.Equal([]string{"z2"}, ruleManager.GetRule("g", "unchanged").LabelConstraints[0].Values)

	mismatchedGroupValue, err := json.Marshal(&placement.RuleGroup{ID: "payload"})
	re.NoError(err)
	_, err = client.Put(ctx, keypath.RuleGroupIDPath("storage"), string(mismatchedGroupValue))
	re.NoError(err)
	_, err = rw.reconcileRuleSnapshot(ctx)
	re.ErrorContains(err, "placement rule group snapshot key does not match payload identity")
	re.Equal(updatedRuleResp.Header.Revision, rw.ruleRevision)
	re.Nil(ruleManager.GetRuleGroup("payload"))

	mismatchedRule := &placement.Rule{GroupID: "payload", ID: "rule", Role: placement.Learner, Count: 1}
	mismatchedRuleValue, err := json.Marshal(mismatchedRule)
	re.NoError(err)
	storageRule := &placement.Rule{GroupID: "storage", ID: "rule"}
	_, err = client.Txn(ctx).Then(
		clientv3.OpDelete(keypath.RuleGroupIDPath("storage")),
		clientv3.OpPut(keypath.RuleKeyPath(storageRule.StoreKey()), string(mismatchedRuleValue)),
	).Commit()
	re.NoError(err)
	_, err = rw.reconcileRuleSnapshot(ctx)
	re.ErrorContains(err, "placement rule snapshot key does not match payload identity")
	re.Equal(updatedRuleResp.Header.Revision, rw.ruleRevision)
	re.Nil(ruleManager.GetRule("payload", "rule"))
}

func TestRuleWatcherReplaysFailedLiveRuleUpdate(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, false)
	defer clean()

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	conf := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, conf)
	cluster.AddLabelsStore(1, 0, map[string]string{"zone": "z1"})
	ruleManager := placement.NewRuleManager(ctx, storage, cluster, conf)
	re.NoError(ruleManager.Initialize(3, nil, "", false))
	re.NoError(ruleManager.SetRule(&placement.Rule{
		GroupID: "g",
		ID:      "r",
		Role:    placement.Learner,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "zone", Op: placement.In, Values: []string{"z1"}},
		},
	}))
	cluster.RuleManager = ruleManager

	opController := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), nil)
	checkerController := checker.NewController(ctx, cluster, cluster.GetCheckerConfig(), opController)
	for _, rule := range ruleManager.GetAllRules() {
		value, err := json.Marshal(rule)
		re.NoError(err)
		_, err = client.Put(ctx, keypath.RuleKeyPath(rule.StoreKey()), string(value))
		re.NoError(err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                 watchCtx,
		cancel:              cancel,
		rulesPathPrefix:     keypath.RulesPathPrefix(),
		ruleGroupPathPrefix: keypath.RuleGroupPathPrefix(),
		etcdClient:          client,
		ruleStorage:         storage,
		ruleManager:         ruleManager,
		checkerController:   checkerController,
	}
	defer rw.Close()
	re.NoError(rw.initializeRuleWatcher())

	logFile := testutil.InitTempFileLogger("info")
	defer os.RemoveAll(logFile)
	updatedRule := ruleManager.GetRule("g", "r")
	updatedRule.LabelConstraints[0].Values = []string{"z2"}
	updatedValue, err := json.Marshal(updatedRule)
	re.NoError(err)
	updated, err := client.Put(ctx, keypath.RuleKeyPath(updatedRule.StoreKey()), string(updatedValue))
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		contents, err := os.ReadFile(logFile)
		return err == nil &&
			strings.Contains(string(contents), "run post event failed in watch loop") &&
			strings.Contains(string(contents), "scheduling-rule-watcher")
	})
	re.Equal([]string{"z1"}, ruleManager.GetRule("g", "r").LabelConstraints[0].Values)

	cluster.SetStoreLabel(1, map[string]string{"zone": "z2"})
	testutil.Eventually(re, func() bool {
		rule := ruleManager.GetRule("g", "r")
		return rule != nil && len(rule.LabelConstraints) == 1 &&
			len(rule.LabelConstraints[0].Values) == 1 && rule.LabelConstraints[0].Values[0] == "z2"
	})
	re.Equal(updated.Header.Revision, rw.ruleRevision)
}
