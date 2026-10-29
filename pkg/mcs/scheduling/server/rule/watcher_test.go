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
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
<<<<<<< HEAD
=======
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

>>>>>>> 57f40aae7b (*: redact region keys and request bodies in logs (#11347))
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
<<<<<<< HEAD
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
=======
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
>>>>>>> 57f40aae7b (*: redact region keys and request bodies in logs (#11347))
)

const (
	clusterID = uint64(20240117)
	rulesNum  = 16384
)

func TestLoadLargeRules(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()
	runWatcherLoadLabelRule(ctx, re, client)
}

func BenchmarkLoadLargeRules(b *testing.B) {
	re := require.New(b)
	ctx, client, clean := prepare(b)
	defer clean()

	b.ResetTimer() // Resets the timer to ignore initialization time in the benchmark

	for n := 0; n < b.N; n++ {
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
		ruleCommonPathPrefix:  keypath.RuleCommonPathPrefix(),
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

func prepare(t require.TestingT) (context.Context, *clientv3.Client, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestSingleConfig()
	cfg.Dir = filepath.Join(os.TempDir(), "/pd_tests")
	os.RemoveAll(cfg.Dir)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls, etcdutil.TestEtcdClientPurpose, true)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	for i := 1; i < rulesNum+1; i++ {
		rule := &labeler.LabelRule{
			ID:       "test_" + strconv.Itoa(i),
			Labels:   []labeler.RegionLabel{{Key: "test", Value: "test"}},
			RuleType: labeler.KeyRange,
			Data:     keyspace.MakeKeyRanges(uint32(i)),
		}
<<<<<<< HEAD
=======
		if len(ops) > 0 {
			_, err = client.Txn(ctx).Then(ops...).Commit()
			re.NoError(err)
		}
	}

	return ctx, client, cleanup
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
		for b.Loop() {
			scanned := 0
			revision, err := rw.scanRuleSnapshotKeys(
				ctx, prefix, rangeEnds, snapshotRevision,
				ruleSnapshotScanBatchSize, int(ruleSnapshotProcessBatchSize),
				func(batch []*mvccpb.KeyValue, _ int64) error {
					scanned += len(batch)
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
		for b.Loop() {
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
	minimum := ruleSnapshotProcessBatchSize
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
		func(batch []*mvccpb.KeyValue, _ int64) error {
			re.LessOrEqual(len(batch), 2)
			for _, item := range batch {
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

func TestWatcherLogRedact(t *testing.T) {
	const startKey, endKey = "7365637265742d7374617274", "7365637265742d7a"
	ctx, client, clean := prepare(t, false)
	defer clean()

	for _, mode := range []struct {
		name string
		typ  logutil.RedactInfoLogType
	}{
		{"off", logutil.RedactInfoLogOFF},
		{"on", logutil.RedactInfoLogON},
		{"marker", logutil.RedactInfoLogMarker},
	} {
		t.Run(mode.name, func(t *testing.T) {
			re := require.New(t)
			_, err := client.Delete(ctx, keypath.RuleCommonPathPrefix(), clientv3.WithPrefix())
			re.NoError(err)
			_, err = client.Delete(ctx, keypath.RegionLabelPathPrefix(), clientv3.WithPrefix())
			re.NoError(err)

			fname := testutil.InitTempFileLogger(t, "debug")
			var (
				lg    *zap.Logger
				props *log.ZapProperties
			)
			re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, mode.typ))
			t.Cleanup(func() {
				re.NoError(logutil.SetupLogger(&log.Config{}, &lg, &props, logutil.RedactInfoLogOFF))
			})
			readLog := func() string {
				output, err := os.ReadFile(fname)
				re.NoError(err)
				return string(output)
			}

			watchCtx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			fixture := newRuleWatcherTestFixture(watchCtx, t)
			regionLabeler, err := labeler.NewRegionLabeler(watchCtx, fixture.storage, time.Hour)
			re.NoError(err)
			rule := &placement.Rule{
				GroupID: "redact", ID: "placement", Role: placement.Learner, Count: 1,
				StartKeyHex: startKey, EndKeyHex: endKey,
			}
			labelRule := &labeler.LabelRule{
				ID: "redact-label", Labels: []labeler.RegionLabel{{Key: "k", Value: "v"}},
				RuleType: labeler.KeyRange, Data: labeler.MakeKeyRanges(startKey, endKey),
			}
			labelJSON, err := json.Marshal(labelRule)
			re.NoError(err)
			defaultRule := fixture.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
			_, err = client.Txn(ctx).Then(
				clientv3.OpPut(keypath.RuleKeyPath(defaultRule.StoreKey()), defaultRule.String()),
				clientv3.OpPut(keypath.RuleKeyPath(rule.StoreKey()), rule.String()),
				clientv3.OpPut(keypath.RegionLabelKeyPath(labelRule.ID), string(labelJSON)),
			).Commit()
			re.NoError(err)
			watcher, err := NewWatcher(watchCtx, client, fixture.storage, fixture.checkerController, fixture.ruleManager, regionLabeler)
			re.NoError(err)
			closeWatcher := sync.OnceFunc(watcher.Close)
			t.Cleanup(closeWatcher)
			re.Equal(startKey, fixture.ruleManager.GetRule(rule.GroupID, rule.ID).StartKeyHex)
			re.Equal(endKey, regionLabeler.GetLabelRule(labelRule.ID).GetKeyRanges()[0].EndKeyHex)

			// Exercise live updates as well as the initial snapshot logs.
			rule.Count, labelRule.Index = 2, 1
			labelJSON, err = json.Marshal(labelRule)
			re.NoError(err)
			_, err = client.Txn(ctx).Then(
				clientv3.OpPut(keypath.RuleKeyPath(rule.StoreKey()), rule.String()),
				clientv3.OpPut(keypath.RegionLabelKeyPath(labelRule.ID), string(labelJSON)),
			).Commit()
			re.NoError(err)
			testutil.Eventually(re, func() bool {
				return fixture.ruleManager.GetRule(rule.GroupID, rule.ID).Count == 2 &&
					regionLabeler.GetLabelRule(labelRule.ID).Index == 1
			})

			closeWatcher()

			output := readLog()
			for _, msg := range []string{"update placement rule", "update region label rule", "put successfully in watch loop when loading", "put successfully in watch loop"} {
				re.Contains(output, msg)
			}
			for _, key := range []string{startKey, endKey} {
				if mode.typ == logutil.RedactInfoLogON {
					re.NotContains(output, key)
				} else {
					re.Contains(output, key)
				}
			}
			switch mode.typ {
			case logutil.RedactInfoLogON:
				re.Contains(output, "[value=?]")
			case logutil.RedactInfoLogMarker:
				re.Contains(output, `[value="‹{`)
			}
			re.Equal(startKey, fixture.ruleManager.GetRule(rule.GroupID, rule.ID).StartKeyHex)
			re.Equal(endKey, regionLabeler.GetLabelRule(labelRule.ID).GetKeyRanges()[0].EndKeyHex)
			stored, err := client.Get(ctx, keypath.RegionLabelKeyPath(labelRule.ID))
			re.NoError(err)
			re.Len(stored.Kvs, 1)
			re.JSONEq(string(labelJSON), string(stored.Kvs[0].Value))
		})
	}
}

func TestRuleWatcherReconcilesInitialLoadRetry(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, false)
	defer clean()

	fixture := newRuleWatcherTestFixture(ctx, t)
	staleRule := &placement.Rule{GroupID: "g", ID: "stale", Role: placement.Learner, Count: 1}
	re.NoError(fixture.ruleManager.SetRule(staleRule))
	defaultRule := fixture.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	defaultValue, err := json.Marshal(defaultRule)
	re.NoError(err)
	_, err = client.Put(ctx, keypath.RuleKeyPath(defaultRule.StoreKey()), string(defaultValue))
	re.NoError(err)
	snapshot, err := client.Get(ctx, keypath.RuleCommonPathPrefix(), clientv3.WithPrefix())
	re.NoError(err)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail", "return(1)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail"))
	}()

	watchCtx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                 watchCtx,
		cancel:              cancel,
		rulesPathPrefix:     keypath.RulesPathPrefix(),
		ruleGroupPathPrefix: keypath.RuleGroupPathPrefix(),
		etcdClient:          client,
		ruleStorage:         fixture.storage,
		ruleManager:         fixture.ruleManager,
		checkerController:   fixture.checkerController,
	}
	closeWatcher := sync.OnceFunc(rw.Close)
	defer closeWatcher()
	re.NoError(rw.initializeRuleWatcher())
	closeWatcher()

	re.Nil(fixture.ruleManager.GetRule(staleRule.GroupID, staleRule.ID))
	re.Equal(snapshot.Header.Revision, rw.ruleRevision)
}

type ruleWatcherTestFixture struct {
	storage           *endpoint.StorageEndpoint
	cluster           *mockcluster.Cluster
	ruleManager       *placement.RuleManager
	checkerController *checker.Controller
}

func newRuleWatcherTestFixture(ctx context.Context, t testing.TB) *ruleWatcherTestFixture {
	re := require.New(t)
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	conf := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, conf)
	cluster.AddLabelsStore(1, 0, map[string]string{"zone": "z1"})
	ruleManager := placement.NewRuleManager(ctx, storage, cluster, conf)
	re.NoError(ruleManager.Initialize(3, nil, "", false))
	cluster.RuleManager = ruleManager
	opController := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), nil)
	return &ruleWatcherTestFixture{
		storage:           storage,
		cluster:           cluster,
		ruleManager:       ruleManager,
		checkerController: checker.NewController(ctx, cluster, cluster.GetCheckerConfig(), opController),
	}
}

func TestReconcileRuleSnapshot(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, false)
	defer clean()

	fixture := newRuleWatcherTestFixture(ctx, t)
	ruleManager := fixture.ruleManager
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

	ops := make([]clientv3.Op, 0, ruleManager.GetRulesCount()+1)
	for _, rule := range ruleManager.GetAllRules() {
>>>>>>> 57f40aae7b (*: redact region keys and request bodies in logs (#11347))
		value, err := json.Marshal(rule)
		re.NoError(err)
		key := keypath.RegionLabelPathPrefix() + "/" + rule.ID
		_, err = clientv3.NewKV(client).Put(ctx, key, string(value))
		re.NoError(err)
	}

	return ctx, client, func() {
		cancel()
		client.Close()
		etcd.Close()
		os.RemoveAll(cfg.Dir)
	}
}
