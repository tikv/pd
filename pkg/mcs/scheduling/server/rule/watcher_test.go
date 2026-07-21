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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"

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
	rulesNum = 16384
)

func TestLoadLargeRules(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()
	runWatcherLoadLabelRule(ctx, re, client, newTestCheckerController(ctx))
}

func BenchmarkLoadLargeRules(b *testing.B) {
	re := require.New(b)
	ctx, client, clean := prepare(b)
	defer clean()
	checkerController := newTestCheckerController(ctx)

	b.ResetTimer() // Resets the timer to ignore initialization time in the benchmark

	for range b.N {
		runWatcherLoadLabelRule(ctx, re, client, checkerController)
	}
}

func TestRuleWatcherReconcilesDeletedRuleOnReload(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepareEtcd(t)
	defer clean()

	rules := []*placement.Rule{
		{
			GroupID: placement.DefaultGroupID,
			ID:      placement.DefaultRuleID,
			Role:    placement.Voter,
			Count:   3,
		},
		{
			GroupID: "test",
			ID:      "deleted",
			Role:    placement.Learner,
			Count:   1,
		},
	}
	for _, rule := range rules {
		value, err := json.Marshal(rule)
		re.NoError(err)
		_, err = client.Put(ctx, keypath.RuleKeyPath(rule.StoreKey()), string(value))
		re.NoError(err)
	}

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	ruleManager := placement.NewRuleManager(ctx, storage, nil, mockconfig.NewTestOptions())
	re.NoError(ruleManager.Initialize(3, nil, "", true))
	watchCtx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                 watchCtx,
		cancel:              cancel,
		rulesPathPrefix:     keypath.RulesPathPrefix(),
		ruleGroupPathPrefix: keypath.RuleGroupPathPrefix(),
		etcdClient:          client,
		ruleStorage:         storage,
		checkerController:   newTestCheckerController(watchCtx),
		ruleManager:         ruleManager,
	}
	re.NoError(rw.initializeRuleWatcher())
	defer rw.Close()
	re.NotNil(ruleManager.GetRule("test", "deleted"))

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"))
	}()
	time.Sleep(1100 * time.Millisecond)
	_, err := client.Delete(ctx, keypath.RuleKeyPath(rules[1].StoreKey()))
	re.NoError(err)
	advanceResp, err := client.Put(ctx, "TestRuleWatcherReconcilesDeletedRuleOnReload", "")
	re.NoError(err)
	_, err = client.Compact(ctx, advanceResp.Header.Revision)
	re.NoError(err)
	time.Sleep(100 * time.Millisecond)

	rw.ruleWatcher.ForceLoad()
	testutil.Eventually(re, func() bool {
		return ruleManager.GetRule("test", "deleted") == nil
	}, testutil.WithWaitFor(3*time.Second), testutil.WithTickInterval(10*time.Millisecond))
}

func newTestCheckerController(ctx context.Context) *checker.Controller {
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	opController := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), nil)
	return checker.NewController(ctx, cluster, cluster.GetCheckerConfig(), opController)
}

func runWatcherLoadLabelRule(
	ctx context.Context,
	re *require.Assertions,
	client *clientv3.Client,
	checkerController *checker.Controller,
) {
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
		checkerController:     checkerController,
	}
	err = rw.initializeRegionLabelWatcher()
	re.NoError(err)
	re.Len(labelerManager.GetAllLabelRules(), rulesNum)
	cancel()
}

func prepareEtcd(t require.TestingT) (context.Context, *clientv3.Client, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestEtcdConfig()
	var err error
	cfg.Dir, err = os.MkdirTemp("", "pd_tests")
	re.NoError(err)
	os.RemoveAll(cfg.Dir)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls, etcdutil.TestEtcdClientPurpose, true)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	return ctx, client, func() {
		cancel()
		client.Close()
		etcd.Close()
		os.RemoveAll(cfg.Dir)
	}
}

func prepare(t require.TestingT) (context.Context, *clientv3.Client, func()) {
	re := require.New(t)
	ctx, client, clean := prepareEtcd(t)

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
		_, err := client.Txn(ctx).Then(ops...).Commit()
		re.NoError(err)
	}

	return ctx, client, clean
}
