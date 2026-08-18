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

func TestReconcileRuleSnapshot(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t, false)
	defer clean()

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	conf := mockconfig.NewTestOptions()
	ruleManager := placement.NewRuleManager(ctx, storage, nil, conf)
	re.NoError(ruleManager.Initialize(3, nil, "", false))
	re.NoError(ruleManager.SetRuleGroup(&placement.RuleGroup{ID: "g", Index: 1}))
	re.NoError(ruleManager.SetRules([]*placement.Rule{
		{GroupID: "g", ID: "deleted", Role: placement.Learner, Count: 1},
		{GroupID: "g", ID: "unchanged", Role: placement.Learner, Count: 1},
	}))

	cluster := mockcluster.NewCluster(ctx, conf)
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
	updatedDefault := ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	updatedDefault.Count = 5
	updatedValue, err := json.Marshal(updatedDefault)
	re.NoError(err)
	updated, err := client.Txn(ctx).Then(
		clientv3.OpDelete(keypath.RuleKeyPath((&placement.Rule{GroupID: "g", ID: "deleted"}).StoreKey())),
		clientv3.OpDelete(keypath.RuleGroupIDPath("g")),
		clientv3.OpPut(keypath.RuleKeyPath(updatedDefault.StoreKey()), string(updatedValue)),
	).Commit()
	re.NoError(err)

	rw := &Watcher{
		rulesPathPrefix:        keypath.RulesPathPrefix(),
		ruleGroupPathPrefix:    keypath.RuleGroupPathPrefix(),
		etcdClient:             client,
		ruleManager:            ruleManager,
		checkerController:      checkerController,
		ruleRevision:           initial.Header.Revision,
		ruleRevisionContinuous: true,
	}
	nextRevision, err := rw.reconcileRuleSnapshot(ctx)
	re.NoError(err)
	re.Equal(updated.Header.Revision+1, nextRevision)
	re.Nil(ruleManager.GetRule("g", "deleted"))
	re.Equal(5, ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID).Count)
	re.Equal(unchangedVersion, ruleManager.GetRule("g", "unchanged").Version)
	re.Zero(ruleManager.GetRuleGroup("g").Index)
	re.Equal(updated.Header.Revision, rw.ruleRevision)
	re.True(rw.ruleRevisionContinuous)
}
