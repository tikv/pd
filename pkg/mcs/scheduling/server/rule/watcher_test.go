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
	"sync"
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

func TestRuleWatcherReconcilesSnapshotOnReload(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepareEtcd(t)
	defer clean()

	defaultRule := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   3,
	}
	deletedRule := &placement.Rule{
		GroupID: "test",
		ID:      "deleted",
		Role:    placement.Learner,
		Count:   1,
	}
	for _, rule := range []*placement.Rule{defaultRule, deletedRule} {
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
	re.NotNil(ruleManager.GetRule(deletedRule.GroupID, deletedRule.ID))

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"))
	}()
	time.Sleep(1100 * time.Millisecond)
	_, err := client.Delete(ctx, keypath.RuleKeyPath(deletedRule.StoreKey()))
	re.NoError(err)
	updatedRule := defaultRule.Clone()
	updatedRule.Count = 5
	value, err := json.Marshal(updatedRule)
	re.NoError(err)
	updateResp, err := client.Put(ctx, keypath.RuleKeyPath(updatedRule.StoreKey()), string(value))
	re.NoError(err)
	_, err = client.Compact(ctx, updateResp.Header.Revision)
	re.NoError(err)
	time.Sleep(100 * time.Millisecond)

	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	releaseSnapshotLoad := func() {
		releaseOnce.Do(func() { close(releaseLoad) })
	}
	re.NoError(failpoint.EnableCall(
		"github.com/tikv/pd/pkg/mcs/scheduling/server/rule/placementRuleSnapshotItemLoaded",
		func() {
			startOnce.Do(func() { close(loadStarted) })
			<-releaseLoad
		},
	))
	defer func() {
		releaseSnapshotLoad()
		re.NoError(failpoint.Disable(
			"github.com/tikv/pd/pkg/mcs/scheduling/server/rule/placementRuleSnapshotItemLoaded"))
	}()

	rw.ruleWatcher.ForceLoad()
	select {
	case <-loadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("placement rule snapshot reload did not start")
	}
	type snapshotReadResult struct {
		loadedRule  *placement.Rule
		deletedRule *placement.Rule
	}
	readDone := make(chan snapshotReadResult, 1)
	go func() {
		readDone <- snapshotReadResult{
			loadedRule:  ruleManager.GetRule(defaultRule.GroupID, defaultRule.ID),
			deletedRule: ruleManager.GetRule(deletedRule.GroupID, deletedRule.ID),
		}
	}()
	select {
	case result := <-readDone:
		re.NotNil(result.loadedRule)
		re.Equal(3, result.loadedRule.Count)
		re.NotNil(result.deletedRule)
	case <-time.After(3 * time.Second):
		t.Fatal("placement rule reads were blocked by snapshot reload")
	}
	releaseSnapshotLoad()
	testutil.Eventually(re, func() bool {
		loadedRule := ruleManager.GetRule(defaultRule.GroupID, defaultRule.ID)
		return ruleManager.GetRule(deletedRule.GroupID, deletedRule.ID) == nil &&
			loadedRule != nil && loadedRule.Count == updatedRule.Count
	}, testutil.WithWaitFor(3*time.Second), testutil.WithTickInterval(10*time.Millisecond))
}

func TestRegionLabelWatcherReconcilesSnapshotOnReload(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepareEtcd(t)
	defer clean()

	rules := []*labeler.LabelRule{
		keyspace.MakeTxnLabelRule(1),
		keyspace.MakeTxnLabelRule(2),
	}
	for _, rule := range rules {
		value, err := json.Marshal(rule)
		re.NoError(err)
		_, err = client.Put(ctx, keypath.RegionLabelKeyPath(rule.ID), string(value))
		re.NoError(err)
	}

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	regionLabeler, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)
	watchCtx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                   watchCtx,
		cancel:                cancel,
		regionLabelPathPrefix: keypath.RegionLabelPathPrefix(),
		etcdClient:            client,
		regionLabeler:         regionLabeler,
		checkerController:     newTestCheckerController(watchCtx),
	}
	re.NoError(rw.initializeRegionLabelWatcher())
	defer rw.Close()
	re.NotNil(regionLabeler.GetLabelRule(rules[1].ID))

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"))
	}()
	time.Sleep(1100 * time.Millisecond)
	_, err = client.Delete(ctx, keypath.RegionLabelKeyPath(rules[1].ID))
	re.NoError(err)
	updatedRule := keyspace.MakeTxnLabelRule(1)
	updatedRule.Labels[0].Value = "updated"
	value, err := json.Marshal(updatedRule)
	re.NoError(err)
	updateResp, err := client.Put(ctx, keypath.RegionLabelKeyPath(updatedRule.ID), string(value))
	re.NoError(err)
	_, err = client.Compact(ctx, updateResp.Header.Revision)
	re.NoError(err)
	time.Sleep(100 * time.Millisecond)

	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	releaseSnapshotLoad := func() {
		releaseOnce.Do(func() { close(releaseLoad) })
	}
	re.NoError(failpoint.EnableCall(
		"github.com/tikv/pd/pkg/mcs/scheduling/server/rule/regionLabelSnapshotRuleLoaded",
		func() {
			startOnce.Do(func() { close(loadStarted) })
			<-releaseLoad
		},
	))
	defer func() {
		releaseSnapshotLoad()
		re.NoError(failpoint.Disable(
			"github.com/tikv/pd/pkg/mcs/scheduling/server/rule/regionLabelSnapshotRuleLoaded"))
	}()

	rw.labelWatcher.ForceLoad()
	select {
	case <-loadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot reload did not start")
	}
	type snapshotReadResult struct {
		loadedRule  *labeler.LabelRule
		deletedRule *labeler.LabelRule
	}
	readDone := make(chan snapshotReadResult, 1)
	go func() {
		readDone <- snapshotReadResult{
			loadedRule:  regionLabeler.GetLabelRule(rules[0].ID),
			deletedRule: regionLabeler.GetLabelRule(rules[1].ID),
		}
	}()
	select {
	case result := <-readDone:
		re.NotNil(result.loadedRule)
		re.Equal(rules[0].Labels[0].Value, result.loadedRule.Labels[0].Value)
		re.NotNil(result.deletedRule)
	case <-time.After(3 * time.Second):
		t.Fatal("region label reads were blocked by snapshot reload")
	}
	releaseSnapshotLoad()
	testutil.Eventually(re, func() bool {
		loadedRule := regionLabeler.GetLabelRule(rules[0].ID)
		return regionLabeler.GetLabelRule(rules[1].ID) == nil && loadedRule != nil &&
			loadedRule.Labels[0].Value == "updated"
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
	defer rw.Close()
	re.Len(labelerManager.GetAllLabelRules(), rulesNum)
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
