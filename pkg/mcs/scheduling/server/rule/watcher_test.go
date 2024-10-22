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
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
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
	client, err := etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	for i := 1; i < rulesNum+1; i++ {
		rule := &labeler.LabelRule{
			ID:       "test_" + strconv.Itoa(i),
			Labels:   []labeler.RegionLabel{{Key: "test", Value: "test"}},
			RuleType: labeler.KeyRange,
			Data:     keyspace.MakeKeyRanges(uint32(i)),
		}
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
