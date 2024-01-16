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
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestLoadLargeRules(t *testing.T) {
	re := require.New(t)
	ctx, client, clusterID, storage, labelerManager, clean := prepare(t)
	defer clean()

	watcher, err := NewWatcher(ctx, client, clusterID, storage, nil, nil, labelerManager)
	re.NoError(err)
	re.NotNil(watcher)
}

func BenchmarkLoadLargeRules(b *testing.B) {
	re := require.New(b)
	ctx, client, clusterID, storage, labelerManager, clean := prepare(b)
	defer clean()

	b.ResetTimer() // Resets the timer to ignore initialization time in the benchmark

	for n := 0; n < b.N; n++ {
		watcher, err := NewWatcher(ctx, client, clusterID, storage, nil, nil, labelerManager)
		re.NoError(err)
		re.NotNil(watcher)
	}
}

func prepare(t require.TestingT) (context.Context, *clientv3.Client, uint64, *endpoint.StorageEndpoint,
	*labeler.RegionLabeler, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestSingleConfig()
	cfg.Dir = os.TempDir() + "/test_etcd"
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	client, err := etcdutil.CreateEtcdClient(nil, cfg.LCUrls)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()

	clusterID := uint64(20240117)
	key := endpoint.RegionLabelPathPrefix(clusterID)
	for i := 1; i < 65536; i++ {
		rule := &labeler.LabelRule{
			ID:       "test",
			Labels:   []labeler.RegionLabel{{Key: "test", Value: "test"}},
			RuleType: labeler.KeyRange,
			Data:     keyspace.MakeKeyRanges(uint32(i)),
		}
		value, err := json.Marshal(rule)
		re.NoError(err)
		_, err = clientv3.NewKV(client).Put(ctx, key, string(value))
		re.NoError(err)
	}

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, time.Hour)
	re.NoError(err)
	return ctx, client, clusterID, storage, labelerManager, func() {
		cancel()
		client.Close()
		etcd.Close()
		os.Remove(cfg.Dir)
	}
}
