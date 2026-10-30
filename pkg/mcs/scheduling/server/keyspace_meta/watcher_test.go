// Copyright 2026 TiKV Project Authors.
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

package keyspace_meta

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func prepare(t *testing.T) (context.Context, *clientv3.Client, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := etcdutil.NewTestEtcdConfig()
	cfg.Dir = t.TempDir()
	re.NoError(os.RemoveAll(cfg.Dir))
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	var client *clientv3.Client
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			cancel()
			if client != nil {
				_ = client.Close()
			}
			etcd.Close()
		})
	}
	t.Cleanup(cleanup)
	client, err = etcdutil.CreateEtcdClient(nil, cfg.ListenClientUrls, etcdutil.TestEtcdClientPurpose, true)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()
	return ctx, client, cleanup
}

func newTestCheckerController(ctx context.Context) *checker.Controller {
	conf := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, conf)
	cluster.AddLabelsStore(1, 0, map[string]string{})
	opController := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), nil)
	return checker.NewController(ctx, cluster, cluster.GetCheckerConfig(), opController)
}

// TestWatcherForceLoadsAfterLivePutFnError verifies that a live put event
// which fails inside putFn is not silently and permanently dropped until the
// next compaction-triggered reload: putFn requests a rate-limited full
// reload via ForceLoad() when it hits an error, and that reload re-applies
// the key's current, valid state from a fresh read - recovering the cache
// even though the original watch event for this key was lost.
func TestWatcherForceLoadsAfterLivePutFnError(t *testing.T) {
	re := require.New(t)
	ctx, client, clean := prepare(t)
	defer clean()

	keyspaceCache := keyspace.NewCache()
	rw, err := NewWatcher(ctx, client, newTestCheckerController(ctx), keyspaceCache)
	re.NoError(err)
	defer rw.Close()
	re.False(keyspaceCache.KeyspaceExist(42))

	meta := &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
		Name:     "ks42",
		State:    keyspacepb.KeyspaceState_ENABLED,
	}
	value, err := proto.Marshal(meta)
	re.NoError(err)

	// ForceLoad() rate-limits itself against the LoopWatcher's own
	// construction time (lastTimeForceLoad starts at time.Now() in
	// NewLoopWatcher), so a call made within that same window would be
	// silently dropped regardless of this fix. Clear that window first so
	// the ForceLoad() call below is actually the thing being tested.
	time.Sleep(1100 * time.Millisecond)

	// Enable a failpoint that fails exactly the first putFn invocation it
	// sees, then (count exhausted) goes inactive on its own - simulating a
	// transient failure without needing to race a manual Disable() against
	// ForceLoad()'s own, separately rate-limited retry. The watch stream
	// only ever delivers this one write - there is no second put to recover
	// via the normal watch path, so the only way keyspace 42 can still show
	// up in the cache is via the ForceLoad() call putFn makes on this error,
	// triggering a reload whose own putFn call for this key now succeeds.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/scheduling/server/keyspace_meta/keyspaceMetaWatcherPutFnError", "1*return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/scheduling/server/keyspace_meta/keyspaceMetaWatcherPutFnError"))
	}()
	_, err = client.Put(ctx, keypath.KeyspaceMetaPath(42), string(value))
	re.NoError(err)

	re.Eventually(func() bool {
		return keyspaceCache.KeyspaceExist(42)
	}, 5*time.Second, 20*time.Millisecond,
		"ForceLoad() must have requested a reload that re-applies this key from a fresh read")
}
