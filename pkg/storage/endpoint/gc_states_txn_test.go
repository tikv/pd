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

package endpoint

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestGCStateReadOnlyTransactionLaggingFollower(t *testing.T) {
	for _, concurrentWrite := range []bool{false, true} {
		name := "completed-write"
		if concurrentWrite {
			name = "concurrent-write"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			servers, _, clean := etcdutil.NewTestEtcdCluster(t, 3, nil)
			defer clean()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			leader, follower := servers[0], servers[1]
			for _, server := range servers {
				if uint64(server.Server.ID()) == server.Server.Lead() {
					leader = server
				} else {
					follower = server
				}
			}
			re.NotEqual(leader.Server.ID(), follower.Server.ID())
			newClient := func(endpoint string) *clientv3.Client {
				client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, Context: ctx})
				re.NoError(err)
				t.Cleanup(func() { re.NoError(client.Close()) })
				return client
			}
			freshClient := newClient(leader.Config().ListenClientUrls[0].String())
			staleClient := newClient(follower.Config().ListenClientUrls[0].String())
			freshKV, staleKV := kv.NewEtcdKVBase(freshClient), kv.NewEtcdKVBase(staleClient)
			writer := NewStorageEndpoint(freshKV, nil).GetGCStateProvider()
			// Pin ordinary reads to the leader and validation to the follower. This
			// reproduces the problematic round-robin routing without relying on chance.
			reader := NewStorageEndpoint(struct {
				kv.Base
				kv.RawTxnCapable
			}{freshKV, staleKV}, nil).GetGCStateProvider()
			write := func() error {
				return writer.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
					return wb.SetGCSafePoint(0, 100)
				})
			}
			re.NoError(write())
			revisionKey := keypath.GCStateRevisionPath()
			before, err := staleClient.Get(ctx, revisionKey)
			re.NoError(err)
			re.Len(before.Kvs, 1)
			re.Equal("1", string(before.Kvs[0].Value))

			// Isolate only the follower's Raft traffic; client RPCs still work and
			// the other two members can commit the next (and final) write.
			for _, server := range servers {
				if server != follower {
					server.Server.CutPeer(follower.Server.ID())
					follower.Server.CutPeer(server.Server.ID())
				}
			}
			// MendPeer restarts remote pipelines and must only be called once.
			mend := sync.OnceFunc(func() {
				for _, server := range servers {
					if server != follower {
						server.Server.MendPeer(follower.Server.ID())
						follower.Server.MendPeer(server.Server.ID())
					}
				}
			})
			defer mend()
			if !concurrentWrite {
				re.NoError(write())
			}

			ready := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- reader.RunInGCStateTransaction(func(_ *GCStateWriteBatch) error {
					defer close(ready)
					if concurrentWrite {
						// Commit after the reader has sampled revision 1, so validation
						// must reject it even though the follower still has revision 1.
						return write()
					}
					return nil
				})
			}()
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			fresh, err := freshClient.Get(ctx, revisionKey)
			re.NoError(err)
			re.Len(fresh.Kvs, 1)
			re.Equal("2", string(fresh.Kvs[0].Value))
			stale, err := staleClient.Get(ctx, revisionKey, clientv3.WithSerializable())
			re.NoError(err)
			re.Equal(before.Kvs, stale.Kvs, "the follower must still be behind after all writes finish")

			// With no new writes, validation must wait for the follower to catch up.
			// An empty Compare-only transaction instead returns immediately using
			// revision 1: a false conflict, or a missed real conflict, respectively.
			var txnErr error
			select {
			case txnErr = <-done:
				mend()
			case <-time.After(500 * time.Millisecond):
				mend()
				select {
				case txnErr = <-done:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if concurrentWrite {
				re.True(errors.ErrorEqual(txnErr, errs.ErrEtcdTxnConflict), "got %v", txnErr)
			} else {
				re.NoError(txnErr)
			}
			after, err := freshClient.Get(ctx, revisionKey)
			re.NoError(err)
			re.Equal(fresh.Kvs, after.Kvs, "read-only validation must not write the GC revision")
			re.Equal(fresh.Header.Revision, after.Header.Revision, "read-only validation must not advance etcd revision")
		})
	}
}

func TestGCStateReadOnlyTransactionRevision(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	re := require.New(t)
	provider := NewStorageEndpoint(kv.NewEtcdKVBase(client), nil).GetGCStateProvider()
	for _, initialized := range []bool{false, true} {
		if initialized {
			re.NoError(provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				return wb.SetGCSafePoint(0, 100)
			}))
		}
		before, err := client.Get(client.Ctx(), keypath.GCStateRevisionPath())
		re.NoError(err)
		for range 2 {
			re.NoError(provider.RunInGCStateTransaction(func(_ *GCStateWriteBatch) error { return nil }))
		}
		after, err := client.Get(client.Ctx(), keypath.GCStateRevisionPath())
		re.NoError(err)
		re.Equal(before.Kvs, after.Kvs)
		re.Equal(before.Header.Revision, after.Header.Revision)
	}
}
