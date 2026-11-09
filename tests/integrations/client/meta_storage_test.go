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

package client_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/tests"
)

func newMetaStorageTestClient(t *testing.T) (meta_storagepb.MetaStorageClient, *clientv3.Client) {
	t.Helper()
	re := require.New(t)
	cluster, err := tests.NewTestCluster(t.Context(), 1)
	re.NoError(err)
	t.Cleanup(cluster.Destroy)
	endpoints := runServer(re, cluster)
	conn, err := grpcutil.GetClientConn(t.Context(), endpoints[0], nil)
	re.NoError(err)
	t.Cleanup(func() { re.NoError(conn.Close()) })
	return meta_storagepb.NewMetaStorageClient(conn), cluster.GetLeaderServer().GetEtcdClient()
}

func TestMetaStorageRejectOutOfScope(t *testing.T) {
	cli, etcd := newMetaStorageTestClient(t)
	protected := keypath.ClusterPath() + "/meta-storage-boundary-test"
	allowed := "resource_group/meta-storage-boundary-test"
	seed, err := etcd.Put(t.Context(), protected, "protected")
	require.NoError(t, err)
	_, err = etcd.Put(t.Context(), allowed, "allowed")
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		key  string
		end  string
	}{
		{name: "protected key", key: protected},
		{name: "whole keyspace", key: "\x00", end: "\x00"},
		{name: "empty key", end: "\x00"},
		{name: "unbounded allowed prefix", key: allowed, end: "\x00"},
		{name: "cross namespace", key: "/ms/", end: "resource_group0"},
		{name: "past prefix boundary", key: "resource_group/", end: "resource_group0\x00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, operation := range []string{"Get", "Delete", "Watch"} {
				t.Run(operation, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					var err error
					switch operation {
					case "Get":
						_, err = cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(tc.key), RangeEnd: []byte(tc.end)})
					case "Delete":
						_, err = cli.Delete(ctx, &meta_storagepb.DeleteRequest{Key: []byte(tc.key), RangeEnd: []byte(tc.end)})
					case "Watch":
						var stream meta_storagepb.MetaStorage_WatchClient
						stream, err = cli.Watch(ctx, &meta_storagepb.WatchRequest{
							Key: []byte(tc.key), RangeEnd: []byte(tc.end), StartRevision: seed.Header.Revision,
						})
						if err == nil {
							_, err = stream.Recv()
						}
					}
					require.Equal(t, codes.PermissionDenied, status.Code(err))
				})
			}
		})
	}

	t.Run("Put", func(t *testing.T) {
		_, err := cli.Put(t.Context(), &meta_storagepb.PutRequest{Key: []byte(protected), Value: []byte("overwritten")})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	resp, err := etcd.Get(t.Context(), protected)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "protected", string(resp.Kvs[0].Value))
	resp, err = etcd.Get(t.Context(), allowed)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "allowed", string(resp.Kvs[0].Value))
}

func TestMetaStorageProtectTSOTimestamps(t *testing.T) {
	cli, etcd := newMetaStorageTestClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := keypath.TimestampPath(1)
	expected := time.Now().Round(0)
	value := typeutil.Uint64ToBytes(uint64(expected.UnixNano()))
	seed, err := etcd.Put(ctx, key, string(value))
	require.NoError(t, err)
	tsoStorage := storage.NewStorageWithEtcdBackend(etcd)

	for _, operation := range []string{"Put", "Delete", "DeleteWithRangeEnd"} {
		t.Run(operation, func(t *testing.T) {
			var err error
			switch operation {
			case "Put":
				_, err = cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(key), Value: []byte("invalid"), PrevKv: true})
			case "Delete":
				_, err = cli.Delete(ctx, &meta_storagepb.DeleteRequest{Key: []byte(key), PrevKv: true})
			case "DeleteWithRangeEnd":
				_, err = cli.Delete(ctx, &meta_storagepb.DeleteRequest{
					Key: []byte(key), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/ms/")), PrevKv: true,
				})
			}
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			stored, err := etcd.Get(ctx, key)
			require.NoError(t, err)
			require.Len(t, stored.Kvs, 1)
			require.Equal(t, value, stored.Kvs[0].Value)
			loaded, err := tsoStorage.LoadTimestamp(1)
			require.NoError(t, err)
			require.Equal(t, expected, loaded)
		})
	}

	// Reading and watching an existing timestamp remain compatible.
	read, err := cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(key)})
	require.NoError(t, err)
	require.Nil(t, read.GetHeader().GetError())
	require.Len(t, read.Kvs, 1)
	require.Equal(t, value, read.Kvs[0].Value)
	watch, err := cli.Watch(ctx, &meta_storagepb.WatchRequest{Key: []byte(key), StartRevision: seed.Header.Revision})
	require.NoError(t, err)
	event, err := watch.Recv()
	require.NoError(t, err)
	require.Nil(t, event.GetHeader().GetError())
	require.Len(t, event.Events, 1)
	require.Equal(t, key, string(event.Events[0].GetKv().GetKey()))
	require.Equal(t, value, event.Events[0].GetKv().GetValue())
}

func TestMetaStorageNamespaceCompatibility(t *testing.T) {
	cli, etcd := newMetaStorageTestClient(t)
	for _, prefix := range []string{
		"/ms/", "resource_group/", "/tidb/br-stream",
		strings.TrimSuffix(keypath.RegionLabelPathPrefix(), "/"),
	} {
		t.Run(prefix, func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			key := strings.TrimSuffix(prefix, "/") + "/meta-storage-boundary/a"
			otherKey := strings.TrimSuffix(prefix, "/") + "/meta-storage-boundary/b"
			lease, err := etcd.Grant(ctx, 60)
			re.NoError(err)
			first, err := cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(key), Value: []byte("first"), Lease: int64(lease.ID)})
			re.NoError(err)
			re.Nil(first.GetHeader().GetError())
			stored, err := etcd.Get(ctx, key)
			re.NoError(err)
			re.Len(stored.Kvs, 1)
			re.Equal(int64(lease.ID), stored.Kvs[0].Lease)
			second, err := cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(key), Value: []byte("second"), PrevKv: true})
			re.NoError(err)
			re.Nil(second.GetHeader().GetError())
			re.Equal("first", string(second.GetPrevKv().GetValue()))

			old, err := cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(key), Revision: first.GetHeader().GetRevision()})
			re.NoError(err)
			re.Nil(old.GetHeader().GetError())
			re.Len(old.GetKvs(), 1)
			re.Equal("first", string(old.Kvs[0].Value))
			_, err = cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(otherKey), Value: []byte("other")})
			re.NoError(err)
			upper := clientv3.GetPrefixRangeEnd(prefix)
			_, err = etcd.Put(ctx, upper, "outside")
			re.NoError(err)
			all, err := cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(prefix), RangeEnd: []byte(upper)})
			re.NoError(err)
			re.Nil(all.GetHeader().GetError())
			re.GreaterOrEqual(len(all.Kvs), 2)
			for _, kv := range all.Kvs {
				re.True(strings.HasPrefix(string(kv.Key), prefix))
			}
			limited, err := cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(prefix), RangeEnd: []byte(upper), Limit: 1})
			re.NoError(err)
			re.Nil(limited.GetHeader().GetError())
			re.Len(limited.GetKvs(), 1)
			re.True(limited.GetMore())

			watchCtx, watchCancel := context.WithCancel(ctx)
			defer watchCancel()
			watch, err := cli.Watch(watchCtx, &meta_storagepb.WatchRequest{
				Key: []byte(prefix), RangeEnd: []byte(upper), StartRevision: second.GetHeader().GetRevision(), PrevKv: true,
			})
			re.NoError(err)
			event, err := watch.Recv()
			re.NoError(err)
			re.Nil(event.GetHeader().GetError())
			re.GreaterOrEqual(len(event.GetEvents()), 1)
			re.Equal(key, string(event.Events[0].GetKv().GetKey()))
			re.Equal("first", string(event.Events[0].GetPrevKv().GetValue()))
			watchCancel()

			// Preserve the existing Delete behavior: RangeEnd does not turn it into
			// a range deletion, even when the whole requested range is allowed.
			deleted, err := cli.Delete(ctx, &meta_storagepb.DeleteRequest{Key: []byte(key), RangeEnd: []byte(upper), PrevKv: true})
			re.NoError(err)
			re.Nil(deleted.GetHeader().GetError())
			re.Len(deleted.GetPrevKvs(), 1)
			re.Equal(key, string(deleted.PrevKvs[0].Key))
			remaining, err := etcd.Get(ctx, otherKey)
			re.NoError(err)
			re.Len(remaining.Kvs, 1)
			re.Equal("other", string(remaining.Kvs[0].Value))
		})
	}
}
