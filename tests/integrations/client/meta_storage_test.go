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

	"github.com/tikv/pd/pkg/mcs/utils/constant"
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
		{name: "protected empty range", key: protected, end: protected},
		{name: "whole keyspace", key: "\x00", end: "\x00"},
		{name: "empty key", end: "\x00"},
		{name: "empty key and range"},
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

func TestMetaStorageProtectOwnedMetadata(t *testing.T) {
	cli, etcd := newMetaStorageTestClient(t)
	for _, tc := range []struct {
		name   string
		key    string
		prefix string
	}{
		{name: "default tso primary", key: keypath.ElectionPath(&keypath.MsParam{ServiceName: constant.TSOServiceName}), prefix: "/ms/"},
		{name: "keyspace group primary", key: keypath.ElectionPath(&keypath.MsParam{ServiceName: constant.TSOServiceName, GroupID: 1}), prefix: "/ms/"},
		{name: "scheduling primary", key: keypath.ElectionPath(&keypath.MsParam{ServiceName: constant.SchedulingServiceName}), prefix: "/ms/"},
		{name: "resource manager primary", key: keypath.ElectionPath(&keypath.MsParam{ServiceName: constant.ResourceManagerServiceName}), prefix: "/ms/"},
		{name: "expected primary", key: keypath.ExpectedPrimaryPath(&keypath.MsParam{ServiceName: constant.TSOServiceName}), prefix: "/ms/"},
		{name: "service registry", key: keypath.RegistryPath(constant.RouterServiceName, "meta-storage-test"), prefix: "/ms/"},
		{name: "controller config", key: keypath.ControllerConfigPath(), prefix: "resource_group/"},
		{name: "default resource group settings", key: "resource_group/settings/default", prefix: "resource_group/"},
		{name: "default resource group state", key: "resource_group/states/default", prefix: "resource_group/"},
		{name: "keyspace resource group settings", key: keypath.KeyspaceResourceGroupSettingPath(42, "default"), prefix: "resource_group/"},
		{name: "keyspace resource group state", key: keypath.KeyspaceResourceGroupStatePath(42, "default"), prefix: "resource_group/"},
		{name: "keyspace service limit", key: keypath.KeyspaceServiceLimitPath(42), prefix: "resource_group/"},
		{name: "region label", key: keypath.RegionLabelKeyPath("meta-storage-test"), prefix: strings.TrimSuffix(keypath.RegionLabelPathPrefix(), "/")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			lease, err := etcd.Grant(ctx, 60)
			re.NoError(err)
			upper := clientv3.GetPrefixRangeEnd(tc.prefix)
			seed, err := etcd.Put(ctx, tc.key, "original", clientv3.WithLease(lease.ID))
			re.NoError(err)

			for _, operation := range []string{"Put", "PutWithLease", "Delete", "DeleteWithRangeEnd"} {
				t.Run(operation, func(t *testing.T) {
					var err error
					switch operation {
					case "Put":
						_, err = cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(tc.key), Value: []byte("overwritten")})
					case "PutWithLease":
						_, err = cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(tc.key), Value: []byte("overwritten"), Lease: int64(lease.ID)})
					case "Delete":
						_, err = cli.Delete(ctx, &meta_storagepb.DeleteRequest{Key: []byte(tc.key)})
					case "DeleteWithRangeEnd":
						_, err = cli.Delete(ctx, &meta_storagepb.DeleteRequest{Key: []byte(tc.key), RangeEnd: []byte(upper)})
					}
					require.Equal(t, codes.PermissionDenied, status.Code(err))
					stored, err := etcd.Get(ctx, tc.key)
					require.NoError(t, err)
					require.Len(t, stored.Kvs, 1)
					require.Equal(t, "original", string(stored.Kvs[0].Value))
					require.Equal(t, int64(lease.ID), stored.Kvs[0].Lease)
					require.Equal(t, seed.Header.Revision, stored.Kvs[0].ModRevision)
				})
			}

			read, err := cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(tc.key)})
			re.NoError(err)
			re.Nil(read.GetHeader().GetError())
			re.Len(read.Kvs, 1)
			re.Equal("original", string(read.Kvs[0].Value))
			all, err := cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(tc.prefix), RangeEnd: []byte(upper)})
			re.NoError(err)
			re.Nil(all.GetHeader().GetError())
			re.NotEmpty(all.Kvs)
			for _, kv := range all.Kvs {
				re.True(strings.HasPrefix(string(kv.Key), tc.prefix))
			}

			watch, err := cli.Watch(ctx, &meta_storagepb.WatchRequest{
				Key: []byte(tc.key), StartRevision: seed.Header.Revision, PrevKv: true,
			})
			re.NoError(err)
			event, err := watch.Recv()
			re.NoError(err)
			re.Nil(event.GetHeader().GetError())
			re.Len(event.Events, 1)
			re.Equal(tc.key, string(event.Events[0].GetKv().GetKey()))
			re.Equal("original", string(event.Events[0].GetKv().GetValue()))

			// The metadata owner can still update etcd directly, and readers
			// continue receiving its changes through MetaStorage.
			_, err = etcd.Put(ctx, tc.key, "updated", clientv3.WithLease(lease.ID))
			re.NoError(err)
			event, err = watch.Recv()
			re.NoError(err)
			re.Nil(event.GetHeader().GetError())
			re.Len(event.Events, 1)
			re.Equal(tc.key, string(event.Events[0].GetKv().GetKey()))
			re.Equal("updated", string(event.Events[0].GetKv().GetValue()))
			re.Equal("original", string(event.Events[0].GetPrevKv().GetValue()))
		})
	}
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
			first, err := etcd.Put(ctx, key, "first", clientv3.WithLease(lease.ID))
			re.NoError(err)
			stored, err := etcd.Get(ctx, key)
			re.NoError(err)
			re.Len(stored.Kvs, 1)
			re.Equal(int64(lease.ID), stored.Kvs[0].Lease)
			second, err := etcd.Put(ctx, key, "second", clientv3.WithPrevKV())
			re.NoError(err)
			re.Equal("first", string(second.PrevKv.Value))

			old, err := cli.Get(ctx, &meta_storagepb.GetRequest{Key: []byte(key), Revision: first.Header.Revision})
			re.NoError(err)
			re.Nil(old.GetHeader().GetError())
			re.Len(old.GetKvs(), 1)
			re.Equal("first", string(old.Kvs[0].Value))
			_, err = etcd.Put(ctx, otherKey, "other")
			re.NoError(err)
			upper := clientv3.GetPrefixRangeEnd(prefix)
			_, err = etcd.Put(ctx, upper, "outside")
			re.NoError(err)
			markerKey := strings.TrimSuffix(prefix, "/") + "/meta-storage-boundary/marker"
			marker, err := etcd.Put(ctx, markerKey, "marker")
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
				Key: []byte(prefix), RangeEnd: []byte(upper), StartRevision: second.Header.Revision, PrevKv: true,
			})
			re.NoError(err)
			event, err := watch.Recv()
			re.NoError(err)
			re.Nil(event.GetHeader().GetError())
			re.GreaterOrEqual(len(event.GetEvents()), 1)
			re.Equal(key, string(event.Events[0].GetKv().GetKey()))
			re.Equal("first", string(event.Events[0].GetPrevKv().GetValue()))
			for seenMarker := false; !seenMarker; {
				for _, e := range event.Events {
					re.True(strings.HasPrefix(string(e.GetKv().GetKey()), prefix))
					if string(e.GetKv().GetKey()) == markerKey {
						re.Equal(marker.Header.Revision, e.GetKv().GetModRevision())
						seenMarker = true
					}
				}
				if !seenMarker {
					event, err = watch.Recv()
					re.NoError(err)
					re.Nil(event.GetHeader().GetError())
				}
			}
			watchCancel()
		})
	}
}

func TestMetaStorageLogBackupWriteCompatibility(t *testing.T) {
	cli, etcd := newMetaStorageTestClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := "/tidb/br-stream/meta-storage-boundary/a"
	otherKey := "/tidb/br-stream/meta-storage-boundary/b"
	lease, err := etcd.Grant(ctx, 60)
	require.NoError(t, err)
	first, err := cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(key), Value: []byte("first"), Lease: int64(lease.ID)})
	require.NoError(t, err)
	require.Nil(t, first.GetHeader().GetError())
	stored, err := etcd.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1)
	require.Equal(t, int64(lease.ID), stored.Kvs[0].Lease)
	second, err := cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(key), Value: []byte("second"), PrevKv: true})
	require.NoError(t, err)
	require.Nil(t, second.GetHeader().GetError())
	require.Equal(t, "first", string(second.GetPrevKv().GetValue()))
	_, err = cli.Put(ctx, &meta_storagepb.PutRequest{Key: []byte(otherKey), Value: []byte("other")})
	require.NoError(t, err)

	// RangeEnd must not turn the existing single-key Delete into a range delete.
	deleted, err := cli.Delete(ctx, &meta_storagepb.DeleteRequest{
		Key: []byte(key), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/tidb/br-stream")), PrevKv: true,
	})
	require.NoError(t, err)
	require.Nil(t, deleted.GetHeader().GetError())
	require.Len(t, deleted.GetPrevKvs(), 1)
	require.Equal(t, key, string(deleted.PrevKvs[0].Key))
	remaining, err := etcd.Get(ctx, otherKey)
	require.NoError(t, err)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, "other", string(remaining.Kvs[0].Value))
}

func TestMetaStorageGetEmptyRange(t *testing.T) {
	cli, etcd := newMetaStorageTestClient(t)
	for _, key := range []string{
		"/ms/meta-storage-boundary", "resource_group/meta-storage-boundary",
		"/tidb/br-stream/meta-storage-boundary", keypath.RegionLabelKeyPath("meta-storage-boundary"),
	} {
		t.Run(key, func(t *testing.T) {
			seed, err := etcd.Put(t.Context(), key, "value")
			require.NoError(t, err)
			resp, err := cli.Get(t.Context(), &meta_storagepb.GetRequest{Key: []byte(key), RangeEnd: []byte(key)})
			require.NoError(t, err)
			require.Nil(t, resp.GetHeader().GetError())
			require.Empty(t, resp.GetKvs())
			require.Zero(t, resp.GetCount())
			require.False(t, resp.GetMore())
			require.GreaterOrEqual(t, resp.GetHeader().GetRevision(), seed.Header.Revision)
		})
	}
}
