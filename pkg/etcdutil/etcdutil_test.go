// Copyright 2016 TiKV Project Authors.
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

package etcdutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
)

func TestMemberHelpers(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfg1 := NewTestSingleConfig(t)
	etcd1, err := embed.StartEtcd(cfg1)
	defer func() {
		etcd1.Close()
	}()
	re.NoError(err)

	ep1 := cfg1.ListenClientUrls[0].String()
	client1, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep1},
	})
	re.NoError(err)

	<-etcd1.Server.ReadyNotify()

	// Test ListEtcdMembers
	listResp1, err := ListEtcdMembers(client1)
	re.NoError(err)
	re.Len(listResp1.Members, 1)
	// types.ID is an alias of uint64.
	re.Equal(uint64(etcd1.Server.ID()), listResp1.Members[0].ID)

	// Test AddEtcdMember
	// Make a new etcd config.
	cfg2 := NewTestSingleConfig(t)
	cfg2.Name = "etcd2"
	cfg2.InitialCluster = cfg1.InitialCluster + fmt.Sprintf(",%s=%s", cfg2.Name, &cfg2.ListenPeerUrls[0])
	cfg2.ClusterState = embed.ClusterStateFlagExisting

	// Add it to the cluster above.
	peerURL := cfg2.ListenPeerUrls[0].String()
	addResp, err := AddEtcdMember(client1, []string{peerURL})
	re.NoError(err)

	etcd2, err := embed.StartEtcd(cfg2)
	defer func() {
		etcd2.Close()
	}()
	re.NoError(err)
	re.Equal(uint64(etcd2.Server.ID()), addResp.Member.ID)

	ep2 := cfg2.ListenClientUrls[0].String()
	client2, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep2},
	})
	re.NoError(err)

	<-etcd2.Server.ReadyNotify()
	re.NoError(err)

	listResp2, err := ListEtcdMembers(client2)
	re.NoError(err)
	re.Len(listResp2.Members, 2)
	for _, m := range listResp2.Members {
		switch m.ID {
		case uint64(etcd1.Server.ID()):
		case uint64(etcd2.Server.ID()):
		default:
			t.Fatalf("unknown member: %v", m)
		}
	}

	// Test CheckClusterID
	urlsMap, err := types.NewURLsMap(cfg2.InitialCluster)
	re.NoError(err)
	err = CheckClusterID(etcd1.Server.Cluster().ID(), urlsMap, &tls.Config{MinVersion: tls.VersionTLS12})
	re.NoError(err)

	// Test RemoveEtcdMember
	_, err = RemoveEtcdMember(client1, uint64(etcd2.Server.ID()))
	re.NoError(err)

	listResp3, err := ListEtcdMembers(client1)
	re.NoError(err)
	re.Len(listResp3.Members, 1)
	re.Equal(uint64(etcd1.Server.ID()), listResp3.Members[0].ID)
}

func TestEtcdKVGet(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfg := NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.ListenClientUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	keys := []string{"test/key1", "test/key2", "test/key3", "test/key4", "test/key5"}
	vals := []string{"val1", "val2", "val3", "val4", "val5"}

	kv := clientv3.NewKV(client)
	for i := range keys {
		_, err = kv.Put(context.TODO(), keys[i], vals[i])
		re.NoError(err)
	}

	// Test simple point get
	resp, err := EtcdKVGet(client, "test/key1")
	re.NoError(err)
	re.Equal("val1", string(resp.Kvs[0].Value))

	// Test range get
	withRange := clientv3.WithRange("test/zzzz")
	withLimit := clientv3.WithLimit(3)
	resp, err = EtcdKVGet(client, "test/", withRange, withLimit, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	re.NoError(err)
	re.Len(resp.Kvs, 3)

	for i := range resp.Kvs {
		re.Equal(keys[i], string(resp.Kvs[i].Key))
		re.Equal(vals[i], string(resp.Kvs[i].Value))
	}

	lastKey := string(resp.Kvs[len(resp.Kvs)-1].Key)
	next := clientv3.GetPrefixRangeEnd(lastKey)
	resp, err = EtcdKVGet(client, next, withRange, withLimit, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	re.NoError(err)
	re.Len(resp.Kvs, 2)
}

func TestEtcdKVPutWithTTL(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	cfg := NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.ListenClientUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	_, err = EtcdKVPutWithTTL(context.TODO(), client, "test/ttl1", "val1", 2)
	re.NoError(err)
	_, err = EtcdKVPutWithTTL(context.TODO(), client, "test/ttl2", "val2", 4)
	re.NoError(err)

	time.Sleep(3 * time.Second)
	// test/ttl1 is outdated
	resp, err := EtcdKVGet(client, "test/ttl1")
	re.NoError(err)
	re.Equal(int64(0), resp.Count)
	// but test/ttl2 is not
	resp, err = EtcdKVGet(client, "test/ttl2")
	re.NoError(err)
	re.Equal("val2", string(resp.Kvs[0].Value))

	time.Sleep(2 * time.Second)

	// test/ttl2 is also outdated
	resp, err = EtcdKVGet(client, "test/ttl2")
	re.NoError(err)
	re.Equal(int64(0), resp.Count)
}
