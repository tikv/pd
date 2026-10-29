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

package keyspace

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestMetaServiceGroupClientLifecycle(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	servers, _, clean := etcdutil.NewTestEtcdCluster(t, 2, nil)
	defer clean()
	addresses := []string{servers[0].Config().AdvertiseClientUrls[0].String(), servers[1].Config().AdvertiseClientUrls[0].String()}
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	manager := NewMetaServiceGroupManager(store, map[string]string{"g": addresses[0]}, nil)
	t.Cleanup(func() { re.NoError(manager.Close()) })

	// Disabled groups still serve existing assignments; concurrent borrowers share one client.
	clients := make([]*clientv3.Client, 16)
	errs := make([]error, len(clients))
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clients[i], errs[i] = manager.GetEtcdClient("g")
		}()
	}
	wg.Wait()
	for i, client := range clients {
		re.NoError(errs[i])
		re.Same(clients[0], client)
	}
	client := clients[0]
	_, err := client.Put(ctx, "test", "value")
	re.NoError(err)

	// Both configuration application paths reconcile the same cached client.
	manager.updateGroups(map[string]string{"g": addresses[1]})
	re.Equal([]string{addresses[1]}, client.Endpoints())
	re.NoError(manager.UpdateGroupsSafely(ctx, map[string]string{"g": addresses[0]}, nil, func() error { return nil }, nil))
	re.Equal([]string{addresses[0]}, client.Endpoints())
	reused, err := manager.GetEtcdClient("g")
	re.NoError(err)
	re.Same(client, reused)
	response, err := client.Get(ctx, "test")
	re.NoError(err)
	re.Equal("value", string(response.Kvs[0].Value))

	persistErr := errors.New("persist failed")
	re.ErrorIs(manager.UpdateGroupsSafely(ctx, map[string]string{"g": addresses[1]}, nil, func() error { return persistErr }, nil), persistErr)
	re.Equal([]string{addresses[0]}, client.Endpoints())
	re.NoError(manager.UpdateGroupsSafely(ctx, nil, []string{"g"}, func() error { return nil }, nil))
	re.Error(client.Ctx().Err())
	_, err = manager.GetEtcdClient("g")
	re.Error(err)

	manager.updateGroups(map[string]string{"g": strings.Join(addresses, ",")})
	replacement, err := manager.GetEtcdClient("g")
	re.NoError(err)
	re.NotSame(client, replacement)
	manager.updateGroups(nil)
	re.Error(replacement.Ctx().Err())
	manager.updateGroups(map[string]string{"g": addresses[0], "other": addresses[1]})
	first, err := manager.GetEtcdClient("g")
	re.NoError(err)
	second, err := manager.GetEtcdClient("other")
	re.NoError(err)
	re.NoError(manager.Close())
	re.NoError(manager.Close())
	re.Error(first.Ctx().Err())
	re.Error(second.Ctx().Err())
	_, err = manager.GetEtcdClient("g")
	re.Error(err)
}

func TestMetaServiceGroupClientInvalidConfig(t *testing.T) {
	for _, addresses := range []string{"", ",", "http://127.0.0.1:2379,", "http://", "http://host:bad", "http://host/path", "ftp://host:2379"} {
		t.Run(addresses, func(t *testing.T) {
			re := require.New(t)
			manager := NewMetaServiceGroupManager(nil, map[string]string{"g": addresses}, nil)
			defer func() { re.NoError(manager.Close()) }()
			_, err := manager.GetEtcdClient("g")
			re.Error(err)
			manager.updateGroups(map[string]string{"g": "127.0.0.1:2379"})
			client, err := manager.GetEtcdClient("g")
			re.NoError(err)
			manager.updateGroups(map[string]string{"g": addresses})
			re.Error(client.Ctx().Err())
			_, err = manager.GetEtcdClient("g")
			re.Error(err)
		})
	}
}

func TestMetaServiceGroupClientTLSFailure(t *testing.T) {
	re := require.New(t)
	tlsErr := errors.New("TLS unavailable")
	manager := NewMetaServiceGroupManager(nil, map[string]string{"g": "http://127.0.0.1:2379"}, func() (*tls.Config, error) { return nil, tlsErr })
	defer func() { re.NoError(manager.Close()) }()
	_, err := manager.GetEtcdClient("missing")
	re.Error(err)
	_, err = manager.GetEtcdClient("g")
	re.ErrorIs(err, tlsErr)
	tlsErr = nil
	client, err := manager.GetEtcdClient("g")
	re.NoError(err)
	re.NotNil(client)
}
