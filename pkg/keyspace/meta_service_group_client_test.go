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
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/server/config"
)

func TestMetaServiceGroupClientConfigSnapshot(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, pdClient, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	store := endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(pdClient), nil)
	groups := make([]map[string]string, 0, 2)
	for range 2 {
		_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
		t.Cleanup(clean)
		groups = append(groups, map[string]string{"g": strings.Join(client.Endpoints(), ",")})
		_, err := client.Put(ctx, "report", groups[len(groups)-1]["g"])
		re.NoError(err)
	}
	manager := NewMetaServiceGroupManager(store, groups[0], nil)
	t.Cleanup(func() { re.NoError(manager.Close()) })
	// Initial file config is usable while the persistent config key is absent.
	initial, revision, err := manager.GetEtcdClientWithConfigRevision(ctx, "g")
	re.NoError(err)
	re.Zero(revision)
	cfg := &config.Config{Keyspace: config.KeyspaceConfig{MetaServiceGroups: groups[1]}}
	re.NoError(store.SaveConfig(cfg))
	_, _, err = manager.GetEtcdClientWithConfigRevision(ctx, "g")
	re.Error(err, "a fresh config revision must not be paired with stale cached endpoints")
	manager.updateGroups(groups[1])
	re.ErrorIs(initial.Ctx().Err(), context.Canceled)
	selected, revision, err := manager.GetEtcdClientWithConfigRevision(ctx, "g")
	re.NoError(err)
	re.Positive(revision)
	response, err := pdClient.Get(ctx, keypath.ConfigPath())
	re.NoError(err)
	re.Equal(response.Kvs[0].ModRevision, revision)
	read, err := selected.Get(ctx, "report")
	re.NoError(err)
	re.Equal(groups[1]["g"], string(read.Kvs[0].Value))

	// A stale local reload has no persistent write for the final GC compare to catch.
	// It must close the selected client without changing that client's backend.
	manager.updateGroups(groups[0])
	re.ErrorIs(selected.Ctx().Err(), context.Canceled)
	re.Equal(strings.Split(groups[1]["g"], ","), selected.Endpoints())
	_, err = selected.Get(ctx, "report")
	re.Error(err)
	_, _, err = manager.GetEtcdClientWithConfigRevision(ctx, "g")
	re.Error(err)
	manager.updateGroups(groups[1])
	replacement, sameRevision, err := manager.GetEtcdClientWithConfigRevision(ctx, "g")
	re.NoError(err)
	re.Equal(revision, sameRevision)
	re.NotSame(selected, replacement)
	read, err = replacement.Get(ctx, "report")
	re.NoError(err)
	re.Equal(groups[1]["g"], string(read.Kvs[0].Value))

	// Once persistent config has been observed, deletion cannot re-enable file fallback.
	re.NoError(store.Remove(keypath.ConfigPath()))
	_, _, err = manager.GetEtcdClientWithConfigRevision(ctx, "g")
	re.ErrorContains(err, "disappeared")
}

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
	wg := &sync.WaitGroup{}
	for i := range clients {
		wg.Go(func() {
			clients[i], errs[i] = manager.getEtcdClient("g")
		})
	}
	wg.Wait()
	for i, client := range clients {
		re.NoError(errs[i])
		re.Same(clients[0], client)
	}
	client := clients[0]
	_, err := client.Put(ctx, "test", "value")
	re.NoError(err)

	// A configuration change closes borrowed clients instead of redirecting them.
	manager.updateGroups(map[string]string{"g": addresses[1]})
	re.ErrorIs(client.Ctx().Err(), context.Canceled)
	re.Equal([]string{addresses[0]}, client.Endpoints())
	replacement, err := manager.getEtcdClient("g")
	re.NoError(err)
	re.Equal([]string{addresses[1]}, replacement.Endpoints())
	re.NoError(manager.UpdateGroupsSafely(ctx, map[string]string{"g": addresses[0]}, nil, func() error { return nil }, nil))
	re.ErrorIs(replacement.Ctx().Err(), context.Canceled)
	reused, err := manager.getEtcdClient("g")
	re.NoError(err)
	re.NotSame(client, reused)
	client = reused
	// Unchanged configuration keeps the shared client usable.
	manager.updateGroups(map[string]string{"g": addresses[0]})
	reused, err = manager.getEtcdClient("g")
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
	_, err = manager.getEtcdClient("g")
	re.Error(err)

	manager.updateGroups(map[string]string{"g": strings.Join(addresses, ",")})
	replacement, err = manager.getEtcdClient("g")
	re.NoError(err)
	re.NotSame(client, replacement)
	manager.updateGroups(nil)
	re.Error(replacement.Ctx().Err())
	manager.updateGroups(map[string]string{"g": addresses[0], "other": addresses[1]})
	first, err := manager.getEtcdClient("g")
	re.NoError(err)
	second, err := manager.getEtcdClient("other")
	re.NoError(err)
	re.NoError(manager.Close())
	re.NoError(manager.Close())
	re.Error(first.Ctx().Err())
	re.Error(second.Ctx().Err())
	_, err = manager.getEtcdClient("g")
	re.Error(err)
}

func TestMetaServiceGroupClientInvalidConfig(t *testing.T) {
	for _, addresses := range []string{"", ",", "http://127.0.0.1:2379,", "http://", "http://host:bad", "http://host/path", "ftp://host:2379"} {
		t.Run(addresses, func(t *testing.T) {
			re := require.New(t)
			manager := NewMetaServiceGroupManager(nil, map[string]string{"g": addresses}, nil)
			defer func() { re.NoError(manager.Close()) }()
			_, err := manager.getEtcdClient("g")
			re.Error(err)
			manager.updateGroups(map[string]string{"g": "127.0.0.1:2379"})
			client, err := manager.getEtcdClient("g")
			re.NoError(err)
			manager.updateGroups(map[string]string{"g": addresses})
			re.Error(client.Ctx().Err())
			_, err = manager.getEtcdClient("g")
			re.Error(err)
		})
	}
}

func TestMetaServiceGroupClientTLSFailure(t *testing.T) {
	re := require.New(t)
	tlsErr := errors.New("TLS unavailable")
	manager := NewMetaServiceGroupManager(nil, map[string]string{"g": "http://127.0.0.1:2379"}, func() (*tls.Config, error) { return nil, tlsErr })
	defer func() { re.NoError(manager.Close()) }()
	_, err := manager.getEtcdClient("missing")
	re.Error(err)
	_, err = manager.getEtcdClient("g")
	re.ErrorIs(err, tlsErr)
	tlsErr = nil
	client, err := manager.getEtcdClient("g")
	re.NoError(err)
	re.NotNil(client)
}

func TestMetaServiceGroupClientTransport(t *testing.T) {
	re := require.New(t)
	serverTLS, err := transport.SelfCert(zap.NewNop(), t.TempDir(), []string{"127.0.0.1:0"}, 1)
	re.NoError(err)
	trust := transport.TLSInfo{TrustedCAFile: serverTLS.CertFile}
	clientTLS, err := trust.ClientConfig()
	re.NoError(err)
	servers, client, clean := etcdutil.NewTestEtcdCluster(t, 1, &etcdutil.TestEtcdClusterOptions{
		ServerCfgModifier: func(cfg *embed.Config) {
			cfg.ClientTLSInfo = serverTLS
			for range 2 {
				secure, err := url.Parse(tempurl.Alloc())
				re.NoError(err)
				secure.Scheme = "https"
				cfg.ListenClientUrls = append(cfg.ListenClientUrls, *secure)
			}
			cfg.AdvertiseClientUrls = cfg.ListenClientUrls
		},
		ClientCfgModifier: func(cfg *clientv3.Config) {
			// The fixture's setup client only needs the plaintext listener.
			cfg.Endpoints = cfg.Endpoints[:1]
		},
	})
	defer clean()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "transport-key", "value")
	re.NoError(err)
	urls := servers[0].Config().AdvertiseClientUrls
	httpEndpoint, httpsEndpoint, otherHTTPS := urls[0].String(), urls[1].String(), urls[2].String()
	read := func(t *testing.T, client *clientv3.Client) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		response, err := client.Get(ctx, "transport-key")
		require.NoError(t, err)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, "value", string(response.Kvs[0].Value))
	}
	for _, tc := range []struct {
		name, before, after string
		plain, local        bool
	}{
		{name: "http-to-https", before: httpEndpoint, after: httpsEndpoint},
		{name: "https-to-http", before: httpsEndpoint, after: httpEndpoint},
		{name: "http-to-unspecified-tls", before: httpEndpoint, after: urls[1].Host},
		{name: "unspecified-tls-to-http", before: urls[1].Host, after: httpEndpoint},
		{name: "unspecified-plain-to-https", before: urls[0].Host, after: httpsEndpoint, plain: true},
		{name: "https-address-change", before: httpsEndpoint, after: otherHTTPS},
		{name: "unspecified-tls-address-change", before: urls[1].Host, after: urls[2].Host},
		{name: "local-https-to-http", before: httpsEndpoint, after: httpEndpoint, local: true},
		{name: "mixed-first-endpoint-change", before: httpEndpoint + "," + httpsEndpoint, after: httpsEndpoint + "," + httpEndpoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			tlsConfig := clientTLS
			if tc.plain {
				tlsConfig = nil
			}
			manager := NewMetaServiceGroupManager(endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), map[string]string{"g": tc.before}, func() (*tls.Config, error) {
				return tlsConfig, nil
			})
			defer func() { re.NoError(manager.Close()) }()
			old, err := manager.getEtcdClient("g")
			re.NoError(err)
			read(t, old)
			watch := old.Watch(ctx, "borrowed-watch", clientv3.WithCreatedNotify())
			select {
			case response := <-watch:
				re.True(response.Created)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			tlsConfig = clientTLS
			groups := map[string]string{"g": tc.after}
			if tc.local {
				manager.updateGroups(groups)
			} else {
				re.NoError(manager.UpdateGroupsSafely(ctx, groups, nil, func() error { return nil }, nil))
			}
			updated, err := manager.getEtcdClient("g")
			re.NoError(err)
			read(t, updated)
			re.NotSame(old, updated)
			re.ErrorIs(old.Ctx().Err(), context.Canceled)
			select {
			case _, open := <-watch:
				re.False(open, "closing the client must release borrowed watches")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
	t.Run("failed-update-preserves-client", func(t *testing.T) {
		re := require.New(t)
		manager := NewMetaServiceGroupManager(endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), map[string]string{"g": httpEndpoint}, func() (*tls.Config, error) { return clientTLS, nil })
		defer func() { re.NoError(manager.Close()) }()
		old, err := manager.getEtcdClient("g")
		re.NoError(err)
		read(t, old)
		persisted := false
		unhealthyCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		err = manager.UpdateGroupsSafely(unhealthyCtx, map[string]string{"g": "https://" + urls[0].Host}, nil, func() error { persisted = true; return nil }, nil)
		re.ErrorIs(err, errs.ErrMetaServiceGroupUnhealthy)
		re.False(persisted)
		persistErr := errors.New("persist failed")
		err = manager.UpdateGroupsSafely(ctx, map[string]string{"g": httpsEndpoint}, nil, func() error { return persistErr }, nil)
		re.ErrorIs(err, persistErr)
		current, err := manager.getEtcdClient("g")
		re.NoError(err)
		re.Same(old, current)
		re.Equal([]string{httpEndpoint}, current.Endpoints())
		re.NoError(old.Ctx().Err())
		read(t, current)
	})
}
