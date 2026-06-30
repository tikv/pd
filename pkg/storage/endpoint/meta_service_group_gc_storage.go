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
	"crypto/tls"
	"strings"

	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// MetaServiceGroupIDProvider provides the meta-service group assignment for a keyspace.
type MetaServiceGroupIDProvider interface {
	GetMetaServiceGroupIDByKeyspaceID(keyspaceID uint32) (string, error)
}

// MetaServiceGroupResolver resolves a meta-service group ID to its etcd endpoints.
type MetaServiceGroupResolver interface {
	GetGroupEndpoints(groupID string) (string, bool)
}

// MetaServiceGroupGCStorageProvider resolves and caches GC state storage for meta-service groups.
type MetaServiceGroupGCStorageProvider struct {
	metaServiceGroupIDProvider MetaServiceGroupIDProvider
	groupResolver              MetaServiceGroupResolver
	tlsConfig                  *tls.Config

	mu                       syncutil.RWMutex
	metaServiceGroupStorages map[string]*cachedMetaServiceGroupStorage
}

type cachedMetaServiceGroupStorage struct {
	endpoints string
	client    *clientv3.Client
	storage   *StorageEndpoint
}

// NewMetaServiceGroupGCStorageProvider creates a provider for meta-service group GC state storage.
func NewMetaServiceGroupGCStorageProvider(
	metaServiceGroupIDProvider MetaServiceGroupIDProvider,
	groupResolver MetaServiceGroupResolver,
	tlsConfig *tls.Config,
) *MetaServiceGroupGCStorageProvider {
	return &MetaServiceGroupGCStorageProvider{
		metaServiceGroupIDProvider: metaServiceGroupIDProvider,
		groupResolver:              groupResolver,
		tlsConfig:                  tlsConfig,
		metaServiceGroupStorages:   make(map[string]*cachedMetaServiceGroupStorage),
	}
}

// GetStorage returns the GC state storage for the keyspace if it is assigned to
// a meta-service group. The second return value is false when local storage
// should be used.
func (p *MetaServiceGroupGCStorageProvider) GetStorage(keyspaceID uint32) (*StorageEndpoint, bool, error) {
	if keyspaceID == constant.NullKeyspaceID || p == nil || p.metaServiceGroupIDProvider == nil || p.groupResolver == nil {
		return nil, false, nil
	}

	metaServiceGroupID, err := p.metaServiceGroupIDProvider.GetMetaServiceGroupIDByKeyspaceID(keyspaceID)
	if err != nil {
		return nil, false, err
	}
	if metaServiceGroupID == "" {
		return nil, false, nil
	}

	metaServiceGroupEndpoints, ok := p.groupResolver.GetGroupEndpoints(metaServiceGroupID)
	if !ok {
		return nil, false, errors.Errorf("unknown meta-service group %s", metaServiceGroupID)
	}

	storage, err := p.getOrCreateStorage(metaServiceGroupID, metaServiceGroupEndpoints)
	if err != nil {
		return nil, false, err
	}
	return storage, true, nil
}

func (p *MetaServiceGroupGCStorageProvider) getOrCreateStorage(groupID, endpoints string) (*StorageEndpoint, error) {
	p.mu.RLock()
	cached := p.metaServiceGroupStorages[groupID]
	if cached != nil && cached.endpoints == endpoints {
		storage := cached.storage
		p.mu.RUnlock()
		return storage, nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	cached = p.metaServiceGroupStorages[groupID]
	if cached != nil && cached.endpoints == endpoints {
		return cached.storage, nil
	}

	normalizedEndpoints, err := normalizeMetaServiceGroupEndpoints(endpoints, p.tlsConfig)
	if err != nil {
		return nil, err
	}
	urls, err := etcdtypes.NewURLs(normalizedEndpoints)
	if err != nil {
		return nil, err
	}
	client, err := etcdutil.CreateEtcdClient(p.tlsConfig, urls, etcdutil.ServerEtcdClientPurpose, true)
	if err != nil {
		return nil, err
	}

	storage := NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
	old := p.metaServiceGroupStorages[groupID]
	p.metaServiceGroupStorages[groupID] = &cachedMetaServiceGroupStorage{
		endpoints: endpoints,
		client:    client,
		storage:   storage,
	}
	if old != nil && old.client != nil {
		_ = old.client.Close()
	}
	return storage, nil
}

func normalizeMetaServiceGroupEndpoints(endpoints string, tlsConfig *tls.Config) ([]string, error) {
	scheme := "http://"
	if tlsConfig != nil {
		scheme = "https://"
	}

	rawEndpoints := strings.Split(endpoints, ",")
	normalizedEndpoints := make([]string, 0, len(rawEndpoints))
	for _, endpoint := range rawEndpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		if !strings.Contains(endpoint, "://") {
			endpoint = scheme + endpoint
		}
		normalizedEndpoints = append(normalizedEndpoints, endpoint)
	}
	if len(normalizedEndpoints) == 0 {
		return nil, errors.New("meta-service group endpoints are empty")
	}
	return normalizedEndpoints, nil
}

// Close closes all cached etcd clients.
func (p *MetaServiceGroupGCStorageProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	var closeErr error
	for groupID, cached := range p.metaServiceGroupStorages {
		if cached.client == nil {
			continue
		}
		if err := cached.client.Close(); err != nil && closeErr == nil {
			closeErr = errors.Wrapf(err, "close meta-service group %s etcd client", groupID)
		}
	}
	p.metaServiceGroupStorages = make(map[string]*cachedMetaServiceGroupStorage)
	return closeErr
}
