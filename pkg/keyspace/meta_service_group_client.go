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
	"fmt"
	"net/url"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server/config"
)

// getEtcdClient returns the shared client for a meta-service group, including disabled groups.
// The manager owns the client; callers must not close it or change its endpoints.
// Configuration changes may close a borrowed client; callers must handle the resulting errors.
func (m *MetaServiceGroupManager) getEtcdClient(groupID string) (*clientv3.Client, error) {
	m.Lock()
	defer m.Unlock()
	return m.getEtcdClientLocked(groupID)
}

// GetEtcdClientWithConfigRevision returns a client whose endpoints match a persisted config snapshot.
// The manager owns the client; callers must not close it or change its endpoints.
// Configuration changes may close a borrowed client; callers must handle the resulting errors.
// The returned revision must be checked when committing writes based on reads from the client.
// Revision zero uses the initial file config and requires the config key to remain absent.
// A stale local config fails closed until it is reloaded. Borrowed clients are never retargeted.
func (m *MetaServiceGroupManager) GetEtcdClientWithConfigRevision(ctx context.Context, groupID string) (*clientv3.Client, int64, error) {
	var cfg struct {
		Keyspace config.KeyspaceConfig `json:"keyspace"`
	}
	revision, err := m.store.LoadConfigWithRevision(ctx, &cfg)
	if err != nil {
		return nil, 0, err
	}
	m.Lock()
	defer m.Unlock()
	groups := cfg.Keyspace.MetaServiceGroups
	if revision == 0 {
		if m.hasPersistedConfig {
			return nil, 0, errors.New("persisted meta-service group config disappeared")
		}
		groups = m.initialMetaServiceGroups
	} else {
		m.hasPersistedConfig = true
	}
	addresses, ok := groups[groupID]
	if !ok {
		return nil, 0, fmt.Errorf("%w: %s", errs.ErrUnknownMetaServiceGroup, groupID)
	}
	if current, exists := m.metaServiceGroups[groupID]; !exists || current != addresses {
		return nil, 0, errs.ErrEtcdTxnConflict.GenWithStackByArgs()
	}
	client, err := m.getEtcdClientLocked(groupID)
	return client, revision, err
}

func (m *MetaServiceGroupManager) getEtcdClientLocked(groupID string) (*clientv3.Client, error) {
	if m.closed {
		return nil, errors.New("meta-service group manager is closed")
	}
	addresses, ok := m.metaServiceGroups[groupID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errs.ErrUnknownMetaServiceGroup, groupID)
	}
	if client := m.etcdClients[groupID]; client != nil {
		return client, nil
	}
	endpoints, err := parseMetaServiceGroupEndpoints(addresses)
	if err != nil {
		return nil, fmt.Errorf("meta-service group %s: %w", groupID, err)
	}
	client, err := m.newEtcdClient(endpoints)
	if err != nil {
		return nil, fmt.Errorf("meta-service group %s client: %w", groupID, err)
	}
	if m.etcdClients == nil {
		m.etcdClients = make(map[string]*clientv3.Client)
	}
	m.etcdClients[groupID] = client
	return client, nil
}

// newEtcdClient applies the shared transport settings. The caller owns the client.
func (m *MetaServiceGroupManager) newEtcdClient(endpoints []string) (*clientv3.Client, error) {
	var tlsConfig *tls.Config
	if m.tlsConfigLoader != nil {
		var err error
		tlsConfig, err = m.tlsConfigLoader()
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdutil.DefaultRequestTimeout,
		TLS:         tlsConfig,
	})
}

func parseMetaServiceGroupEndpoints(addresses string) ([]string, error) {
	endpoints := splitMetaServiceGroupEndpoints(addresses)
	if len(endpoints) == 0 {
		return nil, errors.New("etcd endpoints cannot be empty")
	}
	for _, address := range endpoints {
		value := address
		if !strings.Contains(value, "://") {
			value = "http://" + value
		}
		u, err := url.Parse(value)
		if err != nil || strings.ContainsAny(address, " \t\r\n") ||
			u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") ||
			u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid etcd endpoint %q", address)
		}
	}
	return endpoints, nil
}

// reconcileEtcdClientsLocked applies configuration to borrowed clients under the manager lock.
func (m *MetaServiceGroupManager) reconcileEtcdClientsLocked(groups map[string]string) {
	for groupID, client := range m.etcdClients {
		addresses, exists := groups[groupID]
		if exists && addresses == m.metaServiceGroups[groupID] {
			continue
		}
		// A borrower may have paired this client's endpoints with a persisted config
		// revision. Never retarget it, even for a same-protocol change or a stale reload.
		// Closing also prevents deleted or invalid replacement routes from remaining usable.
		if err := client.Close(); err != nil {
			log.Warn("failed to close meta-service group client", zap.String("group-id", groupID), zap.Error(err))
		}
		delete(m.etcdClients, groupID)
	}
}

// Close closes all owned clients and prevents further client creation. It is idempotent.
func (m *MetaServiceGroupManager) Close() error {
	m.Lock()
	defer m.Unlock()
	m.closed = true
	var closeErr error
	for groupID, client := range m.etcdClients {
		if err := client.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close meta-service group %s client: %w", groupID, err))
		}
		delete(m.etcdClients, groupID)
	}
	return closeErr
}
