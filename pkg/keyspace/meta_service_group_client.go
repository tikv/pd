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
)

// GetEtcdClient returns the shared client for a meta-service group, including disabled groups.
// The manager owns the client; callers must not close it or change its endpoints.
func (m *MetaServiceGroupManager) GetEtcdClient(groupID string) (*clientv3.Client, error) {
	m.Lock()
	defer m.Unlock()
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
	var tlsConfig *tls.Config
	if m.tlsConfigLoader != nil {
		tlsConfig, err = m.tlsConfigLoader()
		if err != nil {
			return nil, fmt.Errorf("meta-service group %s TLS config: %w", groupID, err)
		}
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdutil.DefaultRequestTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("meta-service group %s client: %w", groupID, err)
	}
	if m.etcdClients == nil {
		m.etcdClients = make(map[string]*clientv3.Client)
	}
	m.etcdClients[groupID] = client
	return client, nil
}

func parseMetaServiceGroupEndpoints(addresses string) ([]string, error) {
	endpoints := splitMetaServiceGroupEndpoints(addresses)
	for _, address := range endpoints {
		value := address
		if !strings.Contains(value, "://") {
			value = "http://" + value
		}
		u, err := url.Parse(value)
		if err != nil || address == "" || strings.ContainsAny(address, " \t\r\n") ||
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
		endpoints, err := parseMetaServiceGroupEndpoints(addresses)
		if exists && err == nil {
			client.SetEndpoints(endpoints...)
			continue
		}
		// Invalid replacement configuration must not leave a stale backend readable.
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
