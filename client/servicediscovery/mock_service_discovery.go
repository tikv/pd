// Copyright 2024 TiKV Project Authors.
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

package servicediscovery

import (
	"crypto/tls"
	"sync"

	"google.golang.org/grpc"
)

var _ ServiceDiscovery = (*mockServiceDiscovery)(nil)

type mockServiceDiscovery struct {
	urls    []string
	tlsCfg  *tls.Config
	clients []ServiceClient
}

// NewMockServiceDiscovery creates a mock service discovery.
func NewMockServiceDiscovery(urls []string, tlsCfg *tls.Config) *mockServiceDiscovery {
	return &mockServiceDiscovery{
		urls:   urls,
		tlsCfg: tlsCfg,
	}
}

// Init directly creates the service clients with the given URLs.
func (m *mockServiceDiscovery) Init() error {
	m.clients = make([]ServiceClient, 0, len(m.urls))
	for _, url := range m.urls {
		m.clients = append(m.clients, newPDServiceClient(url, m.urls[0], nil, false))
	}
	return nil
}

// Close clears the service clients.
func (m *mockServiceDiscovery) Close() {
	clear(m.clients)
}

// GetAllServiceClients returns all service clients init in the mock service discovery.
func (m *mockServiceDiscovery) GetAllServiceClients() []ServiceClient {
	return m.clients
}

// GetClusterID implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetClusterID() uint64 { return 0 }

// GetKeyspaceID implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetKeyspaceID() uint32 { return 0 }

// SetKeyspaceID implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) SetKeyspaceID(uint32) {}

// GetKeyspaceGroupID implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetKeyspaceGroupID() uint32 { return 0 }

// GetServiceURLs implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetServiceURLs() []string { return nil }

// GetServingEndpointClientConn implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn { return nil }

// GetClientConns implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetClientConns() *sync.Map { return nil }

// GetServingURL implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetServingURL() string { return "" }

// GetBackupURLs implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetBackupURLs() []string { return nil }

// GetServiceClient implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetServiceClient() ServiceClient { return nil }

// GetServiceClientByKind implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetServiceClientByKind(APIKind) ServiceClient { return nil }

// GetOrCreateGRPCConn implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) GetOrCreateGRPCConn(string) (*grpc.ClientConn, error) {
	return nil, nil
}

// ScheduleCheckMemberChanged implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) ScheduleCheckMemberChanged() {}

// CheckMemberChanged implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) CheckMemberChanged() error { return nil }

// AddServingURLSwitchedCallback implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) AddServingURLSwitchedCallback(...func()) {}

// AddServiceURLsSwitchedCallback implements the ServiceDiscovery interface.
func (*mockServiceDiscovery) AddServiceURLsSwitchedCallback(...func()) {}
