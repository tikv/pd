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

package pd

import (
	"crypto/tls"
	"sync"

	"google.golang.org/grpc"
)

var _ ServiceDiscovery = (*mockPDServiceDiscovery)(nil)

type mockPDServiceDiscovery struct {
	urls    []string
	tlsCfg  *tls.Config
	clients []ServiceClient
}

// NewMockPDServiceDiscovery creates a mock PD service discovery.
func NewMockPDServiceDiscovery(urls []string, tlsCfg *tls.Config) *mockPDServiceDiscovery {
	return &mockPDServiceDiscovery{
		urls:   urls,
		tlsCfg: tlsCfg,
	}
}

// Init directly creates the service clients with the given URLs.
func (m *mockPDServiceDiscovery) Init() error {
	m.clients = make([]ServiceClient, 0, len(m.urls))
	for _, url := range m.urls {
		m.clients = append(m.clients, newPDServiceClient(url, m.urls[0], nil, false))
	}
	return nil
}

// Close clears the service clients.
func (m *mockPDServiceDiscovery) Close() {
	clear(m.clients)
}

// GetAllServiceClients returns all service clients init in the mock PD service discovery.
func (m *mockPDServiceDiscovery) GetAllServiceClients() []ServiceClient {
	return m.clients
}

func (*mockPDServiceDiscovery) GetClusterID() uint64                           { return 0 }
func (*mockPDServiceDiscovery) GetKeyspaceID() uint32                          { return 0 }
func (*mockPDServiceDiscovery) GetKeyspaceGroupID() uint32                     { return 0 }
func (*mockPDServiceDiscovery) GetServiceURLs() []string                       { return nil }
func (*mockPDServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn { return nil }
func (*mockPDServiceDiscovery) GetClientConns() *sync.Map                      { return nil }
func (*mockPDServiceDiscovery) GetServingURL() string                          { return "" }
func (*mockPDServiceDiscovery) GetBackupURLs() []string                        { return nil }
func (*mockPDServiceDiscovery) GetServiceClient() ServiceClient                { return nil }
func (*mockPDServiceDiscovery) GetOrCreateGRPCConn(string) (*grpc.ClientConn, error) {
	return nil, nil
}
func (*mockPDServiceDiscovery) ScheduleCheckMemberChanged()              {}
func (*mockPDServiceDiscovery) CheckMemberChanged() error                { return nil }
func (*mockPDServiceDiscovery) AddServingURLSwitchedCallback(...func())  {}
func (*mockPDServiceDiscovery) AddServiceURLsSwitchedCallback(...func()) {}
