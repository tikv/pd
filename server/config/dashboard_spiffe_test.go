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

package config

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/dashboard/spiffetls"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestDashboardSPIFFEValidation(t *testing.T) {
	valid := DashboardConfig{
		TiDBSPIFFEWorkloadAPIAddr: "unix:///spire-agent-socket/spire-agent.sock",
		TiDBSPIFFEClientIDPattern: "spiffe://example.org/ns/database/sa/pd/*",
		TiDBSPIFFEServerIDPattern: "spiffe://example.org/ns/database/sa/tidb/*",
	}
	tests := []struct {
		name   string
		change func(*DashboardConfig)
		valid  bool
	}{
		{name: "default timeout", valid: true},
		{name: "explicit timeout", change: func(c *DashboardConfig) { c.TiDBSPIFFEWorkloadAPITimeout.Duration = time.Second }, valid: true},
		{name: "missing endpoint", change: func(c *DashboardConfig) { c.TiDBSPIFFEWorkloadAPIAddr = "" }},
		{name: "missing client identity", change: func(c *DashboardConfig) { c.TiDBSPIFFEClientIDPattern = "" }},
		{name: "missing server identity", change: func(c *DashboardConfig) { c.TiDBSPIFFEServerIDPattern = "" }},
		{name: "non unix endpoint", change: func(c *DashboardConfig) { c.TiDBSPIFFEWorkloadAPIAddr = "tcp://127.0.0.1:8081" }},
		{name: "negative timeout", change: func(c *DashboardConfig) { c.TiDBSPIFFEWorkloadAPITimeout.Duration = -time.Second }},
		{name: "mixed CA", change: func(c *DashboardConfig) { c.TiDBCAPath = "ca.pem" }},
		{name: "mixed certificate", change: func(c *DashboardConfig) { c.TiDBCertPath = "cert.pem" }},
		{name: "mixed key", change: func(c *DashboardConfig) { c.TiDBKeyPath = "key.pem" }},
		{name: "partial wildcard", change: func(c *DashboardConfig) { c.TiDBSPIFFEServerIDPattern = "spiffe://example.org/ns/database/sa/tidb-*" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			if test.change != nil {
				test.change(&cfg)
			}
			err := cfg.Validate()
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				pd := NewConfig()
				pd.Dashboard = cfg
				require.ErrorContains(t, pd.Validate(), "invalid dashboard configuration")
			}
		})
	}
	for _, cfg := range []DashboardConfig{{}, {TiDBCAPath: "ca.pem"}} {
		require.NoError(t, cfg.Validate(), "existing configurations must remain valid")
	}
	_, err := valid.ToTiDBTLSConfig()
	require.ErrorContains(t, err, "managed Workload API source", "SPIFFE must never silently produce plaintext")
	plain, err := (&DashboardConfig{}).ToTiDBTLSConfig()
	require.NoError(t, err)
	require.Nil(t, plain)
}

func TestDashboardSPIFFETOML(t *testing.T) {
	var cfg struct {
		Dashboard DashboardConfig `toml:"dashboard"`
	}
	meta, err := toml.Decode(`
[dashboard]
tidb-spiffe-workload-api-addr = "unix:///spire-agent-socket/spire-agent.sock"
tidb-spiffe-workload-api-timeout = "5s"
tidb-spiffe-client-id-pattern = "spiffe://example.org/ns/database/sa/pd/*"
tidb-spiffe-server-id-pattern = "spiffe://example.org/ns/database/sa/tidb/*"
`, &cfg)
	require.NoError(t, err)
	require.Empty(t, meta.Undecoded())
	require.NoError(t, cfg.Dashboard.Validate())
	want := spiffetls.Config{
		Endpoint:        "unix:///spire-agent-socket/spire-agent.sock",
		Timeout:         5 * time.Second,
		ClientIDPattern: "spiffe://example.org/ns/database/sa/pd/*",
		ServerIDPattern: "spiffe://example.org/ns/database/sa/tidb/*",
	}
	if diff := cmp.Diff(want, cfg.Dashboard.SPIFFEConfig()); diff != "" {
		t.Fatalf("SPIFFE configuration mismatch (-want +got):\n%s", diff)
	}
	cfg.Dashboard.TiDBSPIFFEWorkloadAPITimeout = typeutil.Duration{}
	require.Equal(t, 30*time.Second, cfg.Dashboard.SPIFFEConfig().Timeout)
}
