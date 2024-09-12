// Copyright 2023 TiKV Project Authors.
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

package tso

import (
	"time"

	"github.com/tikv/pd/pkg/utils/grpcutil"
)

var _ ServiceConfig = (*TestServiceConfig)(nil)

// TestServiceConfig implements the ServiceConfig interface.
type TestServiceConfig struct {
	Name                      string              // Name of the service.
	BackendEndpoints          string              // Backend endpoints of the service.
	ListenAddr                string              // Address the service listens on.
	AdvertiseListenAddr       string              // Address the service advertises to the clients.
	LeaderLease               int64               // Leader lease.
	LocalTSOEnabled           bool                // Whether local TSO is enabled.
	TSOUpdatePhysicalInterval time.Duration       // Interval to update TSO in physical storage.
	TSOSaveInterval           time.Duration       // Interval to save TSO to physical storage.
	MaxResetTSGap             time.Duration       // Maximum gap to reset TSO.
	TLSConfig                 *grpcutil.TLSConfig // TLS configuration.
}

// GetName returns the Name field of TestServiceConfig.
func (c *TestServiceConfig) GetName() string {
	return c.Name
}

// GeBackendEndpoints returns the BackendEndpoints field of TestServiceConfig.
func (c *TestServiceConfig) GeBackendEndpoints() string {
	return c.BackendEndpoints
}

// GetListenAddr returns the ListenAddr field of TestServiceConfig.
func (c *TestServiceConfig) GetListenAddr() string {
	return c.ListenAddr
}

// GetAdvertiseListenAddr returns the AdvertiseListenAddr field of TestServiceConfig.
func (c *TestServiceConfig) GetAdvertiseListenAddr() string {
	return c.AdvertiseListenAddr
}

// GetLeaderLease returns the LeaderLease field of TestServiceConfig.
func (c *TestServiceConfig) GetLeaderLease() int64 {
	return c.LeaderLease
}

// IsLocalTSOEnabled returns the LocalTSOEnabled field of TestServiceConfig.
func (c *TestServiceConfig) IsLocalTSOEnabled() bool {
	return c.LocalTSOEnabled
}

// GetTSOUpdatePhysicalInterval returns the TSOUpdatePhysicalInterval field of TestServiceConfig.
func (c *TestServiceConfig) GetTSOUpdatePhysicalInterval() time.Duration {
	return c.TSOUpdatePhysicalInterval
}

// GetTSOSaveInterval returns the TSOSaveInterval field of TestServiceConfig.
func (c *TestServiceConfig) GetTSOSaveInterval() time.Duration {
	return c.TSOSaveInterval
}

// GetMaxResetTSGap returns the MaxResetTSGap field of TestServiceConfig.
func (c *TestServiceConfig) GetMaxResetTSGap() time.Duration {
	return c.MaxResetTSGap
}

// GetTLSConfig returns the TLSConfig field of TestServiceConfig.
func (c *TestServiceConfig) GetTLSConfig() *grpcutil.TLSConfig {
	return c.TLSConfig
}
