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

// ServiceConfig defines the configuration interface for the TSO service.
type ServiceConfig interface {
	// GetName returns the Name
	GetName() string
	// GeBackendEndpoints returns the BackendEndpoints
	GeBackendEndpoints() string
	// GetListenAddr returns the ListenAddr
	GetListenAddr() string
	// GetAdvertiseListenAddr returns the AdvertiseListenAddr
	GetAdvertiseListenAddr() string
	// TSO-related configuration
	Config
}

// Config is used to provide TSO configuration.
type Config interface {
	// GetLeaderLease returns the leader lease.
	GetLeaderLease() int64
	// GetTSOUpdatePhysicalInterval returns TSO update physical interval.
	GetTSOUpdatePhysicalInterval() time.Duration
	// GetTSOSaveInterval returns TSO save interval.
	GetTSOSaveInterval() time.Duration
	// GetMaxResetTSGap returns the MaxResetTSGap.
	GetMaxResetTSGap() time.Duration
	// GetTLSConfig returns the TLS config.
	GetTLSConfig() *grpcutil.TLSConfig
}
