// Copyright 2022 TiKV Project Authors.
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
	"errors"
	"sync/atomic"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/storage/endpoint"
)

// ServiceMiddlewarePersistOptions wraps all service middleware configurations that need to persist to storage and
// allows to access them safely.
type ServiceMiddlewarePersistOptions struct {
	audit         atomic.Value
	rateLimit     atomic.Value
	grpcRateLimit atomic.Value
}

// NewServiceMiddlewarePersistOptions creates a new ServiceMiddlewarePersistOptions instance.
func NewServiceMiddlewarePersistOptions(cfg *ServiceMiddlewareConfig) *ServiceMiddlewarePersistOptions {
	o := &ServiceMiddlewarePersistOptions{}
	o.audit.Store(&cfg.AuditConfig)
	o.rateLimit.Store(&cfg.RateLimitConfig)
	o.grpcRateLimit.Store(&cfg.GRPCRateLimitConfig)
	return o
}

// GetAuditConfig returns pd service middleware configurations.
func (o *ServiceMiddlewarePersistOptions) GetAuditConfig() *AuditConfig {
	return o.audit.Load().(*AuditConfig)
}

// SetAuditConfig sets the PD service middleware configuration.
func (o *ServiceMiddlewarePersistOptions) SetAuditConfig(cfg *AuditConfig) {
	o.audit.Store(cfg)
}

// IsAuditEnabled returns whether audit middleware is enabled
func (o *ServiceMiddlewarePersistOptions) IsAuditEnabled() bool {
	return o.GetAuditConfig().EnableAudit
}

// GetRateLimitConfig returns pd service middleware configurations.
func (o *ServiceMiddlewarePersistOptions) GetRateLimitConfig() *RateLimitConfig {
	return o.rateLimit.Load().(*RateLimitConfig)
}

// SetRateLimitConfig sets the PD service middleware configuration.
func (o *ServiceMiddlewarePersistOptions) SetRateLimitConfig(cfg *RateLimitConfig) {
	o.rateLimit.Store(cfg)
}

// IsRateLimitEnabled returns whether rate limit middleware is enabled
func (o *ServiceMiddlewarePersistOptions) IsRateLimitEnabled() bool {
	return o.GetRateLimitConfig().EnableRateLimit
}

// GetGRPCRateLimitConfig returns pd service middleware configurations.
func (o *ServiceMiddlewarePersistOptions) GetGRPCRateLimitConfig() *GRPCRateLimitConfig {
	return o.grpcRateLimit.Load().(*GRPCRateLimitConfig)
}

// SetGRPCRateLimitConfig sets the PD service middleware configuration.
func (o *ServiceMiddlewarePersistOptions) SetGRPCRateLimitConfig(cfg *GRPCRateLimitConfig) {
	o.grpcRateLimit.Store(cfg)
}

// IsGRPCRateLimitEnabled returns whether rate limit middleware is enabled
func (o *ServiceMiddlewarePersistOptions) IsGRPCRateLimitEnabled() bool {
	return o.GetGRPCRateLimitConfig().EnableRateLimit
}

// Persist saves the configuration to the storage.
func (o *ServiceMiddlewarePersistOptions) Persist(storage endpoint.ServiceMiddlewareStorage) error {
	cfg := &ServiceMiddlewareConfig{
		AuditConfig:         *o.GetAuditConfig(),
		RateLimitConfig:     *o.GetRateLimitConfig(),
		GRPCRateLimitConfig: *o.GetGRPCRateLimitConfig(),
	}
	err := storage.SaveServiceMiddlewareConfig(cfg)
	failpoint.Inject("persistServiceMiddlewareFail", func() {
		err = errors.New("fail to persist")
	})
	return err
}

// Reload reloads the configuration from the storage.
func (o *ServiceMiddlewarePersistOptions) Reload(storage endpoint.ServiceMiddlewareStorage) error {
	cfg := NewServiceMiddlewareConfig()

	isExist, err := storage.LoadServiceMiddlewareConfig(cfg)
	if err != nil {
		return err
	}
	if isExist {
		o.audit.Store(&cfg.AuditConfig)
		o.rateLimit.Store(&cfg.RateLimitConfig)
		o.grpcRateLimit.Store(&cfg.GRPCRateLimitConfig)
	}
	return nil
}
