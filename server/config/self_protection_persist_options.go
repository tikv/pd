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
	"github.com/tikv/pd/server/storage/endpoint"
)

// SelfProtectionPersistOptions wraps all self protection configurations that need to persist to storage and
// allows to access them safely.
type SelfProtectionPersistOptions struct {
	audit atomic.Value
}

// NewSelfProtectionPersistOptions creates a new SelfProtectionPersistOptions instance.
func NewSelfProtectionPersistOptions(cfg *SelfProtectionConfig) *SelfProtectionPersistOptions {
	o := &SelfProtectionPersistOptions{}
	o.audit.Store(&cfg.AuditConfig)
	return o
}

// GetSelfProtectionConfig returns pd self protection configurations.
func (o *SelfProtectionPersistOptions) GetAuditConfig() *AuditConfig {
	return o.audit.Load().(*AuditConfig)
}

// SetSelfProtectionConfig sets the PD self protection configuration.
func (o *SelfProtectionPersistOptions) SetAuditConfig(cfg *AuditConfig) {
	o.audit.Store(cfg)
}

// IsAuditEnabled returns whether audit middleware is enabled
func (o *SelfProtectionPersistOptions) IsAuditEnabled() bool {
	return o.GetAuditConfig().EnableAudit
}

// Persist saves the configuration to the storage.
func (o *SelfProtectionPersistOptions) Persist(storage endpoint.SelfProtectionStorage) error {
	cfg := &SelfProtectionConfig{
		AuditConfig: *o.GetAuditConfig(),
	}
	err := storage.SaveSelfProtectionConfig(cfg)
	failpoint.Inject("persistSelfProtectionFail", func() {
		err = errors.New("fail to persist")
	})
	return err
}

// Reload reloads the configuration from the storage.
func (o *SelfProtectionPersistOptions) Reload(storage endpoint.SelfProtectionStorage) error {
	cfg := NewSelfProtectionConfig()

	isExist, err := storage.LoadSelfProtectionConfig(cfg)
	if err != nil {
		return err
	}
	if isExist {
		o.audit.Store(&cfg.AuditConfig)
	}
	return nil
}
