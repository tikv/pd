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
	"sync"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

const (
	legacySafePointVersion     = "safe_point_version"
	legacySafePointV2          = "v2"
	gcConfigDiagnosticCapacity = 4096
)

// EffectiveGCManagementType resolves the GC mode of persisted metadata, including
// legacy metadata. A present new field takes precedence, even when malformed.
// Kernel defaults apply only to new keyspaces, never to this resolver.
func EffectiveGCManagementType(config map[string]string) string {
	if mode, present := config[GCManagementType]; present {
		if mode == KeyspaceLevelGC {
			return KeyspaceLevelGC
		}
		return UnifiedGC
	}
	if config[legacySafePointVersion] == legacySafePointV2 {
		return KeyspaceLevelGC
	}
	return UnifiedGC
}

func defaultGCManagementType() string {
	if kerneltype.IsNextGen() {
		return KeyspaceLevelGC
	}
	return UnifiedGC
}

func isValidGCManagementType(mode string) bool {
	return mode == UnifiedGC || mode == KeyspaceLevelGC
}

// prepareNewGCConfig validates the original input before inserting a default.
func prepareNewGCConfig(config map[string]string) (map[string]string, error) {
	if _, present := config[legacySafePointVersion]; present {
		return nil, errs.ErrKeyspaceLegacyGCConfigUnsupported
	}
	mode, present := config[GCManagementType]
	if present && !isValidGCManagementType(mode) {
		return nil, errs.ErrKeyspaceGCConfigInvalid.FastGenByArgs(mode)
	}
	defaultMode := defaultGCManagementType()
	if present && mode != defaultMode {
		return nil, errs.ErrKeyspaceGCModeMismatch.FastGenByArgs(mode, kerneltype.KernelType)
	}
	if config == nil {
		config = make(map[string]string, 1)
	}
	config[GCManagementType] = defaultMode
	return config, nil
}

func applyGCConfigMutations(config map[string]string, mutations []*Mutation) error {
	oldMode := EffectiveGCManagementType(config)
	// Validate each requested write, including writes superseded later in the list.
	// Untouched historical fields may contain values no longer accepted on writes.
	for _, mutation := range mutations {
		if mutation.Key == legacySafePointVersion {
			return errs.ErrKeyspaceLegacyGCConfigUnsupported
		}
		if mutation.Key == GCManagementType && mutation.Op == OpPut && !isValidGCManagementType(mutation.Value) {
			return errs.ErrKeyspaceGCConfigInvalid.FastGenByArgs(mutation.Value)
		}
	}
	if err := applyKeyspaceConfigMutations(config, mutations); err != nil {
		return err
	}
	newMode := EffectiveGCManagementType(config)
	if newMode != oldMode {
		return errs.ErrKeyspaceGCModeImmutable.FastGenByArgs(oldMode, newMode)
	}
	if _, present := config[GCManagementType]; !present {
		config[GCManagementType] = newMode
	}
	return nil
}

type gcConfigFingerprint struct {
	mode          string
	modePresent   bool
	legacyVersion string
	legacyPresent bool
	nextGen       bool
}

type gcConfigDiagnostics struct {
	sync.Mutex
	// The LRU contains at most 4096 offending keyspaces. This outer lock makes
	// the fingerprint check and insertion atomic across concurrent observations.
	violations cache.Cache
}

// ObserveGCConfig reports malformed or kernel-incompatible persisted GC config.
// It performs no storage IO, never changes metadata, and never rejects reads.
// Repeated violations are suppressed while retained in the bounded Manager cache;
// valid observations clear entries, and eviction permits a later report.
func (manager *Manager) ObserveGCConfig(meta *keyspacepb.KeyspaceMeta) {
	if manager == nil || meta == nil || meta.GetId() == constant.NullKeyspaceID {
		return
	}
	fingerprint := gcConfigFingerprint{nextGen: kerneltype.IsNextGen()}
	fingerprint.mode, fingerprint.modePresent = meta.Config[GCManagementType]
	fingerprint.legacyVersion, fingerprint.legacyPresent = meta.Config[legacySafePointVersion]
	effective := EffectiveGCManagementType(meta.Config)
	malformed := fingerprint.modePresent && !isValidGCManagementType(fingerprint.mode)
	mismatch := (effective == KeyspaceLevelGC) != fingerprint.nextGen
	diagnostics := &manager.gcDiagnostics
	diagnostics.Lock()
	if !malformed && !mismatch {
		if diagnostics.violations != nil {
			diagnostics.violations.Remove(uint64(meta.GetId()))
		}
		diagnostics.Unlock()
		return
	}
	if diagnostics.violations == nil {
		diagnostics.violations = cache.NewCache(gcConfigDiagnosticCapacity, cache.LRUCache)
	}
	if previous, ok := diagnostics.violations.Get(uint64(meta.GetId())); ok && previous == fingerprint {
		diagnostics.Unlock()
		return
	}
	diagnostics.violations.Put(uint64(meta.GetId()), fingerprint)
	diagnostics.Unlock()
	reason := "effective gc mode does not match kernel"
	if malformed {
		reason = "malformed gc_management_type"
		if mismatch {
			reason += "; effective gc mode does not match kernel"
		}
	}
	log.Error("[keyspace] gc configuration violates policy",
		zap.Uint32("keyspace-id", meta.GetId()),
		zap.String("keyspace-name", meta.GetName()),
		zap.String("kernel", kerneltype.KernelType),
		zap.String("effective-gc-mode", effective),
		zap.Bool("gc-management-type-present", fingerprint.modePresent),
		zap.String("gc-management-type", fingerprint.mode),
		zap.Bool("safe-point-version-present", fingerprint.legacyPresent),
		zap.String("safe-point-version", fingerprint.legacyVersion),
		zap.String("reason", reason))
}
