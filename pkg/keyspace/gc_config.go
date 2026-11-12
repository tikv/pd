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
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

const (
	legacySafePointVersion = "safe_point_version"
	legacySafePointV2      = "v2"
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
