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

package gc

import (
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
)

// gcConfigLoadDiagnostics belongs to one NextGen full-load attempt. Parallel
// pages count every violation but retain only a fixed number of details.
// A nil receiver disables collection for Classic. Failed loads discard it.
type gcConfigLoadDiagnostics struct {
	mu      sync.Mutex
	count   int
	details [20]gcConfigDiagnostic
}

type gcConfigDiagnostic struct {
	id            uint32
	name          string
	mode          string
	modePresent   bool
	legacyVersion string
	legacyPresent bool
	effectiveMode string
	reason        string
}

func (d *gcConfigLoadDiagnostics) observe(meta *keyspacepb.KeyspaceMeta) {
	if d == nil || meta.GetId() == constant.NullKeyspaceID {
		return
	}
	mode, present := meta.Config[keyspace.GCManagementType]
	effective := keyspace.EffectiveGCManagementType(meta.Config)
	malformed := present && mode != keyspace.UnifiedGC && mode != keyspace.KeyspaceLevelGC
	if !malformed && effective == keyspace.KeyspaceLevelGC {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.count++
	if d.count > len(d.details) {
		return
	}
	legacyVersion, legacyPresent := meta.Config["safe_point_version"]
	reason := "effective gc mode does not match kernel"
	if malformed {
		reason = "malformed gc_management_type; " + reason
	}
	d.details[d.count-1] = gcConfigDiagnostic{
		id: meta.GetId(), name: meta.GetName(), mode: mode, modePresent: present,
		legacyVersion: legacyVersion, legacyPresent: legacyPresent,
		effectiveMode: effective, reason: reason,
	}
}

// report runs only after a successful load has joined all its workers.
func (d *gcConfigLoadDiagnostics) report(revision int64) {
	if d == nil || d.count == 0 {
		return
	}
	details := d.details[:min(d.count, len(d.details))]
	log.Error("GC keyspace configuration violations found during full load",
		zap.Int64("revision", revision), zap.Int("violation-count", d.count),
		zap.Int("omitted-count", d.count-len(details)), zap.Objects("details", details))
}

// MarshalLogObject encodes a sampled persisted configuration without hiding absent fields.
func (d gcConfigDiagnostic) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint32("keyspace-id", d.id)
	encoder.AddString("keyspace-name", d.name)
	encoder.AddString("effective-gc-mode", d.effectiveMode)
	encoder.AddBool("gc-management-type-present", d.modePresent)
	encoder.AddString("gc-management-type", d.mode)
	encoder.AddBool("safe-point-version-present", d.legacyPresent)
	encoder.AddString("safe-point-version", d.legacyVersion)
	encoder.AddString("reason", d.reason)
	return nil
}
