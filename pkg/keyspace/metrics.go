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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

const (
	namespace = "pd"
	subsystem = "keyspace"
)

var (
	createKeyspaceStepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "create_keyspace_step_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of each step in create keyspace operation.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"step"})
)

func init() {
	if err := prometheus.Register(createKeyspaceStepDuration); err != nil {
		// If registration fails, log the error but don't panic
		// This allows the code to continue working even if metrics registration fails
		log.Warn("[keyspace] failed to register create_keyspace_step_duration_seconds metric",
			zap.Error(err))
	}
}

// createKeyspaceStep represents the steps in create keyspace operation
const (
	stepAllocateID = "allocate_id"
	stepGetConfig           = "get_config"
	stepSaveKeyspaceMeta    = "save_keyspace_meta"
	stepSplitRegion         = "split_region"
	stepEnableKeyspace      = "enable_keyspace"
	stepUpdateKeyspaceGroup = "update_keyspace_group"
)

// createKeyspaceTracer traces create-keyspace steps: one callback per step (same pattern as RegionHeartbeatProcessTracer), records metrics and logs per step.
type createKeyspaceTracer struct {
	lastCheckTime time.Time
	keyspaceID    uint32
	keyspaceName  string
}

// Begin starts the tracing.
func (t *createKeyspaceTracer) Begin() {
	t.lastCheckTime = time.Now()
}

// SetKeyspace sets keyspace id and name for step logs (call with 0, name before allocate; with newID, name after allocate).
func (t *createKeyspaceTracer) SetKeyspace(keyspaceID uint32, keyspaceName string) {
	t.keyspaceID = keyspaceID
	t.keyspaceName = keyspaceName
}

// OnAllocateIDFinished is called when allocate ID step is finished.
func (t *createKeyspaceTracer) OnAllocateIDFinished() {
	t.onStepFinished(stepAllocateID)
}

// OnGetConfigFinished is called when get config step is finished.
func (t *createKeyspaceTracer) OnGetConfigFinished() {
	t.onStepFinished(stepGetConfig)
}

// OnSaveKeyspaceMetaFinished is called when save keyspace meta step is finished.
func (t *createKeyspaceTracer) OnSaveKeyspaceMetaFinished() {
	t.onStepFinished(stepSaveKeyspaceMeta)
}

// OnSplitRegionFinished is called when split region step is finished.
func (t *createKeyspaceTracer) OnSplitRegionFinished() {
	t.onStepFinished(stepSplitRegion)
}

// OnEnableKeyspaceFinished is called when enable keyspace step is finished.
func (t *createKeyspaceTracer) OnEnableKeyspaceFinished() {
	t.onStepFinished(stepEnableKeyspace)
}

// OnUpdateKeyspaceGroupFinished is called when update keyspace group step is finished.
func (t *createKeyspaceTracer) OnUpdateKeyspaceGroupFinished() {
	t.onStepFinished(stepUpdateKeyspaceGroup)
}

func (t *createKeyspaceTracer) onStepFinished(step string) {
	now := time.Now()
	duration := now.Sub(t.lastCheckTime)
	t.lastCheckTime = now
	createKeyspaceStepDuration.WithLabelValues(step).Observe(duration.Seconds())
	if duration > time.Second {
		log.Warn("[create-keyspace] step slow",
			zap.String("step", step),
			zap.Uint32("keyspace-id", t.keyspaceID),
			zap.String("keyspace-name", t.keyspaceName),
			zap.Duration("duration", duration),
		)
	}
}
