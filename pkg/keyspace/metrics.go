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

package keyspace

import (
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
	} else {
		log.Info("[keyspace] successfully registered create_keyspace_step_duration_seconds metric")
	}
}

// createKeyspaceStep represents the steps in create keyspace operation
const (
	stepValidateName        = "validate_name"
	stepAllocateID          = "allocate_id"
	stepGetConfig           = "get_config"
	stepSaveKeyspaceMeta    = "save_keyspace_meta"
	stepSplitRegion         = "split_region"
	stepEnableKeyspace      = "enable_keyspace"
	stepUpdateKeyspaceGroup = "update_keyspace_group"
)
