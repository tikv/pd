// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"time"

	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/storelimit"
)

// ScheduleOptions is an interface to access configurations.
// TODO: merge the Options to schedule.Options
type ScheduleOptions interface {
	GetLocationLabels() []string
	GetIsolationLevel() string

	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64
	GetTolerantSizeRatio() float64
	GetStoreLimitByType(storeID uint64, typ storelimit.Type) float64

	GetSchedulerMaxWaitingOperator() uint64
	GetLeaderScheduleLimit() uint64
	GetRegionScheduleLimit() uint64
	GetReplicaScheduleLimit() uint64
	GetMergeScheduleLimit() uint64
	GetHotRegionScheduleLimit() uint64
	GetMaxReplicas() int
	GetHotRegionCacheHitsThreshold() int
	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetLeaderSchedulePolicy() core.SchedulePolicy
	GetKeyType() core.KeyType

	IsMakeUpReplicaEnabled() bool
	IsRemoveExtraReplicaEnabled() bool
	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool

	GetMaxStoreDownTime() time.Duration
}
