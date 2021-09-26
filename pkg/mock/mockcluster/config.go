// Copyright 2020 TiKV Project Authors.
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

package mockcluster

import (
	"time"

	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
)

// SetMaxMergeRegionSize updates the MaxMergeRegionSize configuration.
func (mc *Cluster) SetMaxMergeRegionSize(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxMergeRegionSize = uint64(v) })
}

// SetMaxMergeRegionKeys updates the MaxMergeRegionKeys configuration.
func (mc *Cluster) SetMaxMergeRegionKeys(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxMergeRegionKeys = uint64(v) })
}

// SetSplitMergeInterval updates the SplitMergeInterval configuration.
func (mc *Cluster) SetSplitMergeInterval(v time.Duration) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.SplitMergeInterval = typeutil.NewDuration(v) })
}

// SetEnableOneWayMerge updates the EnableOneWayMerge configuration.
func (mc *Cluster) SetEnableOneWayMerge(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableOneWayMerge = v })
}

// SetMaxSnapshotCount updates the MaxSnapshotCount configuration.
func (mc *Cluster) SetMaxSnapshotCount(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxSnapshotCount = uint64(v) })
}

// SetEnableMakeUpReplica updates the EnableMakeUpReplica configuration.
func (mc *Cluster) SetEnableMakeUpReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableMakeUpReplica = v })
}

// SetEnableRemoveExtraReplica updates the EnableRemoveExtraReplica configuration.
func (mc *Cluster) SetEnableRemoveExtraReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableRemoveExtraReplica = v })
}

// SetEnableLocationReplacement updates the EnableLocationReplacement configuration.
func (mc *Cluster) SetEnableLocationReplacement(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableLocationReplacement = v })
}

// SetEnableRemoveDownReplica updates the EnableRemoveDownReplica configuration.
func (mc *Cluster) SetEnableRemoveDownReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableRemoveDownReplica = v })
}

// SetEnableReplaceOfflineReplica updates the EnableReplaceOfflineReplica configuration.
func (mc *Cluster) SetEnableReplaceOfflineReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableReplaceOfflineReplica = v })
}

// SetLeaderSchedulePolicy updates the LeaderSchedulePolicy configuration.
func (mc *Cluster) SetLeaderSchedulePolicy(v string) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.LeaderSchedulePolicy = v })
}

// SetTolerantSizeRatio updates the TolerantSizeRatio configuration.
func (mc *Cluster) SetTolerantSizeRatio(v float64) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.TolerantSizeRatio = v })
}

// SetRegionScoreFormulaVersion updates the RegionScoreFormulaVersion configuration.
func (mc *Cluster) SetRegionScoreFormulaVersion(v string) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.RegionScoreFormulaVersion = v })
}

// SetLeaderScheduleLimit updates the LeaderScheduleLimit configuration.
func (mc *Cluster) SetLeaderScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.LeaderScheduleLimit = uint64(v) })
}

// SetRegionScheduleLimit updates the RegionScheduleLimit configuration.
func (mc *Cluster) SetRegionScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.RegionScheduleLimit = uint64(v) })
}

// SetMergeScheduleLimit updates the MergeScheduleLimit configuration.
func (mc *Cluster) SetMergeScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MergeScheduleLimit = uint64(v) })
}

// SetHotRegionScheduleLimit updates the HotRegionScheduleLimit configuration.
func (mc *Cluster) SetHotRegionScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.HotRegionScheduleLimit = uint64(v) })
}

// SetHotRegionCacheHitsThreshold updates the HotRegionCacheHitsThreshold configuration.
func (mc *Cluster) SetHotRegionCacheHitsThreshold(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.HotRegionCacheHitsThreshold = uint64(v) })
}

// SetEnablePlacementRules updates the EnablePlacementRules configuration.
func (mc *Cluster) SetEnablePlacementRules(v bool) {
	mc.updateReplicationConfig(func(r *config.ReplicationConfig) { r.EnablePlacementRules = v })
	if v {
		mc.initRuleManager()
	}
}

// SetMaxReplicas updates the maxReplicas configuration.
func (mc *Cluster) SetMaxReplicas(v int) {
	mc.updateReplicationConfig(func(r *config.ReplicationConfig) { r.MaxReplicas = uint64(v) })
}

// SetLocationLabels updates the LocationLabels configuration.
func (mc *Cluster) SetLocationLabels(v []string) {
	mc.updateReplicationConfig(func(r *config.ReplicationConfig) { r.LocationLabels = v })
}

// SetIsolationLevel updates the IsolationLevel configuration.
func (mc *Cluster) SetIsolationLevel(v string) {
	mc.updateReplicationConfig(func(r *config.ReplicationConfig) { r.IsolationLevel = v })
}

func (mc *Cluster) updateScheduleConfig(f func(*config.ScheduleConfig)) {
	s := mc.GetScheduleConfig().Clone()
	f(s)
	mc.SetScheduleConfig(s)
}

func (mc *Cluster) updateReplicationConfig(f func(*config.ReplicationConfig)) {
	r := mc.GetReplicationConfig().Clone()
	f(r)
	mc.SetReplicationConfig(r)
}
