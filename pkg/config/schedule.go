// Copyright 2016 PingCAP, Inc.
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

package config

import "github.com/pingcap/pd/pkg/typeutil"

// ScheduleConfig is the schedule configuration.
type ScheduleConfig struct {
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	MaxSnapshotCount uint64 `toml:"max-snapshot-count,omitempty" json:"max-snapshot-count"`
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownTime typeutil.Duration `toml:"max-store-down-time,omitempty" json:"max-store-down-time"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit,omitempty" json:"leader-schedule-limit"`
	// RegionScheduleLimit is the max coexist region schedules.
	RegionScheduleLimit uint64 `toml:"region-schedule-limit,omitempty" json:"region-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `toml:"replica-schedule-limit,omitempty" json:"replica-schedule-limit"`
}

func (c *ScheduleConfig) adjust() {
	adjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	adjustDuration(&c.MaxStoreDownTime, defaultMaxStoreDownTime)
	adjustUint64(&c.LeaderScheduleLimit, defaultLeaderScheduleLimit)
	adjustUint64(&c.RegionScheduleLimit, defaultRegionScheduleLimit)
	adjustUint64(&c.ReplicaScheduleLimit, defaultReplicaScheduleLimit)
}
