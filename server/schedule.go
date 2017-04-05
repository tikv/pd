// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/pd/pkg/config"
)

// ScheduleOption is a wrapper to access the configuration safely.
type scheduleOption struct {
	v   atomic.Value
	rep *Replication
}

func newScheduleOption(cfg *config.Config) *scheduleOption {
	o := &scheduleOption{}
	o.store(&cfg.Schedule)
	o.rep = newReplication(&cfg.Replication)
	return o
}

func (o *scheduleOption) load() *config.ScheduleConfig {
	return o.v.Load().(*config.ScheduleConfig)
}

func (o *scheduleOption) store(cfg *config.ScheduleConfig) {
	o.v.Store(cfg)
}

func (o *scheduleOption) GetReplication() *Replication {
	return o.rep
}

func (o *scheduleOption) GetMaxReplicas() int {
	return o.rep.GetMaxReplicas()
}

func (o *scheduleOption) SetMaxReplicas(replicas int) {
	o.rep.SetMaxReplicas(replicas)
}

func (o *scheduleOption) GetMaxSnapshotCount() uint64 {
	return o.load().MaxSnapshotCount
}

func (o *scheduleOption) GetMaxStoreDownTime() time.Duration {
	return o.load().MaxStoreDownTime.Duration
}

func (o *scheduleOption) GetLeaderScheduleLimit() uint64 {
	return o.load().LeaderScheduleLimit
}

func (o *scheduleOption) GetRegionScheduleLimit() uint64 {
	return o.load().RegionScheduleLimit
}

func (o *scheduleOption) GetReplicaScheduleLimit() uint64 {
	return o.load().ReplicaScheduleLimit
}

func (o *scheduleOption) persist(kv *kv) error {
	return kv.saveScheduleOption(o)
}
