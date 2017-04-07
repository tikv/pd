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

import (
	"time"

	"github.com/pingcap/pd/pkg/typeutil"
)

const (
	defaultMaxReplicas          = 3
	defaultMaxSnapshotCount     = 3
	defaultMaxStoreDownTime     = time.Hour
	defaultLeaderScheduleLimit  = 1024
	defaultRegionScheduleLimit  = 12
	defaultReplicaScheduleLimit = 16
)

// ReplicationConfig is the replication configuration.
type ReplicationConfig struct {
	// MaxReplicas is the number of replicas for each region.
	MaxReplicas uint64 `toml:"max-replicas,omitempty" json:"max-replicas"`

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels typeutil.StringSlice `toml:"location-labels,omitempty" json:"location-labels"`
}

// Clone is used to deepcopy ReplicationConfig
func (c *ReplicationConfig) Clone() *ReplicationConfig {
	locationLabels := make(typeutil.StringSlice, 0, len(c.LocationLabels))
	copy(locationLabels, c.LocationLabels)
	return &ReplicationConfig{
		MaxReplicas:    c.MaxReplicas,
		LocationLabels: locationLabels,
	}
}

func (c *ReplicationConfig) adjust() {
	adjustUint64(&c.MaxReplicas, defaultMaxReplicas)
}
