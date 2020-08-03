// Copyright 2020 PingCAP, Inc.
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

package metrics

import "time"

// MemberType represents different members of a TiDB cluster
type MemberType string

const (
	TiDB = "tidb"
	TiKV = "tikv"
)

// MetricType represents types of resources (CPU, storage, etc...)
type MetricType string

const (
	CPU              = "cpu"
	StorageAvailable = "available"
	StorageCapacity  = "capacity"
)

// QueryOptions includes parameters for later metrics query
type QueryOptions struct {
	cluster   string
	member    MemberType
	metric    MetricType
	instances []string
	timestamp int64
	duration  time.Duration
}

// QueryResult stores metrics value for each instance
type QueryResult map[string]float64

// Store provides interfaces to query metrics
type Store interface {
	// Query metrics range of duration UNTIL timestamp
	Query(options *QueryOptions) (QueryResult, error)
}

// NewQueryOptions constructs a new QueryOptions for metrics
func NewQueryOptions(cluster string, member MemberType, metric MetricType, instances []string, timestamp int64, duration time.Duration) *QueryOptions {
	return &QueryOptions{
		cluster,
		member,
		metric,
		instances,
		timestamp,
		duration,
	}
}
