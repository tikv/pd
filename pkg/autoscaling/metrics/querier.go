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

// QueryResult stores metrics value for each instance
type QueryResult map[string]float64

// Querier provides interfaces to query metrics
type Querier interface {
	// Query does the real query with options
	Query(options *QueryOptions) (QueryResult, error)
}

// ComponentType represents different components of a TiDB cluster
type ComponentType string

const (
	// TiDB represents TiDB component of a TiDB cluster
	TiDB = "tidb"
	// TiKV represents TiKV component of a TiDB cluster
	TiKV = "tikv"
)

// MetricType represents types of resources (CPU, storage, etc...)
type MetricType string

const (
	// CPUUsage represents cpu time cost
	CPUUsage = "cpu_usage"
	// CPUQuota represents cpu cores quota
	CPUQuota = "cpu_quota"
)

// QueryOptions includes parameters for later metrics query
type QueryOptions struct {
	cluster   string
	component ComponentType
	metric    MetricType
	instances []string
	timestamp time.Time
	duration  time.Duration
}

// NewQueryOptions constructs a new QueryOptions for metrics
// The options will be used to query metrics of `duration` long UNTIL `timestamp`
// which has `metric` type (CPU, Storage) for a specific `component` type in a `cluster`
// and returns metrics value for each instance in `instances`
func NewQueryOptions(cluster string, component ComponentType, metric MetricType, instances []string, timestamp time.Time, duration time.Duration) *QueryOptions {
	return &QueryOptions{
		cluster,
		component,
		metric,
		instances,
		timestamp,
		duration,
	}
}
