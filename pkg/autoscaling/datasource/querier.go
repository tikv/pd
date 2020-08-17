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
// See the License for the specific language governing permissions and
// limitations under the License.

package datasource

import (
	"time"

	types "github.com/tikv/pd/pkg/autoscaling"
)

// QueryResult stores metrics value for each instance
type QueryResult map[string]float64

// Querier provides interfaces to query metrics
type Querier interface {
	// Query does the real query with options
	Query(options *QueryOptions) (QueryResult, error)
}

// QueryOptions includes parameters for later metrics query
type QueryOptions struct {
	component types.ComponentType
	metric    types.MetricType
	instances []string
	timestamp time.Time
	duration  time.Duration
}

// NewQueryOptions constructs a new QueryOptions for metrics
// The options will be used to query metrics of `duration` long UNTIL `timestamp`
// which has `metric` type (CPU, Storage) for a specific `component` type
// and returns metrics value for each instance in `instances`
func NewQueryOptions(component types.ComponentType, metric types.MetricType, instances []string, timestamp time.Time, duration time.Duration) *QueryOptions {
	return &QueryOptions{
		component,
		metric,
		instances,
		timestamp,
		duration,
	}
}
