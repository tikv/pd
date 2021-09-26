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

package autoscaling

import "time"

// QueryResult stores metrics value for each instance
type QueryResult map[string]float64

// Querier provides interfaces to query metrics
type Querier interface {
	// Query does the real query with options
	Query(options *QueryOptions) (QueryResult, error)
}

// QueryOptions includes parameters for later metrics query
type QueryOptions struct {
	component ComponentType
	metric    MetricType
	addresses []string
	timestamp time.Time
	duration  time.Duration
}

// NewQueryOptions constructs a new QueryOptions for metrics
// The options will be used to query metrics of `duration` long UNTIL `timestamp`
// which has `metric` type (CPU, Storage) for a specific `component` type
// and returns metrics value for each instance in `instances`
func NewQueryOptions(component ComponentType, metric MetricType, addresses []string, timestamp time.Time, duration time.Duration) *QueryOptions {
	return &QueryOptions{
		component,
		metric,
		addresses,
		timestamp,
		duration,
	}
}
