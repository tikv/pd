// Copyright 2018 PingCAP, Inc.
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

package placement

import "github.com/pingcap/pd/server/core"

// Config is consist of a list of constraints.
type Config struct {
	Constraints []*Constraint
}

// Constraint represents a user-defined region placement rule. Each constraint
// is configured as an expression, for example 'count(zone:z1,rack:r1,host)>=3'.
type Constraint struct {
	Function string   // One of "count", "label_values", "count_leader", "isolation_level".
	Filters  []Filter // "key:value" formed parameters.
	Labels   []string // "key" formed parameters.
	Op       string   // One of "<", "<=", "=", ">=", ">".
	Value    int      // Expected expression evaluate value.
}

// Filter is used for filtering replicas of a region. The form in the
// configuration is "key:value", which appears in the function argument of the
// expression.
type Filter struct {
	Key, Value string
}

var functionList = []string{"count", "label_values", "count_leader", "isolation_level"}

// Cluster provides an overview of a cluster's region distribution.
type Cluster interface {
	GetRegion(id uint64) *core.RegionInfo
	GetStores() []*core.StoreInfo
	GetStore(id uint64) *core.StoreInfo
	GetRegionStores(region *core.RegionInfo) []*core.StoreInfo
}
