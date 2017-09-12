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

package schedule

import "github.com/pingcap/pd/server/core"

// NamespaceChecker ensures region to go to the right place.
type NamespaceChecker struct {
	opt     Options
	cluster Cluster
	filters []Filter
}

// NewNamespaceChecker creates a namespace checker.
func NewNamespaceChecker(opt Options, cluster Cluster) *NamespaceChecker {
	filters := []Filter{
		NewHealthFilter(opt),
		NewSnapshotCountFilter(opt),
	}

	return &NamespaceChecker{
		opt:     opt,
		cluster: cluster,
		filters: filters,
	}
}

// Check verifies a region's namespace, creating an Operator if nee.
func (n *NamespaceChecker) Check(region *core.RegionInfo) *Operator {
	return nil
}

