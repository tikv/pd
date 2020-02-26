// Copyright 2019 PingCAP, Inc.
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

package operator

import "github.com/pingcap/pd/v4/server/core"

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	StoresInfluence map[uint64]*StoreInfluence
}

// GetStoreInfluence get storeInfluence of specific store.
func (m OpInfluence) GetStoreInfluence(id uint64) *StoreInfluence {
	storeInfluence, ok := m.StoresInfluence[id]
	if !ok {
		storeInfluence = &StoreInfluence{}
		m.StoresInfluence[id] = storeInfluence
	}
	return storeInfluence
}

// StoreInfluence records influences that pending operators will make.
type StoreInfluence struct {
	RegionSize  int64
	RegionCount int64
	LeaderSize  int64
	LeaderCount int64
	StepCost    int64
}

// ResourceProperty returns delta size of leader/region by influence.
func (s StoreInfluence) ResourceProperty(kind core.ScheduleKind) int64 {
	switch kind.Resource {
	case core.LeaderKind:
		switch kind.Policy {
		case core.ByCount:
			return s.LeaderCount
		case core.BySize:
			return s.LeaderSize
		default:
			return 0
		}
	case core.RegionKind:
		return s.RegionSize
	default:
		return 0
	}
}
