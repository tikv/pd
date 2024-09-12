// Copyright 2019 TiKV Project Authors.
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

package operator

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
)

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	StoresInfluence map[uint64]*StoreInfluence
}

// NewOpInfluence creates a OpInfluence.
func NewOpInfluence() *OpInfluence {
	return &OpInfluence{
		StoresInfluence: make(map[uint64]*StoreInfluence),
	}
}

// Add adds another influence.
func (m *OpInfluence) Add(other *OpInfluence) {
	for id, v := range other.StoresInfluence {
		m.GetStoreInfluence(id).add(v)
	}
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
	RegionSize   int64
	RegionCount  int64
	LeaderSize   int64
	LeaderCount  int64
	WitnessCount int64
	StepCost     map[storelimit.Type]int64
}

func (s *StoreInfluence) add(other *StoreInfluence) {
	s.RegionCount += other.RegionCount
	s.RegionSize += other.RegionSize
	s.LeaderSize += other.LeaderSize
	s.LeaderCount += other.LeaderCount
	s.WitnessCount += other.WitnessCount
	for _, v := range storelimit.TypeNameValue {
		s.AddStepCost(v, other.GetStepCost(v))
	}
}

// ResourceProperty returns delta size of leader/region by influence.
func (s *StoreInfluence) ResourceProperty(kind constant.ScheduleKind) int64 {
	switch kind.Resource {
	case constant.LeaderKind:
		switch kind.Policy {
		case constant.ByCount:
			return s.LeaderCount
		case constant.BySize:
			return s.LeaderSize
		default:
			return 0
		}
	case constant.RegionKind:
		return s.RegionSize
	case constant.WitnessKind:
		return s.WitnessCount
	default:
		return 0
	}
}

// GetStepCost returns the specific type step cost
func (s *StoreInfluence) GetStepCost(limitType storelimit.Type) int64 {
	if s.StepCost == nil {
		return 0
	}
	return s.StepCost[limitType]
}

// AddStepCost add cost to the influence.
func (s *StoreInfluence) AddStepCost(limitType storelimit.Type, cost int64) {
	if s.StepCost == nil {
		s.StepCost = make(map[storelimit.Type]int64)
	}
	s.StepCost[limitType] += cost
}

// AdjustStepCost adjusts the step cost of specific type store limit according to region size
func (s *StoreInfluence) AdjustStepCost(limitType storelimit.Type, regionSize int64) {
	if regionSize > storelimit.SmallRegionThreshold {
		s.AddStepCost(limitType, storelimit.RegionInfluence[limitType])
	} else if regionSize > core.EmptyRegionApproximateSize {
		s.AddStepCost(limitType, storelimit.SmallRegionInfluence[limitType])
	}
}
