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

package prioritySchedule

// HighPrioritySchedule control the regions whether need high priority scheduling
type HighPrioritySchedule interface {
	AddHighPriorityRegions(regionsIDs ...uint64)
	GetHighPriorityRegions() []uint64
	CancelRegionHighPrioritySchedule(id uint64)
}

type MockHighPrioritySchedule struct {
	ids map[uint64]struct{}
}

func NewMockHighPrioritySchedule() *MockHighPrioritySchedule {
	return &MockHighPrioritySchedule{
		ids: map[uint64]struct{}{},
	}
}

func (mh *MockHighPrioritySchedule) AddHighPriorityRegions(regionsIDs ...uint64) {
	for _, id := range regionsIDs {
		mh.ids[id] = struct{}{}
	}
}

func (mh *MockHighPrioritySchedule) GetHighPriorityRegions() []uint64 {
	if len(mh.ids) < 1 {
		return nil
	}
	returned := make([]uint64, 0, len(mh.ids))
	for id := range mh.ids {
		returned = append(returned, id)
	}
	return returned
}

func (mh *MockHighPrioritySchedule) CancelRegionHighPrioritySchedule(id uint64) {
	delete(mh.ids, id)
}

func (mh *MockHighPrioritySchedule) Reset() {
	mh.ids = map[uint64]struct{}{}
}
