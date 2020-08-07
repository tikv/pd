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

package priorityscheduling

// HighPrioritySchedule control the regions whether need high priority scheduling
type HighPrioritySchedule interface {
	AddHighPriorityScheduleRegions(regionsIDs ...uint64)
	GetHighPriorityScheduleRegions() []uint64
	CancelRegionHighPrioritySchedule(id uint64)
}

// MockHighPrioritySchedule provide a mock HighPrioritySchedule, only used for unit test
type MockHighPrioritySchedule struct {
	ids map[uint64]struct{}
}

// NewMockHighPrioritySchedule return a mock HighPrioritySchedule
func NewMockHighPrioritySchedule() *MockHighPrioritySchedule {
	return &MockHighPrioritySchedule{
		ids: map[uint64]struct{}{},
	}
}

// AddHighPriorityScheduleRegions implements the HighPrioritySchedule.AddHighPriorityScheduleRegions
func (mh *MockHighPrioritySchedule) AddHighPriorityScheduleRegions(regionsIDs ...uint64) {
	for _, id := range regionsIDs {
		mh.ids[id] = struct{}{}
	}
}

// GetHighPriorityScheduleRegions implements the HighPrioritySchedule.GetHighPriorityScheduleRegions
func (mh *MockHighPrioritySchedule) GetHighPriorityScheduleRegions() []uint64 {
	if len(mh.ids) < 1 {
		return nil
	}
	returned := make([]uint64, 0, len(mh.ids))
	for id := range mh.ids {
		returned = append(returned, id)
	}
	return returned
}

// CancelRegionHighPrioritySchedule implements the HighPrioritySchedule.CancelRegionHighPrioritySchedule
func (mh *MockHighPrioritySchedule) CancelRegionHighPrioritySchedule(id uint64) {
	delete(mh.ids, id)
}

// Reset is only used for unit test
func (mh *MockHighPrioritySchedule) Reset() {
	mh.ids = map[uint64]struct{}{}
}
