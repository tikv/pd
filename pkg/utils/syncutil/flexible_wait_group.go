// Copyright 2023 TiKV Project Authors.
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

package syncutil

import (
	"sync"
)

// FlexibleWaitGroup is a flexible version of sync.WaitGroup.
// It supports adding to the counter after Wait() has been called,
// which is not allowed in sync.WaitGroup.
type FlexibleWaitGroup struct {
	sync.Mutex
	count int
	cond  *sync.Cond
}

// NewFlexibleWaitGroup creates and returns a new FlexibleWaitGroup.
func NewFlexibleWaitGroup() *FlexibleWaitGroup {
	dwg := &FlexibleWaitGroup{}
	dwg.cond = sync.NewCond(&dwg.Mutex)
	return dwg
}

// Add adds delta (which may be negative) to the FlexibleWaitGroup counter.
// If the counter becomes zero or negative, all goroutines blocked on Wait are released.
func (fwg *FlexibleWaitGroup) Add(delta int) {
	fwg.Lock()
	defer fwg.Unlock()

	fwg.count += delta
	if fwg.count <= 0 {
		fwg.cond.Broadcast()
		fwg.count = 0
	}
}

// Done decrements the FlexibleWaitGroup counter by one.
func (fwg *FlexibleWaitGroup) Done() {
	fwg.Add(-1)
}

// Wait blocks until the FlexibleWaitGroup counter is zero or negative.
func (fwg *FlexibleWaitGroup) Wait() {
	fwg.Lock()
	for fwg.count > 0 {
		fwg.cond.Wait()
	}
	fwg.Unlock()
}

// getCount returns the current count of the FlexibleWaitGroup.
// It is only used for testing.
func (fwg *FlexibleWaitGroup) getCount() int {
	fwg.Lock()
	defer fwg.Unlock()
	return fwg.count
}
