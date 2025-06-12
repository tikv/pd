// Copyright 2024 TiKV Project Authors.
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

package simutil

// IDAllocator is used to alloc unique ID.
type idAllocator struct {
	id uint64
}

// NextID gets the next unique ID.
func (a *idAllocator) NextID() uint64 {
	a.id++
	return a.id
}

// ResetID resets the IDAllocator.
func (a *idAllocator) ResetID() {
	a.id = 0
}

// GetID gets the current ID.
func (a *idAllocator) GetID() uint64 {
	return a.id
}

// IDAllocator is used to alloc unique ID.
var IDAllocator idAllocator
