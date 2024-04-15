// Copyright 2018 TiKV Project Authors.
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

package cases

// EventDescriptor is a detail template for custom events.
type EventDescriptor interface {
	Type() string
}

// WriteFlowOnSpotDescriptor writes bytes in some range.
type WriteFlowOnSpotDescriptor struct {
	Step func(tick int64) map[string]int64
}

// Type implements the EventDescriptor interface.
func (*WriteFlowOnSpotDescriptor) Type() string {
	return "write-flow-on-spot"
}

// WriteFlowOnRegionDescriptor writes bytes in some region.
type WriteFlowOnRegionDescriptor struct {
	Step func(tick int64) map[uint64]int64
}

// Type implements the EventDescriptor interface.
func (*WriteFlowOnRegionDescriptor) Type() string {
	return "write-flow-on-region"
}

// ReadFlowOnRegionDescriptor reads bytes in some region.
type ReadFlowOnRegionDescriptor struct {
	Step func(tick int64) map[uint64]int64
}

// Type implements the EventDescriptor interface.
func (*ReadFlowOnRegionDescriptor) Type() string {
	return "read-flow-on-region"
}

// AddNodesDescriptor adds nodes.
type AddNodesDescriptor struct {
	Step func(tick int64) uint64
}

// Type implements the EventDescriptor interface.
func (*AddNodesDescriptor) Type() string {
	return "add-nodes"
}

// DeleteNodesDescriptor removes nodes.
type DeleteNodesDescriptor struct {
	Step func(tick int64) uint64
}

// Type implements the EventDescriptor interface.
func (*DeleteNodesDescriptor) Type() string {
	return "delete-nodes"
}
