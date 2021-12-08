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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

// FlowKind is a identify Flow types.
type FlowKind uint32

// Flags for flow.
const (
	WriteFlow FlowKind = iota
	ReadFlow
)

func (k FlowKind) String() string {
	switch k {
	case WriteFlow:
		return "write"
	case ReadFlow:
		return "read"
	}
	return "unimplemented"
}

// sourceKind represents the statistics item source.
type sourceKind int

const (
	direct  sourceKind = iota // there is a corresponding peer in this store.
	inherit                   // there is no a corresponding peer in this store and there is a peer just deleted.
	adopt                     // there is no corresponding peer in this store and there is no peer just deleted, we need to copy from other stores.
)

func (k sourceKind) String() string {
	switch k {
	case direct:
		return "direct"
	case inherit:
		return "inherit"
	case adopt:
		return "adopt"
	}
	return "unknown"
}
