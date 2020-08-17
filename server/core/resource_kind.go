// Copyright 2017 TiKV Project Authors.
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

package core

// PriorityLevel lower level means higher priority
type PriorityLevel int

// Built-in priority level
const (
	LowPriority PriorityLevel = iota
	NormalPriority
	HighPriority
)

// ScheduleKind distinguishes resources and schedule policy.
type ScheduleKind struct {
	Resource ResourceKind
	Policy   SchedulePolicy
}

// NewScheduleKind creates a schedule kind with resource kind and schedule policy.
func NewScheduleKind(Resource ResourceKind, Policy SchedulePolicy) ScheduleKind {
	return ScheduleKind{
		Resource: Resource,
		Policy:   Policy,
	}
}

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	// LeaderKind indicates the leader kind resource
	LeaderKind ResourceKind = iota
	// RegionKind indicates the region kind resource
	RegionKind
)

func (k ResourceKind) String() string {
	switch k {
	case LeaderKind:
		return "leader"
	case RegionKind:
		return "region"
	default:
		return "unknown"
	}
}

// SchedulePolicy distinguishes different kinds of schedule policy
type SchedulePolicy int

const (
	// ByCount indicates that balance by count
	ByCount SchedulePolicy = iota
	// BySize indicates that balance by size
	BySize
)

func (k SchedulePolicy) String() string {
	switch k {
	case ByCount:
		return "count"
	case BySize:
		return "size"
	default:
		return "unknown"
	}
}

// StringToSchedulePolicy creates a schedule policy with string.
func StringToSchedulePolicy(input string) SchedulePolicy {
	switch input {
	case BySize.String():
		return BySize
	case ByCount.String():
		return ByCount
	default:
		panic("invalid schedule policy: " + input)
	}
}
