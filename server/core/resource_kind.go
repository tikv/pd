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

package core

// PriorityLevel lower level means higher priority
type PriorityLevel int

// Built-in priority level
const (
	LowPriority PriorityLevel = iota
	NormalPriority
	HighPriority
)

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	// LeaderKind indicates the leader kind resource
	LeaderKind = iota
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

// LeaderScheduleKind distinguishes different kinds of schedule strategy
type LeaderScheduleKind int

const (
	// ByCount indicates that balance leader by count
	ByCount = iota
	// BySize indicates that balance leader by size
	BySize
)

func (k LeaderScheduleKind) String() string {
	switch k {
	case ByCount:
		return "ByCount"
	case BySize:
		return "BySize"
	default:
		return "unknown"
	}
}
