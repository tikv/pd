// Copyright 2020 TiKV Project Authors.
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

package autoscaling

import "github.com/pingcap/kvproto/pkg/metapb"

// Strategy within a HTTP request provides rules and resources to help make decision for auto scaling.
type Strategy struct {
	// The basic unit of MaxCPUQuota is milli-core.
	MaxCPUQuota uint64      `json:"max_cpu_quota"`
	Rules       []*Rule     `json:"rules"`
	Resources   []*Resource `json:"resources"`
}

// Rule is a set of constraints for a kind of component.
type Rule struct {
	Component               string       `json:"component"`
	CPURule                 *CPURule     `json:"cpu_rule,omitempty"`
	StorageRule             *StorageRule `json:"storage_rule,omitempty"`
	ScaleOutIntervalSeconds uint64       `json:"scale_out_interval_seconds"`
	ScaleInIntervalSeconds  uint64       `json:"scale_in_interval_seconds"`
}

// CPURule is the constraints about CPU.
type CPURule struct {
	MaxThreshold  float64  `json:"max_threshold"`
	MinThreshold  float64  `json:"min_threshold"`
	ResourceTypes []string `json:"resource_types"`
}

// StorageRule is the constraints about storage.
type StorageRule struct {
	MinThreshold  float64  `json:"min_threshold"`
	ResourceTypes []string `json:"resource_types"`
}

// Resource represents a kind of resource set including CPU, memory, storage.
type Resource struct {
	ResourceType string `json:"resource_type"`
	// The basic unit of CPU is milli-core.
	CPU uint64 `json:"cpu"`
	// The basic unit of memory is byte.
	Memory uint64 `json:"memory"`
	// The basic unit of storage is byte.
	Storage uint64 `json:"storage"`
	Count   uint64 `json:"count"`
}

// Plan is the final result of auto scaling, which indicates how to scale in or scale out.
type Plan struct {
	Component    string               `json:"component"`
	Count        uint64               `json:"count"`
	ResourceType string               `json:"resource_type"`
	Labels       []*metapb.StoreLabel `json:"labels"`
}

// ComponentType distinguishes different kinds of components.
type ComponentType int

const (
	// TiKV indicates the TiKV component
	TiKV ComponentType = iota
	// TiDB indicates the TiDB component
	TiDB
)

func (c ComponentType) String() string {
	switch c {
	case TiKV:
		return "tikv"
	case TiDB:
		return "tidb"
	default:
		return "unknown"
	}
}

// MetricType distinguishes different kinds of metrics
type MetricType int

const (
	// CPUUsage is used cpu time in the duration
	CPUUsage MetricType = iota
	// CPUQuota is cpu cores quota for each instance
	CPUQuota
)

func (c MetricType) String() string {
	switch c {
	case CPUUsage:
		return "cpu_usage"
	case CPUQuota:
		return "cpu_quota"
	default:
		return "unknown"
	}
}

type instance struct {
	id      uint64
	address string
}
