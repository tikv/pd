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

package http

import (
	"fmt"
	"net/url"
	"time"
)

// The following constants are the paths of PD HTTP APIs.
const (
	// Metadata
	HotRead                = "/pd/api/v1/hotspot/regions/read"
	HotWrite               = "/pd/api/v1/hotspot/regions/write"
	HotHistory             = "/pd/api/v1/hotspot/regions/history"
	RegionByIDPrefix       = "/pd/api/v1/region/id"
	regionByKey            = "/pd/api/v1/region/key"
	Regions                = "/pd/api/v1/regions"
	regionsByKey           = "/pd/api/v1/regions/key"
	RegionsByStoreIDPrefix = "/pd/api/v1/regions/store"
	EmptyRegions           = "/pd/api/v1/regions/check/empty-region"
	accelerateSchedule     = "/pd/api/v1/regions/accelerate-schedule"
	store                  = "/pd/api/v1/store"
	Stores                 = "/pd/api/v1/stores"
	StatsRegion            = "/pd/api/v1/stats/region"
	// Config
	Config          = "/pd/api/v1/config"
	ClusterVersion  = "/pd/api/v1/config/cluster-version"
	ScheduleConfig  = "/pd/api/v1/config/schedule"
	ReplicateConfig = "/pd/api/v1/config/replicate"
	// Rule
	PlacementRule         = "/pd/api/v1/config/rule"
	PlacementRules        = "/pd/api/v1/config/rules"
	placementRulesByGroup = "/pd/api/v1/config/rules/group"
	RegionLabelRule       = "/pd/api/v1/config/region-label/rule"
	// Scheduler
	Schedulers            = "/pd/api/v1/schedulers"
	scatterRangeScheduler = "/pd/api/v1/schedulers/scatter-range-"
	// Admin
	ResetTS                = "/pd/api/v1/admin/reset-ts"
	BaseAllocID            = "/pd/api/v1/admin/base-alloc-id"
	SnapshotRecoveringMark = "/pd/api/v1/admin/cluster/markers/snapshot-recovering"
	// Debug
	PProfProfile   = "/pd/api/v1/debug/pprof/profile"
	PProfHeap      = "/pd/api/v1/debug/pprof/heap"
	PProfMutex     = "/pd/api/v1/debug/pprof/mutex"
	PProfAllocs    = "/pd/api/v1/debug/pprof/allocs"
	PProfBlock     = "/pd/api/v1/debug/pprof/block"
	PProfGoroutine = "/pd/api/v1/debug/pprof/goroutine"
	// Others
	MinResolvedTSPrefix = "/pd/api/v1/min-resolved-ts"
	Status              = "/pd/api/v1/status"
	Version             = "/pd/api/v1/version"
)

// RegionByID returns the path of PD HTTP API to get region by ID.
func RegionByID(regionID uint64) string {
	return fmt.Sprintf("%s/%d", RegionByIDPrefix, regionID)
}

// RegionByKey returns the path of PD HTTP API to get region by key.
func RegionByKey(key []byte) string {
	return fmt.Sprintf("%s/%s", regionByKey, url.QueryEscape(string(key)))
}

// RegionsByKey returns the path of PD HTTP API to scan regions with given start key, end key and limit parameters.
func RegionsByKey(startKey, endKey []byte, limit int) string {
	return fmt.Sprintf("%s?start_key=%s&end_key=%s&limit=%d",
		regionsByKey,
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)),
		limit)
}

// RegionsByStoreID returns the path of PD HTTP API to get regions by store ID.
func RegionsByStoreID(storeID uint64) string {
	return fmt.Sprintf("%s/%d", RegionsByStoreIDPrefix, storeID)
}

// RegionStatsByKeyRange returns the path of PD HTTP API to get region stats by start key and end key.
func RegionStatsByKeyRange(startKey, endKey []byte) string {
	return fmt.Sprintf("%s?start_key=%s&end_key=%s",
		StatsRegion,
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)))
}

// StoreByID returns the store API with store ID parameter.
func StoreByID(id uint64) string {
	return fmt.Sprintf("%s/%d", store, id)
}

// StoreLabelByID returns the store label API with store ID parameter.
func StoreLabelByID(id uint64) string {
	return fmt.Sprintf("%s/%d/label", store, id)
}

// ConfigWithTTLSeconds returns the config API with the TTL seconds parameter.
func ConfigWithTTLSeconds(ttlSeconds float64) string {
	return fmt.Sprintf("%s?ttlSecond=%.0f", Config, ttlSeconds)
}

// PlacementRulesByGroup returns the path of PD HTTP API to get placement rules by group.
func PlacementRulesByGroup(group string) string {
	return fmt.Sprintf("%s/%s", placementRulesByGroup, group)
}

// PlacementRuleByGroupAndID returns the path of PD HTTP API to get placement rule by group and ID.
func PlacementRuleByGroupAndID(group, id string) string {
	return fmt.Sprintf("%s/%s/%s", PlacementRule, group, id)
}

// SchedulerByName returns the scheduler API with the given scheduler name.
func SchedulerByName(name string) string {
	return fmt.Sprintf("%s/%s", Schedulers, name)
}

// ScatterRangeSchedulerWithName returns the scatter range scheduler API with name parameter.
func ScatterRangeSchedulerWithName(name string) string {
	return fmt.Sprintf("%s%s", scatterRangeScheduler, name)
}

// PProfProfileAPIWithInterval returns the pprof profile API with interval parameter.
func PProfProfileAPIWithInterval(interval time.Duration) string {
	return fmt.Sprintf("%s?seconds=%d", PProfProfile, interval/time.Second)
}

// PProfGoroutineWithDebugLevel returns the pprof goroutine API with debug level parameter.
func PProfGoroutineWithDebugLevel(level int) string {
	return fmt.Sprintf("%s?debug=%d", PProfGoroutine, level)
}
