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
	HotRead                   = "/pd/api/v1/hotspot/regions/read"
	HotWrite                  = "/pd/api/v1/hotspot/regions/write"
	HotHistory                = "/pd/api/v1/hotspot/regions/history"
	RegionByIDPrefix          = "/pd/api/v1/region/id"
	regionByKey               = "/pd/api/v1/region/key"
	Regions                   = "/pd/api/v1/regions"
	regionsByKey              = "/pd/api/v1/regions/key"
	RegionsByStoreIDPrefix    = "/pd/api/v1/regions/store"
	regionsReplicated         = "/pd/api/v1/regions/replicated"
	EmptyRegions              = "/pd/api/v1/regions/check/empty-region"
	AccelerateSchedule        = "/pd/api/v1/regions/accelerate-schedule"
	AccelerateScheduleInBatch = "/pd/api/v1/regions/accelerate-schedule/batch"
	store                     = "/pd/api/v1/store"
	Stores                    = "/pd/api/v1/stores"
	StatsRegion               = "/pd/api/v1/stats/region"
	membersPrefix             = "/pd/api/v1/members"
	leaderPrefix              = "/pd/api/v1/leader"
	transferLeader            = "/pd/api/v1/leader/transfer"
	health                    = "/pd/api/v1/health"
	// Config
	Config          = "/pd/api/v1/config"
	ClusterVersion  = "/pd/api/v1/config/cluster-version"
	ScheduleConfig  = "/pd/api/v1/config/schedule"
	ReplicateConfig = "/pd/api/v1/config/replicate"
	// Rule
	PlacementRule         = "/pd/api/v1/config/rule"
	PlacementRules        = "/pd/api/v1/config/rules"
	PlacementRulesInBatch = "/pd/api/v1/config/rules/batch"
	placementRulesByGroup = "/pd/api/v1/config/rules/group"
	PlacementRuleBundle   = "/pd/api/v1/config/placement-rule"
	placementRuleGroup    = "/pd/api/v1/config/rule_group"
	placementRuleGroups   = "/pd/api/v1/config/rule_groups"
	RegionLabelRule       = "/pd/api/v1/config/region-label/rule"
	RegionLabelRules      = "/pd/api/v1/config/region-label/rules"
	RegionLabelRulesByIDs = "/pd/api/v1/config/region-label/rules/ids"
	// Scheduler
	Schedulers            = "/pd/api/v1/schedulers"
	scatterRangeScheduler = "/pd/api/v1/schedulers/scatter-range-scheduler-"
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
	Cluster             = "/pd/api/v1/cluster"
	ClusterStatus       = "/pd/api/v1/cluster/status"
	Status              = "/pd/api/v1/status"
	Version             = "/pd/api/v1/version"
	operators           = "/pd/api/v1/operators"
	safepoint           = "/pd/api/v1/gc/safepoint"
	// Micro Service
	microServicePrefix = "/pd/api/v2/ms"
	// Keyspace
	KeyspaceConfig        = "/pd/api/v2/keyspaces/%s/config"
	GetKeyspaceMetaByName = "/pd/api/v2/keyspaces/%s"
)

// RegionByID returns the path of PD HTTP API to get region by ID.
func RegionByID(regionID uint64) string {
	return fmt.Sprintf("%s/%d", RegionByIDPrefix, regionID)
}

// RegionByKey returns the path of PD HTTP API to get region by key.
func RegionByKey(key []byte) string {
	return fmt.Sprintf("%s/%s", regionByKey, url.QueryEscape(string(key)))
}

// RegionsByKeyRange returns the path of PD HTTP API to scan regions with given start key, end key and limit parameters.
func RegionsByKeyRange(keyRange *KeyRange, limit int) string {
	startKeyStr, endKeyStr := keyRange.EscapeAsUTF8Str()
	return fmt.Sprintf("%s?key=%s&end_key=%s&limit=%d",
		regionsByKey, startKeyStr, endKeyStr, limit)
}

// RegionsByStoreID returns the path of PD HTTP API to get regions by store ID.
func RegionsByStoreID(storeID uint64) string {
	return fmt.Sprintf("%s/%d", RegionsByStoreIDPrefix, storeID)
}

// RegionsReplicatedByKeyRange returns the path of PD HTTP API to get replicated regions with given start key and end key.
func RegionsReplicatedByKeyRange(keyRange *KeyRange) string {
	startKeyStr, endKeyStr := keyRange.EscapeAsHexStr()
	return fmt.Sprintf("%s?startKey=%s&endKey=%s",
		regionsReplicated, startKeyStr, endKeyStr)
}

// RegionStatsByKeyRange returns the path of PD HTTP API to get region stats by start key and end key.
func RegionStatsByKeyRange(keyRange *KeyRange, onlyCount bool) string {
	startKeyStr, endKeyStr := keyRange.EscapeAsUTF8Str()
	if onlyCount {
		return fmt.Sprintf("%s?start_key=%s&end_key=%s&count",
			StatsRegion, startKeyStr, endKeyStr)
	}
	return fmt.Sprintf("%s?start_key=%s&end_key=%s",
		StatsRegion, startKeyStr, endKeyStr)
}

// StoreByID returns the store API with store ID parameter.
func StoreByID(id uint64) string {
	return fmt.Sprintf("%s/%d", store, id)
}

// StoreLabelByID returns the store label API with store ID parameter.
func StoreLabelByID(id uint64) string {
	return fmt.Sprintf("%s/%d/label", store, id)
}

// LabelByStoreID returns the path of PD HTTP API to set store label.
func LabelByStoreID(storeID int64) string {
	return fmt.Sprintf("%s/%d/label", store, storeID)
}

// TransferLeaderByID returns the path of PD HTTP API to transfer leader by ID.
func TransferLeaderByID(leaderID string) string {
	return fmt.Sprintf("%s/%s", transferLeader, leaderID)
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

// PlacementRuleBundleByGroup returns the path of PD HTTP API to get placement rule bundle by group.
func PlacementRuleBundleByGroup(group string) string {
	return fmt.Sprintf("%s/%s", PlacementRuleBundle, group)
}

// PlacementRuleBundleWithPartialParameter returns the path of PD HTTP API to get placement rule bundle with partial parameter.
func PlacementRuleBundleWithPartialParameter(partial bool) string {
	return fmt.Sprintf("%s?partial=%t", PlacementRuleBundle, partial)
}

// PlacementRuleGroupByID returns the path of PD HTTP API to get placement rule group by ID.
func PlacementRuleGroupByID(id string) string {
	return fmt.Sprintf("%s/%s", placementRuleGroup, id)
}

// SchedulerByName returns the scheduler API with the given scheduler name.
func SchedulerByName(name string) string {
	return fmt.Sprintf("%s/%s", Schedulers, name)
}

// ScatterRangeSchedulerWithName returns the scatter range scheduler API with name parameter.
// It is used in https://github.com/pingcap/tidb/blob/2a3352c45dd0f8dd5102adb92879bbfa964e7f5f/pkg/server/handler/tikvhandler/tikv_handler.go#L1252.
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

// MicroServiceMembers returns the path of PD HTTP API to get the members of microservice.
func MicroServiceMembers(service string) string {
	return fmt.Sprintf("%s/members/%s", microServicePrefix, service)
}

// MicroServicePrimary returns the path of PD HTTP API to get the primary of microservice.
func MicroServicePrimary(service string) string {
	return fmt.Sprintf("%s/primary/%s", microServicePrefix, service)
}

// GetUpdateKeyspaceConfigURL returns the path of PD HTTP API to update keyspace config.
func GetUpdateKeyspaceConfigURL(keyspaceName string) string {
	return fmt.Sprintf(KeyspaceConfig, keyspaceName)
}

// GetKeyspaceMetaByNameURL returns the path of PD HTTP API to get keyspace meta by keyspace name.
func GetKeyspaceMetaByNameURL(keyspaceName string) string {
	return fmt.Sprintf(GetKeyspaceMetaByName, keyspaceName)
}

// GetDeleteSafePointURI returns the URI for delete safepoint service
func GetDeleteSafePointURI(serviceID string) string {
	return fmt.Sprintf("%s/%s", safepoint, serviceID)
}
