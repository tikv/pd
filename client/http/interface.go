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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// Client is a PD (Placement Driver) HTTP client.
type Client interface {
	/* Member-related interfaces */
	GetMembers(context.Context) (*MembersInfo, error)
	GetLeader(context.Context) (*pdpb.Member, error)
	TransferLeader(context.Context, string) error
	/* Meta-related interfaces */
	GetRegionByID(context.Context, uint64) (*RegionInfo, error)
	GetRegionByKey(context.Context, []byte) (*RegionInfo, error)
	GetRegions(context.Context) (*RegionsInfo, error)
	GetRegionsByKeyRange(context.Context, *KeyRange, int) (*RegionsInfo, error)
	GetRegionsByStoreID(context.Context, uint64) (*RegionsInfo, error)
	GetRegionsReplicatedStateByKeyRange(context.Context, *KeyRange) (string, error)
	GetHotReadRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHotWriteRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHistoryHotRegions(context.Context, *HistoryHotRegionsRequest) (*HistoryHotRegions, error)
	GetRegionStatusByKeyRange(context.Context, *KeyRange, bool) (*RegionStats, error)
	GetStores(context.Context) (*StoresInfo, error)
	GetStore(context.Context, uint64) (*StoreInfo, error)
	SetStoreLabels(context.Context, int64, map[string]string) error
	/* Config-related interfaces */
	GetScheduleConfig(context.Context) (map[string]interface{}, error)
	SetScheduleConfig(context.Context, map[string]interface{}) error
	GetClusterVersion(context.Context) (string, error)
	/* Scheduler-related interfaces */
	GetSchedulers(context.Context) ([]string, error)
	CreateScheduler(ctx context.Context, name string, storeID uint64) error
	SetSchedulerDelay(context.Context, string, int64) error
	/* Rule-related interfaces */
	GetAllPlacementRuleBundles(context.Context) ([]*GroupBundle, error)
	GetPlacementRuleBundleByGroup(context.Context, string) (*GroupBundle, error)
	GetPlacementRulesByGroup(context.Context, string) ([]*Rule, error)
	SetPlacementRule(context.Context, *Rule) error
	SetPlacementRuleInBatch(context.Context, []*RuleOp) error
	SetPlacementRuleBundles(context.Context, []*GroupBundle, bool) error
	DeletePlacementRule(context.Context, string, string) error
	GetAllPlacementRuleGroups(context.Context) ([]*RuleGroup, error)
	GetPlacementRuleGroupByID(context.Context, string) (*RuleGroup, error)
	SetPlacementRuleGroup(context.Context, *RuleGroup) error
	DeletePlacementRuleGroupByID(context.Context, string) error
	GetAllRegionLabelRules(context.Context) ([]*LabelRule, error)
	GetRegionLabelRulesByIDs(context.Context, []string) ([]*LabelRule, error)
	SetRegionLabelRule(context.Context, *LabelRule) error
	PatchRegionLabelRules(context.Context, *LabelRulePatch) error
	/* Scheduling-related interfaces */
	AccelerateSchedule(context.Context, *KeyRange) error
	AccelerateScheduleInBatch(context.Context, []*KeyRange) error
	/* Other interfaces */
	GetMinResolvedTSByStoresIDs(context.Context, []uint64) (uint64, map[uint64]uint64, error)
	/* Micro Service interfaces */
	GetMicroServiceMembers(context.Context, string) ([]string, error)

	/* Client-related methods */
	// WithCallerID sets and returns a new client with the given caller ID.
	WithCallerID(string) Client
	// WithRespHandler sets and returns a new client with the given HTTP response handler.
	// This allows the caller to customize how the response is handled, including error handling logic.
	// Additionally, it is important for the caller to handle the content of the response body properly
	// in order to ensure that it can be read and marshaled correctly into `res`.
	WithRespHandler(func(resp *http.Response, res interface{}) error) Client
	// Close gracefully closes the HTTP client.
	Close()
}

var _ Client = (*client)(nil)

// GetMembers gets the members info of PD cluster.
func (c *client) GetMembers(ctx context.Context) (*MembersInfo, error) {
	var members MembersInfo
	err := c.request(ctx,
		"GetMembers", membersPrefix,
		http.MethodGet, nil, &members)
	if err != nil {
		return nil, err
	}
	return &members, nil
}

// GetLeader gets the leader of PD cluster.
func (c *client) GetLeader(ctx context.Context) (*pdpb.Member, error) {
	var leader pdpb.Member
	err := c.request(ctx,
		"GetLeader", leaderPrefix,
		http.MethodGet, nil, &leader)
	if err != nil {
		return nil, err
	}
	return &leader, nil
}

// TransferLeader transfers the PD leader.
func (c *client) TransferLeader(ctx context.Context, newLeader string) error {
	return c.request(ctx,
		"TransferLeader", TransferLeaderByID(newLeader),
		http.MethodPost, nil, nil)
}

// GetRegionByID gets the region info by ID.
func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	var region RegionInfo
	err := c.request(ctx,
		"GetRegionByID", RegionByID(regionID),
		http.MethodGet, nil, &region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegionByKey gets the region info by key.
func (c *client) GetRegionByKey(ctx context.Context, key []byte) (*RegionInfo, error) {
	var region RegionInfo
	err := c.request(ctx,
		"GetRegionByKey", RegionByKey(key),
		http.MethodGet, nil, &region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegions gets the regions info.
func (c *client) GetRegions(ctx context.Context) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx,
		"GetRegions", Regions,
		http.MethodGet, nil, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByKeyRange gets the regions info by key range. If the limit is -1, it will return all regions within the range.
// The keys in the key range should be encoded in the UTF-8 bytes format.
func (c *client) GetRegionsByKeyRange(ctx context.Context, keyRange *KeyRange, limit int) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx,
		"GetRegionsByKeyRange", RegionsByKeyRange(keyRange, limit),
		http.MethodGet, nil, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByStoreID gets the regions info by store ID.
func (c *client) GetRegionsByStoreID(ctx context.Context, storeID uint64) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx,
		"GetRegionsByStoreID", RegionsByStoreID(storeID),
		http.MethodGet, nil, &regions)
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsReplicatedStateByKeyRange gets the regions replicated state info by key range.
// The keys in the key range should be encoded in the hex bytes format (without encoding to the UTF-8 bytes).
func (c *client) GetRegionsReplicatedStateByKeyRange(ctx context.Context, keyRange *KeyRange) (string, error) {
	var state string
	err := c.request(ctx,
		"GetRegionsReplicatedStateByKeyRange", RegionsReplicatedByKeyRange(keyRange),
		http.MethodGet, nil, &state)
	if err != nil {
		return "", err
	}
	return state, nil
}

// GetHotReadRegions gets the hot read region statistics info.
func (c *client) GetHotReadRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotReadRegions StoreHotPeersInfos
	err := c.request(ctx,
		"GetHotReadRegions", HotRead,
		http.MethodGet, nil, &hotReadRegions)
	if err != nil {
		return nil, err
	}
	return &hotReadRegions, nil
}

// GetHotWriteRegions gets the hot write region statistics info.
func (c *client) GetHotWriteRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotWriteRegions StoreHotPeersInfos
	err := c.request(ctx,
		"GetHotWriteRegions", HotWrite,
		http.MethodGet, nil, &hotWriteRegions)
	if err != nil {
		return nil, err
	}
	return &hotWriteRegions, nil
}

// GetHistoryHotRegions gets the history hot region statistics info.
func (c *client) GetHistoryHotRegions(ctx context.Context, req *HistoryHotRegionsRequest) (*HistoryHotRegions, error) {
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var historyHotRegions HistoryHotRegions
	err = c.request(ctx,
		"GetHistoryHotRegions", HotHistory,
		http.MethodGet, reqJSON, &historyHotRegions,
		WithAllowFollowerHandle())
	if err != nil {
		return nil, err
	}
	return &historyHotRegions, nil
}

// GetRegionStatusByKeyRange gets the region status by key range.
// If the `onlyCount` flag is true, the result will only include the count of regions.
// The keys in the key range should be encoded in the UTF-8 bytes format.
func (c *client) GetRegionStatusByKeyRange(ctx context.Context, keyRange *KeyRange, onlyCount bool) (*RegionStats, error) {
	var regionStats RegionStats
	err := c.request(ctx,
		"GetRegionStatusByKeyRange", RegionStatsByKeyRange(keyRange, onlyCount),
		http.MethodGet, nil, &regionStats,
	)
	if err != nil {
		return nil, err
	}
	return &regionStats, nil
}

// SetStoreLabels sets the labels of a store.
func (c *client) SetStoreLabels(ctx context.Context, storeID int64, storeLabels map[string]string) error {
	jsonInput, err := json.Marshal(storeLabels)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetStoreLabel", LabelByStoreID(storeID),
		http.MethodPost, jsonInput, nil)
}

// GetScheduleConfig gets the schedule configurations.
func (c *client) GetScheduleConfig(ctx context.Context) (map[string]interface{}, error) {
	var config map[string]interface{}
	err := c.request(ctx,
		"GetScheduleConfig", ScheduleConfig,
		http.MethodGet, nil, &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// SetScheduleConfig sets the schedule configurations.
func (c *client) SetScheduleConfig(ctx context.Context, config map[string]interface{}) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetScheduleConfig", ScheduleConfig,
		http.MethodPost, configJSON, nil)
}

// GetStores gets the stores info.
func (c *client) GetStores(ctx context.Context) (*StoresInfo, error) {
	var stores StoresInfo
	err := c.request(ctx,
		"GetStores", Stores,
		http.MethodGet, nil, &stores)
	if err != nil {
		return nil, err
	}
	return &stores, nil
}

// GetStore gets the store info by ID.
func (c *client) GetStore(ctx context.Context, storeID uint64) (*StoreInfo, error) {
	var store StoreInfo
	err := c.request(ctx,
		"GetStore", StoreByID(storeID),
		http.MethodGet, nil, &store)
	if err != nil {
		return nil, err
	}
	return &store, nil
}

// GetClusterVersion gets the cluster version.
func (c *client) GetClusterVersion(ctx context.Context) (string, error) {
	var version string
	err := c.request(ctx,
		"GetClusterVersion", ClusterVersion,
		http.MethodGet, nil, &version)
	if err != nil {
		return "", err
	}
	return version, nil
}

// GetAllPlacementRuleBundles gets all placement rules bundles.
func (c *client) GetAllPlacementRuleBundles(ctx context.Context) ([]*GroupBundle, error) {
	var bundles []*GroupBundle
	err := c.request(ctx,
		"GetPlacementRuleBundle", PlacementRuleBundle,
		http.MethodGet, nil, &bundles)
	if err != nil {
		return nil, err
	}
	return bundles, nil
}

// GetPlacementRuleBundleByGroup gets the placement rules bundle by group.
func (c *client) GetPlacementRuleBundleByGroup(ctx context.Context, group string) (*GroupBundle, error) {
	var bundle GroupBundle
	err := c.request(ctx,
		"GetPlacementRuleBundleByGroup", PlacementRuleBundleByGroup(group),
		http.MethodGet, nil, &bundle)
	if err != nil {
		return nil, err
	}
	return &bundle, nil
}

// GetPlacementRulesByGroup gets the placement rules by group.
func (c *client) GetPlacementRulesByGroup(ctx context.Context, group string) ([]*Rule, error) {
	var rules []*Rule
	err := c.request(ctx,
		"GetPlacementRulesByGroup", PlacementRulesByGroup(group),
		http.MethodGet, nil, &rules)
	if err != nil {
		return nil, err
	}
	return rules, nil
}

// SetPlacementRule sets the placement rule.
func (c *client) SetPlacementRule(ctx context.Context, rule *Rule) error {
	ruleJSON, err := json.Marshal(rule)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetPlacementRule", PlacementRule,
		http.MethodPost, ruleJSON, nil)
}

// SetPlacementRuleInBatch sets the placement rules in batch.
func (c *client) SetPlacementRuleInBatch(ctx context.Context, ruleOps []*RuleOp) error {
	ruleOpsJSON, err := json.Marshal(ruleOps)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetPlacementRuleInBatch", PlacementRulesInBatch,
		http.MethodPost, ruleOpsJSON, nil)
}

// SetPlacementRuleBundles sets the placement rule bundles.
// If `partial` is false, all old configurations will be over-written and dropped.
func (c *client) SetPlacementRuleBundles(ctx context.Context, bundles []*GroupBundle, partial bool) error {
	bundlesJSON, err := json.Marshal(bundles)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetPlacementRuleBundles", PlacementRuleBundleWithPartialParameter(partial),
		http.MethodPost, bundlesJSON, nil)
}

// DeletePlacementRule deletes the placement rule.
func (c *client) DeletePlacementRule(ctx context.Context, group, id string) error {
	return c.request(ctx,
		"DeletePlacementRule", PlacementRuleByGroupAndID(group, id),
		http.MethodDelete, nil, nil)
}

// GetAllPlacementRuleGroups gets all placement rule groups.
func (c *client) GetAllPlacementRuleGroups(ctx context.Context) ([]*RuleGroup, error) {
	var ruleGroups []*RuleGroup
	err := c.request(ctx,
		"GetAllPlacementRuleGroups", placementRuleGroups,
		http.MethodGet, nil, &ruleGroups)
	if err != nil {
		return nil, err
	}
	return ruleGroups, nil
}

// GetPlacementRuleGroupByID gets the placement rule group by ID.
func (c *client) GetPlacementRuleGroupByID(ctx context.Context, id string) (*RuleGroup, error) {
	var ruleGroup RuleGroup
	err := c.request(ctx,
		"GetPlacementRuleGroupByID", PlacementRuleGroupByID(id),
		http.MethodGet, nil, &ruleGroup)
	if err != nil {
		return nil, err
	}
	return &ruleGroup, nil
}

// SetPlacementRuleGroup sets the placement rule group.
func (c *client) SetPlacementRuleGroup(ctx context.Context, ruleGroup *RuleGroup) error {
	ruleGroupJSON, err := json.Marshal(ruleGroup)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetPlacementRuleGroup", placementRuleGroup,
		http.MethodPost, ruleGroupJSON, nil)
}

// DeletePlacementRuleGroupByID deletes the placement rule group by ID.
func (c *client) DeletePlacementRuleGroupByID(ctx context.Context, id string) error {
	return c.request(ctx,
		"DeletePlacementRuleGroupByID", PlacementRuleGroupByID(id),
		http.MethodDelete, nil, nil)
}

// GetAllRegionLabelRules gets all region label rules.
func (c *client) GetAllRegionLabelRules(ctx context.Context) ([]*LabelRule, error) {
	var labelRules []*LabelRule
	err := c.request(ctx,
		"GetAllRegionLabelRules", RegionLabelRules,
		http.MethodGet, nil, &labelRules)
	if err != nil {
		return nil, err
	}
	return labelRules, nil
}

// GetRegionLabelRulesByIDs gets the region label rules by IDs.
func (c *client) GetRegionLabelRulesByIDs(ctx context.Context, ruleIDs []string) ([]*LabelRule, error) {
	idsJSON, err := json.Marshal(ruleIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var labelRules []*LabelRule
	err = c.request(ctx,
		"GetRegionLabelRulesByIDs", RegionLabelRulesByIDs,
		http.MethodGet, idsJSON, &labelRules)
	if err != nil {
		return nil, err
	}
	return labelRules, nil
}

// SetRegionLabelRule sets the region label rule.
func (c *client) SetRegionLabelRule(ctx context.Context, labelRule *LabelRule) error {
	labelRuleJSON, err := json.Marshal(labelRule)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetRegionLabelRule", RegionLabelRule,
		http.MethodPost, labelRuleJSON, nil)
}

// PatchRegionLabelRules patches the region label rules.
func (c *client) PatchRegionLabelRules(ctx context.Context, labelRulePatch *LabelRulePatch) error {
	labelRulePatchJSON, err := json.Marshal(labelRulePatch)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"PatchRegionLabelRules", RegionLabelRules,
		http.MethodPatch, labelRulePatchJSON, nil)
}

// GetSchedulers gets the schedulers from PD cluster.
func (c *client) GetSchedulers(ctx context.Context) ([]string, error) {
	var schedulers []string
	err := c.request(ctx,
		"GetSchedulers", Schedulers,
		http.MethodGet, nil, &schedulers)
	if err != nil {
		return nil, err
	}
	return schedulers, nil
}

// CreateScheduler creates a scheduler to PD cluster.
func (c *client) CreateScheduler(ctx context.Context, name string, storeID uint64) error {
	inputJSON, err := json.Marshal(map[string]interface{}{
		"name":     name,
		"store_id": storeID,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"CreateScheduler", Schedulers,
		http.MethodPost, inputJSON, nil)
}

// AccelerateSchedule accelerates the scheduling of the regions within the given key range.
// The keys in the key range should be encoded in the hex bytes format (without encoding to the UTF-8 bytes).
func (c *client) AccelerateSchedule(ctx context.Context, keyRange *KeyRange) error {
	startKey, endKey := keyRange.EscapeAsHexStr()
	inputJSON, err := json.Marshal(map[string]string{
		"start_key": startKey,
		"end_key":   endKey,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"AccelerateSchedule", AccelerateSchedule,
		http.MethodPost, inputJSON, nil)
}

// AccelerateScheduleInBatch accelerates the scheduling of the regions within the given key ranges in batch.
// The keys in the key ranges should be encoded in the hex bytes format (without encoding to the UTF-8 bytes).
func (c *client) AccelerateScheduleInBatch(ctx context.Context, keyRanges []*KeyRange) error {
	input := make([]map[string]string, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		startKey, endKey := keyRange.EscapeAsHexStr()
		input = append(input, map[string]string{
			"start_key": startKey,
			"end_key":   endKey,
		})
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"AccelerateScheduleInBatch", AccelerateScheduleInBatch,
		http.MethodPost, inputJSON, nil)
}

// SetSchedulerDelay sets the delay of given scheduler.
func (c *client) SetSchedulerDelay(ctx context.Context, scheduler string, delaySec int64) error {
	m := map[string]int64{
		"delay": delaySec,
	}
	inputJSON, err := json.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx,
		"SetSchedulerDelay", SchedulerByName(scheduler),
		http.MethodPost, inputJSON, nil)
}

// GetMinResolvedTSByStoresIDs get min-resolved-ts by stores IDs.
// - When storeIDs has zero length, it will return (cluster-level's min_resolved_ts, nil, nil) when no error.
// - When storeIDs is {"cluster"}, it will return (cluster-level's min_resolved_ts, stores_min_resolved_ts, nil) when no error.
// - When storeID is specified to ID lists, it will return (min_resolved_ts of given stores, stores_min_resolved_ts, nil) when no error.
func (c *client) GetMinResolvedTSByStoresIDs(ctx context.Context, storeIDs []uint64) (uint64, map[uint64]uint64, error) {
	uri := MinResolvedTSPrefix
	// scope is an optional parameter, it can be `cluster` or specified store IDs.
	// - When no scope is given, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be nil.
	// - When scope is `cluster`, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be filled.
	// - When scope given a list of stores, min_resolved_ts will be provided for each store
	//      and the scope-specific min_resolved_ts will be returned.
	if len(storeIDs) != 0 {
		storeIDStrs := make([]string, len(storeIDs))
		for idx, id := range storeIDs {
			storeIDStrs[idx] = fmt.Sprintf("%d", id)
		}
		uri = fmt.Sprintf("%s?scope=%s", uri, strings.Join(storeIDStrs, ","))
	}
	resp := struct {
		MinResolvedTS       uint64            `json:"min_resolved_ts"`
		IsRealTime          bool              `json:"is_real_time,omitempty"`
		StoresMinResolvedTS map[uint64]uint64 `json:"stores_min_resolved_ts"`
	}{}
	err := c.request(ctx,
		"GetMinResolvedTSByStoresIDs", uri,
		http.MethodGet, nil, &resp)
	if err != nil {
		return 0, nil, err
	}
	if !resp.IsRealTime {
		return 0, nil, errors.Trace(errors.New("min resolved ts is not enabled"))
	}
	return resp.MinResolvedTS, resp.StoresMinResolvedTS, nil
}

// GetMicroServiceMembers gets the members of the microservice.
func (c *client) GetMicroServiceMembers(ctx context.Context, service string) ([]string, error) {
	var members []string
	err := c.request(ctx,
		"GetMicroServiceMembers", MicroServiceMembers(service),
		http.MethodGet, nil, &members)
	if err != nil {
		return nil, err
	}
	return members, nil
}
