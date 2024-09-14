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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/client/retry"
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
	GetEmptyRegions(context.Context) (*RegionsInfo, error)
	GetRegionsReplicatedStateByKeyRange(context.Context, *KeyRange) (string, error)
	GetHotReadRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHotWriteRegions(context.Context) (*StoreHotPeersInfos, error)
	GetHistoryHotRegions(context.Context, *HistoryHotRegionsRequest) (*HistoryHotRegions, error)
	GetRegionStatusByKeyRange(context.Context, *KeyRange, bool) (*RegionStats, error)
	GetStores(context.Context) (*StoresInfo, error)
	GetStore(context.Context, uint64) (*StoreInfo, error)
	DeleteStore(context.Context, uint64) error
	SetStoreLabels(context.Context, int64, map[string]string) error
	DeleteStoreLabel(ctx context.Context, storeID int64, labelKey string) error
	GetHealthStatus(context.Context) ([]Health, error)
	/* Config-related interfaces */
	GetConfig(context.Context) (map[string]any, error)
	SetConfig(context.Context, map[string]any, ...float64) error
	GetScheduleConfig(context.Context) (map[string]any, error)
	SetScheduleConfig(context.Context, map[string]any) error
	GetClusterVersion(context.Context) (string, error)
	GetCluster(context.Context) (*metapb.Cluster, error)
	GetClusterStatus(context.Context) (*ClusterState, error)
	GetStatus(context.Context) (*State, error)
	GetReplicateConfig(context.Context) (map[string]any, error)
	/* Scheduler-related interfaces */
	GetSchedulers(context.Context) ([]string, error)
	CreateScheduler(ctx context.Context, name string, storeID uint64) error
	DeleteScheduler(ctx context.Context, name string) error
	SetSchedulerDelay(context.Context, string, int64) error
	/* Rule-related interfaces */
	GetAllPlacementRuleBundles(context.Context) ([]*GroupBundle, error)
	GetPlacementRuleBundleByGroup(context.Context, string) (*GroupBundle, error)
	GetPlacementRulesByGroup(context.Context, string) ([]*Rule, error)
	GetPlacementRule(context.Context, string, string) (*Rule, error)
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
	// `SetRegionLabelRule` sets the label rule for a region.
	// When a label rule (deny scheduler) is set,
	//  1. All schedulers will be disabled except for the evict-leader-scheduler.
	//  2. The merge-checker will be disabled, preventing these regions from being merged.
	SetRegionLabelRule(context.Context, *LabelRule) error
	PatchRegionLabelRules(context.Context, *LabelRulePatch) error
	/* Scheduling-related interfaces */
	AccelerateSchedule(context.Context, *KeyRange) error
	AccelerateScheduleInBatch(context.Context, []*KeyRange) error
	/* Admin-related interfaces */
	ResetTS(context.Context, uint64, bool) error
	ResetBaseAllocID(context.Context, uint64) error
	SetSnapshotRecoveringMark(context.Context) error
	DeleteSnapshotRecoveringMark(context.Context) error
	/* Other interfaces */
	GetMinResolvedTSByStoresIDs(context.Context, []uint64) (uint64, map[uint64]uint64, error)
	GetPDVersion(context.Context) (string, error)
	GetGCSafePoint(context.Context) (ListServiceGCSafepoint, error)
	DeleteGCSafePoint(context.Context, string) (string, error)
	/* Micro Service interfaces */
	GetMicroServiceMembers(context.Context, string) ([]MicroServiceMember, error)
	GetMicroServicePrimary(context.Context, string) (string, error)
	DeleteOperators(context.Context) error

	/* Keyspace interface */

	// UpdateKeyspaceGCManagementType update the `gc_management_type` in keyspace meta config.
	// If `gc_management_type` is `global_gc`, it means the current keyspace requires a tidb without 'keyspace-name'
	// configured to run a global gc worker to calculate a global gc safe point.
	// If `gc_management_type` is `keyspace_level_gc` it means the current keyspace can calculate gc safe point by its own.
	UpdateKeyspaceGCManagementType(ctx context.Context, keyspaceName string, keyspaceGCManagementType *KeyspaceGCManagementTypeConfig) error
	GetKeyspaceMetaByName(ctx context.Context, keyspaceName string) (*keyspacepb.KeyspaceMeta, error)

	/* Client-related methods */
	// WithCallerID sets and returns a new client with the given caller ID.
	WithCallerID(string) Client
	// WithRespHandler sets and returns a new client with the given HTTP response handler.
	// This allows the caller to customize how the response is handled, including error handling logic.
	// Additionally, it is important for the caller to handle the content of the response body properly
	// in order to ensure that it can be read and marshaled correctly into `res`.
	WithRespHandler(func(resp *http.Response, res any) error) Client
	// WithBackoffer sets and returns a new client with the given backoffer.
	WithBackoffer(*retry.Backoffer) Client
	// WithTargetURL sets and returns a new client with the given target URL.
	WithTargetURL(string) Client
	// Close gracefully closes the HTTP client.
	Close()
}

var _ Client = (*client)(nil)

// GetMembers gets the members info of PD cluster.
func (c *client) GetMembers(ctx context.Context) (*MembersInfo, error) {
	var members MembersInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getMembersName).
		WithURI(membersPrefix).
		WithMethod(http.MethodGet).
		WithResp(&members))
	if err != nil {
		return nil, err
	}
	return &members, nil
}

// GetLeader gets the leader of PD cluster.
func (c *client) GetLeader(ctx context.Context) (*pdpb.Member, error) {
	var leader pdpb.Member
	err := c.request(ctx, newRequestInfo().
		WithName(getLeaderName).
		WithURI(leaderPrefix).
		WithMethod(http.MethodGet).
		WithResp(&leader))
	if err != nil {
		return nil, err
	}
	return &leader, nil
}

// TransferLeader transfers the PD leader.
func (c *client) TransferLeader(ctx context.Context, newLeader string) error {
	return c.request(ctx, newRequestInfo().
		WithName(transferLeaderName).
		WithURI(TransferLeaderByID(newLeader)).
		WithMethod(http.MethodPost))
}

// GetRegionByID gets the region info by ID.
func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	var region RegionInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getRegionByIDName).
		WithURI(RegionByID(regionID)).
		WithMethod(http.MethodGet).
		WithResp(&region))
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegionByKey gets the region info by key.
func (c *client) GetRegionByKey(ctx context.Context, key []byte) (*RegionInfo, error) {
	var region RegionInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getRegionByKeyName).
		WithURI(RegionByKey(key)).
		WithMethod(http.MethodGet).
		WithResp(&region))
	if err != nil {
		return nil, err
	}
	return &region, nil
}

// GetRegions gets the regions info.
func (c *client) GetRegions(ctx context.Context) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getRegionsName).
		WithURI(Regions).
		WithMethod(http.MethodGet).
		WithResp(&regions))
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByKeyRange gets the regions info by key range. If the limit is -1, it will return all regions within the range.
// The keys in the key range should be encoded in the UTF-8 bytes format.
func (c *client) GetRegionsByKeyRange(ctx context.Context, keyRange *KeyRange, limit int) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getRegionsByKeyRangeName).
		WithURI(RegionsByKeyRange(keyRange, limit)).
		WithMethod(http.MethodGet).
		WithResp(&regions))
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsByStoreID gets the regions info by store ID.
func (c *client) GetRegionsByStoreID(ctx context.Context, storeID uint64) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getRegionsByStoreIDName).
		WithURI(RegionsByStoreID(storeID)).
		WithMethod(http.MethodGet).
		WithResp(&regions))
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetEmptyRegions gets the empty regions info.
func (c *client) GetEmptyRegions(ctx context.Context) (*RegionsInfo, error) {
	var regions RegionsInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getEmptyRegionsName).
		WithURI(EmptyRegions).
		WithMethod(http.MethodGet).
		WithResp(&regions))
	if err != nil {
		return nil, err
	}
	return &regions, nil
}

// GetRegionsReplicatedStateByKeyRange gets the regions replicated state info by key range.
// The keys in the key range should be encoded in the hex bytes format (without encoding to the UTF-8 bytes).
func (c *client) GetRegionsReplicatedStateByKeyRange(ctx context.Context, keyRange *KeyRange) (string, error) {
	var state string
	err := c.request(ctx, newRequestInfo().
		WithName(getRegionsReplicatedStateByKeyRangeName).
		WithURI(RegionsReplicatedByKeyRange(keyRange)).
		WithMethod(http.MethodGet).
		WithResp(&state))
	if err != nil {
		return "", err
	}
	return state, nil
}

// GetHotReadRegions gets the hot read region statistics info.
func (c *client) GetHotReadRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotReadRegions StoreHotPeersInfos
	err := c.request(ctx, newRequestInfo().
		WithName(getHotReadRegionsName).
		WithURI(HotRead).
		WithMethod(http.MethodGet).
		WithResp(&hotReadRegions))
	if err != nil {
		return nil, err
	}
	return &hotReadRegions, nil
}

// GetHotWriteRegions gets the hot write region statistics info.
func (c *client) GetHotWriteRegions(ctx context.Context) (*StoreHotPeersInfos, error) {
	var hotWriteRegions StoreHotPeersInfos
	err := c.request(ctx, newRequestInfo().
		WithName(getHotWriteRegionsName).
		WithURI(HotWrite).
		WithMethod(http.MethodGet).
		WithResp(&hotWriteRegions))
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
	err = c.request(ctx, newRequestInfo().
		WithName(getHistoryHotRegionsName).
		WithURI(HotHistory).
		WithMethod(http.MethodGet).
		WithBody(reqJSON).
		WithResp(&historyHotRegions),
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
	err := c.request(ctx, newRequestInfo().
		WithName(getRegionStatusByKeyRangeName).
		WithURI(RegionStatsByKeyRange(keyRange, onlyCount)).
		WithMethod(http.MethodGet).
		WithResp(&regionStats))
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
	return c.request(ctx, newRequestInfo().
		WithName(setStoreLabelsName).
		WithURI(LabelByStoreID(storeID)).
		WithMethod(http.MethodPost).
		WithBody(jsonInput))
}

// DeleteStoreLabel deletes the labels of a store.
func (c *client) DeleteStoreLabel(ctx context.Context, storeID int64, labelKey string) error {
	jsonInput, err := json.Marshal(labelKey)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(deleteStoreLabelName).
		WithURI(LabelByStoreID(storeID)).
		WithMethod(http.MethodDelete).
		WithBody(jsonInput))
}

// GetHealthStatus gets the health status of the cluster.
func (c *client) GetHealthStatus(ctx context.Context) ([]Health, error) {
	var healths []Health
	err := c.request(ctx, newRequestInfo().
		WithName(getHealthStatusName).
		WithURI(health).
		WithMethod(http.MethodGet).
		WithResp(&healths))
	if err != nil {
		return nil, err
	}
	return healths, nil
}

// GetConfig gets the configurations.
func (c *client) GetConfig(ctx context.Context) (map[string]any, error) {
	var config map[string]any
	err := c.request(ctx, newRequestInfo().
		WithName(getConfigName).
		WithURI(Config).
		WithMethod(http.MethodGet).
		WithResp(&config))
	if err != nil {
		return nil, err
	}
	return config, nil
}

// SetConfig sets the configurations. ttlSecond is optional.
func (c *client) SetConfig(ctx context.Context, config map[string]any, ttlSecond ...float64) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return errors.Trace(err)
	}
	var uri string
	if len(ttlSecond) > 0 {
		uri = ConfigWithTTLSeconds(ttlSecond[0])
	} else {
		uri = Config
	}
	return c.request(ctx, newRequestInfo().
		WithName(setConfigName).
		WithURI(uri).
		WithMethod(http.MethodPost).
		WithBody(configJSON))
}

// GetScheduleConfig gets the schedule configurations.
func (c *client) GetScheduleConfig(ctx context.Context) (map[string]any, error) {
	var config map[string]any
	err := c.request(ctx, newRequestInfo().
		WithName(getScheduleConfigName).
		WithURI(ScheduleConfig).
		WithMethod(http.MethodGet).
		WithResp(&config))
	if err != nil {
		return nil, err
	}
	return config, nil
}

// SetScheduleConfig sets the schedule configurations.
func (c *client) SetScheduleConfig(ctx context.Context, config map[string]any) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(setScheduleConfigName).
		WithURI(ScheduleConfig).
		WithMethod(http.MethodPost).
		WithBody(configJSON))
}

// GetStores gets the stores info.
func (c *client) GetStores(ctx context.Context) (*StoresInfo, error) {
	var stores StoresInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getStoresName).
		WithURI(Stores).
		WithMethod(http.MethodGet).
		WithResp(&stores))
	if err != nil {
		return nil, err
	}
	return &stores, nil
}

// GetStore gets the store info by ID.
func (c *client) GetStore(ctx context.Context, storeID uint64) (*StoreInfo, error) {
	var store StoreInfo
	err := c.request(ctx, newRequestInfo().
		WithName(getStoreName).
		WithURI(StoreByID(storeID)).
		WithMethod(http.MethodGet).
		WithResp(&store))
	if err != nil {
		return nil, err
	}
	return &store, nil
}

// DeleteStore deletes the store by ID.
func (c *client) DeleteStore(ctx context.Context, storeID uint64) error {
	return c.request(ctx, newRequestInfo().
		WithName(deleteStoreName).
		WithURI(StoreByID(storeID)).
		WithMethod(http.MethodDelete))
}

// GetClusterVersion gets the cluster version.
func (c *client) GetClusterVersion(ctx context.Context) (string, error) {
	var version string
	err := c.request(ctx, newRequestInfo().
		WithName(getClusterVersionName).
		WithURI(ClusterVersion).
		WithMethod(http.MethodGet).
		WithResp(&version))
	if err != nil {
		return "", err
	}
	return version, nil
}

// GetCluster gets the cluster meta information.
func (c *client) GetCluster(ctx context.Context) (*metapb.Cluster, error) {
	var clusterInfo *metapb.Cluster
	err := c.request(ctx, newRequestInfo().
		WithName(getClusterName).
		WithURI(Cluster).
		WithMethod(http.MethodGet).
		WithResp(&clusterInfo))
	if err != nil {
		return nil, err
	}
	return clusterInfo, nil
}

// GetClusterStatus gets the cluster status.
func (c *client) GetClusterStatus(ctx context.Context) (*ClusterState, error) {
	var clusterStatus *ClusterState
	err := c.request(ctx, newRequestInfo().
		WithName(getClusterName).
		WithURI(ClusterStatus).
		WithMethod(http.MethodGet).
		WithResp(&clusterStatus))
	if err != nil {
		return nil, err
	}
	return clusterStatus, nil
}

// GetStatus gets the status of PD.
func (c *client) GetStatus(ctx context.Context) (*State, error) {
	var status *State
	err := c.request(ctx, newRequestInfo().
		WithName(getStatusName).
		WithURI(Status).
		WithMethod(http.MethodGet).
		WithResp(&status),
		WithAllowFollowerHandle())
	if err != nil {
		return nil, err
	}
	return status, nil
}

// GetReplicateConfig gets the replication configurations.
func (c *client) GetReplicateConfig(ctx context.Context) (map[string]any, error) {
	var config map[string]any
	err := c.request(ctx, newRequestInfo().
		WithName(getReplicateConfigName).
		WithURI(ReplicateConfig).
		WithMethod(http.MethodGet).
		WithResp(&config))
	if err != nil {
		return nil, err
	}
	return config, nil
}

// GetAllPlacementRuleBundles gets all placement rules bundles.
func (c *client) GetAllPlacementRuleBundles(ctx context.Context) ([]*GroupBundle, error) {
	var bundles []*GroupBundle
	err := c.request(ctx, newRequestInfo().
		WithName(getAllPlacementRuleBundlesName).
		WithURI(PlacementRuleBundle).
		WithMethod(http.MethodGet).
		WithResp(&bundles))
	if err != nil {
		return nil, err
	}
	return bundles, nil
}

// GetPlacementRuleBundleByGroup gets the placement rules bundle by group.
func (c *client) GetPlacementRuleBundleByGroup(ctx context.Context, group string) (*GroupBundle, error) {
	var bundle GroupBundle
	err := c.request(ctx, newRequestInfo().
		WithName(getPlacementRuleBundleByGroupName).
		WithURI(PlacementRuleBundleByGroup(group)).
		WithMethod(http.MethodGet).
		WithResp(&bundle))
	if err != nil {
		return nil, err
	}
	return &bundle, nil
}

// GetPlacementRulesByGroup gets the placement rules by group.
func (c *client) GetPlacementRulesByGroup(ctx context.Context, group string) ([]*Rule, error) {
	var rules []*Rule
	err := c.request(ctx, newRequestInfo().
		WithName(getPlacementRulesByGroupName).
		WithURI(PlacementRulesByGroup(group)).
		WithMethod(http.MethodGet).
		WithResp(&rules))
	if err != nil {
		return nil, err
	}
	return rules, nil
}

// GetPlacementRule gets the placement rule by group and ID.
func (c *client) GetPlacementRule(ctx context.Context, group, id string) (*Rule, error) {
	var rule Rule
	err := c.request(ctx, newRequestInfo().
		WithName(getPlacementRuleName).
		WithURI(PlacementRuleByGroupAndID(group, id)).
		WithMethod(http.MethodGet).
		WithResp(&rule))
	if err != nil {
		return nil, err
	}
	return &rule, nil
}

// SetPlacementRule sets the placement rule.
func (c *client) SetPlacementRule(ctx context.Context, rule *Rule) error {
	ruleJSON, err := json.Marshal(rule)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(setPlacementRuleName).
		WithURI(PlacementRule).
		WithMethod(http.MethodPost).
		WithBody(ruleJSON))
}

// SetPlacementRuleInBatch sets the placement rules in batch.
func (c *client) SetPlacementRuleInBatch(ctx context.Context, ruleOps []*RuleOp) error {
	ruleOpsJSON, err := json.Marshal(ruleOps)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(setPlacementRuleInBatchName).
		WithURI(PlacementRulesInBatch).
		WithMethod(http.MethodPost).
		WithBody(ruleOpsJSON))
}

// SetPlacementRuleBundles sets the placement rule bundles.
// If `partial` is false, all old configurations will be over-written and dropped.
func (c *client) SetPlacementRuleBundles(ctx context.Context, bundles []*GroupBundle, partial bool) error {
	bundlesJSON, err := json.Marshal(bundles)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(setPlacementRuleBundlesName).
		WithURI(PlacementRuleBundleWithPartialParameter(partial)).
		WithMethod(http.MethodPost).
		WithBody(bundlesJSON))
}

// DeletePlacementRule deletes the placement rule.
func (c *client) DeletePlacementRule(ctx context.Context, group, id string) error {
	return c.request(ctx, newRequestInfo().
		WithName(deletePlacementRuleName).
		WithURI(PlacementRuleByGroupAndID(group, id)).
		WithMethod(http.MethodDelete))
}

// GetAllPlacementRuleGroups gets all placement rule groups.
func (c *client) GetAllPlacementRuleGroups(ctx context.Context) ([]*RuleGroup, error) {
	var ruleGroups []*RuleGroup
	err := c.request(ctx, newRequestInfo().
		WithName(getAllPlacementRuleGroupsName).
		WithURI(placementRuleGroups).
		WithMethod(http.MethodGet).
		WithResp(&ruleGroups))
	if err != nil {
		return nil, err
	}
	return ruleGroups, nil
}

// GetPlacementRuleGroupByID gets the placement rule group by ID.
func (c *client) GetPlacementRuleGroupByID(ctx context.Context, id string) (*RuleGroup, error) {
	var ruleGroup RuleGroup
	err := c.request(ctx, newRequestInfo().
		WithName(getPlacementRuleGroupByIDName).
		WithURI(PlacementRuleGroupByID(id)).
		WithMethod(http.MethodGet).
		WithResp(&ruleGroup))
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
	return c.request(ctx, newRequestInfo().
		WithName(setPlacementRuleGroupName).
		WithURI(placementRuleGroup).
		WithMethod(http.MethodPost).
		WithBody(ruleGroupJSON))
}

// DeletePlacementRuleGroupByID deletes the placement rule group by ID.
func (c *client) DeletePlacementRuleGroupByID(ctx context.Context, id string) error {
	return c.request(ctx, newRequestInfo().
		WithName(deletePlacementRuleGroupByIDName).
		WithURI(PlacementRuleGroupByID(id)).
		WithMethod(http.MethodDelete))
}

// GetAllRegionLabelRules gets all region label rules.
func (c *client) GetAllRegionLabelRules(ctx context.Context) ([]*LabelRule, error) {
	var labelRules []*LabelRule
	err := c.request(ctx, newRequestInfo().
		WithName(getAllRegionLabelRulesName).
		WithURI(RegionLabelRules).
		WithMethod(http.MethodGet).
		WithResp(&labelRules))
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
	err = c.request(ctx, newRequestInfo().
		WithName(getRegionLabelRulesByIDsName).
		WithURI(RegionLabelRulesByIDs).
		WithMethod(http.MethodGet).
		WithBody(idsJSON).
		WithResp(&labelRules))
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
	return c.request(ctx, newRequestInfo().
		WithName(setRegionLabelRuleName).
		WithURI(RegionLabelRule).
		WithMethod(http.MethodPost).
		WithBody(labelRuleJSON))
}

// PatchRegionLabelRules patches the region label rules.
func (c *client) PatchRegionLabelRules(ctx context.Context, labelRulePatch *LabelRulePatch) error {
	labelRulePatchJSON, err := json.Marshal(labelRulePatch)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(patchRegionLabelRulesName).
		WithURI(RegionLabelRules).
		WithMethod(http.MethodPatch).
		WithBody(labelRulePatchJSON))
}

// GetSchedulers gets the schedulers from PD cluster.
func (c *client) GetSchedulers(ctx context.Context) ([]string, error) {
	var schedulers []string
	err := c.request(ctx, newRequestInfo().
		WithName(getSchedulersName).
		WithURI(Schedulers).
		WithMethod(http.MethodGet).
		WithResp(&schedulers))
	if err != nil {
		return nil, err
	}
	return schedulers, nil
}

// CreateScheduler creates a scheduler to PD cluster.
func (c *client) CreateScheduler(ctx context.Context, name string, storeID uint64) error {
	inputJSON, err := json.Marshal(map[string]any{
		"name":     name,
		"store_id": storeID,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(createSchedulerName).
		WithURI(Schedulers).
		WithMethod(http.MethodPost).
		WithBody(inputJSON))
}

// DeleteScheduler deletes a scheduler from PD cluster.
func (c *client) DeleteScheduler(ctx context.Context, name string) error {
	return c.request(ctx, newRequestInfo().
		WithName(deleteSchedulerName).
		WithURI(SchedulerByName(name)).
		WithMethod(http.MethodDelete))
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
	return c.request(ctx, newRequestInfo().
		WithName(accelerateScheduleName).
		WithURI(AccelerateSchedule).
		WithMethod(http.MethodPost).
		WithBody(inputJSON))
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
	return c.request(ctx, newRequestInfo().
		WithName(accelerateScheduleInBatchName).
		WithURI(AccelerateScheduleInBatch).
		WithMethod(http.MethodPost).
		WithBody(inputJSON))
}

// ResetTS resets the PD's TS.
func (c *client) ResetTS(ctx context.Context, ts uint64, forceUseLarger bool) error {
	reqData, err := json.Marshal(struct {
		Tso            string `json:"tso"`
		ForceUseLarger bool   `json:"force-use-larger"`
	}{
		Tso:            strconv.FormatUint(ts, 10),
		ForceUseLarger: forceUseLarger,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(resetTSName).
		WithURI(ResetTS).
		WithMethod(http.MethodPost).
		WithBody(reqData))
}

// ResetBaseAllocID resets the PD's base alloc ID.
func (c *client) ResetBaseAllocID(ctx context.Context, id uint64) error {
	reqData, err := json.Marshal(struct {
		ID string `json:"id"`
	}{
		ID: strconv.FormatUint(id, 10),
	})
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(resetBaseAllocIDName).
		WithURI(BaseAllocID).
		WithMethod(http.MethodPost).
		WithBody(reqData))
}

// SetSnapshotRecoveringMark sets the snapshot recovering mark.
func (c *client) SetSnapshotRecoveringMark(ctx context.Context) error {
	return c.request(ctx, newRequestInfo().
		WithName(setSnapshotRecoveringMarkName).
		WithURI(SnapshotRecoveringMark).
		WithMethod(http.MethodPost))
}

// DeleteSnapshotRecoveringMark deletes the snapshot recovering mark.
func (c *client) DeleteSnapshotRecoveringMark(ctx context.Context) error {
	return c.request(ctx, newRequestInfo().
		WithName(deleteSnapshotRecoveringMarkName).
		WithURI(SnapshotRecoveringMark).
		WithMethod(http.MethodDelete))
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
	return c.request(ctx, newRequestInfo().
		WithName(setSchedulerDelayName).
		WithURI(SchedulerByName(scheduler)).
		WithMethod(http.MethodPost).
		WithBody(inputJSON))
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
	err := c.request(ctx, newRequestInfo().
		WithName(getMinResolvedTSByStoresIDsName).
		WithURI(uri).
		WithMethod(http.MethodGet).
		WithResp(&resp))
	if err != nil {
		return 0, nil, err
	}
	if !resp.IsRealTime {
		return 0, nil, errors.Trace(errors.New("min resolved ts is not enabled"))
	}
	return resp.MinResolvedTS, resp.StoresMinResolvedTS, nil
}

// GetMicroServiceMembers gets the members of the microservice.
func (c *client) GetMicroServiceMembers(ctx context.Context, service string) ([]MicroServiceMember, error) {
	var members []MicroServiceMember
	err := c.request(ctx, newRequestInfo().
		WithName(getMicroServiceMembersName).
		WithURI(MicroServiceMembers(service)).
		WithMethod(http.MethodGet).
		WithResp(&members))
	if err != nil {
		return nil, err
	}
	return members, nil
}

// GetMicroServicePrimary gets the primary of the microservice.
func (c *client) GetMicroServicePrimary(ctx context.Context, service string) (string, error) {
	var primary string
	err := c.request(ctx, newRequestInfo().
		WithName(getMicroServicePrimaryName).
		WithURI(MicroServicePrimary(service)).
		WithMethod(http.MethodGet).
		WithResp(&primary))
	return primary, err
}

// GetPDVersion gets the release version of the PD binary.
func (c *client) GetPDVersion(ctx context.Context) (string, error) {
	var ver struct {
		Version string `json:"version"`
	}
	err := c.request(ctx, newRequestInfo().
		WithName(getPDVersionName).
		WithURI(Version).
		WithMethod(http.MethodGet).
		WithResp(&ver))
	return ver.Version, err
}

// DeleteOperators deletes the running operators.
func (c *client) DeleteOperators(ctx context.Context) error {
	return c.request(ctx, newRequestInfo().
		WithName(deleteOperators).
		WithURI(operators).
		WithMethod(http.MethodDelete))
}

// UpdateKeyspaceGCManagementType patches the keyspace config.
func (c *client) UpdateKeyspaceGCManagementType(ctx context.Context, keyspaceName string, keyspaceGCmanagementType *KeyspaceGCManagementTypeConfig) error {
	keyspaceConfigPatchJSON, err := json.Marshal(keyspaceGCmanagementType)
	if err != nil {
		return errors.Trace(err)
	}
	return c.request(ctx, newRequestInfo().
		WithName(UpdateKeyspaceGCManagementTypeName).
		WithURI(GetUpdateKeyspaceConfigURL(keyspaceName)).
		WithMethod(http.MethodPatch).
		WithBody(keyspaceConfigPatchJSON))
}

// GetKeyspaceMetaByName get the given keyspace meta.
func (c *client) GetKeyspaceMetaByName(ctx context.Context, keyspaceName string) (*keyspacepb.KeyspaceMeta, error) {
	var (
		tempKeyspaceMeta tempKeyspaceMeta
		keyspaceMetaPB   keyspacepb.KeyspaceMeta
	)
	err := c.request(ctx, newRequestInfo().
		WithName(GetKeyspaceMetaByNameName).
		WithURI(GetKeyspaceMetaByNameURL(keyspaceName)).
		WithMethod(http.MethodGet).
		WithResp(&tempKeyspaceMeta))

	if err != nil {
		return nil, err
	}

	keyspaceState, err := stringToKeyspaceState(tempKeyspaceMeta.State)
	if err != nil {
		return nil, err
	}

	keyspaceMetaPB = keyspacepb.KeyspaceMeta{
		Name:           tempKeyspaceMeta.Name,
		Id:             tempKeyspaceMeta.ID,
		Config:         tempKeyspaceMeta.Config,
		CreatedAt:      tempKeyspaceMeta.CreatedAt,
		StateChangedAt: tempKeyspaceMeta.StateChangedAt,
		State:          keyspaceState,
	}
	return &keyspaceMetaPB, nil
}

// GetGCSafePoint gets the GC safe point list.
func (c *client) GetGCSafePoint(ctx context.Context) (ListServiceGCSafepoint, error) {
	var gcSafePoint ListServiceGCSafepoint
	err := c.request(ctx, newRequestInfo().
		WithName(GetGCSafePointName).
		WithURI(safepoint).
		WithMethod(http.MethodGet).
		WithResp(&gcSafePoint))
	if err != nil {
		return gcSafePoint, err
	}
	return gcSafePoint, nil
}

// DeleteGCSafePoint deletes a GC safe point with the given service ID.
func (c *client) DeleteGCSafePoint(ctx context.Context, serviceID string) (string, error) {
	var msg string
	err := c.request(ctx, newRequestInfo().
		WithName(DeleteGCSafePointName).
		WithURI(GetDeleteSafePointURI(serviceID)).
		WithMethod(http.MethodDelete).
		WithResp(&msg))
	if err != nil {
		return msg, err
	}
	return msg, nil
}
