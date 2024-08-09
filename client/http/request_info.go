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

	"github.com/tikv/pd/client/retry"
	"go.uber.org/zap"
)

// The following constants are the names of the requests.
const (
	getMembersName                          = "GetMembers"
	getLeaderName                           = "GetLeader"
	transferLeaderName                      = "TransferLeader"
	getRegionByIDName                       = "GetRegionByID"
	getRegionByKeyName                      = "GetRegionByKey"
	getRegionsName                          = "GetRegions"
	getRegionsByKeyRangeName                = "GetRegionsByKeyRange"
	getRegionsByStoreIDName                 = "GetRegionsByStoreID"
	getEmptyRegionsName                     = "GetEmptyRegions"
	getRegionsReplicatedStateByKeyRangeName = "GetRegionsReplicatedStateByKeyRange"
	getHotReadRegionsName                   = "GetHotReadRegions"
	getHotWriteRegionsName                  = "GetHotWriteRegions"
	getHistoryHotRegionsName                = "GetHistoryHotRegions"
	getRegionStatusByKeyRangeName           = "GetRegionStatusByKeyRange"
	getStoresName                           = "GetStores"
	getStoreName                            = "GetStore"
	deleteStoreName                         = "DeleteStore"
	setStoreLabelsName                      = "SetStoreLabels"
	deleteStoreLabelName                    = "DeleteStoreLabel"
	getHealthStatusName                     = "GetHealthStatus"
	getConfigName                           = "GetConfig"
	setConfigName                           = "SetConfig"
	getScheduleConfigName                   = "GetScheduleConfig"
	setScheduleConfigName                   = "SetScheduleConfig"
	getClusterVersionName                   = "GetClusterVersion"
	getClusterName                          = "GetCluster"
	getClusterStatusName                    = "GetClusterStatus"
	getStatusName                           = "GetStatus"
	getReplicateConfigName                  = "GetReplicateConfig"
	getSchedulersName                       = "GetSchedulers"
	createSchedulerName                     = "CreateScheduler"
	deleteSchedulerName                     = "DeleteScheduler"
	setSchedulerDelayName                   = "SetSchedulerDelay"
	getAllPlacementRuleBundlesName          = "GetAllPlacementRuleBundles"
	getPlacementRuleBundleByGroupName       = "GetPlacementRuleBundleByGroup"
	getPlacementRulesByGroupName            = "GetPlacementRulesByGroup"
	getPlacementRuleName                    = "GetPlacementRule"
	setPlacementRuleName                    = "SetPlacementRule"
	setPlacementRuleInBatchName             = "SetPlacementRuleInBatch"
	setPlacementRuleBundlesName             = "SetPlacementRuleBundles"
	deletePlacementRuleName                 = "DeletePlacementRule"
	getAllPlacementRuleGroupsName           = "GetAllPlacementRuleGroups"
	getPlacementRuleGroupByIDName           = "GetPlacementRuleGroupByID"
	setPlacementRuleGroupName               = "SetPlacementRuleGroup"
	deletePlacementRuleGroupByIDName        = "DeletePlacementRuleGroupByID"
	getAllRegionLabelRulesName              = "GetAllRegionLabelRules"
	getRegionLabelRulesByIDsName            = "GetRegionLabelRulesByIDs"
	setRegionLabelRuleName                  = "SetRegionLabelRule"
	patchRegionLabelRulesName               = "PatchRegionLabelRules"
	accelerateScheduleName                  = "AccelerateSchedule"
	accelerateScheduleInBatchName           = "AccelerateScheduleInBatch"
	getMinResolvedTSByStoresIDsName         = "GetMinResolvedTSByStoresIDs"
	getMicroServiceMembersName              = "GetMicroServiceMembers"
	getMicroServicePrimaryName              = "GetMicroServicePrimary"
	getPDVersionName                        = "GetPDVersion"
	resetTSName                             = "ResetTS"
	resetBaseAllocIDName                    = "ResetBaseAllocID"
	setSnapshotRecoveringMarkName           = "SetSnapshotRecoveringMark"
	deleteSnapshotRecoveringMarkName        = "DeleteSnapshotRecoveringMark"
	deleteOperators                         = "DeleteOperators"
	UpdateKeyspaceGCManagementTypeName      = "UpdateKeyspaceGCManagementType"
	GetKeyspaceMetaByNameName               = "GetKeyspaceMetaByName"
	GetGCSafePointName                      = "GetGCSafePoint"
	DeleteGCSafePointName                   = "DeleteGCSafePoint"
)

type requestInfo struct {
	callerID    string
	name        string
	uri         string
	method      string
	body        []byte
	res         any
	respHandler respHandleFunc
	bo          *retry.Backoffer
	targetURL   string
}

// newRequestInfo creates a new request info.
func newRequestInfo() *requestInfo {
	return &requestInfo{}
}

// WithCallerID sets the caller ID of the request.
func (ri *requestInfo) WithCallerID(callerID string) *requestInfo {
	ri.callerID = callerID
	return ri
}

// WithName sets the name of the request.
func (ri *requestInfo) WithName(name string) *requestInfo {
	ri.name = name
	return ri
}

// WithURI sets the URI of the request.
func (ri *requestInfo) WithURI(uri string) *requestInfo {
	ri.uri = uri
	return ri
}

// WithMethod sets the method of the request.
func (ri *requestInfo) WithMethod(method string) *requestInfo {
	ri.method = method
	return ri
}

// WithBody sets the body of the request.
func (ri *requestInfo) WithBody(body []byte) *requestInfo {
	ri.body = body
	return ri
}

// WithResp sets the response struct of the request.
func (ri *requestInfo) WithResp(res any) *requestInfo {
	ri.res = res
	return ri
}

// WithRespHandler sets the response handle function of the request.
func (ri *requestInfo) WithRespHandler(respHandler respHandleFunc) *requestInfo {
	ri.respHandler = respHandler
	return ri
}

// WithBackoffer sets the backoffer of the request.
func (ri *requestInfo) WithBackoffer(bo *retry.Backoffer) *requestInfo {
	ri.bo = bo
	return ri
}

// WithTargetURL sets the target URL of the request.
func (ri *requestInfo) WithTargetURL(targetURL string) *requestInfo {
	ri.targetURL = targetURL
	return ri
}

func (ri *requestInfo) getURL(addr string) string {
	return fmt.Sprintf("%s%s", addr, ri.uri)
}

func (ri *requestInfo) logFields() []zap.Field {
	return []zap.Field{
		zap.String("caller-id", ri.callerID),
		zap.String("name", ri.name),
		zap.String("uri", ri.uri),
		zap.String("method", ri.method),
		zap.String("target-url", ri.targetURL),
	}
}
