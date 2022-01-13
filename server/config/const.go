// Copyright 2022 TiKV Project Authors.
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

package config

const (
	// GetOperators is the logic service for HTTP API which url path is `/pd/api/v1/operators` and method is `GET`
	GetOperators = "GetOperators"
	// SetOperators is the logic service for HTTP API which url path is `/pd/api/v1/operators` and method is `POST`
	SetOperators = "SetOperators"
	// GetRegionOperator is the logic service for HTTP API which url path is `/pd/api/v1/operators/{region_id}` and method is `GET`
	GetRegionOperator = "GetRegionOperator"
	// DeleteRegionOperator is the logic service for HTTP API which url path is `DeleteRegionOperators` and method is `DELETE`
	DeleteRegionOperator = "DeleteRegionOperator"
	// SetChecker is the logic service for HTTP API which url path is `/pd/api/v1/checker/{name}` and method is `POST`
	SetChecker = "SetChecker"
	// GetChecker is the logic service for HTTP API which url path is `/pd/api/v1/checker/{name}` and method is `GET`
	GetChecker = "GetChecker"
	// GetSchedulers is the logic service for HTTP API which url path is `/pd/api/v1/schedulers` and method is `GET`
	GetSchedulers = "GetSchedulers"
	// AddScheduler is the logic service for HTTP API which url path is `/pd/api/v1/schedulers` and method is `POST`
	AddScheduler = "AddScheduler"
	// DeleteScheduler is the logic service for HTTP API which url path is `/pd/api/v1/schedulers/{name}` and method is `DELETE`
	DeleteScheduler = "DeleteScheduler"
	// PauseOrResumeScheduler is the logic service for HTTP API which url path is `/pd/api/v1/schedulers/{name}` and method is `POST`
	PauseOrResumeScheduler = "PauseOrResumeScheduler"
	// GetSchedulerConfig is the logic service for HTTP API which url path is `/pd/api/v1/scheduler-config `
	GetSchedulerConfig = "GetSchedulerConfig"
	// GetCluster is the logic service for HTTP API which url path is `/pd/api/v1/cluster` and method is `GET`
	GetCluster = "GetCluster"
	// GetClusterStatus is the logic service for HTTP API which url path is `/pd/api/v1/cluster/status` and method is `GET`
	GetClusterStatus = "GetClusterStatus"
	// GetConfig is the logic service for HTTP API which url path is `/pd/api/v1/config` and method is `GET`
	GetConfig = "GetConfig"
	// SetConfig is the logic service for HTTP API which url path is `/pd/api/v1/config` and method is `POST`
	SetConfig = "SetConfig"
	// GetDefaultConfig is the logic service for HTTP API which url path is `/pd/api/v1/config/default` and method is `GET`
	GetDefaultConfig = "GetDefaultConfig"
	// GetScheduleConfig is the logic service for HTTP API which url path is `/pd/api/v1/config/schedule` and method is `GET`
	GetScheduleConfig = "GetScheduleConfig"
	// is the logic service for HTTP API which url path is `/pd/api/v1/config/schedule` and method is `POST`
	SetScheduleConfig = "SetScheduleConfig"
	// GetPDServerConfig is the logic service for HTTP API which url path is `/pd/api/v1/config/pd-server` and method is `GET`
	GetPDServerConfig = "GetPDServerConfig"
	// GetReplicationConfig is the logic service for HTTP API which url path is `/pd/api/v1/config/replicate` and method is `GET`
	GetReplicationConfig = "GetReplicationConfig"
	// SetReplicationConfig is the logic service for HTTP API which url path is `/pd/api/v1/config/replicate` and method is `POST`
	SetReplicationConfig = "SetReplicationConfig"
	// GetLabelProperty is the logic service for HTTP API which url path is `/pd/api/v1/config/label-property` and method is `GET`
	GetLabelProperty = "GetLabelProperty"
	// SetLabelProperty is the logic service for HTTP API which url path is `/pd/api/v1/config/label-property` and method is `POST`
	SetLabelProperty = "SetLabelProperty"
	// GetClusterVersion is the logic service for HTTP API which url path is `/pd/api/v1/config/cluster-version` and method is `GET`
	GetClusterVersion = "GetClusterVersion"
	// SetClusterVersion is the logic service for HTTP API which url path is `/pd/api/v1/config/cluster-version` and method is `POST`
	SetClusterVersion = "SetClusterVersion"
	// GetReplicationMode is the logic service for HTTP API which url path is `/pd/api/v1/config/replication-mode` and method is `GET`
	GetReplicationMode = "GetReplicationMode"
	// SetReplicationMode is the logic service for HTTP API which url path is `/pd/api/v1/config/replication-mode` and method is `POST`
	SetReplicationMode = "SetReplicationMode"
	// GetAllRules is the logic service for HTTP API which url path is `/pd/api/v1/config/rules` and method is `GET`
	GetAllRules = "GetAllRules"
	// SetAllRules is the logic service for HTTP API which url path is `/pd/api/v1/config/rules` and method is `POST`
	SetAllRules = "SetAllRules"
	// SetBatchRules is the logic service for HTTP API which url path `/pd/api/v1/config/rules/batch` is and method is `POST`
	SetBatchRules = "SetBatchRules"
	// GetRuleByGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/rules/group/{group}` and method is `GET`
	GetRuleByGroup = "GetRuleByGroup"
	// GetRuleByByRegion is the logic service for HTTP API which url path is `/pd/api/v1/config/rules/region/{region} ` and method is `GET`
	GetRuleByByRegion = "GetRuleByByRegion"
	// GetRuleByKey is the logic service for HTTP API which url path is `/pd/api/v1/config/rules/key/{key} ` and method is `GET`
	GetRuleByKey = "GetRuleByKey"
	// GetRuleByGroupAndID is the logic service for HTTP API which url path is `/pd/api/v1/config/rule/{group}/{id}` and method is `GET`
	GetRuleByGroupAndID = "GetRuleByGroupAndID"
	// SetRule is the logic service for HTTP API which url path is `/pd/api/v1/config/rule` and method is `POST`
	SetRule = "SetRule"
	// DeleteRuleByGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/rule/{group}/{id}` and method is `DELETE`
	DeleteRuleByGroup = "DeleteRuleByGroup"
	// GetAllRegionLabelRule is the logic service for HTTP API which url path is `/pd/api/v1/config/rule/{group}/{id}` and method is `DELETE`
	GetAllRegionLabelRule = "GetAllRegionLabelRule"
	// GetRegionLabelRulesByIDs is the logic service for HTTP API which url path is `/pd/api/v1/config/region-label/rules` and method is `GET`
	GetRegionLabelRulesByIDs = "GetRegionLabelRulesByIDs"
	// GetRegionLabelRuleByID is the logic service for HTTP API which url path is `/pd/api/v1/config/region-label/rule/{id}`and method is `GET`
	GetRegionLabelRuleByID = "GetRegionLabelRuleByID"
	// DeleteRegionLabelRule is the logic service for HTTP API which url path is `/pd/api/v1/config/region-label/rule/{id}` and method is `DELETE`
	DeleteRegionLabelRule = "DeleteRegionLabelRule"
	// SetRegionLabelRule is the logic service for HTTP API which url path is `/pd/api/v1/config/region-label/rule` and method is `POST`
	SetRegionLabelRule = "SetRegionLabelRule"
	// PatchRegionLabelRules is the logic service for HTTP API which url path is `/pd/api/v1/config/region-label/rules` and method is `PATCH`
	PatchRegionLabelRules = "PatchRegionLabelRules"
	// GetRegionLabelByKey is the logic service for HTTP API which url path is `/pd/api/v1/region/id/{id}/label/{key}` and method is `GET`
	GetRegionLabelByKey = "GetRegionLabelByKey"
	// GetAllRegionLabels is the logic service for HTTP API which url path is `/pd/api/v1/region/id/{id}/labels ` and method is `GET`
	GetAllRegionLabels = "GetAllRegionLabels"
	// GetRuleGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/rule_group/{id}` and method is `GET`
	GetRuleGroup = "GetRuleGroup"
	// SetRuleGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/rule_group` and method is `POST`
	SetRuleGroup = "SetRuleGroup"
	// DeleteRuleGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/rule_group/{id}` and method is `DELETE`
	DeleteRuleGroup = "DeleteRuleGroup"
	// GetAllRuleGroups is the logic service for HTTP API which url path is `/pd/api/v1/config/rule_groups` and method is `GET`
	GetAllRuleGroups = "GetAllRuleGroups"
	// GetAllPlacementRules is the logic service for HTTP API which url path is `/pd/api/v1/config/placement-rule` and method is `GET`
	GetAllPlacementRules = "GetAllPlacementRules"
	// SetAllPlacementRules is the logic service for HTTP API which url path is `/pd/api/v1/config/placement-rule` and method is `POST`
	SetAllPlacementRules = "SetAllPlacementRules"
	// GetPlacementRuleByGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/placement-rule/{group}` and method is `GET`
	GetPlacementRuleByGroup = "GetPlacementRuleByGroup"
	// SetPlacementRuleByGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/placement-rule/{group}` and method is `POST`
	SetPlacementRuleByGroup = "SetPlacementRuleByGroup"
	// DeletePlacementRuleByGroup is the logic service for HTTP API which url path is `/pd/api/v1/config/placement-rule/{group}` and method is `DELETE`
	DeletePlacementRuleByGroup = "DeletePlacementRuleByGroup"
	// GetStore is the logic service for HTTP API which url path is `/pd/api/v1/store/{id}` and method is `GET`
	// and is also the logic service for `GetStore` gRPC interface
	GetStore = "GetStore"
	// DeleteStore is the logic service for HTTP API which url path is `/pd/api/v1/store/{id}` and method is `DELETE`
	DeleteStore = "DeleteStore"
	// SetStoreState is the logic service for HTTP API which url path is `/pd/api/v1/store/{id}/state` and method is `POST`
	SetStoreState = "SetStoreState"
	// SetStoreLabel is the logic service for HTTP API which url path is `/pd/api/v1/store/{id}/label` and method is `POST`
	SetStoreLabel = "SetStoreLabel"
	// SetStoreWeight is the logic service for HTTP API which url path is `/pd/api/v1/store/{id}/weight` and method is `POST`
	SetStoreWeight = "SetStoreWeight"
	// SetStoreLimit is the logic service for HTTP API which url path is `/pd/api/v1/store/{id}/limit` and method is `POST`
	SetStoreLimit = "SetStoreLimit"
	// GetAllStores is the logic service for HTTP API which url path is `/pd/api/v1/stores` and method is `GET`
	// and is also the logic service for `GetAllStores` gRPC interface
	GetAllStores = "GetAllStores"
	// RemoveTombstone is the logic service for HTTP API which url path is `/pd/api/v1/stores/remove-tombstone` and method is `DELETE`
	RemoveTombstone = "RemoveTombstone"
	// GetAllStoresLimit is the logic service for HTTP API which url path is `/pd/api/v1/stores/limit` and method is `GET`
	GetAllStoresLimit = "GetAllStoresLimit"
	// SetAllStoresLimit is the logic service for HTTP API which url path is `/pd/api/v1/stores/limit` and method is `POST`
	SetAllStoresLimit = "SetAllStoresLimit"
	// SetStoreSceneLimit is the logic service for HTTP API which url path is `/pd/api/v1/stores/limit/scene` and method is `POST`
	SetStoreSceneLimit = "SetStoreSceneLimit"
	// GetStoreSceneLimit is the logic service for HTTP API which url path is `/pd/api/v1/stores/limit/scene` and method is `GET`
	GetStoreSceneLimit = "GetStoreSceneLimit"
	// GetLabels is the logic service for HTTP API which url path is `/pd/api/v1/labels` and method is `GET`
	GetLabels = "GetLabels"
	// GetStoresByLabel is the logic service for HTTP API which url path is `/pd/api/v1/labels/stores` and method is `GET`
	GetStoresByLabel = "GetStoresByLabel"
	// GetHotspotWriteRegion is the logic service for HTTP API which url path is `/pd/api/v1/hotspot/regions/write` and method is `GET`
	GetHotspotWriteRegion = "GetHotspotWriteRegion"
	// GetHotspotReadRegion is the logic service for HTTP API which url path is `/pd/api/v1/hotspot/regions/read` and method is `GET`
	GetHotspotReadRegion = "GetHotspotReadRegion"
	// GetHotspotStores is the logic service for HTTP API which url path is `/pd/api/v1/hotspot/stores` and method is `GET`
	GetHotspotStores = "GetHotspotStores"
	// GetHistoryHotspotRegion is the logic service for HTTP API which url path is `/pd/api/v1/hotspot/regions/history` and method is `GET`
	GetHistoryHotspotRegion = "GetHistoryHotspotRegion"
	// GetRegionByID is the logic service for HTTP API which url path is `/pd/api/v1/region/id/{id}` and method is `GET`
	// and is also logic service for `GetRegionByID` gRPC interface
	GetRegionByID = "GetRegionByID"
	// GetRegionByKey is the logic service for HTTP API which url path is `/pd/api/v1/region/key/{key}` and method is `GET`
	// and is also logic service for `GetRegion` gRPC interface
	GetRegionByKey = "GetRegionByKey"
	// GetAllRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions` and method is `GET`
	GetAllRegions = "GetAllRegions"
	// ScanRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/key` and method is `GET`
	// and is also logic service for `ScanRegions` gRPC interface
	ScanRegions = "ScanRegions"
	// CountRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/count` and method is `GET`
	CountRegions = "CountRegions"
	// GetRegionsByStore is the logic service for HTTP API which url path is `/pd/api/v1/regions/store/{id}` and method is `GetRegionsByStore`
	GetRegionsByStore = "GetRegionsByStore"
	// GetTopWriteRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/writeflow` and method is `GET`
	TopWriteRegions = "TopWriteRegions"
	// GetTopReadRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/readflow` and method is `GET`
	TopReadRegions = "TopReadRegions"
	// GetTopConfverRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/confver` and method is `GET`
	GetTopConfverRegions = "GetTopConfverRegions"
	// GetTopVersionRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/version` and method is `GET`
	GetTopVersionRegions = "GetTopVersionRegions"
	// GetTopSizeRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/size` and method is `GET`
	GetTopSizeRegions = "GetTopSizeRegions"
	// GetMissPeerRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/miss-peer` and method is `GET`
	GetMissPeerRegions = "GetMissPeerRegions"
	// GetExtraPeerRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/extra-peer` and method is `GET`
	GetExtraPeerRegions = "GetExtraPeerRegions"
	// GetPendingPeerRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/pending-peer` and method is `GET`
	GetPendingPeerRegions = "GetPendingPeerRegions"
	// GetDownPeerRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/down-peer` and method is `GET`
	GetDownPeerRegions = "GetDownPeerRegions"
	// GetLearnerPeerRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/learner-peer` and method is `GET`
	GetLearnerPeerRegions = "GetLearnerPeerRegions"
	// GetEmptyRegion is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/empty-region` and method is `GET`
	GetEmptyRegion = "GetEmptyRegion"
	// GetOfflinePeer is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/offline-peer` and method is `GET`
	GetOfflinePeer = "GetOfflinePeer"
	// GetSizeHistogram is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/hist-size` and method is `GET`
	GetSizeHistogram = "GetSizeHistogram"
	// GetKeysHistogram is the logic service for HTTP API which url path is `/pd/api/v1/regions/check/hist-keys` and method is `GET`
	GetKeysHistogram = "GetKeysHistogram"
	// GetRegionSiblings is the logic service for HTTP API which url path is `/pd/api/v1/regions/sibling/{id}` and method is `GET`
	GetRegionSiblings = "GetRegionSiblings"
	// AccelerateRegionsSchedule is the logic service for HTTP API which url path is `/pd/api/v1/regions/accelerate-schedule` and method is `POST`
	AccelerateRegionsSchedule = "AccelerateRegionsSchedule"
	// ScatterRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/scatter` and method is `POST`
	ScatterRegions = "ScatterRegions"
	// SplitRegions is the logic service for HTTP API which url path is `/pd/api/v1/regions/split` and method is `POST`
	SplitRegions = "SplitRegions"
	// GetRangeHoles is the logic service for HTTP API which url path is `/pd/api/v1/regions/range-holes` and method is `GET`
	GetRangeHoles = "GetRangeHoles"
	// CheckRegionsReplicated is the logic service for HTTP API which url path is `/pd/api/v1/regions/replicated` and method is `GET`
	CheckRegionsReplicated = "CheckRegionsReplicated"
	// GetVersion is the logic service for HTTP API which url path is `/pd/api/v1/version` and method is `GET`
	GetVersion = "GetVersion"
	// GetPDStatus is the logic service for HTTP API which url path is `/pd/api/v1/status` and method is `GET`
	GetPDStatus = "GetPDStatus"
	// GetMembers is the logic service for HTTP API which url path is `/pd/api/v1/members` and method is `GET`
	// and is also the logic service for `GetMembers` gRPC interface
	GetMembers = "GetMembers"
	// DeleteMemberByName is the logic service for HTTP API which url path is `/pd/api/v1/members/name/{name}` and method is `DELETE`
	DeleteMemberByName = "DeleteMemberByName"
	// DeleteMemberByID is the logic service for HTTP API which url path is `/pd/api/v1/members/id/{id}` and method is `DELETE`
	DeleteMemberByID = "DeleteMemberByID"
	// SetMemberByName is the logic service for HTTP API which url path is `/pd/api/v1/members/name/{name}` and method is `POST`
	SetMemberByName = "SetMemberByName"
	// GetLeader is the logic service for HTTP API which url path is `/pd/api/v1/leader` and method is `GET`
	GetLeader = "GetLeader"
	// ResignLeader is the logic service for HTTP API which url path is `/pd/api/v1/leader/resign` and method is `POST`
	ResignLeader = "ResignLeader"
	// TransferLeaderis the logic service for HTTP API which url path is `/pd/api/v1/leader/transfer/{next_leader}` and method is `POST`
	TransferLeader = "TransferLeader"
	// GetRegionStatus is the logic service for HTTP API which url path is `/pd/api/v1/stats/region` and method is `GET`
	GetRegionStatus = "GetRegionStatus"
	// GetTrend is the logic service for HTTP API which url path is `/pd/api/v1/trend` and method is `GET`
	GetTrend = "GetTrend"
	// DeleteRegionCache is the logic service for HTTP API which url path is `/pd/api/v1/admin/cache/region/{id}` and method is `DELETE`
	DeleteRegionCache = "DeleteRegionCache"
	// ResetTS is the logic service for HTTP API which url path is `/pd/api/v1/admin/reset-ts` and method is `POST`
	ResetTS = "ResetTS"
	// SavePersistFile is the logic service for HTTP API which url path is `/pd/api/v1/admin/persist-file/{file_name}` and method is `POST`
	SavePersistFile = "SavePersistFile"
	// SetWaitAsyncTime is the logic service for HTTP API which url path is `/pd/api/v1/admin/replication_mode/wait-async` and method is `POST`
	SetWaitAsyncTime = "SetWaitAsyncTime"
	// SetLogLevel is the logic service for HTTP API which url path is `/pd/api/v1/admin/log` and method is `POST`
	SetLogLevel = "SetLogLevel"
	// GetReplicationModeStatus is the logic service for HTTP API which url path is `/pd/api/v1/replication_mode/status`
	GetReplicationModeStatus = "GetReplicationModeStatus"
	// SetPlugin is the logic service for HTTP API which url path is `/pd/api/v1/plugin` and method is `POST`
	SetPlugin = "SetPlugin"
	// DeletePlugin is the logic service for HTTP API which url path is `/pd/api/v1/plugin` and method is `DELETE`
	DeletePlugin = "DeletePlugin"
	// GetHealthStatus is the logic service for HTTP API which url path is `/pd/api/v1/health` and method is `GET`
	GetHealthStatus = "GetHealthStatus"
	// GetDiagnoseInfo is the logic service for HTTP API which url path is `/pd/api/v1/diagnose` and method is `GET`
	GetDiagnoseInfo = "GetDiagnoseInfo"
	// Ping is the logic service for HTTP API which url path is `/pd/api/v1/ping` and method is `GET`
	Ping = "Ping"
	// QueryMetric is the logic service for HTTP APIs which url paths are `/pd/api/v1/metric/query` and `/pd/api/v1/metric/query_range`  and method is in (`POST`,"GET")
	QueryMetric = "QueryMetric"
	// TransferLocalTSOAllocator is the logic service for HTTP API which url path is `/pd/api/v1/tso/allocator/transfer/{name} ` and method is `POST`
	TransferLocalTSOAllocator = "TransferLocalTSOAllocator"
	// DebugPProfProfile is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/profile`
	DebugPProfProfile = "DebugPProfProfile"
	// DebugPProfTrace is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/trace`
	DebugPProfTrace = "DebugPProfTrace"
	// DebugPProfSymbol is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/symbol`
	DebugPProfSymbol = "DebugPProfSymbol"
	// DebugPProfHeap is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/heap`
	DebugPProfHeap = "DebugPProfHeap"
	// DebugPProfMutex is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/mutex`
	DebugPProfMutex = "DebugPProfMutex"
	// DebugPProfAllocs is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/allocs`
	DebugPProfAllocs = "DebugPProfAllocs"
	// DebugPProfBlock is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/block`
	DebugPProfBlock = "DebugPProfBlock"
	// DebugPProfGoroutine is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/goroutine`
	DebugPProfGoroutine = "DebugPProfGoroutine"
	// DebugPProfThreadCreate is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/threadcreate`
	DebugPProfThreadCreate = "DebugPProfThreadCreate"
	// DebugPProfZip is the logic service for HTTP API which url path is `/pd/api/v1/debug/pprof/zip`
	DebugPProfZip = "DebugPProfZip"
	// GetGCSafePoint is the logic service for HTTP API which url path is `/pd/api/v1/gc/safepoint` and method is `GET`
	GetGCSafePoint = "GetGCSafePoint"
	// DeleteGCSafePoint is the logic service for HTTP API which url path is `/pd/api/v1/gc/safepoint/{service_id}` and method is `DELETE`
	DeleteGCSafePoint = "DeleteGCSafePoint"
	// RemoveFailedStoresUnsafely is the logic service for HTTP API which url path is `/pd/api/v1/admin/unsafe/remove-failed-stores` and method is `POST`
	RemoveFailedStoresUnsafely = "RemoveFailedStoresUnsafely"
	// GetOngoingFailedStoresRemoval is the logic service for HTTP API which url path is `/pd/api/v1/admin/unsafe/remove-failed-stores/show` and method is `GET`
	GetOngoingFailedStoresRemoval = "GetOngoingFailedStoresRemoval"
	// GetHistoryFailedStoresRemoval is the logic service for HTTP API which url path is `/pd/api/v1/admin/unsafe/remove-failed-stores/history` and method is `GET`
	GetHistoryFailedStoresRemoval = "GetHistoryFailedStoresRemoval"
)

// HTTPRegisteredSericeLabel is the service labels and http api mapping data
var HTTPRegisteredSericeLabel map[string]string = map[string]string{
	"/pd/api/v1/operators/GET":                                 GetOperators,
	"/pd/api/v1/operators/POST":                                SetOperators,
	"/pd/api/v1/operators//GET":                                GetRegionOperator,
	"/pd/api/v1/operators//DELETE":                             DeleteRegionOperator,
	"/pd/api/v1/checker//POST":                                 SetChecker,
	"/pd/api/v1/checker//GET":                                  GetChecker,
	"/pd/api/v1/schedulers/GET":                                GetSchedulers,
	"/pd/api/v1/schedulers/POST":                               AddScheduler,
	"/pd/api/v1/schedulers//DELETE":                            DeleteScheduler,
	"/pd/api/v1/schedulers//POST":                              PauseOrResumeScheduler,
	"/pd/api/v1/scheduler-config/":                             GetSchedulerConfig,
	"/pd/api/v1/cluster/GET":                                   GetCluster,
	"/pd/api/v1/cluster/status/GET":                            GetClusterStatus,
	"/pd/api/v1/config/GET":                                    GetConfig,
	"/pd/api/v1/config/POST":                                   SetConfig,
	"/pd/api/v1/config/default/GET":                            GetDefaultConfig,
	"/pd/api/v1/config/schedule/GET":                           GetScheduleConfig,
	"/pd/api/v1/config/schedule/POST":                          SetScheduleConfig,
	"/pd/api/v1/config/pd-server/GET":                          GetPDServerConfig,
	"/pd/api/v1/config/replicate/GET":                          GetReplicationConfig,
	"/pd/api/v1/config/replicate/POST":                         SetReplicationConfig,
	"/pd/api/v1/config/label-property/GET":                     GetLabelProperty,
	"/pd/api/v1/config/label-property/POST":                    SetLabelProperty,
	"/pd/api/v1/config/cluster-version/GET":                    GetClusterVersion,
	"/pd/api/v1/config/cluster-version/POST":                   SetClusterVersion,
	"/pd/api/v1/config/replication-mode/GET":                   GetReplicationMode,
	"/pd/api/v1/config/replication-mode/POST":                  SetReplicationMode,
	"/pd/api/v1/config/rules/GET":                              GetAllRules,
	"/pd/api/v1/config/rules/POST":                             SetAllRules,
	"/pd/api/v1/config/rules/batch/POST":                       SetBatchRules,
	"/pd/api/v1/config/rules/group//GET":                       GetRuleByGroup,
	"/pd/api/v1/config/rules/region//GET":                      GetRuleByByRegion,
	"/pd/api/v1/config/rules/key//GET":                         GetRuleByKey,
	"/pd/api/v1/config/rule///GET":                             GetRuleByGroupAndID,
	"/pd/api/v1/config/rule/POST":                              SetRule,
	"/pd/api/v1/config/rule///DELETE":                          DeleteRuleByGroup,
	"/pd/api/v1/config/region-label/rules/GET":                 GetAllRegionLabelRule,
	"/pd/api/v1/config/region-label/rules/ids/GET":             GetRegionLabelRulesByIDs,
	"/pd/api/v1/config/region-label/rule//GET":                 GetRegionLabelRuleByID,
	"/pd/api/v1/config/region-label/rule//DELETE":              DeleteRegionLabelRule,
	"/pd/api/v1/config/region-label/rule/POST":                 SetRegionLabelRule,
	"/pd/api/v1/config/region-label/rules/PATCH":               PatchRegionLabelRules,
	"/pd/api/v1/region/id//label//GET":                         GetRegionLabelByKey,
	"/pd/api/v1/region/id//labels/GET":                         GetAllRegionLabels,
	"/pd/api/v1/config/rule_group//GET":                        GetRuleGroup,
	"/pd/api/v1/config/rule_group/POST":                        SetRuleGroup,
	"/pd/api/v1/config/rule_group//DELETE":                     DeleteRuleGroup,
	"/pd/api/v1/config/rule_groups/GET":                        GetAllRuleGroups,
	"/pd/api/v1/config/placement-rule/GET":                     GetAllPlacementRules,
	"/pd/api/v1/config/placement-rule/POST":                    SetAllPlacementRules,
	"/pd/api/v1/config/placement-rule//GET":                    GetPlacementRuleByGroup,
	"/pd/api/v1/config/placement-rule//POST":                   SetPlacementRuleByGroup,
	"/pd/api/v1/config/placement-rule//DELETE":                 DeletePlacementRuleByGroup,
	"/pd/api/v1/store//GET":                                    GetStore,
	"/pd/api/v1/store//DELETE":                                 DeleteStore,
	"/pd/api/v1/store//state/POST":                             SetStoreState,
	"/pd/api/v1/store//label/POST":                             SetStoreLabel,
	"/pd/api/v1/store//weight/POST":                            SetStoreWeight,
	"/pd/api/v1/store//limit/POST":                             SetStoreLimit,
	"/pd/api/v1/stores/GET":                                    GetAllStores,
	"/pd/api/v1/stores/remove-tombstone/DELETE":                RemoveTombstone,
	"/pd/api/v1/stores/limit/GET":                              GetAllStoresLimit,
	"/pd/api/v1/stores/limit/POST":                             SetAllStoresLimit,
	"/pd/api/v1/stores/limit/scene/POST":                       SetStoreSceneLimit,
	"/pd/api/v1/stores/limit/scene/GET":                        GetStoreSceneLimit,
	"/pd/api/v1/labels/GET":                                    GetLabels,
	"/pd/api/v1/labels/stores/GET":                             GetStoresByLabel,
	"/pd/api/v1/hotspot/regions/write/GET":                     GetHotspotWriteRegion,
	"/pd/api/v1/hotspot/regions/read/GET":                      GetHotspotReadRegion,
	"/pd/api/v1/hotspot/stores/GET":                            GetHotspotStores,
	"/pd/api/v1/hotspot/regions/history/GET":                   GetHistoryHotspotRegion,
	"/pd/api/v1/region/id//GET":                                GetRegionByID,
	"/pd/api/v1/region/key//GET":                               GetRegionByKey,
	"/pd/api/v1/regions/GET":                                   GetAllRegions,
	"/pd/api/v1/regions/key/GET":                               ScanRegions,
	"/pd/api/v1/regions/count/GET":                             CountRegions,
	"/pd/api/v1/regions/store//GET":                            GetRegionsByStore,
	"/pd/api/v1/regions/writeflow/GET":                         TopWriteRegions,
	"/pd/api/v1/regions/readflow/GET":                          TopReadRegions,
	"/pd/api/v1/regions/confver/GET":                           GetTopConfverRegions,
	"/pd/api/v1/regions/version/GET":                           GetTopVersionRegions,
	"/pd/api/v1/regions/size/GET":                              GetTopSizeRegions,
	"/pd/api/v1/regions/check/miss-peer/GET":                   GetMissPeerRegions,
	"/pd/api/v1/regions/check/extra-peer/GET":                  GetExtraPeerRegions,
	"/pd/api/v1/regions/check/pending-peer/GET":                GetPendingPeerRegions,
	"/pd/api/v1/regions/check/down-peer/GET":                   GetDownPeerRegions,
	"/pd/api/v1/regions/check/learner-peer/GET":                GetLearnerPeerRegions,
	"/pd/api/v1/regions/check/empty-region/GET":                GetEmptyRegion,
	"/pd/api/v1/regions/check/offline-peer/GET":                GetOfflinePeer,
	"/pd/api/v1/regions/check/hist-size/GET":                   GetSizeHistogram,
	"/pd/api/v1/regions/check/hist-keys/GET":                   GetKeysHistogram,
	"/pd/api/v1/regions/sibling//GET":                          GetRegionSiblings,
	"/pd/api/v1/regions/accelerate-schedule/POST":              AccelerateRegionsSchedule,
	"/pd/api/v1/regions/scatter/POST":                          ScatterRegions,
	"/pd/api/v1/regions/split/POST":                            SplitRegions,
	"/pd/api/v1/regions/range-holes/GET":                       GetRangeHoles,
	"/pd/api/v1/regions/replicated/GET":                        CheckRegionsReplicated,
	"/pd/api/v1/version/GET":                                   GetVersion,
	"/pd/api/v1/status/GET":                                    GetPDStatus,
	"/pd/api/v1/members/GET":                                   GetMembers,
	"/pd/api/v1/members/name//DELETE":                          DeleteMemberByName,
	"/pd/api/v1/members/id//DELETE":                            DeleteMemberByID,
	"/pd/api/v1/members/name//POST":                            SetMemberByName,
	"/pd/api/v1/leader/GET":                                    GetLeader,
	"/pd/api/v1/leader/resign/POST":                            ResignLeader,
	"/pd/api/v1/leader/transfer//POST":                         TransferLeader,
	"/pd/api/v1/stats/region/GET":                              GetRegionStatus,
	"/pd/api/v1/trend/GET":                                     GetTrend,
	"/pd/api/v1/admin/cache/region//DELETE":                    DeleteRegionCache,
	"/pd/api/v1/admin/reset-ts/POST":                           ResetTS,
	"/pd/api/v1/admin/persist-file//POST":                      SavePersistFile,
	"/pd/api/v1/admin/replication_mode/wait-async/POST":        SetWaitAsyncTime,
	"/pd/api/v1/admin/log/POST":                                SetLogLevel,
	"/pd/api/v1/replication_mode/status/":                      GetReplicationModeStatus,
	"/pd/api/v1/plugin/POST":                                   SetPlugin,
	"/pd/api/v1/plugin/DELETE":                                 DeletePlugin,
	"/pd/api/v1/health/GET":                                    GetHealthStatus,
	"/pd/api/v1/diagnose/GET":                                  GetDiagnoseInfo,
	"/pd/api/v1/ping/GET":                                      Ping,
	"/pd/api/v1/metric/query/GET":                              QueryMetric,
	"/pd/api/v1/metric/query/POST":                             QueryMetric,
	"/pd/api/v1/metric/query_range/GET":                        QueryMetric,
	"/pd/api/v1/metric/query_range/POST":                       QueryMetric,
	"/pd/api/v1/tso/allocator/transfer//POST":                  TransferLocalTSOAllocator,
	"/pd/api/v1/debug/pprof/profile/":                          DebugPProfProfile,
	"/pd/api/v1/debug/pprof/trace/":                            DebugPProfTrace,
	"/pd/api/v1/debug/pprof/symbol/":                           DebugPProfSymbol,
	"/pd/api/v1/debug/pprof/heap/":                             DebugPProfHeap,
	"/pd/api/v1/debug/pprof/mutex/":                            DebugPProfMutex,
	"/pd/api/v1/debug/pprof/allocs/":                           DebugPProfAllocs,
	"/pd/api/v1/debug/pprof/block/":                            DebugPProfBlock,
	"/pd/api/v1/debug/pprof/goroutine/":                        DebugPProfGoroutine,
	"/pd/api/v1/debug/pprof/threadcreate/":                     DebugPProfThreadCreate,
	"/pd/api/v1/debug/pprof/zip/":                              DebugPProfZip,
	"/pd/api/v1/gc/safepoint/GET":                              GetGCSafePoint,
	"/pd/api/v1/gc/safepoint//DELETE":                          DeleteGCSafePoint,
	"/pd/api/v1/admin/unsafe/remove-failed-stores/POST":        RemoveFailedStoresUnsafely,
	"/pd/api/v1/admin/unsafe/remove-failed-stores/show/GET":    GetOngoingFailedStoresRemoval,
	"/pd/api/v1/admin/unsafe/remove-failed-stores/history/GET": GetHistoryFailedStoresRemoval,
}
