// Copyright 2016 TiKV Project Authors.
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

package api

import (
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

// createRouteOption is used to register service for mux.Route
type createRouteOption func(route *mux.Route)

// setMethods is used to add HTTP Method matcher for mux.Route
func setMethods(method ...string) createRouteOption {
	return func(route *mux.Route) {
		route.Methods(method...)
	}
}

// setQueries is used to add queries for mux.Route
func setQueries(pairs ...string) createRouteOption {
	return func(route *mux.Route) {
		route.Queries(pairs...)
	}
}

var (
	getMethod        = setMethods("GET")
	postMethod       = setMethods("POST")
	getAndPostMethod = setMethods("GET", "POST")
	deleteMethod     = setMethods("DELETE")
	patchMethod      = setMethods("PATCH")
)

func createStreamingRender() *render.Render {
	return render.New(render.Options{
		StreamingJSON: true,
	})
}

func createIndentRender() *render.Render {
	return render.New(render.Options{
		IndentJSON: true,
	})
}

// The returned function is used as a lazy router to avoid the data race problem.
// @title Placement Driver Core API
// @version 1.0
// @description This is placement driver.
// @contact.name Placement Driver Support
// @contact.url https://github.com/tikv/pd/issues
// @contact.email info@pingcap.com
// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath /pd/api/v1
func createRouter(prefix string, svr *server.Server) *mux.Router {
	rd := createIndentRender()

	rootRouter := mux.NewRouter().PathPrefix(prefix).Subrouter()
	handler := svr.GetHandler()

	apiPrefix := "/api/v1"
	apiRouter := rootRouter.PathPrefix(apiPrefix).Subrouter()

	clusterRouter := apiRouter.NewRoute().Subrouter()
	clusterRouter.Use(newClusterMiddleware(svr).Middleware)

	escapeRouter := clusterRouter.NewRoute().Subrouter().UseEncodedPath()

	serviceBuilder := newServiceMiddlewareBuilder(svr)
	register := serviceBuilder.registerRouteHandler
	registerPrefix := serviceBuilder.registerPathPrefixRouteHandler
	registerFunc := serviceBuilder.registerRouteHandleFunc

	operatorHandler := newOperatorHandler(handler, rd)
	registerFunc(apiRouter, "GetOperators", "/operators", operatorHandler.List, getMethod)
	registerFunc(apiRouter, "SetOperators", "/operators", operatorHandler.Post, postMethod)
	registerFunc(apiRouter, "GetRegionOperator", "/operators/{region_id}", operatorHandler.Get, getMethod)
	registerFunc(apiRouter, "DeleteRegionOperator", "/operators/{region_id}", operatorHandler.Delete, deleteMethod)

	checkerHandler := newCheckerHandler(svr, rd)
	registerFunc(apiRouter, "SetChecker", "/checker/{name}", checkerHandler.PauseOrResume, postMethod)
	registerFunc(apiRouter, "GetChecker", "/checker/{name}", checkerHandler.GetStatus, getMethod)

	schedulerHandler := newSchedulerHandler(svr, rd)
	registerFunc(apiRouter, "GetSchedulers", "/schedulers", schedulerHandler.List, getMethod)
	registerFunc(apiRouter, "AddScheduler", "/schedulers", schedulerHandler.Post, postMethod)
	registerFunc(apiRouter, "DeleteScheduler", "/schedulers/{name}", schedulerHandler.Delete, deleteMethod)
	registerFunc(apiRouter, "PauseOrResumeScheduler", "/schedulers/{name}", schedulerHandler.PauseOrResume, postMethod)

	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	registerPrefix(apiRouter, "GetSchedulerConfig", "/scheduler-config", schedulerConfigHandler)

	clusterHandler := newClusterHandler(svr, rd)
	register(apiRouter, "GetCluster", "/cluster", clusterHandler, getMethod)
	registerFunc(apiRouter, "GetClusterStatus", "/cluster/status", clusterHandler.GetClusterStatus)

	confHandler := newConfHandler(svr, rd)
	registerFunc(apiRouter, "GetConfig", "/config", confHandler.Get, getMethod)
	registerFunc(apiRouter, "SetConfig", "/config", confHandler.Post, postMethod)
	registerFunc(apiRouter, "GetDefaultConfig", "/config/default", confHandler.GetDefault, getMethod)
	registerFunc(apiRouter, "GetScheduleConfig", "/config/schedule", confHandler.GetSchedule, getMethod)
	registerFunc(apiRouter, "SetScheduleConfig", "/config/schedule", confHandler.SetSchedule, postMethod)
	registerFunc(apiRouter, "GetPDServerConfig", "/config/pd-server", confHandler.GetPDServer, getMethod)
	registerFunc(apiRouter, "GetReplicationConfig", "/config/replicate", confHandler.GetReplication, getMethod)
	registerFunc(apiRouter, "SetReplicationConfig", "/config/replicate", confHandler.SetReplication, postMethod)
	registerFunc(apiRouter, "GetLabelProperty", "/config/label-property", confHandler.GetLabelProperty, getMethod)
	registerFunc(apiRouter, "SetLabelProperty", "/config/label-property", confHandler.SetLabelProperty, postMethod)
	registerFunc(apiRouter, "GetClusterVersion", "/config/cluster-version", confHandler.GetClusterVersion, getMethod)
	registerFunc(apiRouter, "SetClusterVersion", "/config/cluster-version", confHandler.SetClusterVersion, postMethod)
	registerFunc(apiRouter, "GetReplicationMode", "/config/replication-mode", confHandler.GetReplicationMode, getMethod)
	registerFunc(apiRouter, "SetReplicationMode", "/config/replication-mode", confHandler.SetReplicationMode, postMethod)

	rulesHandler := newRulesHandler(svr, rd)
	registerFunc(clusterRouter, "GetAllRules", "/config/rules", rulesHandler.GetAll, getMethod)
	registerFunc(clusterRouter, "SetAllRules", "/config/rules", rulesHandler.SetAll, postMethod)
	registerFunc(clusterRouter, "SetBatchRules", "/config/rules/batch", rulesHandler.Batch, postMethod)
	registerFunc(clusterRouter, "GetRuleByGroup", "/config/rules/group/{group}", rulesHandler.GetAllByGroup, getMethod)
	registerFunc(clusterRouter, "GetRuleByByRegion", "/config/rules/region/{region}", rulesHandler.GetAllByRegion, getMethod)
	registerFunc(clusterRouter, "GetRuleByKey", "/config/rules/key/{key}", rulesHandler.GetAllByKey, getMethod)
	registerFunc(clusterRouter, "GetRuleByGroupAndID", "/config/rule/{group}/{id}", rulesHandler.Get, getMethod)
	registerFunc(clusterRouter, "SetRule", "/config/rule", rulesHandler.Set, postMethod)
	registerFunc(clusterRouter, "DeleteRuleByGroup", "/config/rule/{group}/{id}", rulesHandler.Delete, deleteMethod)

	regionLabelHandler := newRegionLabelHandler(svr, rd)
	registerFunc(clusterRouter, "GetAllRegionLabelRule", "/config/region-label/rules", regionLabelHandler.GetAllRules, getMethod)
	registerFunc(clusterRouter, "GetRegionLabelRulesByIDs", "/config/region-label/rules/ids", regionLabelHandler.GetRulesByIDs, getMethod)
	// {id} can be a string with special characters, we should enable path encode to support it.
	registerFunc(escapeRouter, "GetRegionLabelRuleByID", "/config/region-label/rule/{id}", regionLabelHandler.GetRule, getMethod)
	registerFunc(escapeRouter, "DeleteRegionLabelRule", "/config/region-label/rule/{id}", regionLabelHandler.DeleteRule, deleteMethod)
	registerFunc(clusterRouter, "SetRegionLabelRule", "/config/region-label/rule", regionLabelHandler.SetRule, postMethod)
	registerFunc(clusterRouter, "PatchRegionLabelRules", "/config/region-label/rules", regionLabelHandler.Patch, patchMethod)

	registerFunc(clusterRouter, "GetRegionLabelByKey", "/region/id/{id}/label/{key}", regionLabelHandler.GetRegionLabel, getMethod)
	registerFunc(clusterRouter, "GetAllRegionLabels", "/region/id/{id}/labels", regionLabelHandler.GetRegionLabels, getMethod)

	registerFunc(clusterRouter, "GetRuleGroup", "/config/rule_group/{id}", rulesHandler.GetGroupConfig, getMethod)
	registerFunc(clusterRouter, "SetRuleGroup", "/config/rule_group", rulesHandler.SetGroupConfig, postMethod)
	registerFunc(clusterRouter, "DeleteRuleGroup", "/config/rule_group/{id}", rulesHandler.DeleteGroupConfig, deleteMethod)
	registerFunc(clusterRouter, "GetAllRuleGroups", "/config/rule_groups", rulesHandler.GetAllGroupConfigs, getMethod)

	registerFunc(clusterRouter, "GetAllPlacementRules", "/config/placement-rule", rulesHandler.GetAllGroupBundles, getMethod)
	registerFunc(clusterRouter, "SetAllPlacementRules", "/config/placement-rule", rulesHandler.SetAllGroupBundles, postMethod)
	// {group} can be a regular expression, we should enable path encode to
	// support special characters.
	registerFunc(clusterRouter, "GetPlacementRuleByGroup", "/config/placement-rule/{group}", rulesHandler.GetGroupBundle, getMethod)
	registerFunc(clusterRouter, "SetPlacementRuleByGroup", "/config/placement-rule/{group}", rulesHandler.SetGroupBundle, postMethod)
	registerFunc(escapeRouter, "DeletePlacementRuleByGroup", "/config/placement-rule/{group}", rulesHandler.DeleteGroupBundle, deleteMethod)

	storeHandler := newStoreHandler(handler, rd)
	registerFunc(clusterRouter, "GetStore", "/store/{id}", storeHandler.Get, getMethod)
	registerFunc(clusterRouter, "DeleteStore", "/store/{id}", storeHandler.Delete, deleteMethod)
	registerFunc(clusterRouter, "SetStoreState", "/store/{id}/state", storeHandler.SetState, postMethod)
	registerFunc(clusterRouter, "SetStoreLabel", "/store/{id}/label", storeHandler.SetLabels, postMethod)
	registerFunc(clusterRouter, "SetStoreWeight", "/store/{id}/weight", storeHandler.SetWeight, postMethod)
	registerFunc(clusterRouter, "SetStoreLimit", "/store/{id}/limit", storeHandler.SetLimit, postMethod)

	storesHandler := newStoresHandler(handler, rd)
	register(clusterRouter, "GetAllStores", "/stores", storesHandler, getMethod)
	registerFunc(clusterRouter, "RemoveTombstone", "/stores/remove-tombstone", storesHandler.RemoveTombStone, deleteMethod)
	registerFunc(clusterRouter, "GetAllStoresLimit", "/stores/limit", storesHandler.GetAllLimit, getMethod)
	registerFunc(clusterRouter, "SetAllStoresLimit", "/stores/limit", storesHandler.SetAllLimit, postMethod)
	registerFunc(clusterRouter, "SetStoreSceneLimit", "/stores/limit/scene", storesHandler.SetStoreLimitScene, postMethod)
	registerFunc(clusterRouter, "GetStoreSceneLimit", "/stores/limit/scene", storesHandler.GetStoreLimitScene, getMethod)

	labelsHandler := newLabelsHandler(svr, rd)
	registerFunc(clusterRouter, "GetLabels", "/labels", labelsHandler.Get, getMethod)
	registerFunc(clusterRouter, "GetStoresByLabel", "/labels/stores", labelsHandler.GetStores, getMethod)

	hotStatusHandler := newHotStatusHandler(handler, rd)
	registerFunc(apiRouter, "GetHotspotWriteRegion", "/hotspot/regions/write", hotStatusHandler.GetHotWriteRegions, getMethod)
	registerFunc(apiRouter, "GetHotspotReadRegion", "/hotspot/regions/read", hotStatusHandler.GetHotReadRegions, getMethod)
	registerFunc(apiRouter, "GetHotspotStores", "/hotspot/regions/history", hotStatusHandler.GetHistoryHotRegions, getMethod)
	registerFunc(apiRouter, "GetHistoryHotspotRegion", "/hotspot/stores", hotStatusHandler.GetHotStores, getMethod)

	regionHandler := newRegionHandler(svr, rd)
	registerFunc(clusterRouter, "GetRegionByID", "/region/id/{id}", regionHandler.GetRegionByID, getMethod)
	registerFunc(clusterRouter.UseEncodedPath(), "GetRegion", "/region/key/{key}", regionHandler.GetRegionByKey, getMethod)

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	registerFunc(clusterRouter, "GetAllRegions", "/regions", regionsAllHandler.GetAll, getMethod)

	regionsHandler := newRegionsHandler(svr, rd)
	registerFunc(clusterRouter, "ScanRegions", "/regions/key", regionsHandler.ScanRegions, getMethod)
	registerFunc(clusterRouter, "CountRegions", "/regions/count", regionsHandler.GetRegionCount, getMethod)
	registerFunc(clusterRouter, "GetRegionsByStore", "/regions/store/{id}", regionsHandler.GetStoreRegions, getMethod)
	registerFunc(clusterRouter, "GetTopWriteRegions", "/regions/writeflow", regionsHandler.GetTopWriteFlow, getMethod)
	registerFunc(clusterRouter, "GetTopReadRegions", "/regions/readflow", regionsHandler.GetTopReadFlow, getMethod)
	registerFunc(clusterRouter, "GetTopConfverRegions", "/regions/confver", regionsHandler.GetTopConfVer, getMethod)
	registerFunc(clusterRouter, "GetTopVersionRegions", "/regions/version", regionsHandler.GetTopVersion, getMethod)
	registerFunc(clusterRouter, "GetTopSizeRegions", "/regions/size", regionsHandler.GetTopSize, getMethod)
	registerFunc(clusterRouter, "GetMissPeerRegions", "/regions/check/miss-peer", regionsHandler.GetMissPeerRegions, getMethod)
	registerFunc(clusterRouter, "GetExtraPeerRegions", "/regions/check/extra-peer", regionsHandler.GetExtraPeerRegions, getMethod)
	registerFunc(clusterRouter, "GetPendingPeerRegions", "/regions/check/pending-peer", regionsHandler.GetPendingPeerRegions, getMethod)
	registerFunc(clusterRouter, "GetDownPeerRegions", "/regions/check/down-peer", regionsHandler.GetDownPeerRegions, getMethod)
	registerFunc(clusterRouter, "GetLearnerPeerRegions", "/regions/check/learner-peer", regionsHandler.GetLearnerPeerRegions, getMethod)
	registerFunc(clusterRouter, "GetEmptyRegion", "/regions/check/empty-region", regionsHandler.GetEmptyRegion, getMethod)
	registerFunc(clusterRouter, "GetOfflinePeer", "/regions/check/offline-peer", regionsHandler.GetOfflinePeer, getMethod)

	registerFunc(clusterRouter, "GetSizeHistogram", "/regions/check/hist-size", regionsHandler.GetSizeHistogram, getMethod)
	registerFunc(clusterRouter, "GetKeysHistogram", "/regions/check/hist-keys", regionsHandler.GetKeysHistogram, getMethod)
	registerFunc(clusterRouter, "GetRegionSiblings", "/regions/sibling/{id}", regionsHandler.GetRegionSiblings, getMethod)
	registerFunc(clusterRouter, "AccelerateRegionsSchedule", "/regions/accelerate-schedule", regionsHandler.AccelerateRegionsScheduleInRange, getMethod)
	registerFunc(clusterRouter, "ScatterRegions", "/regions/scatter", regionsHandler.ScatterRegions, getMethod)
	registerFunc(clusterRouter, "SplitRegions", "/regions/split", regionsHandler.SplitRegions, getMethod)
	registerFunc(clusterRouter, "GetRangeHoles", "/regions/range-holes", regionsHandler.GetRangeHoles, getMethod)
	registerFunc(clusterRouter, "CheckRegionsReplicated", "/regions/replicated", regionsHandler.CheckRegionsReplicated, getMethod, setQueries("startKey", "{startKey}", "endKey", "{endKey}"))

	register(apiRouter, "GetVersion", "/version", newVersionHandler(rd), getMethod)
	register(apiRouter, "GetPDStatus", "/status", newStatusHandler(svr, rd), getMethod)

	memberHandler := newMemberHandler(svr, rd)
	registerFunc(apiRouter, "GetMembers", "/members", memberHandler.ListMembers, getMethod)
	registerFunc(apiRouter, "DeleteMemberByName", "/members/name/{name}", memberHandler.DeleteByName, deleteMethod)
	registerFunc(apiRouter, "DeleteMemberByID", "/members/id/{id}", memberHandler.DeleteByID, deleteMethod)
	registerFunc(apiRouter, "SetMemberByName", "/members/name/{name}", memberHandler.SetMemberPropertyByName, postMethod)

	leaderHandler := newLeaderHandler(svr, rd)
	registerFunc(apiRouter, "GetLeader", "/leader", leaderHandler.Get, getMethod)
	registerFunc(apiRouter, "ResignLeader", "/leader/resign", leaderHandler.Resign, postMethod)
	registerFunc(apiRouter, "TransferLeader", "/leader/transfer/{next_leader}", leaderHandler.Transfer, postMethod)

	statsHandler := newStatsHandler(svr, rd)
	registerFunc(clusterRouter, "GetRegionStatus", "/stats/region", statsHandler.Region, getMethod)

	trendHandler := newTrendHandler(svr, rd)
	registerFunc(apiRouter, "GetTrend", "/trend", trendHandler.Handle, getMethod)

	adminHandler := newAdminHandler(svr, rd)
	registerFunc(clusterRouter, "DeleteRegionCache", "/admin/cache/region/{id}", adminHandler.HandleDropCacheRegion, deleteMethod)
	registerFunc(clusterRouter, "ResetTS", "/admin/reset-ts", adminHandler.ResetTS, postMethod)
	registerFunc(apiRouter, "SavePersistFile", "/admin/persist-file/{file_name}", adminHandler.persistFile, postMethod)
	registerFunc(clusterRouter, "SetWaitAsyncTime", "/admin/replication_mode/wait-async", adminHandler.UpdateWaitAsyncTime, postMethod)
	registerFunc(apiRouter, "SwitchServiceMiddleware", "/admin/service-middleware", adminHandler.HanldeServiceMiddlewareSwitch, postMethod)
	registerFunc(apiRouter, "SwitchAuditMiddleware", "/admin/audit-middleware", adminHandler.HanldeAuditMiddlewareSwitch, postMethod)

	logHandler := newLogHandler(svr, rd)
	registerFunc(apiRouter, "SetLogLevel", "/admin/log", logHandler.Handle, postMethod)

	replicationModeHandler := newReplicationModeHandler(svr, rd)
	registerFunc(clusterRouter, "GetReplicationModeStatus", "/replication_mode/status", replicationModeHandler.GetStatus)

	// Deprecated: component exists for historical compatibility and should not be used anymore. See https://github.com/tikv/tikv/issues/11472.
	componentHandler := newComponentHandler(svr, rd)
	clusterRouter.HandleFunc("/component", componentHandler.Register).Methods("POST")
	clusterRouter.HandleFunc("/component/{component}/{addr}", componentHandler.UnRegister).Methods("DELETE")
	clusterRouter.HandleFunc("/component", componentHandler.GetAllAddress).Methods("GET")
	clusterRouter.HandleFunc("/component/{type}", componentHandler.GetAddress).Methods("GET")

	pluginHandler := newPluginHandler(handler, rd)
	registerFunc(apiRouter, "SetPlugin", "/plugin", pluginHandler.LoadPlugin, postMethod)
	registerFunc(apiRouter, "DeletePlugin", "/plugin", pluginHandler.UnloadPlugin, deleteMethod)

	register(apiRouter, "GetHealthStatus", "/health", newHealthHandler(svr, rd), getMethod)
	// Deprecated: This API is no longer maintained anymore.
	apiRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	registerFunc(apiRouter, "Ping", "/ping", func(w http.ResponseWriter, r *http.Request) {}, getMethod)

	// metric query use to query metric data, the protocol is compatible with prometheus.
	register(apiRouter, "QueryMetric", "/metric/query", newQueryMetric(svr), getAndPostMethod)
	register(apiRouter, "QueryMetric", "/metric/query_range", newQueryMetric(svr), getAndPostMethod)

	// tso API
	tsoHandler := newTSOHandler(svr, rd)
	registerFunc(apiRouter, "TransferLocalTSOAllocator", "/tso/allocator/transfer/{name}", tsoHandler.TransferLocalTSOAllocator, postMethod)

	// profile API
	registerFunc(apiRouter, "DebugPProfProfile", "/debug/pprof/profile", pprof.Profile)
	registerFunc(apiRouter, "DebugPProfTrace", "/debug/pprof/trace", pprof.Trace)
	registerFunc(apiRouter, "DebugPProfSymbol", "/debug/pprof/symbol", pprof.Symbol)
	register(apiRouter, "DebugPProfHeap", "/debug/pprof/heap", pprof.Handler("heap"))
	register(apiRouter, "DebugPProfMutex", "/debug/pprof/mutex", pprof.Handler("mutex"))
	register(apiRouter, "DebugPProfAllocs", "/debug/pprof/allocs", pprof.Handler("allocs"))
	register(apiRouter, "DebugPProfBlock", "/debug/pprof/block", pprof.Handler("block"))
	register(apiRouter, "DebugPProfGoroutine", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	register(apiRouter, "DebugPProfThreadCreate", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	register(apiRouter, "DebugPProfZip", "/debug/pprof/zip", newProfHandler(svr, rd))

	// service GC safepoint API
	serviceGCSafepointHandler := newServiceGCSafepointHandler(svr, rd)
	registerFunc(apiRouter, "GetGCSafePoint", "/gc/safepoint", serviceGCSafepointHandler.List, getMethod)
	registerFunc(apiRouter, "DeleteGCSafePoint", "/gc/safepoint/{service_id}", serviceGCSafepointHandler.Delete, deleteMethod)

	// unsafe admin operation API
	unsafeOperationHandler := newUnsafeOperationHandler(svr, rd)
	registerFunc(clusterRouter, "RemoveFailedStoresUnsafely", "/admin/unsafe/remove-failed-stores",
		unsafeOperationHandler.RemoveFailedStores, postMethod)
	registerFunc(clusterRouter, "GetOngoingFailedStoresRemoval", "/admin/unsafe/remove-failed-stores/show",
		unsafeOperationHandler.GetFailedStoresRemovalStatus, getMethod)
	registerFunc(clusterRouter, "GetHistoryFailedStoresRemoval", "/admin/unsafe/remove-failed-stores/history",
		unsafeOperationHandler.GetFailedStoresRemovalHistory, getMethod)

	// API to set or unset failpoints
	failpoint.Inject("enableFailpointAPI", func() {
		apiRouter.PathPrefix("/fail").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The HTTP handler of failpoint requires the full path to be the failpoint path.
			r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix+apiPrefix+"/fail")
			new(failpoint.HttpHandler).ServeHTTP(w, r)
		}).Name("failpoint")
	})

	// Deprecated: use /pd/api/v1/health instead.
	rootRouter.Handle("/health", newHealthHandler(svr, rd)).Methods("GET")
	// Deprecated: use /pd/api/v1/diagnose instead.
	rootRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	// Deprecated: use /pd/api/v1/ping instead.
	rootRouter.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")

	rigisterServiceLabels := make([]string, 0)
	rootRouter.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		rigisterServiceLabels = append(rigisterServiceLabels, route.GetName())
		return nil
	})

	return rootRouter
}
