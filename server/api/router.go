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

	serviceMiddlewares := newMiddlewareBuilder(svr)

	operatorHandler := newOperatorHandler(handler, rd)
	apiRouter.HandleFunc("/operators", serviceMiddlewares.middlewareFunc(operatorHandler.List)).Methods("GET").Name("GetOperators")
	apiRouter.HandleFunc("/operators", serviceMiddlewares.middlewareFunc(operatorHandler.Post)).Methods("POST").Name("SetOperators")
	apiRouter.HandleFunc("/operators/{region_id}", serviceMiddlewares.middlewareFunc(operatorHandler.Get)).Methods("GET").Name("GetRegionOperator")
	apiRouter.HandleFunc("/operators/{region_id}", serviceMiddlewares.middlewareFunc(operatorHandler.Delete)).Methods("DELETE").Name("DeleteRegionOperator")

	checkerHandler := newCheckerHandler(svr, rd)
	apiRouter.HandleFunc("/checker/{name}", serviceMiddlewares.middlewareFunc(checkerHandler.PauseOrResume)).Methods("POST").Name("SetChecker")
	apiRouter.HandleFunc("/checker/{name}", serviceMiddlewares.middlewareFunc(checkerHandler.GetStatus)).Methods("GET").Name("GetChecker")

	schedulerHandler := newSchedulerHandler(svr, rd)
	apiRouter.HandleFunc("/schedulers", serviceMiddlewares.middlewareFunc(schedulerHandler.List)).Methods("GET").Name("GetSchedulers")
	apiRouter.HandleFunc("/schedulers", serviceMiddlewares.middlewareFunc(schedulerHandler.Post)).Methods("POST").Name("AddScheduler")
	apiRouter.HandleFunc("/schedulers/{name}", serviceMiddlewares.middlewareFunc(schedulerHandler.Delete)).Methods("DELETE").Name("DeleteScheduler")
	apiRouter.HandleFunc("/schedulers/{name}", serviceMiddlewares.middlewareFunc(schedulerHandler.PauseOrResume)).Methods("POST").Name("PauseOrResumeScheduler")

	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	apiRouter.PathPrefix("/scheduler-config").Handler(schedulerConfigHandler).Name("GetSchedulerConfig")

	clusterHandler := newClusterHandler(svr, rd)
	apiRouter.Handle("/cluster", serviceMiddlewares.middleware(clusterHandler)).Methods("GET").Name("GetCluster").Name("GetCluster")
	apiRouter.HandleFunc("/cluster/status", serviceMiddlewares.middlewareFunc(clusterHandler.GetClusterStatus)).Methods("GET").Name("GetClusterStatus")

	confHandler := newConfHandler(svr, rd)
	apiRouter.HandleFunc("/config", serviceMiddlewares.middlewareFunc(confHandler.Get)).Methods("GET").Name("GetConfig")
	apiRouter.HandleFunc("/config", serviceMiddlewares.middlewareFunc(confHandler.Post)).Methods("POST").Name("SetConfig")
	apiRouter.HandleFunc("/config/default", serviceMiddlewares.middlewareFunc(confHandler.GetDefault)).Methods("GET").Name("GetDefaultConfig")
	apiRouter.HandleFunc("/config/schedule", serviceMiddlewares.middlewareFunc(confHandler.GetSchedule)).Methods("GET").Name("GetScheduleConfig")
	apiRouter.HandleFunc("/config/schedule", serviceMiddlewares.middlewareFunc(confHandler.SetSchedule)).Methods("POST").Name("SetScheduleConfig")
	apiRouter.HandleFunc("/config/pd-server", serviceMiddlewares.middlewareFunc(confHandler.GetPDServer)).Methods("GET").Name("GetPDServerConfig")
	apiRouter.HandleFunc("/config/replicate", serviceMiddlewares.middlewareFunc(confHandler.GetReplication)).Methods("GET").Name("GetReplicationConfig")
	apiRouter.HandleFunc("/config/replicate", serviceMiddlewares.middlewareFunc(confHandler.SetReplication)).Methods("POST").Name("SetReplicationConfig")
	apiRouter.HandleFunc("/config/label-property", serviceMiddlewares.middlewareFunc(confHandler.GetLabelProperty)).Methods("GET").Name("GetLabelProperty")
	apiRouter.HandleFunc("/config/label-property", serviceMiddlewares.middlewareFunc(confHandler.SetLabelProperty)).Methods("POST").Name("SetLabelProperty")
	apiRouter.HandleFunc("/config/cluster-version", serviceMiddlewares.middlewareFunc(confHandler.GetClusterVersion)).Methods("GET").Name("GetClusterVersion")
	apiRouter.HandleFunc("/config/cluster-version", serviceMiddlewares.middlewareFunc(confHandler.SetClusterVersion)).Methods("POST").Name("SetClusterVersion")
	apiRouter.HandleFunc("/config/replication-mode", serviceMiddlewares.middlewareFunc(confHandler.GetReplicationMode)).Methods("GET").Name("GetReplicationMode")
	apiRouter.HandleFunc("/config/replication-mode", serviceMiddlewares.middlewareFunc(confHandler.SetReplicationMode)).Methods("POST").Name("SetReplicationMode")

	rulesHandler := newRulesHandler(svr, rd)
	clusterRouter.HandleFunc("/config/rules", serviceMiddlewares.middlewareFunc(rulesHandler.GetAll)).Methods("GET").Name("GetAllRules")
	clusterRouter.HandleFunc("/config/rules", serviceMiddlewares.middlewareFunc(rulesHandler.SetAll)).Methods("POST").Name("SetAllRules")
	clusterRouter.HandleFunc("/config/rules/batch", serviceMiddlewares.middlewareFunc(rulesHandler.Batch)).Methods("POST").Name("SetBatchRules")
	clusterRouter.HandleFunc("/config/rules/group/{group}", serviceMiddlewares.middlewareFunc(rulesHandler.GetAllByGroup)).Methods("GET").Name("GetRuleByGroup")
	clusterRouter.HandleFunc("/config/rules/region/{region}", serviceMiddlewares.middlewareFunc(rulesHandler.GetAllByRegion)).Methods("GET").Name("GetRuleByByRegion")
	clusterRouter.HandleFunc("/config/rules/key/{key}", serviceMiddlewares.middlewareFunc(rulesHandler.GetAllByKey)).Methods("GET").Name("GetRuleByKey")
	clusterRouter.HandleFunc("/config/rule/{group}/{id}", serviceMiddlewares.middlewareFunc(rulesHandler.Get)).Methods("GET").Name("GetRuleByGroupAndID")
	clusterRouter.HandleFunc("/config/rule", serviceMiddlewares.middlewareFunc(rulesHandler.Set)).Methods("POST").Name("SetRule")
	clusterRouter.HandleFunc("/config/rule/{group}/{id}", serviceMiddlewares.middlewareFunc(rulesHandler.Delete)).Methods("DELETE").Name("DeleteRuleByGroup")

	regionLabelHandler := newRegionLabelHandler(svr, rd)
	clusterRouter.HandleFunc("/config/region-label/rules", serviceMiddlewares.middlewareFunc(regionLabelHandler.GetAllRules)).Methods("GET").Name("GetAllRegionLabelRule")
	clusterRouter.HandleFunc("/config/region-label/rules/ids", serviceMiddlewares.middlewareFunc(regionLabelHandler.GetRulesByIDs)).Methods("GET").Name("GetRegionLabelRulesByIDs")
	// {id} can be a string with special characters, we should enable path encode to support it.
	escapeRouter.HandleFunc("/config/region-label/rule/{id}", serviceMiddlewares.middlewareFunc(regionLabelHandler.GetRule)).Methods("GET").Name("GetRegionLabelRuleByID")
	escapeRouter.HandleFunc("/config/region-label/rule/{id}", serviceMiddlewares.middlewareFunc(regionLabelHandler.DeleteRule)).Methods("DELETE").Name("DeleteRegionLabelRule")
	clusterRouter.HandleFunc("/config/region-label/rule", serviceMiddlewares.middlewareFunc(regionLabelHandler.SetRule)).Methods("POST").Name("SetRegionLabelRule")
	clusterRouter.HandleFunc("/config/region-label/rules", serviceMiddlewares.middlewareFunc(regionLabelHandler.Patch)).Methods("PATCH").Name("PatchRegionLabelRules")

	clusterRouter.HandleFunc("/region/id/{id}/label/{key}", serviceMiddlewares.middlewareFunc(regionLabelHandler.GetRegionLabel)).Methods("GET").Name("GetRegionLabelByKey")
	clusterRouter.HandleFunc("/region/id/{id}/labels", serviceMiddlewares.middlewareFunc(regionLabelHandler.GetRegionLabels)).Methods("GET").Name("GetAllRegionLabels")

	clusterRouter.HandleFunc("/config/rule_group/{id}", serviceMiddlewares.middlewareFunc(rulesHandler.GetGroupConfig)).Methods("GET").Name("GetRuleGroup")
	clusterRouter.HandleFunc("/config/rule_group", serviceMiddlewares.middlewareFunc(rulesHandler.SetGroupConfig)).Methods("POST").Name("SetRuleGroup")
	clusterRouter.HandleFunc("/config/rule_group/{id}", serviceMiddlewares.middlewareFunc(rulesHandler.DeleteGroupConfig)).Methods("DELETE").Name("DeleteRuleGroup")
	clusterRouter.HandleFunc("/config/rule_groups", serviceMiddlewares.middlewareFunc(rulesHandler.GetAllGroupConfigs)).Methods("GET").Name("GetAllRuleGroups")

	clusterRouter.HandleFunc("/config/placement-rule", serviceMiddlewares.middlewareFunc(rulesHandler.GetAllGroupBundles)).Methods("GET").Name("GetAllPlacementRules")
	clusterRouter.HandleFunc("/config/placement-rule", serviceMiddlewares.middlewareFunc(rulesHandler.SetAllGroupBundles)).Methods("POST").Name("SetAllPlacementRules")
	// {group} can be a regular expression, we should enable path encode to
	// support special characters.
	clusterRouter.HandleFunc("/config/placement-rule/{group}", serviceMiddlewares.middlewareFunc(rulesHandler.GetGroupBundle)).Methods("GET").Name("GetPlacementRuleByGroup")
	clusterRouter.HandleFunc("/config/placement-rule/{group}", serviceMiddlewares.middlewareFunc(rulesHandler.SetGroupBundle)).Methods("POST").Name("SetPlacementRuleByGroup")
	escapeRouter.HandleFunc("/config/placement-rule/{group}", serviceMiddlewares.middlewareFunc(rulesHandler.DeleteGroupBundle)).Methods("DELETE").Name("DeletePlacementRuleByGroup")

	storeHandler := newStoreHandler(handler, rd)
	clusterRouter.HandleFunc("/store/{id}", serviceMiddlewares.middlewareFunc(storeHandler.Get)).Methods("GET").Name("GetStore")
	clusterRouter.HandleFunc("/store/{id}", serviceMiddlewares.middlewareFunc(storeHandler.Delete)).Methods("DELETE").Name("DeleteStore")
	clusterRouter.HandleFunc("/store/{id}/state", serviceMiddlewares.middlewareFunc(storeHandler.SetState)).Methods("POST").Name("SetStoreState")
	clusterRouter.HandleFunc("/store/{id}/label", serviceMiddlewares.middlewareFunc(storeHandler.SetLabels)).Methods("POST").Name("SetStoreLabel")
	clusterRouter.HandleFunc("/store/{id}/weight", serviceMiddlewares.middlewareFunc(storeHandler.SetWeight)).Methods("POST").Name("SetStoreWeight")
	clusterRouter.HandleFunc("/store/{id}/limit", serviceMiddlewares.middlewareFunc(storeHandler.SetLimit)).Methods("POST").Name("SetStoreLimit")
	storesHandler := newStoresHandler(handler, rd)
	clusterRouter.Handle("/stores", serviceMiddlewares.middleware(storesHandler)).Methods("GET").Name("GetAllStores")
	clusterRouter.HandleFunc("/stores/remove-tombstone", serviceMiddlewares.middlewareFunc(storesHandler.RemoveTombStone)).Methods("DELETE").Name("RemoveTombstone")
	clusterRouter.HandleFunc("/stores/limit", serviceMiddlewares.middlewareFunc(storesHandler.GetAllLimit)).Methods("GET").Name("GetAllStoresLimit")
	clusterRouter.HandleFunc("/stores/limit", serviceMiddlewares.middlewareFunc(storesHandler.SetAllLimit)).Methods("POST").Name("SetAllStoresLimit")
	clusterRouter.HandleFunc("/stores/limit/scene", serviceMiddlewares.middlewareFunc(storesHandler.SetStoreLimitScene)).Methods("POST").Name("SetStoreSceneLimit")
	clusterRouter.HandleFunc("/stores/limit/scene", serviceMiddlewares.middlewareFunc(storesHandler.GetStoreLimitScene)).Methods("GET").Name("GetStoreSceneLimit")

	labelsHandler := newLabelsHandler(svr, rd)
	clusterRouter.HandleFunc("/labels", serviceMiddlewares.middlewareFunc(labelsHandler.Get)).Methods("GET").Name("GetLabels")
	clusterRouter.HandleFunc("/labels/stores", serviceMiddlewares.middlewareFunc(labelsHandler.GetStores)).Methods("GET").Name("GetStoresByLabel")

	hotStatusHandler := newHotStatusHandler(handler, rd)
	apiRouter.HandleFunc("/hotspot/regions/write", serviceMiddlewares.middlewareFunc(hotStatusHandler.GetHotWriteRegions)).Methods("GET").Name("GetHotspotWriteRegion")
	apiRouter.HandleFunc("/hotspot/regions/read", serviceMiddlewares.middlewareFunc(hotStatusHandler.GetHotReadRegions)).Methods("GET").Name("GetHotspotReadRegion")
	apiRouter.HandleFunc("/hotspot/regions/history", serviceMiddlewares.middlewareFunc(hotStatusHandler.GetHistoryHotRegions)).Methods("GET").Name("GetHotspotStores")
	apiRouter.HandleFunc("/hotspot/stores", serviceMiddlewares.middlewareFunc(hotStatusHandler.GetHotStores)).Methods("GET").Name("GetHistoryHotspotRegion")

	regionHandler := newRegionHandler(svr, rd)
	clusterRouter.HandleFunc("/region/id/{id}", serviceMiddlewares.middlewareFunc(regionHandler.GetRegionByID)).Methods("GET").Name("GetRegionByID")
	clusterRouter.UseEncodedPath().HandleFunc("/region/key/{key}", serviceMiddlewares.middlewareFunc(regionHandler.GetRegionByKey)).Methods("GET").Name("GetRegion")

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	clusterRouter.HandleFunc("/regions", serviceMiddlewares.middlewareFunc(regionsAllHandler.GetAll)).Methods("GET").Name("GetAllRegions")

	regionsHandler := newRegionsHandler(svr, rd)
	clusterRouter.HandleFunc("/regions/key", serviceMiddlewares.middlewareFunc(regionsHandler.ScanRegions)).Methods("GET").Name("ScanRegions")
	clusterRouter.HandleFunc("/regions/count", serviceMiddlewares.middlewareFunc(regionsHandler.GetRegionCount)).Methods("GET").Name("CountRegions")
	clusterRouter.HandleFunc("/regions/store/{id}", serviceMiddlewares.middlewareFunc(regionsHandler.GetStoreRegions)).Methods("GET").Name("GetRegionsByStore")
	clusterRouter.HandleFunc("/regions/writeflow", serviceMiddlewares.middlewareFunc(regionsHandler.GetTopWriteFlow)).Methods("GET").Name("GetTopWriteRegions")
	clusterRouter.HandleFunc("/regions/readflow", serviceMiddlewares.middlewareFunc(regionsHandler.GetTopReadFlow)).Methods("GET").Name("GetTopReadRegions")
	clusterRouter.HandleFunc("/regions/confver", serviceMiddlewares.middlewareFunc(regionsHandler.GetTopConfVer)).Methods("GET").Name("GetTopConfverRegions")
	clusterRouter.HandleFunc("/regions/version", serviceMiddlewares.middlewareFunc(regionsHandler.GetTopVersion)).Methods("GET").Name("GetTopVersionRegions")
	clusterRouter.HandleFunc("/regions/size", serviceMiddlewares.middlewareFunc(regionsHandler.GetTopSize)).Methods("GET").Name("GetTopSizeRegions")
	clusterRouter.HandleFunc("/regions/check/miss-peer", serviceMiddlewares.middlewareFunc(regionsHandler.GetMissPeerRegions)).Methods("GET").Name("GetMissPeerRegions")
	clusterRouter.HandleFunc("/regions/check/extra-peer", serviceMiddlewares.middlewareFunc(regionsHandler.GetExtraPeerRegions)).Methods("GET").Name("GetExtraPeerRegions")
	clusterRouter.HandleFunc("/regions/check/pending-peer", serviceMiddlewares.middlewareFunc(regionsHandler.GetPendingPeerRegions)).Methods("GET").Name("GetPendingPeerRegions")
	clusterRouter.HandleFunc("/regions/check/down-peer", serviceMiddlewares.middlewareFunc(regionsHandler.GetDownPeerRegions)).Methods("GET").Name("GetDownPeerRegions")
	clusterRouter.HandleFunc("/regions/check/learner-peer", serviceMiddlewares.middlewareFunc(regionsHandler.GetLearnerPeerRegions)).Methods("GET").Name("GetLearnerPeerRegions")
	clusterRouter.HandleFunc("/regions/check/empty-region", serviceMiddlewares.middlewareFunc(regionsHandler.GetEmptyRegion)).Methods("GET").Name("GetEmptyRegion")
	clusterRouter.HandleFunc("/regions/check/offline-peer", serviceMiddlewares.middlewareFunc(regionsHandler.GetOfflinePeer)).Methods("GET").Name("GetOfflinePeer")

	clusterRouter.HandleFunc("/regions/check/hist-size", serviceMiddlewares.middlewareFunc(regionsHandler.GetSizeHistogram)).Methods("GET").Name("GetSizeHistogram")
	clusterRouter.HandleFunc("/regions/check/hist-keys", serviceMiddlewares.middlewareFunc(regionsHandler.GetKeysHistogram)).Methods("GET").Name("GetKeysHistogram")
	clusterRouter.HandleFunc("/regions/sibling/{id}", serviceMiddlewares.middlewareFunc(regionsHandler.GetRegionSiblings)).Methods("GET").Name("GetRegionSiblings")
	clusterRouter.HandleFunc("/regions/accelerate-schedule", serviceMiddlewares.middlewareFunc(regionsHandler.AccelerateRegionsScheduleInRange)).Methods("POST").Name("AccelerateRegionsSchedule")
	clusterRouter.HandleFunc("/regions/scatter", serviceMiddlewares.middlewareFunc(regionsHandler.ScatterRegions)).Methods("POST").Name("ScatterRegions")
	clusterRouter.HandleFunc("/regions/split", serviceMiddlewares.middlewareFunc(regionsHandler.SplitRegions)).Methods("POST").Name("SplitRegions")
	clusterRouter.HandleFunc("/regions/range-holes", serviceMiddlewares.middlewareFunc(regionsHandler.GetRangeHoles)).Methods("GET").Name("GetRangeHoles")
	clusterRouter.HandleFunc("/regions/replicated", serviceMiddlewares.middlewareFunc(regionsHandler.CheckRegionsReplicated)).Methods("GET").Queries("startKey", "{startKey}", "endKey", "{endKey}").Name("CheckRegionsReplicated")

	apiRouter.Handle("/version", serviceMiddlewares.middleware(newVersionHandler(rd))).Methods("GET").Name("GetVersion")
	apiRouter.Handle("/status", serviceMiddlewares.middleware(newStatusHandler(svr, rd))).Methods("GET").Name("GetPDStatus")

	memberHandler := newMemberHandler(svr, rd)
	apiRouter.HandleFunc("/members", serviceMiddlewares.middlewareFunc(memberHandler.ListMembers)).Methods("GET").Name("GetMembers")
	apiRouter.HandleFunc("/members/name/{name}", serviceMiddlewares.middlewareFunc(memberHandler.DeleteByName)).Methods("DELETE").Name("DeleteMemberByName")
	apiRouter.HandleFunc("/members/id/{id}", serviceMiddlewares.middlewareFunc(memberHandler.DeleteByID)).Methods("DELETE").Name("DeleteMemberByID")
	apiRouter.HandleFunc("/members/name/{name}", serviceMiddlewares.middlewareFunc(memberHandler.SetMemberPropertyByName)).Methods("POST").Name("SetMemberByName")

	leaderHandler := newLeaderHandler(svr, rd)
	apiRouter.HandleFunc("/leader", serviceMiddlewares.middlewareFunc(leaderHandler.Get)).Methods("GET").Name("GetLeader")
	apiRouter.HandleFunc("/leader/resign", serviceMiddlewares.middlewareFunc(leaderHandler.Resign)).Methods("POST").Name("ResignLeader")
	apiRouter.HandleFunc("/leader/transfer/{next_leader}", serviceMiddlewares.middlewareFunc(leaderHandler.Transfer)).Methods("POST").Name("TransferLeader")

	statsHandler := newStatsHandler(svr, rd)
	clusterRouter.HandleFunc("/stats/region", serviceMiddlewares.middlewareFunc(statsHandler.Region)).Methods("GET").Name("GetRegionStatus")

	trendHandler := newTrendHandler(svr, rd)
	apiRouter.HandleFunc("/trend", serviceMiddlewares.middlewareFunc(trendHandler.Handle)).Methods("GET").Name("GetTrend")

	adminHandler := newAdminHandler(svr, rd)
	clusterRouter.HandleFunc("/admin/cache/region/{id}", serviceMiddlewares.middlewareFunc(adminHandler.HandleDropCacheRegion)).Methods("DELETE").Name("DeleteRegionCache")
	clusterRouter.HandleFunc("/admin/reset-ts", serviceMiddlewares.middlewareFunc(adminHandler.ResetTS)).Methods("POST").Name("ResetTS")
	apiRouter.HandleFunc("/admin/persist-file/{file_name}", serviceMiddlewares.middlewareFunc(adminHandler.persistFile)).Methods("POST").Name("SavePersistFile")
	clusterRouter.HandleFunc("/admin/replication_mode/wait-async", serviceMiddlewares.middlewareFunc(adminHandler.UpdateWaitAsyncTime)).Methods("POST").Name("SetWaitAsyncTime")
	apiRouter.HandleFunc("/admin/service-middleware", serviceMiddlewares.middlewareFunc(adminHandler.HanldeServiceMiddlewareSwitch)).Methods("POST").Name("SwitchServiceMiddleware")

	logHandler := newLogHandler(svr, rd)
	apiRouter.HandleFunc("/admin/log", serviceMiddlewares.middlewareFunc(logHandler.Handle)).Methods("POST").Name("SetLogLevel")

	replicationModeHandler := newReplicationModeHandler(svr, rd)
	clusterRouter.HandleFunc("/replication_mode/status", serviceMiddlewares.middlewareFunc(replicationModeHandler.GetStatus)).Name("GetReplicationModeStatus")

	// Deprecated: component exists for historical compatibility and should not be used anymore. See https://github.com/tikv/tikv/issues/11472.
	componentHandler := newComponentHandler(svr, rd)
	clusterRouter.HandleFunc("/component", componentHandler.Register).Methods("POST")
	clusterRouter.HandleFunc("/component/{component}/{addr}", componentHandler.UnRegister).Methods("DELETE")
	clusterRouter.HandleFunc("/component", componentHandler.GetAllAddress).Methods("GET")
	clusterRouter.HandleFunc("/component/{type}", componentHandler.GetAddress).Methods("GET")

	pluginHandler := newPluginHandler(handler, rd)
	apiRouter.HandleFunc("/plugin", serviceMiddlewares.middlewareFunc(pluginHandler.LoadPlugin)).Methods("POST").Name("SetPlugin")
	apiRouter.HandleFunc("/plugin", serviceMiddlewares.middlewareFunc(pluginHandler.UnloadPlugin)).Methods("DELETE").Name("DeletePlugin")

	apiRouter.Handle("/health", serviceMiddlewares.middleware(newHealthHandler(svr, rd))).Methods("GET").Name("GetHealthStatus")
	// Deprecated: This API is no longer maintained anymore.
	apiRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	apiRouter.HandleFunc("/ping", serviceMiddlewares.middlewareFunc(func(w http.ResponseWriter, r *http.Request) {})).Methods("GET").Name("Ping")

	// metric query use to query metric data, the protocol is compatible with prometheus.
	apiRouter.Handle("/metric/query", serviceMiddlewares.middleware(newQueryMetric(svr))).Methods("GET", "POST").Name("QueryMetric")
	apiRouter.Handle("/metric/query_range", serviceMiddlewares.middleware(newQueryMetric(svr))).Methods("GET", "POST").Name("QueryMetric")

	// tso API
	tsoHandler := newTSOHandler(svr, rd)
	apiRouter.HandleFunc("/tso/allocator/transfer/{name}", serviceMiddlewares.middlewareFunc(tsoHandler.TransferLocalTSOAllocator)).Methods("POST").Name("TransferLocalTSOAllocator")

	// profile API
	apiRouter.HandleFunc("/debug/pprof/profile", serviceMiddlewares.middlewareFunc(pprof.Profile)).Name("DebugPProfProfile")
	apiRouter.HandleFunc("/debug/pprof/trace", serviceMiddlewares.middlewareFunc(pprof.Trace)).Name("DebugPProfTrace")
	apiRouter.HandleFunc("/debug/pprof/symbol", serviceMiddlewares.middlewareFunc(pprof.Symbol)).Name("DebugPProfSymbol")
	apiRouter.Handle("/debug/pprof/heap", serviceMiddlewares.middleware(pprof.Handler("heap"))).Name("DebugPProfHeap")
	apiRouter.Handle("/debug/pprof/mutex", serviceMiddlewares.middleware(pprof.Handler("mutex"))).Name("DebugPProfMutex")
	apiRouter.Handle("/debug/pprof/allocs", serviceMiddlewares.middleware(pprof.Handler("allocs"))).Name("DebugPProfAllocs")
	apiRouter.Handle("/debug/pprof/block", serviceMiddlewares.middleware(pprof.Handler("block"))).Name("DebugPProfBlock")
	apiRouter.Handle("/debug/pprof/goroutine", serviceMiddlewares.middleware(pprof.Handler("goroutine"))).Name("DebugPProfGoroutine")
	apiRouter.Handle("/debug/pprof/threadcreate", serviceMiddlewares.middleware(pprof.Handler("threadcreate"))).Name("DebugPProfThreadCreate")
	apiRouter.Handle("/debug/pprof/zip", serviceMiddlewares.middleware(newProfHandler(svr, rd))).Name("DebugPProfZip")

	// service GC safepoint API
	serviceGCSafepointHandler := newServiceGCSafepointHandler(svr, rd)
	apiRouter.HandleFunc("/gc/safepoint", serviceMiddlewares.middlewareFunc(serviceGCSafepointHandler.List)).Methods("GET").Name("GetGCSafePoint")
	apiRouter.HandleFunc("/gc/safepoint/{service_id}", serviceMiddlewares.middlewareFunc(serviceGCSafepointHandler.Delete)).Methods("DELETE").Name("DeleteGCSafePoint")

	// unsafe admin operation API
	unsafeOperationHandler := newUnsafeOperationHandler(svr, rd)
	clusterRouter.HandleFunc("/admin/unsafe/remove-failed-stores",
		serviceMiddlewares.middlewareFunc(unsafeOperationHandler.RemoveFailedStores)).Methods("POST").Name("RemoveFailedStoresUnsafely")
	clusterRouter.HandleFunc("/admin/unsafe/remove-failed-stores/show",
		serviceMiddlewares.middlewareFunc(unsafeOperationHandler.GetFailedStoresRemovalStatus)).Methods("GET").Name("GetOngoingFailedStoresRemoval")
	clusterRouter.HandleFunc("/admin/unsafe/remove-failed-stores/history",
		serviceMiddlewares.middlewareFunc(unsafeOperationHandler.GetFailedStoresRemovalHistory)).Methods("GET").Name("GetHistoryFailedStoresRemoval")

	// API to set or unset failpoints
	failpoint.Inject("enableFailpointAPI", func() {
		apiRouter.PathPrefix("/fail").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The HTTP handler of failpoint requires the full path to be the failpoint path.
			r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix+apiPrefix+"/fail")
			new(failpoint.HttpHandler).ServeHTTP(w, r)
		})
	})

	// Deprecated: use /pd/api/v1/health instead.
	rootRouter.Handle("/health", newHealthHandler(svr, rd)).Methods("GET")
	// Deprecated: use /pd/api/v1/diagnose instead.
	rootRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	// Deprecated: use /pd/api/v1/ping instead.
	rootRouter.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")

	return rootRouter
}
