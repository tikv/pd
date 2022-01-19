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
	mw := serviceMiddlewares.middleware
	mwFunc := serviceMiddlewares.middlewareFunc

	operatorHandler := newOperatorHandler(handler, rd)
	apiRouter.HandleFunc("/operators", mwFunc(operatorHandler.List)).Methods("GET").Name("GetOperators")
	apiRouter.HandleFunc("/operators", mwFunc(operatorHandler.Post)).Methods("POST").Name("SetOperators")
	apiRouter.HandleFunc("/operators/{region_id}", mwFunc(operatorHandler.Get)).Methods("GET").Name("GetRegionOperator")
	apiRouter.HandleFunc("/operators/{region_id}", mwFunc(operatorHandler.Delete)).Methods("DELETE").Name("DeleteRegionOperator")

	checkerHandler := newCheckerHandler(svr, rd)
	apiRouter.HandleFunc("/checker/{name}", mwFunc(checkerHandler.PauseOrResume)).Methods("POST").Name("SetChecker")
	apiRouter.HandleFunc("/checker/{name}", mwFunc(checkerHandler.GetStatus)).Methods("GET").Name("GetChecker")

	schedulerHandler := newSchedulerHandler(svr, rd)
	apiRouter.HandleFunc("/schedulers", mwFunc(schedulerHandler.List)).Methods("GET").Name("GetSchedulers")
	apiRouter.HandleFunc("/schedulers", mwFunc(schedulerHandler.Post)).Methods("POST").Name("AddScheduler")
	apiRouter.HandleFunc("/schedulers/{name}", mwFunc(schedulerHandler.Delete)).Methods("DELETE").Name("DeleteScheduler")
	apiRouter.HandleFunc("/schedulers/{name}", mwFunc(schedulerHandler.PauseOrResume)).Methods("POST").Name("PauseOrResumeScheduler")

	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	apiRouter.PathPrefix("/scheduler-config").Handler(mw(schedulerConfigHandler)).Name("GetSchedulerConfig")

	clusterHandler := newClusterHandler(svr, rd)
	apiRouter.Handle("/cluster", mw(clusterHandler)).Methods("GET").Name("GetCluster")
	apiRouter.HandleFunc("/cluster/status", mwFunc(clusterHandler.GetClusterStatus)).Methods("GET").Name("GetClusterStatus")

	confHandler := newConfHandler(svr, rd)
	apiRouter.HandleFunc("/config", mwFunc(confHandler.Get)).Methods("GET").Name("GetConfig")
	apiRouter.HandleFunc("/config", mwFunc(confHandler.Post)).Methods("POST").Name("SetConfig")
	apiRouter.HandleFunc("/config/default", mwFunc(confHandler.GetDefault)).Methods("GET").Name("GetDefaultConfig")
	apiRouter.HandleFunc("/config/schedule", mwFunc(confHandler.GetSchedule)).Methods("GET").Name("GetScheduleConfig")
	apiRouter.HandleFunc("/config/schedule", mwFunc(confHandler.SetSchedule)).Methods("POST").Name("SetScheduleConfig")
	apiRouter.HandleFunc("/config/pd-server", mwFunc(confHandler.GetPDServer)).Methods("GET").Name("GetPDServerConfig")
	apiRouter.HandleFunc("/config/replicate", mwFunc(confHandler.GetReplication)).Methods("GET").Name("GetReplicationConfig")
	apiRouter.HandleFunc("/config/replicate", mwFunc(confHandler.SetReplication)).Methods("POST").Name("SetReplicationConfig")
	apiRouter.HandleFunc("/config/label-property", mwFunc(confHandler.GetLabelProperty)).Methods("GET").Name("GetLabelProperty")
	apiRouter.HandleFunc("/config/label-property", mwFunc(confHandler.SetLabelProperty)).Methods("POST").Name("SetLabelProperty")
	apiRouter.HandleFunc("/config/cluster-version", mwFunc(confHandler.GetClusterVersion)).Methods("GET").Name("GetClusterVersion")
	apiRouter.HandleFunc("/config/cluster-version", mwFunc(confHandler.SetClusterVersion)).Methods("POST").Name("SetClusterVersion")
	apiRouter.HandleFunc("/config/replication-mode", mwFunc(confHandler.GetReplicationMode)).Methods("GET").Name("GetReplicationMode")
	apiRouter.HandleFunc("/config/replication-mode", mwFunc(confHandler.SetReplicationMode)).Methods("POST").Name("SetReplicationMode")

	rulesHandler := newRulesHandler(svr, rd)
	clusterRouter.HandleFunc("/config/rules", mwFunc(rulesHandler.GetAll)).Methods("GET").Name("GetAllRules")
	clusterRouter.HandleFunc("/config/rules", mwFunc(rulesHandler.SetAll)).Methods("POST").Name("SetAllRules")
	clusterRouter.HandleFunc("/config/rules/batch", mwFunc(rulesHandler.Batch)).Methods("POST").Name("SetBatchRules")
	clusterRouter.HandleFunc("/config/rules/group/{group}", mwFunc(rulesHandler.GetAllByGroup)).Methods("GET").Name("GetRuleByGroup")
	clusterRouter.HandleFunc("/config/rules/region/{region}", mwFunc(rulesHandler.GetAllByRegion)).Methods("GET").Name("GetRuleByByRegion")
	clusterRouter.HandleFunc("/config/rules/key/{key}", mwFunc(rulesHandler.GetAllByKey)).Methods("GET").Name("GetRuleByKey")
	clusterRouter.HandleFunc("/config/rule/{group}/{id}", mwFunc(rulesHandler.Get)).Methods("GET").Name("GetRuleByGroupAndID")
	clusterRouter.HandleFunc("/config/rule", mwFunc(rulesHandler.Set)).Methods("POST").Name("SetRule")
	clusterRouter.HandleFunc("/config/rule/{group}/{id}", mwFunc(rulesHandler.Delete)).Methods("DELETE").Name("DeleteRuleByGroup")

	regionLabelHandler := newRegionLabelHandler(svr, rd)
	clusterRouter.HandleFunc("/config/region-label/rules", mwFunc(regionLabelHandler.GetAllRules)).Methods("GET").Name("GetAllRegionLabelRule")
	clusterRouter.HandleFunc("/config/region-label/rules/ids", mwFunc(regionLabelHandler.GetRulesByIDs)).Methods("GET").Name("GetRegionLabelRulesByIDs")
	// {id} can be a string with special characters, we should enable path encode to support it.
	escapeRouter.HandleFunc("/config/region-label/rule/{id}", mwFunc(regionLabelHandler.GetRule)).Methods("GET").Name("GetRegionLabelRuleByID")
	escapeRouter.HandleFunc("/config/region-label/rule/{id}", mwFunc(regionLabelHandler.DeleteRule)).Methods("DELETE").Name("DeleteRegionLabelRule")
	clusterRouter.HandleFunc("/config/region-label/rule", mwFunc(regionLabelHandler.SetRule)).Methods("POST").Name("SetRegionLabelRule")
	clusterRouter.HandleFunc("/config/region-label/rules", mwFunc(regionLabelHandler.Patch)).Methods("PATCH").Name("PatchRegionLabelRules")

	clusterRouter.HandleFunc("/region/id/{id}/label/{key}", mwFunc(regionLabelHandler.GetRegionLabel)).Methods("GET").Name("GetRegionLabelByKey")
	clusterRouter.HandleFunc("/region/id/{id}/labels", mwFunc(regionLabelHandler.GetRegionLabels)).Methods("GET").Name("GetAllRegionLabels")

	clusterRouter.HandleFunc("/config/rule_group/{id}", mwFunc(rulesHandler.GetGroupConfig)).Methods("GET").Name("GetRuleGroup")
	clusterRouter.HandleFunc("/config/rule_group", mwFunc(rulesHandler.SetGroupConfig)).Methods("POST").Name("SetRuleGroup")
	clusterRouter.HandleFunc("/config/rule_group/{id}", mwFunc(rulesHandler.DeleteGroupConfig)).Methods("DELETE").Name("DeleteRuleGroup")
	clusterRouter.HandleFunc("/config/rule_groups", mwFunc(rulesHandler.GetAllGroupConfigs)).Methods("GET").Name("GetAllRuleGroups")

	clusterRouter.HandleFunc("/config/placement-rule", mwFunc(rulesHandler.GetAllGroupBundles)).Methods("GET").Name("GetAllPlacementRules")
	clusterRouter.HandleFunc("/config/placement-rule", mwFunc(rulesHandler.SetAllGroupBundles)).Methods("POST").Name("SetAllPlacementRules")
	// {group} can be a regular expression, we should enable path encode to
	// support special characters.
	clusterRouter.HandleFunc("/config/placement-rule/{group}", mwFunc(rulesHandler.GetGroupBundle)).Methods("GET").Name("GetPlacementRuleByGroup")
	clusterRouter.HandleFunc("/config/placement-rule/{group}", mwFunc(rulesHandler.SetGroupBundle)).Methods("POST").Name("SetPlacementRuleByGroup")
	escapeRouter.HandleFunc("/config/placement-rule/{group}", mwFunc(rulesHandler.DeleteGroupBundle)).Methods("DELETE").Name("DeletePlacementRuleByGroup")

	storeHandler := newStoreHandler(handler, rd)
	clusterRouter.HandleFunc("/store/{id}", mwFunc(storeHandler.Get)).Methods("GET").Name("GetStore")
	clusterRouter.HandleFunc("/store/{id}", mwFunc(storeHandler.Delete)).Methods("DELETE").Name("DeleteStore")
	clusterRouter.HandleFunc("/store/{id}/state", mwFunc(storeHandler.SetState)).Methods("POST").Name("SetStoreState")
	clusterRouter.HandleFunc("/store/{id}/label", mwFunc(storeHandler.SetLabels)).Methods("POST").Name("SetStoreLabel")
	clusterRouter.HandleFunc("/store/{id}/weight", mwFunc(storeHandler.SetWeight)).Methods("POST").Name("SetStoreWeight")
	clusterRouter.HandleFunc("/store/{id}/limit", mwFunc(storeHandler.SetLimit)).Methods("POST").Name("SetStoreLimit")
	storesHandler := newStoresHandler(handler, rd)
	clusterRouter.Handle("/stores", mw(storesHandler)).Methods("GET").Name("GetAllStores")
	clusterRouter.HandleFunc("/stores/remove-tombstone", mwFunc(storesHandler.RemoveTombStone)).Methods("DELETE").Name("RemoveTombstone")
	clusterRouter.HandleFunc("/stores/limit", mwFunc(storesHandler.GetAllLimit)).Methods("GET").Name("GetAllStoresLimit")
	clusterRouter.HandleFunc("/stores/limit", mwFunc(storesHandler.SetAllLimit)).Methods("POST").Name("SetAllStoresLimit")
	clusterRouter.HandleFunc("/stores/limit/scene", mwFunc(storesHandler.SetStoreLimitScene)).Methods("POST").Name("SetStoreSceneLimit")
	clusterRouter.HandleFunc("/stores/limit/scene", mwFunc(storesHandler.GetStoreLimitScene)).Methods("GET").Name("GetStoreSceneLimit")

	labelsHandler := newLabelsHandler(svr, rd)
	clusterRouter.HandleFunc("/labels", mwFunc(labelsHandler.Get)).Methods("GET").Name("GetLabels")
	clusterRouter.HandleFunc("/labels/stores", mwFunc(labelsHandler.GetStores)).Methods("GET").Name("GetStoresByLabel")

	hotStatusHandler := newHotStatusHandler(handler, rd)
	apiRouter.HandleFunc("/hotspot/regions/write", mwFunc(hotStatusHandler.GetHotWriteRegions)).Methods("GET").Name("GetHotspotWriteRegion")
	apiRouter.HandleFunc("/hotspot/regions/read", mwFunc(hotStatusHandler.GetHotReadRegions)).Methods("GET").Name("GetHotspotReadRegion")
	apiRouter.HandleFunc("/hotspot/regions/history", mwFunc(hotStatusHandler.GetHistoryHotRegions)).Methods("GET").Name("GetHotspotStores")
	apiRouter.HandleFunc("/hotspot/stores", mwFunc(hotStatusHandler.GetHotStores)).Methods("GET").Name("GetHistoryHotspotRegion")

	regionHandler := newRegionHandler(svr, rd)
	clusterRouter.HandleFunc("/region/id/{id}", mwFunc(regionHandler.GetRegionByID)).Methods("GET").Name("GetRegionByID")
	clusterRouter.UseEncodedPath().HandleFunc("/region/key/{key}", mwFunc(regionHandler.GetRegionByKey)).Methods("GET").Name("GetRegion")

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	clusterRouter.HandleFunc("/regions", mwFunc(regionsAllHandler.GetAll)).Methods("GET").Name("GetAllRegions")

	regionsHandler := newRegionsHandler(svr, rd)
	clusterRouter.HandleFunc("/regions/key", mwFunc(regionsHandler.ScanRegions)).Methods("GET").Name("ScanRegions")
	clusterRouter.HandleFunc("/regions/count", mwFunc(regionsHandler.GetRegionCount)).Methods("GET").Name("CountRegions")
	clusterRouter.HandleFunc("/regions/store/{id}", mwFunc(regionsHandler.GetStoreRegions)).Methods("GET").Name("GetRegionsByStore")
	clusterRouter.HandleFunc("/regions/writeflow", mwFunc(regionsHandler.GetTopWriteFlow)).Methods("GET").Name("GetTopWriteRegions")
	clusterRouter.HandleFunc("/regions/readflow", mwFunc(regionsHandler.GetTopReadFlow)).Methods("GET").Name("GetTopReadRegions")
	clusterRouter.HandleFunc("/regions/confver", mwFunc(regionsHandler.GetTopConfVer)).Methods("GET").Name("GetTopConfverRegions")
	clusterRouter.HandleFunc("/regions/version", mwFunc(regionsHandler.GetTopVersion)).Methods("GET").Name("GetTopVersionRegions")
	clusterRouter.HandleFunc("/regions/size", mwFunc(regionsHandler.GetTopSize)).Methods("GET").Name("GetTopSizeRegions")
	clusterRouter.HandleFunc("/regions/check/miss-peer", mwFunc(regionsHandler.GetMissPeerRegions)).Methods("GET").Name("GetMissPeerRegions")
	clusterRouter.HandleFunc("/regions/check/extra-peer", mwFunc(regionsHandler.GetExtraPeerRegions)).Methods("GET").Name("GetExtraPeerRegions")
	clusterRouter.HandleFunc("/regions/check/pending-peer", mwFunc(regionsHandler.GetPendingPeerRegions)).Methods("GET").Name("GetPendingPeerRegions")
	clusterRouter.HandleFunc("/regions/check/down-peer", mwFunc(regionsHandler.GetDownPeerRegions)).Methods("GET").Name("GetDownPeerRegions")
	clusterRouter.HandleFunc("/regions/check/learner-peer", mwFunc(regionsHandler.GetLearnerPeerRegions)).Methods("GET").Name("GetLearnerPeerRegions")
	clusterRouter.HandleFunc("/regions/check/empty-region", mwFunc(regionsHandler.GetEmptyRegion)).Methods("GET").Name("GetEmptyRegion")
	clusterRouter.HandleFunc("/regions/check/offline-peer", mwFunc(regionsHandler.GetOfflinePeer)).Methods("GET").Name("GetOfflinePeer")

	clusterRouter.HandleFunc("/regions/check/hist-size", mwFunc(regionsHandler.GetSizeHistogram)).Methods("GET").Name("GetSizeHistogram")
	clusterRouter.HandleFunc("/regions/check/hist-keys", mwFunc(regionsHandler.GetKeysHistogram)).Methods("GET").Name("GetKeysHistogram")
	clusterRouter.HandleFunc("/regions/sibling/{id}", mwFunc(regionsHandler.GetRegionSiblings)).Methods("GET").Name("GetRegionSiblings")
	clusterRouter.HandleFunc("/regions/accelerate-schedule", mwFunc(regionsHandler.AccelerateRegionsScheduleInRange)).Methods("POST").Name("AccelerateRegionsSchedule")
	clusterRouter.HandleFunc("/regions/scatter", mwFunc(regionsHandler.ScatterRegions)).Methods("POST").Name("ScatterRegions")
	clusterRouter.HandleFunc("/regions/split", mwFunc(regionsHandler.SplitRegions)).Methods("POST").Name("SplitRegions")
	clusterRouter.HandleFunc("/regions/range-holes", mwFunc(regionsHandler.GetRangeHoles)).Methods("GET").Name("GetRangeHoles")
	clusterRouter.HandleFunc("/regions/replicated", mwFunc(regionsHandler.CheckRegionsReplicated)).Methods("GET").Queries("startKey", "{startKey}", "endKey", "{endKey}").Name("CheckRegionsReplicated")

	apiRouter.Handle("/version", mw(newVersionHandler(rd))).Methods("GET").Name("GetVersion")
	apiRouter.Handle("/status", mw(newStatusHandler(svr, rd))).Methods("GET").Name("GetPDStatus")

	memberHandler := newMemberHandler(svr, rd)
	apiRouter.HandleFunc("/members", mwFunc(memberHandler.ListMembers)).Methods("GET").Name("GetMembers")
	apiRouter.HandleFunc("/members/name/{name}", mwFunc(memberHandler.DeleteByName)).Methods("DELETE").Name("DeleteMemberByName")
	apiRouter.HandleFunc("/members/id/{id}", mwFunc(memberHandler.DeleteByID)).Methods("DELETE").Name("DeleteMemberByID")
	apiRouter.HandleFunc("/members/name/{name}", mwFunc(memberHandler.SetMemberPropertyByName)).Methods("POST").Name("SetMemberByName")

	leaderHandler := newLeaderHandler(svr, rd)
	apiRouter.HandleFunc("/leader", mwFunc(leaderHandler.Get)).Methods("GET").Name("GetLeader")
	apiRouter.HandleFunc("/leader/resign", mwFunc(leaderHandler.Resign)).Methods("POST").Name("ResignLeader")
	apiRouter.HandleFunc("/leader/transfer/{next_leader}", mwFunc(leaderHandler.Transfer)).Methods("POST").Name("TransferLeader")

	statsHandler := newStatsHandler(svr, rd)
	clusterRouter.HandleFunc("/stats/region", mwFunc(statsHandler.Region)).Methods("GET").Name("GetRegionStatus")

	trendHandler := newTrendHandler(svr, rd)
	apiRouter.HandleFunc("/trend", mwFunc(trendHandler.Handle)).Methods("GET").Name("GetTrend")

	adminHandler := newAdminHandler(svr, rd)
	clusterRouter.HandleFunc("/admin/cache/region/{id}", mwFunc(adminHandler.HandleDropCacheRegion)).Methods("DELETE").Name("DeleteRegionCache")
	clusterRouter.HandleFunc("/admin/reset-ts", mwFunc(adminHandler.ResetTS)).Methods("POST").Name("ResetTS")
	apiRouter.HandleFunc("/admin/persist-file/{file_name}", mwFunc(adminHandler.persistFile)).Methods("POST").Name("SavePersistFile")
	clusterRouter.HandleFunc("/admin/replication_mode/wait-async", mwFunc(adminHandler.UpdateWaitAsyncTime)).Methods("POST").Name("SetWaitAsyncTime")
	apiRouter.HandleFunc("/admin/service-middleware", mwFunc(adminHandler.HanldeServiceMiddlewareSwitch)).Methods("POST").Name("SwitchServiceMiddleware")

	logHandler := newLogHandler(svr, rd)
	apiRouter.HandleFunc("/admin/log", mwFunc(logHandler.Handle)).Methods("POST").Name("SetLogLevel")

	replicationModeHandler := newReplicationModeHandler(svr, rd)
	clusterRouter.HandleFunc("/replication_mode/status", mwFunc(replicationModeHandler.GetStatus)).Name("GetReplicationModeStatus")

	// Deprecated: component exists for historical compatibility and should not be used anymore. See https://github.com/tikv/tikv/issues/11472.
	componentHandler := newComponentHandler(svr, rd)
	clusterRouter.HandleFunc("/component", componentHandler.Register).Methods("POST")
	clusterRouter.HandleFunc("/component/{component}/{addr}", componentHandler.UnRegister).Methods("DELETE")
	clusterRouter.HandleFunc("/component", componentHandler.GetAllAddress).Methods("GET")
	clusterRouter.HandleFunc("/component/{type}", componentHandler.GetAddress).Methods("GET")

	pluginHandler := newPluginHandler(handler, rd)
	apiRouter.HandleFunc("/plugin", mwFunc(pluginHandler.LoadPlugin)).Methods("POST").Name("SetPlugin")
	apiRouter.HandleFunc("/plugin", mwFunc(pluginHandler.UnloadPlugin)).Methods("DELETE").Name("DeletePlugin")

	apiRouter.Handle("/health", mw(newHealthHandler(svr, rd))).Methods("GET").Name("GetHealthStatus")
	// Deprecated: This API is no longer maintained anymore.
	apiRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	apiRouter.HandleFunc("/ping", mwFunc(func(w http.ResponseWriter, r *http.Request) {})).Methods("GET").Name("Ping")

	// metric query use to query metric data, the protocol is compatible with prometheus.
	apiRouter.Handle("/metric/query", mw(newQueryMetric(svr))).Methods("GET", "POST").Name("QueryMetric")
	apiRouter.Handle("/metric/query_range", mw(newQueryMetric(svr))).Methods("GET", "POST").Name("QueryMetric")

	// tso API
	tsoHandler := newTSOHandler(svr, rd)
	apiRouter.HandleFunc("/tso/allocator/transfer/{name}", mwFunc(tsoHandler.TransferLocalTSOAllocator)).Methods("POST").Name("TransferLocalTSOAllocator")

	// profile API
	apiRouter.HandleFunc("/debug/pprof/profile", mwFunc(pprof.Profile)).Name("DebugPProfProfile")
	apiRouter.HandleFunc("/debug/pprof/trace", mwFunc(pprof.Trace)).Name("DebugPProfTrace")
	apiRouter.HandleFunc("/debug/pprof/symbol", mwFunc(pprof.Symbol)).Name("DebugPProfSymbol")
	apiRouter.Handle("/debug/pprof/heap", mw(pprof.Handler("heap"))).Name("DebugPProfHeap")
	apiRouter.Handle("/debug/pprof/mutex", mw(pprof.Handler("mutex"))).Name("DebugPProfMutex")
	apiRouter.Handle("/debug/pprof/allocs", mw(pprof.Handler("allocs"))).Name("DebugPProfAllocs")
	apiRouter.Handle("/debug/pprof/block", mw(pprof.Handler("block"))).Name("DebugPProfBlock")
	apiRouter.Handle("/debug/pprof/goroutine", mw(pprof.Handler("goroutine"))).Name("DebugPProfGoroutine")
	apiRouter.Handle("/debug/pprof/threadcreate", mw(pprof.Handler("threadcreate"))).Name("DebugPProfThreadCreate")
	apiRouter.Handle("/debug/pprof/zip", mw(newProfHandler(svr, rd))).Name("DebugPProfZip")

	// service GC safepoint API
	serviceGCSafepointHandler := newServiceGCSafepointHandler(svr, rd)
	apiRouter.HandleFunc("/gc/safepoint", mwFunc(serviceGCSafepointHandler.List)).Methods("GET").Name("GetGCSafePoint")
	apiRouter.HandleFunc("/gc/safepoint/{service_id}", mwFunc(serviceGCSafepointHandler.Delete)).Methods("DELETE").Name("DeleteGCSafePoint")

	// unsafe admin operation API
	unsafeOperationHandler := newUnsafeOperationHandler(svr, rd)
	clusterRouter.HandleFunc("/admin/unsafe/remove-failed-stores",
		mwFunc(unsafeOperationHandler.RemoveFailedStores)).Methods("POST").Name("RemoveFailedStoresUnsafely")
	clusterRouter.HandleFunc("/admin/unsafe/remove-failed-stores/show",
		mwFunc(unsafeOperationHandler.GetFailedStoresRemovalStatus)).Methods("GET").Name("GetOngoingFailedStoresRemoval")
	clusterRouter.HandleFunc("/admin/unsafe/remove-failed-stores/history",
		mwFunc(unsafeOperationHandler.GetFailedStoresRemovalHistory)).Methods("GET").Name("GetHistoryFailedStoresRemoval")

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
