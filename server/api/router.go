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
	"github.com/urfave/negroni"
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

// middlewareBuilder is used to build service middleware for HTTP api
type serviceMiddlewareBuilder struct {
	svr     *server.Server
	handler http.Handler
}

func newServiceMiddlewareBuilder(s *server.Server) *serviceMiddlewareBuilder {
	return &serviceMiddlewareBuilder{
		svr: s,
		handler: negroni.New(
			newRequestInfoMiddleware(s),
			newAuditMiddleware(s),
			// todo: add rate limit middleware
		),
	}
}

// registerRouteHandleFunc is used to registers a new route which will be registered matcher or service by opts for the URL path
func (s *serviceMiddlewareBuilder) registerRouteHandleFunc(router *mux.Router, serviceLabel, path string,
	handleFunc func(http.ResponseWriter, *http.Request), opts ...createRouteOption) *mux.Route {
	route := router.HandleFunc(path, s.middlewareFunc(handleFunc)).Name(serviceLabel)
	for _, opt := range opts {
		opt(route)
	}
	return route
}

// registerRouteHandler is used to registers a new route which will be registered matcher or service by opts for the URL path
func (s *serviceMiddlewareBuilder) registerRouteHandler(router *mux.Router, serviceLabel, path string,
	handler http.Handler, opts ...createRouteOption) *mux.Route {
	route := router.Handle(path, s.middleware(handler)).Name(serviceLabel)
	for _, opt := range opts {
		opt(route)
	}
	return route
}

// registerPathPrefixRouteHandler is used to registers a new route which will be registered matcher or service by opts for the URL path prefix.
func (s *serviceMiddlewareBuilder) registerPathPrefixRouteHandler(router *mux.Router, serviceLabel, prefix string,
	handler http.Handler, opts ...createRouteOption) *mux.Route {
	route := router.PathPrefix(prefix).Handler(s.middleware(handler)).Name(serviceLabel)
	for _, opt := range opts {
		opt(route)
	}
	return route
}

func (s *serviceMiddlewareBuilder) middleware(handler http.Handler) http.Handler {
	return negroni.New(negroni.Wrap(s.handler), negroni.Wrap(handler))
}

func (s *serviceMiddlewareBuilder) middlewareFunc(next func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s.handler.ServeHTTP(w, r)
		next(w, r)
	}
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
	setAudit := func(labels ...string) createRouteOption {
		return func(route *mux.Route) {
			svr.RegistServiceForHTTP(route, labels...)
		}
	}

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
	registerFunc(apiRouter, "GetOperators", "/operators", operatorHandler.List, setMethods("GET"))
	registerFunc(apiRouter, "SetOperators", "/operators", operatorHandler.Post, setMethods("POST"))
	registerFunc(apiRouter, "GetRegionOperator", "/operators/{region_id}", operatorHandler.Get, setMethods("GET"))
	registerFunc(apiRouter, "DeleteRegionOperator", "/operators/{region_id}", operatorHandler.Delete, setMethods("DELETE"))

	checkerHandler := newCheckerHandler(svr, rd)
	registerFunc(apiRouter, "SetChecker", "/checker/{name}", checkerHandler.PauseOrResume, setMethods("POST"))
	registerFunc(apiRouter, "GetChecker", "/checker/{name}", checkerHandler.GetStatus, setMethods("GET"))

	schedulerHandler := newSchedulerHandler(svr, rd)
	registerFunc(apiRouter, "GetSchedulers", "/schedulers", schedulerHandler.List, setMethods("GET"))
	registerFunc(apiRouter, "AddScheduler", "/schedulers", schedulerHandler.Post, setMethods("POST"))
	registerFunc(apiRouter, "DeleteScheduler", "/schedulers/{name}", schedulerHandler.Delete, setMethods("DELETE"))
	registerFunc(apiRouter, "PauseOrResumeScheduler", "/schedulers/{name}", schedulerHandler.PauseOrResume, setMethods("POST"))

	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	registerPrefix(apiRouter, "GetSchedulerConfig", "/scheduler-config", schedulerConfigHandler)

	clusterHandler := newClusterHandler(svr, rd)
	register(apiRouter, "GetCluster", "/cluster", clusterHandler, setMethods("GET"))
	registerFunc(apiRouter, "GetClusterStatus", "/cluster/status", clusterHandler.GetClusterStatus)

	confHandler := newConfHandler(svr, rd)
	registerFunc(apiRouter, "GetConfig", "/config", confHandler.Get, setMethods("GET"))
	registerFunc(apiRouter, "SetConfig", "/config", confHandler.Post, setMethods("POST"))
	registerFunc(apiRouter, "GetDefaultConfig", "/config/default", confHandler.GetDefault, setMethods("GET"))
	registerFunc(apiRouter, "GetScheduleConfig", "/config/schedule", confHandler.GetSchedule, setMethods("GET"))
	registerFunc(apiRouter, "SetScheduleConfig", "/config/schedule", confHandler.SetSchedule, setMethods("POST"))
	registerFunc(apiRouter, "GetPDServerConfig", "/config/pd-server", confHandler.GetPDServer, setMethods("GET"))
	registerFunc(apiRouter, "GetReplicationConfig", "/config/replicate", confHandler.GetReplication, setMethods("GET"))
	registerFunc(apiRouter, "SetReplicationConfig", "/config/replicate", confHandler.SetReplication, setMethods("POST"))
	registerFunc(apiRouter, "GetLabelProperty", "/config/label-property", confHandler.GetLabelProperty, setMethods("GET"))
	registerFunc(apiRouter, "SetLabelProperty", "/config/label-property", confHandler.SetLabelProperty, setMethods("POST"))
	registerFunc(apiRouter, "GetClusterVersion", "/config/cluster-version", confHandler.GetClusterVersion, setMethods("GET"))
	registerFunc(apiRouter, "SetClusterVersion", "/config/cluster-version", confHandler.SetClusterVersion, setMethods("POST"))
	registerFunc(apiRouter, "GetReplicationMode", "/config/replication-mode", confHandler.GetReplicationMode, setMethods("GET"))
	registerFunc(apiRouter, "SetReplicationMode", "/config/replication-mode", confHandler.SetReplicationMode, setMethods("POST"))

	rulesHandler := newRulesHandler(svr, rd)
	registerFunc(clusterRouter, "GetAllRules", "/config/rules", rulesHandler.GetAll, setMethods("GET"))
	registerFunc(clusterRouter, "SetAllRules", "/config/rules", rulesHandler.SetAll, setMethods("POST"))
	registerFunc(clusterRouter, "SetBatchRules", "/config/rules/batch", rulesHandler.Batch, setMethods("POST"))
	registerFunc(clusterRouter, "GetRuleByGroup", "/config/rules/group/{group}", rulesHandler.GetAllByGroup, setMethods("GET"))
	registerFunc(clusterRouter, "GetRuleByByRegion", "/config/rules/region/{region}", rulesHandler.GetAllByRegion, setMethods("GET"))
	registerFunc(clusterRouter, "GetRuleByKey", "/config/rules/key/{key}", rulesHandler.GetAllByKey, setMethods("GET"))
	registerFunc(clusterRouter, "GetRuleByGroupAndID", "/config/rule/{group}/{id}", rulesHandler.Get, setMethods("GET"))
	registerFunc(clusterRouter, "SetRule", "/config/rule", rulesHandler.Set, setMethods("POST"))
	registerFunc(clusterRouter, "DeleteRuleByGroup", "/config/rule/{group}/{id}", rulesHandler.Delete, setMethods("DELETE"))

	regionLabelHandler := newRegionLabelHandler(svr, rd)
	registerFunc(clusterRouter, "GetAllRegionLabelRule", "/config/region-label/rules", regionLabelHandler.GetAllRules, setMethods("GET"))
	registerFunc(clusterRouter, "GetRegionLabelRulesByIDs", "/config/region-label/rules/ids", regionLabelHandler.GetRulesByIDs, setMethods("GET"))
	// {id} can be a string with special characters, we should enable path encode to support it.
	registerFunc(escapeRouter, "GetRegionLabelRuleByID", "/config/region-label/rule/{id}", regionLabelHandler.GetRule, setMethods("GET"))
	registerFunc(escapeRouter, "DeleteRegionLabelRule", "/config/region-label/rule/{id}", regionLabelHandler.DeleteRule, setMethods("DELETE"))
	registerFunc(clusterRouter, "SetRegionLabelRule", "/config/region-label/rule", regionLabelHandler.SetRule, setMethods("POST"))
	registerFunc(clusterRouter, "PatchRegionLabelRules", "/config/region-label/rules", regionLabelHandler.Patch, setMethods("PATCH"))

	registerFunc(clusterRouter, "GetRegionLabelByKey", "/region/id/{id}/label/{key}", regionLabelHandler.GetRegionLabel, setMethods("GET"))
	registerFunc(clusterRouter, "GetAllRegionLabels", "/region/id/{id}/labels", regionLabelHandler.GetRegionLabels, setMethods("GET"))

	registerFunc(clusterRouter, "GetRuleGroup", "/config/rule_group/{id}", rulesHandler.GetGroupConfig, setMethods("GET"))
	registerFunc(clusterRouter, "SetRuleGroup", "/config/rule_group", rulesHandler.SetGroupConfig, setMethods("POST"))
	registerFunc(clusterRouter, "DeleteRuleGroup", "/config/rule_group/{id}", rulesHandler.DeleteGroupConfig, setMethods("DELETE"))
	registerFunc(clusterRouter, "GetAllRuleGroups", "/config/rule_groups", rulesHandler.GetAllGroupConfigs, setMethods("GET"))

	registerFunc(clusterRouter, "GetAllPlacementRules", "/config/placement-rule", rulesHandler.GetAllGroupBundles, setMethods("GET"))
	registerFunc(clusterRouter, "SetAllPlacementRules", "/config/placement-rule", rulesHandler.SetAllGroupBundles, setMethods("POST"))
	// {group} can be a regular expression, we should enable path encode to
	// support special characters.
	registerFunc(clusterRouter, "GetPlacementRuleByGroup", "/config/placement-rule/{group}", rulesHandler.GetGroupBundle, setMethods("GET"))
	registerFunc(clusterRouter, "SetPlacementRuleByGroup", "/config/placement-rule/{group}", rulesHandler.SetGroupBundle, setMethods("POST"))
	registerFunc(escapeRouter, "DeletePlacementRuleByGroup", "/config/placement-rule/{group}", rulesHandler.DeleteGroupBundle, setMethods("DELETE"))

	storeHandler := newStoreHandler(handler, rd)
	registerFunc(clusterRouter, "GetStore", "/store/{id}", storeHandler.Get, setMethods("GET"))
	registerFunc(clusterRouter, "DeleteStore", "/store/{id}", storeHandler.Delete, setMethods("DELETE"))
	registerFunc(clusterRouter, "SetStoreState", "/store/{id}/state", storeHandler.SetState, setMethods("POST"))
	registerFunc(clusterRouter, "SetStoreLabel", "/store/{id}/label", storeHandler.SetLabels, setMethods("POST"))
	registerFunc(clusterRouter, "SetStoreWeight", "/store/{id}/weight", storeHandler.SetWeight, setMethods("POST"))
	registerFunc(clusterRouter, "SetStoreLimit", "/store/{id}/limit", storeHandler.SetLimit, setMethods("POST"))

	storesHandler := newStoresHandler(handler, rd)
	register(clusterRouter, "GetAllStores", "/stores", storesHandler, setMethods("GET"))
	registerFunc(clusterRouter, "RemoveTombstone", "/stores/remove-tombstone", storesHandler.RemoveTombStone, setMethods("DELETE"))
	registerFunc(clusterRouter, "GetAllStoresLimit", "/stores/limit", storesHandler.GetAllLimit, setMethods("GET"))
	registerFunc(clusterRouter, "SetAllStoresLimit", "/stores/limit", storesHandler.SetAllLimit, setMethods("POST"))
	registerFunc(clusterRouter, "SetStoreSceneLimit", "/stores/limit/scene", storesHandler.SetStoreLimitScene, setMethods("POST"))
	registerFunc(clusterRouter, "GetStoreSceneLimit", "/stores/limit/scene", storesHandler.GetStoreLimitScene, setMethods("GET"))

	labelsHandler := newLabelsHandler(svr, rd)
	registerFunc(clusterRouter, "GetLabels", "/labels", labelsHandler.Get, setMethods("GET"))
	registerFunc(clusterRouter, "GetStoresByLabel", "/labels/stores", labelsHandler.GetStores, setMethods("GET"))

	hotStatusHandler := newHotStatusHandler(handler, rd)
	registerFunc(apiRouter, "GetHotspotWriteRegion", "/hotspot/regions/write", hotStatusHandler.GetHotWriteRegions, setMethods("GET"))
	registerFunc(apiRouter, "GetHotspotReadRegion", "/hotspot/regions/read", hotStatusHandler.GetHotReadRegions, setMethods("GET"))
	registerFunc(apiRouter, "GetHotspotStores", "/hotspot/regions/history", hotStatusHandler.GetHistoryHotRegions, setMethods("GET"))
	registerFunc(apiRouter, "GetHistoryHotspotRegion", "/hotspot/stores", hotStatusHandler.GetHotStores, setMethods("GET"))

	regionHandler := newRegionHandler(svr, rd)
	registerFunc(clusterRouter, "GetRegionByID", "/region/id/{id}", regionHandler.GetRegionByID, setMethods("GET"))
	registerFunc(clusterRouter.UseEncodedPath(), "GetRegion", "/region/key/{key}", regionHandler.GetRegionByKey, setMethods("GET"))

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	registerFunc(clusterRouter, "GetAllRegions", "/regions", regionsAllHandler.GetAll, setMethods("GET"))

	regionsHandler := newRegionsHandler(svr, rd)
	registerFunc(clusterRouter, "ScanRegions", "/regions/key", regionsHandler.ScanRegions, setMethods("GET"))
	registerFunc(clusterRouter, "CountRegions", "/regions/count", regionsHandler.GetRegionCount, setMethods("GET"))
	registerFunc(clusterRouter, "GetRegionsByStore", "/regions/store/{id}", regionsHandler.GetStoreRegions, setMethods("GET"))
	registerFunc(clusterRouter, "GetTopWriteRegions", "/regions/writeflow", regionsHandler.GetTopWriteFlow, setMethods("GET"))
	registerFunc(clusterRouter, "GetTopReadRegions", "/regions/readflow", regionsHandler.GetTopReadFlow, setMethods("GET"))
	registerFunc(clusterRouter, "GetTopConfverRegions", "/regions/confver", regionsHandler.GetTopConfVer, setMethods("GET"))
	registerFunc(clusterRouter, "GetTopVersionRegions", "/regions/version", regionsHandler.GetTopVersion, setMethods("GET"))
	registerFunc(clusterRouter, "GetTopSizeRegions", "/regions/size", regionsHandler.GetTopSize, setMethods("GET"))
	registerFunc(clusterRouter, "GetMissPeerRegions", "/regions/check/miss-peer", regionsHandler.GetMissPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "GetExtraPeerRegions", "/regions/check/extra-peer", regionsHandler.GetExtraPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "GetPendingPeerRegions", "/regions/check/pending-peer", regionsHandler.GetPendingPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "GetDownPeerRegions", "/regions/check/down-peer", regionsHandler.GetDownPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "GetLearnerPeerRegions", "/regions/check/learner-peer", regionsHandler.GetLearnerPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "GetEmptyRegion", "/regions/check/empty-region", regionsHandler.GetEmptyRegion, setMethods("GET"))
	registerFunc(clusterRouter, "GetOfflinePeer", "/regions/check/offline-peer", regionsHandler.GetOfflinePeer, setMethods("GET"))

	registerFunc(clusterRouter, "GetSizeHistogram", "/regions/check/hist-size", regionsHandler.GetSizeHistogram, setMethods("GET"))
	registerFunc(clusterRouter, "GetKeysHistogram", "/regions/check/hist-keys", regionsHandler.GetKeysHistogram, setMethods("GET"))
	registerFunc(clusterRouter, "GetRegionSiblings", "/regions/sibling/{id}", regionsHandler.GetRegionSiblings, setMethods("GET"))
	registerFunc(clusterRouter, "AccelerateRegionsSchedule", "/regions/accelerate-schedule", regionsHandler.AccelerateRegionsScheduleInRange, setMethods("POST"))
	registerFunc(clusterRouter, "ScatterRegions", "/regions/scatter", regionsHandler.ScatterRegions, setMethods("POST"))
	registerFunc(clusterRouter, "SplitRegions", "/regions/split", regionsHandler.SplitRegions, setMethods("POST"))
	registerFunc(clusterRouter, "GetRangeHoles", "/regions/range-holes", regionsHandler.GetRangeHoles, setMethods("GET"))
	registerFunc(clusterRouter, "CheckRegionsReplicated", "/regions/replicated", regionsHandler.CheckRegionsReplicated, setMethods("GET"), setQueries("startKey", "{startKey}", "endKey", "{endKey}"))

	register(apiRouter, "GetVersion", "/version", newVersionHandler(rd), setMethods("GET"))
	register(apiRouter, "GetPDStatus", "/status", newStatusHandler(svr, rd), setMethods("GET"))

	memberHandler := newMemberHandler(svr, rd)
	registerFunc(apiRouter, "GetMembers", "/members", memberHandler.ListMembers, setMethods("GET"))
	registerFunc(apiRouter, "DeleteMemberByName", "/members/name/{name}", memberHandler.DeleteByName, setMethods("DELETE"))
	registerFunc(apiRouter, "DeleteMemberByID", "/members/id/{id}", memberHandler.DeleteByID, setMethods("DELETE"))
	registerFunc(apiRouter, "SetMemberByName", "/members/name/{name}", memberHandler.SetMemberPropertyByName, setMethods("POST"))

	leaderHandler := newLeaderHandler(svr, rd)
	registerFunc(apiRouter, "GetLeader", "/leader", leaderHandler.Get, setMethods("GET"))
	registerFunc(apiRouter, "ResignLeader", "/leader/resign", leaderHandler.Resign, setMethods("POST"))
	registerFunc(apiRouter, "TransferLeader", "/leader/transfer/{next_leader}", leaderHandler.Transfer, setMethods("POST"))

	statsHandler := newStatsHandler(svr, rd)
	registerFunc(clusterRouter, "GetRegionStatus", "/stats/region", statsHandler.Region, setMethods("GET"))

	trendHandler := newTrendHandler(svr, rd)
	registerFunc(apiRouter, "GetTrend", "/trend", trendHandler.Handle, setMethods("GET"))

	adminHandler := newAdminHandler(svr, rd)
	registerFunc(clusterRouter, "DeleteRegionCache", "/admin/cache/region/{id}", adminHandler.HandleDropCacheRegion, setMethods("DELETE"))
	registerFunc(clusterRouter, "ResetTS", "/admin/reset-ts", adminHandler.ResetTS, setMethods("POST"))
	registerFunc(apiRouter, "SavePersistFile", "/admin/persist-file/{file_name}", adminHandler.persistFile, setMethods("POST"))
	registerFunc(clusterRouter, "SetWaitAsyncTime", "/admin/replication_mode/wait-async", adminHandler.UpdateWaitAsyncTime, setMethods("POST"))
	registerFunc(apiRouter, "SwitchServiceMiddleware", "/admin/service-middleware", adminHandler.HanldeServiceMiddlewareSwitch, setMethods("POST"))
	registerFunc(apiRouter, "SwitchAuditMiddleware", "/admin/audit-middleware", adminHandler.HanldeAuditMiddlewareSwitch, setMethods("POST"))

	logHandler := newLogHandler(svr, rd)
	registerFunc(apiRouter, "SetLogLevel", "/admin/log", logHandler.Handle, setMethods("POST"))

	replicationModeHandler := newReplicationModeHandler(svr, rd)
	registerFunc(clusterRouter, "GetReplicationModeStatus", "/replication_mode/status", replicationModeHandler.GetStatus)

	// Deprecated: component exists for historical compatibility and should not be used anymore. See https://github.com/tikv/tikv/issues/11472.
	componentHandler := newComponentHandler(svr, rd)
	clusterRouter.HandleFunc("/component", componentHandler.Register).Methods("POST")
	clusterRouter.HandleFunc("/component/{component}/{addr}", componentHandler.UnRegister).Methods("DELETE")
	clusterRouter.HandleFunc("/component", componentHandler.GetAllAddress).Methods("GET")
	clusterRouter.HandleFunc("/component/{type}", componentHandler.GetAddress).Methods("GET")

	pluginHandler := newPluginHandler(handler, rd)
	registerFunc(apiRouter, "SetPlugin", "/plugin", pluginHandler.LoadPlugin, setMethods("POST"))
	registerFunc(apiRouter, "DeletePlugin", "/plugin", pluginHandler.UnloadPlugin, setMethods("DELETE"))

	register(apiRouter, "GetHealthStatus", "/health", newHealthHandler(svr, rd), setMethods("GET"))
	// Deprecated: This API is no longer maintained anymore.
	apiRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	registerFunc(apiRouter, "Ping", "/ping", func(w http.ResponseWriter, r *http.Request) {}, setMethods("GET"))

	// metric query use to query metric data, the protocol is compatible with prometheus.
	register(apiRouter, "QueryMetric", "/metric/query", newQueryMetric(svr), setMethods("GET", "POST"))
	register(apiRouter, "QueryMetric", "/metric/query_range", newQueryMetric(svr), setMethods("GET", "POST"))

	// tso API
	tsoHandler := newTSOHandler(svr, rd)
	registerFunc(apiRouter, "TransferLocalTSOAllocator", "/tso/allocator/transfer/{name}", tsoHandler.TransferLocalTSOAllocator, setMethods("POST"))

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
	registerFunc(apiRouter, "GetGCSafePoint", "/gc/safepoint", serviceGCSafepointHandler.List, setMethods("GET"))
	registerFunc(apiRouter, "DeleteGCSafePoint", "/gc/safepoint/{service_id}", serviceGCSafepointHandler.Delete, setMethods("DELETE"))

	// unsafe admin operation API
	unsafeOperationHandler := newUnsafeOperationHandler(svr, rd)
	registerFunc(clusterRouter, "RemoveFailedStoresUnsafely", "/admin/unsafe/remove-failed-stores",
		unsafeOperationHandler.RemoveFailedStores, setMethods("POST"))
	registerFunc(clusterRouter, "GetOngoingFailedStoresRemoval", "/admin/unsafe/remove-failed-stores/show",
		unsafeOperationHandler.GetFailedStoresRemovalStatus, setMethods("GET"))
	registerFunc(clusterRouter, "GetHistoryFailedStoresRemoval", "/admin/unsafe/remove-failed-stores/history",
		unsafeOperationHandler.GetFailedStoresRemovalHistory, setMethods("GET"))

	// API to set or unset failpoints
	failpoint.Inject("enableFailpointAPI", func() {
		registerPrefix(apiRouter, "failpoint", "/fail", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The HTTP handler of failpoint requires the full path to be the failpoint path.
			r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix+apiPrefix+"/fail")
			new(failpoint.HttpHandler).ServeHTTP(w, r)
		}), setAudit("test"))
	})

	// Deprecated: use /pd/api/v1/health instead.
	rootRouter.Handle("/health", newHealthHandler(svr, rd)).Methods("GET")
	// Deprecated: use /pd/api/v1/diagnose instead.
	rootRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	// Deprecated: use /pd/api/v1/ping instead.
	rootRouter.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")

	return rootRouter
}
