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
	"reflect"
	"runtime"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/audit"
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

// routeCreateFunc is used to registers a new route which will be registered matcher or service by opts for the URL path
func routeCreateFunc(route *mux.Route, handler http.Handler, name string, opts ...createRouteOption) {
	route = route.Handler(handler).Name(name)
	for _, opt := range opts {
		opt(route)
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

func getFunctionName(f interface{}) string {
	strs := strings.Split(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), ".")
	return strings.Split(strs[len(strs)-1], "-")[0]
}

// The returned function is used as a lazy router to avoid the data race problem.
// @title          Placement Driver Core API
// @version        1.0
// @description    This is placement driver.
// @contact.name   Placement Driver Support
// @contact.url    https://github.com/tikv/pd/issues
// @contact.email  info@pingcap.com
// @license.name   Apache 2.0
// @license.url    http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath       /pd/api/v1
func createRouter(prefix string, svr *server.Server) *mux.Router {
	serviceMiddle := newServiceMiddlewareBuilder(svr)
	registerPrefix := func(router *mux.Router, prefixPath string,
		handleFunc func(http.ResponseWriter, *http.Request), opts ...createRouteOption) {
		routeCreateFunc(router.PathPrefix(prefixPath), serviceMiddle.createHandler(handleFunc),
			getFunctionName(handleFunc), opts...)
	}
	registerFunc := func(router *mux.Router, path string,
		handleFunc func(http.ResponseWriter, *http.Request), opts ...createRouteOption) {
		routeCreateFunc(router.Path(path), serviceMiddle.createHandler(handleFunc),
			getFunctionName(handleFunc), opts...)
	}

	setAuditBackend := func(labels ...string) createRouteOption {
		return func(route *mux.Route) {
			if len(labels) > 0 {
				svr.SetServiceAuditBackendLabels(route.GetName(), labels)
			}
		}
	}

	localLog := audit.LocalLogLabel
	// Please don't use PrometheusHistogram in the hot path.
	prometheus := audit.PrometheusHistogram

	rd := createIndentRender()
	rootRouter := mux.NewRouter().PathPrefix(prefix).Subrouter()
	handler := svr.GetHandler()

	apiPrefix := "/api/v1"
	apiRouter := rootRouter.PathPrefix(apiPrefix).Subrouter()

	clusterRouter := apiRouter.NewRoute().Subrouter()
	clusterRouter.Use(newClusterMiddleware(svr).Middleware)

	escapeRouter := clusterRouter.NewRoute().Subrouter().UseEncodedPath()

	operatorHandler := newOperatorHandler(handler, rd)
	registerFunc(apiRouter, "/operators", operatorHandler.GetOperators, setMethods("GET"))
	registerFunc(apiRouter, "/operators", operatorHandler.CreateOperator, setMethods("POST"), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/operators/records", operatorHandler.GetOperatorRecords, setMethods("GET"))
	registerFunc(apiRouter, "/operators/{region_id}", operatorHandler.GetOperatorsByRegion, setMethods("GET"))
	registerFunc(apiRouter, "/operators/{region_id}", operatorHandler.DeleteOperatorByRegion, setMethods("DELETE"))

	checkerHandler := newCheckerHandler(svr, rd)
	registerFunc(apiRouter, "/checker/{name}", checkerHandler.PauseOrResumeChecker, setMethods("POST"))
	registerFunc(apiRouter, "/checker/{name}", checkerHandler.GetCheckerStatus, setMethods("GET"))

	schedulerHandler := newSchedulerHandler(svr, rd)
	registerFunc(apiRouter, "/schedulers", schedulerHandler.GetSchedulers, setMethods("GET"))
	registerFunc(apiRouter, "/schedulers", schedulerHandler.CreateScheduler, setMethods("POST"))
	registerFunc(apiRouter, "/schedulers/{name}", schedulerHandler.DeleteScheduler, setMethods("DELETE"))
	registerFunc(apiRouter, "/schedulers/{name}", schedulerHandler.PauseOrResumeScheduler, setMethods("POST"))

	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	registerPrefix(apiRouter, "/scheduler-config", schedulerConfigHandler.GetSchedulerConfig)

	clusterHandler := newClusterHandler(svr, rd)
	registerFunc(apiRouter, "/cluster", clusterHandler.GetCluster, setMethods("GET"))
	registerFunc(apiRouter, "/cluster/status", clusterHandler.GetClusterStatus)

	confHandler := newConfHandler(svr, rd)
	registerFunc(apiRouter, "/config", confHandler.GetConfig, setMethods("GET"))
	registerFunc(apiRouter, "/config", confHandler.SetConfig, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/config/default", confHandler.GetDefaultConfig, setMethods("GET"))
	registerFunc(apiRouter, "/config/schedule", confHandler.GetScheduleConfig, setMethods("GET"))
	registerFunc(apiRouter, "/config/schedule", confHandler.SetScheduleConfig, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/config/pd-server", confHandler.GetPDServerConfig, setMethods("GET"))
	registerFunc(apiRouter, "/config/replicate", confHandler.GetReplicationConfig, setMethods("GET"))
	registerFunc(apiRouter, "/config/replicate", confHandler.SetReplicationConfig, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/config/label-property", confHandler.GetLabelPropertyConfig, setMethods("GET"))
	registerFunc(apiRouter, "/config/label-property", confHandler.SetLabelPropertyConfig, setMethods("POST"))
	registerFunc(apiRouter, "/config/cluster-version", confHandler.GetClusterVersion, setMethods("GET"))
	registerFunc(apiRouter, "/config/cluster-version", confHandler.SetClusterVersion, setMethods("POST"))
	registerFunc(apiRouter, "/config/replication-mode", confHandler.GetReplicationModeConfig, setMethods("GET"))
	registerFunc(apiRouter, "/config/replication-mode", confHandler.SetReplicationModeConfig, setMethods("POST"))

	rulesHandler := newRulesHandler(svr, rd)
	registerFunc(clusterRouter, "/config/rules", rulesHandler.GetAllRules, setMethods("GET"))
	registerFunc(clusterRouter, "/config/rules", rulesHandler.SetAllRules, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rules/batch", rulesHandler.BatchRules, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rules/group/{group}", rulesHandler.GetRuleByGroup, setMethods("GET"))
	registerFunc(clusterRouter, "/config/rules/region/{region}", rulesHandler.GetRulesByRegion, setMethods("GET"))
	registerFunc(clusterRouter, "/config/rules/key/{key}", rulesHandler.GetRulesByKey, setMethods("GET"))
	registerFunc(clusterRouter, "/config/rule/{group}/{id}", rulesHandler.GetRuleByGroupAndID, setMethods("GET"))
	registerFunc(clusterRouter, "/config/rule", rulesHandler.SetRule, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rule/{group}/{id}", rulesHandler.DeleteRuleByGroup, setMethods("DELETE"), setAuditBackend(localLog))

	registerFunc(clusterRouter, "/config/rule_group/{id}", rulesHandler.GetGroupConfig, setMethods("GET"))
	registerFunc(clusterRouter, "/config/rule_group", rulesHandler.SetGroupConfig, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rule_group/{id}", rulesHandler.DeleteGroupConfig, setMethods("DELETE"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rule_groups", rulesHandler.GetAllGroupConfigs, setMethods("GET"))

	registerFunc(clusterRouter, "/config/placement-rule", rulesHandler.GetPlacementRules, setMethods("GET"))
	registerFunc(clusterRouter, "/config/placement-rule", rulesHandler.SetPlacementRules, setMethods("POST"), setAuditBackend(localLog))
	// {group} can be a regular expression, we should enable path encode to
	// support special characters.
	registerFunc(clusterRouter, "/config/placement-rule/{group}", rulesHandler.GetPlacementRuleByGroup, setMethods("GET"))
	registerFunc(clusterRouter, "/config/placement-rule/{group}", rulesHandler.SetPlacementRuleByGroup, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(escapeRouter, "/config/placement-rule/{group}", rulesHandler.DeletePlacementRuleByGroup, setMethods("DELETE"), setAuditBackend(localLog))

	regionLabelHandler := newRegionLabelHandler(svr, rd)
	registerFunc(clusterRouter, "/config/region-label/rules", regionLabelHandler.GetAllRegionLabelRules, setMethods("GET"))
	registerFunc(clusterRouter, "/config/region-label/rules/ids", regionLabelHandler.GetRegionLabelRulesByIDs, setMethods("GET"))
	// {id} can be a string with special characters, we should enable path encode to support it.
	registerFunc(escapeRouter, "/config/region-label/rule/{id}", regionLabelHandler.GetRegionLabelRuleByID, setMethods("GET"))
	registerFunc(escapeRouter, "/config/region-label/rule/{id}", regionLabelHandler.DeleteRegionLabelRule, setMethods("DELETE"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/region-label/rule", regionLabelHandler.SetRegionLabelRule, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/region-label/rules", regionLabelHandler.PatchRegionLabelRules, setMethods("PATCH"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/region/id/{id}/label/{key}", regionLabelHandler.GetRegionLabelByKey, setMethods("GET"))
	registerFunc(clusterRouter, "/region/id/{id}/labels", regionLabelHandler.GetRegionLabels, setMethods("GET"))

	storeHandler := newStoreHandler(handler, rd)
	registerFunc(clusterRouter, "/store/{id}", storeHandler.GetStore, setMethods("GET"))
	registerFunc(clusterRouter, "/store/{id}", storeHandler.DeleteStore, setMethods("DELETE"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/state", storeHandler.SetStoreState, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/label", storeHandler.SetStoreLabel, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/weight", storeHandler.SetStoreWeight, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/limit", storeHandler.SetStoreLimit, setMethods("POST"), setAuditBackend(localLog))

	storesHandler := newStoresHandler(handler, rd)
	registerFunc(clusterRouter, "/stores", storesHandler.GetStores, setMethods("GET"))
	registerFunc(clusterRouter, "/stores/remove-tombstone", storesHandler.RemoveTombStone, setMethods("DELETE"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/stores/limit", storesHandler.GetAllStoresLimit, setMethods("GET"))
	registerFunc(clusterRouter, "/stores/limit", storesHandler.SetAllStoresLimit, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/stores/limit/scene", storesHandler.SetStoreLimitScene, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/stores/limit/scene", storesHandler.GetStoreLimitScene, setMethods("GET"))
	registerFunc(clusterRouter, "/stores/progress", storesHandler.GetStoresProgress, setMethods("GET"))

	labelsHandler := newLabelsHandler(svr, rd)
	registerFunc(clusterRouter, "/labels", labelsHandler.GetLabels, setMethods("GET"))
	registerFunc(clusterRouter, "/labels/stores", labelsHandler.GetStoresByLabel, setMethods("GET"))

	hotStatusHandler := newHotStatusHandler(handler, rd)
	registerFunc(apiRouter, "/hotspot/regions/write", hotStatusHandler.GetHotWriteRegions, setMethods("GET"), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/regions/read", hotStatusHandler.GetHotReadRegions, setMethods("GET"), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/regions/history", hotStatusHandler.GetHistoryHotRegions, setMethods("GET"), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/stores", hotStatusHandler.GetHotStores, setMethods("GET"), setAuditBackend(prometheus))

	regionHandler := newRegionHandler(svr, rd)
	registerFunc(clusterRouter, "/region/id/{id}", regionHandler.GetRegionByID, setMethods("GET"), setAuditBackend(prometheus))
	registerFunc(clusterRouter.UseEncodedPath(), "/region/key/{key}", regionHandler.GetRegion, setMethods("GET"), setAuditBackend(prometheus))

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	registerFunc(clusterRouter, "/regions", regionsAllHandler.GetRegions, setMethods("GET"), setAuditBackend(prometheus))

	regionsHandler := newRegionsHandler(svr, rd)
	registerFunc(clusterRouter, "/regions/key", regionsHandler.ScanRegions, setMethods("GET"), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/count", regionsHandler.GetRegionCount, setMethods("GET"), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/store/{id}", regionsHandler.GetStoreRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/writeflow", regionsHandler.GetTopWriteFlowRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/readflow", regionsHandler.GetTopReadFlowRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/confver", regionsHandler.GetTopConfVerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/version", regionsHandler.GetTopVersionRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/size", regionsHandler.GetTopSizeRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/miss-peer", regionsHandler.GetMissPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/extra-peer", regionsHandler.GetExtraPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/pending-peer", regionsHandler.GetPendingPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/down-peer", regionsHandler.GetDownPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/learner-peer", regionsHandler.GetLearnerPeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/empty-region", regionsHandler.GetEmptyRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/offline-peer", regionsHandler.GetOfflinePeerRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/oversized-region", regionsHandler.GetOverSizedRegions, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/undersized-region", regionsHandler.GetUndersizedRegions, setMethods("GET"))

	registerFunc(clusterRouter, "/regions/check/hist-size", regionsHandler.GetSizeHistogram, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/check/hist-keys", regionsHandler.GetKeysHistogram, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/sibling/{id}", regionsHandler.GetRegionSiblings, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/accelerate-schedule", regionsHandler.AccelerateRegionsScheduleInRange, setMethods("POST"), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/scatter", regionsHandler.ScatterRegions, setMethods("POST"), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/split", regionsHandler.SplitRegions, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/regions/range-holes", regionsHandler.GetRangeHoles, setMethods("GET"))
	registerFunc(clusterRouter, "/regions/replicated", regionsHandler.CheckRegionsReplicated, setMethods("GET"), setQueries("startKey", "{startKey}", "endKey", "{endKey}"))

	registerFunc(apiRouter, "/version", newVersionHandler(rd).GetVersion, setMethods("GET"))
	registerFunc(apiRouter, "/status", newStatusHandler(svr, rd).GetPDStatus, setMethods("GET"))

	memberHandler := newMemberHandler(svr, rd)
	registerFunc(apiRouter, "/members", memberHandler.GetMembers, setMethods("GET"))
	registerFunc(apiRouter, "/members/name/{name}", memberHandler.DeleteMemberByName, setMethods("DELETE"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/members/id/{id}", memberHandler.DeleteMemberByID, setMethods("DELETE"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/members/name/{name}", memberHandler.SetMemberPropertyByName, setMethods("POST"), setAuditBackend(localLog))

	leaderHandler := newLeaderHandler(svr, rd)
	registerFunc(apiRouter, "/leader", leaderHandler.GetLeader, setMethods("GET"))
	registerFunc(apiRouter, "/leader/resign", leaderHandler.ResignLeader, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/leader/transfer/{next_leader}", leaderHandler.TransferLeader, setMethods("POST"), setAuditBackend(localLog))

	statsHandler := newStatsHandler(svr, rd)
	registerFunc(clusterRouter, "/stats/region", statsHandler.GetRegionStatus, setMethods("GET"))

	trendHandler := newTrendHandler(svr, rd)
	registerFunc(apiRouter, "/trend", trendHandler.GetTrend, setMethods("GET"), setAuditBackend(prometheus))

	adminHandler := newAdminHandler(svr, rd)
	registerFunc(clusterRouter, "/admin/cache/region/{id}", adminHandler.DeleteRegionCache, setMethods("DELETE"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/admin/reset-ts", adminHandler.ResetTS, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/admin/persist-file/{file_name}", adminHandler.SavePersistFile, setMethods("POST"), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/admin/replication_mode/wait-async", adminHandler.UpdateWaitAsyncTime, setMethods("POST"), setAuditBackend(localLog))

	serviceMiddlewareHandler := newServiceMiddlewareHandler(svr, rd)
	registerFunc(apiRouter, "/service-middleware/config", serviceMiddlewareHandler.GetServiceMiddlewareConfig, setMethods("GET"))
	registerFunc(apiRouter, "/service-middleware/config", serviceMiddlewareHandler.SetServiceMiddlewareConfig, setMethods("POST"), setAuditBackend(localLog))

	logHandler := newLogHandler(svr, rd)
	registerFunc(apiRouter, "/admin/log", logHandler.SetLogLevel, setMethods("POST"), setAuditBackend(localLog))
	replicationModeHandler := newReplicationModeHandler(svr, rd)
	registerFunc(clusterRouter, "/replication_mode/status", replicationModeHandler.GetReplicationModeStatus)

	pluginHandler := newPluginHandler(handler, rd)
	registerFunc(apiRouter, "/plugin", pluginHandler.LoadPlugin, setMethods("POST"))
	registerFunc(apiRouter, "/plugin", pluginHandler.UnloadPlugin, setMethods("DELETE"))

	healthHandler := newHealthHandler(svr, rd)
	registerFunc(apiRouter, "/health", healthHandler.GetHealthStatus, setMethods("GET"))
	registerFunc(apiRouter, "/ping", healthHandler.Ping, setMethods("GET"))

	// metric query use to query metric data, the protocol is compatible with prometheus.
	registerFunc(apiRouter, "/metric/query", newQueryMetric(svr).QueryMetric, setMethods("GET", "POST"))
	registerFunc(apiRouter, "/metric/query_range", newQueryMetric(svr).QueryMetric, setMethods("GET", "POST"))

	// tso API
	tsoHandler := newTSOHandler(svr, rd)
	registerFunc(apiRouter, "/tso/allocator/transfer/{name}", tsoHandler.TransferLocalTSOAllocator, setMethods("POST"), setAuditBackend(localLog))

	pprofHandler := newPprofHandler(svr, rd)
	// profile API
	registerFunc(apiRouter, "/debug/pprof/profile", pprof.Profile)
	registerFunc(apiRouter, "/debug/pprof/trace", pprof.Trace)
	registerFunc(apiRouter, "/debug/pprof/symbol", pprof.Symbol)
	registerFunc(apiRouter, "/debug/pprof/heap", pprofHandler.PProfHeap)
	registerFunc(apiRouter, "/debug/pprof/mutex", pprofHandler.PProfMutex)
	registerFunc(apiRouter, "/debug/pprof/allocs", pprofHandler.PProfAllocs)
	registerFunc(apiRouter, "/debug/pprof/block", pprofHandler.PProfBlock)
	registerFunc(apiRouter, "/debug/pprof/goroutine", pprofHandler.PProfGoroutine)
	registerFunc(apiRouter, "/debug/pprof/threadcreate", pprofHandler.PProfThreadcreate)
	registerFunc(apiRouter, "/debug/pprof/zip", pprofHandler.PProfZip)

	// service GC safepoint API
	serviceGCSafepointHandler := newServiceGCSafepointHandler(svr, rd)
	registerFunc(apiRouter, "/gc/safepoint", serviceGCSafepointHandler.GetGCSafePoint, setMethods("GET"), setAuditBackend(localLog))
	registerFunc(apiRouter, "/gc/safepoint/{service_id}", serviceGCSafepointHandler.DeleteGCSafePoint, setMethods("DELETE"), setAuditBackend(localLog))

	// min resolved ts API
	minResolvedTSHandler := newMinResolvedTSHandler(svr, rd)
	registerFunc(apiRouter, "/min-resolved-ts", minResolvedTSHandler.GetMinResolvedTS, setMethods("GET"))

	// unsafe admin operation API
	unsafeOperationHandler := newUnsafeOperationHandler(svr, rd)
	registerFunc(clusterRouter, "/admin/unsafe/remove-failed-stores",
		unsafeOperationHandler.RemoveFailedStores, setMethods("POST"))
	registerFunc(clusterRouter, "/admin/unsafe/remove-failed-stores/show",
		unsafeOperationHandler.GetFailedStoresRemovalStatus, setMethods("GET"))

	// API to set or unset failpoints
	failpoint.Inject("enableFailpointAPI", func() {
		// this function will be named to "func2". It may be used in test
		registerPrefix(apiRouter, "/fail", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The HTTP handler of failpoint requires the full path to be the failpoint path.
			r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix+apiPrefix+"/fail")
			new(failpoint.HttpHandler).ServeHTTP(w, r)
		}), setAuditBackend("test"))
	})

	// Deprecated: use /pd/api/v1/health instead.
	rootRouter.HandleFunc("/health", healthHandler.GetHealthStatus).Methods("GET")
	// Deprecated: use /pd/api/v1/ping instead.
	rootRouter.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")

	rootRouter.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		serviceLabel := route.GetName()
		methods, _ := route.GetMethods()
		path, _ := route.GetPathTemplate()
		if len(serviceLabel) == 0 {
			return nil
		}
		if len(methods) > 0 {
			for _, method := range methods {
				svr.AddServiceLabel(serviceLabel, apiutil.NewAccessPath(path, method))
			}
		} else {
			svr.AddServiceLabel(serviceLabel, apiutil.NewAccessPath(path, ""))
		}
		return nil
	})

	return rootRouter
}
