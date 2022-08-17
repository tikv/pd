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
	"github.com/tikv/pd/pkg/ratelimit"
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

	setRateLimitAllowList := func() createRouteOption {
		return func(route *mux.Route) {
			svr.UpdateServiceRateLimiter(route.GetName(), ratelimit.AddLabelAllowList())
		}
	}

	rd := createIndentRender()
	rootRouter := mux.NewRouter().PathPrefix(prefix).Subrouter()
	handler := svr.GetHandler()

	apiPrefix := "/api/v1"
	apiRouter := rootRouter.PathPrefix(apiPrefix).Subrouter()

	clusterRouter := apiRouter.NewRoute().Subrouter()
	clusterRouter.Use(newClusterMiddleware(svr).Middleware)

	escapeRouter := clusterRouter.NewRoute().Subrouter().UseEncodedPath()

	operatorHandler := newOperatorHandler(handler, rd)
	registerFunc(apiRouter, "/operators", operatorHandler.GetOperators, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/operators", operatorHandler.CreateOperator, setMethods(http.MethodPost), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/operators/records", operatorHandler.GetOperatorRecords, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/operators/{region_id}", operatorHandler.GetOperatorsByRegion, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/operators/{region_id}", operatorHandler.DeleteOperatorByRegion, setMethods(http.MethodDelete))

	checkerHandler := newCheckerHandler(svr, rd)
	registerFunc(apiRouter, "/checker/{name}", checkerHandler.PauseOrResumeChecker, setMethods(http.MethodPost))
	registerFunc(apiRouter, "/checker/{name}", checkerHandler.GetCheckerStatus, setMethods(http.MethodGet))

	schedulerHandler := newSchedulerHandler(svr, rd)
	registerFunc(apiRouter, "/schedulers", schedulerHandler.GetSchedulers, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/schedulers", schedulerHandler.CreateScheduler, setMethods(http.MethodPost))
	registerFunc(apiRouter, "/schedulers/{name}", schedulerHandler.DeleteScheduler, setMethods(http.MethodDelete))
	registerFunc(apiRouter, "/schedulers/{name}", schedulerHandler.PauseOrResumeScheduler, setMethods(http.MethodPost))

	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	registerPrefix(apiRouter, "/scheduler-config", schedulerConfigHandler.GetSchedulerConfig)

	clusterHandler := newClusterHandler(svr, rd)
	registerFunc(apiRouter, "/cluster", clusterHandler.GetCluster, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/cluster/status", clusterHandler.GetClusterStatus)

	confHandler := newConfHandler(svr, rd)
	registerFunc(apiRouter, "/config", confHandler.GetConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config", confHandler.SetConfig, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(apiRouter, "/config/default", confHandler.GetDefaultConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config/schedule", confHandler.GetScheduleConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config/schedule", confHandler.SetScheduleConfig, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(apiRouter, "/config/pd-server", confHandler.GetPDServerConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config/replicate", confHandler.GetReplicationConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config/replicate", confHandler.SetReplicationConfig, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(apiRouter, "/config/label-property", confHandler.GetLabelPropertyConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config/label-property", confHandler.SetLabelPropertyConfig, setMethods(http.MethodPost))
	registerFunc(apiRouter, "/config/cluster-version", confHandler.GetClusterVersion, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config/cluster-version", confHandler.SetClusterVersion, setMethods(http.MethodPost))
	registerFunc(apiRouter, "/config/replication-mode", confHandler.GetReplicationModeConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/config/replication-mode", confHandler.SetReplicationModeConfig, setMethods(http.MethodPost))

	rulesHandler := newRulesHandler(svr, rd)
	registerFunc(clusterRouter, "/config/rules", rulesHandler.GetAllRules, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/rules", rulesHandler.SetAllRules, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rules/batch", rulesHandler.BatchRules, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rules/group/{group}", rulesHandler.GetRuleByGroup, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/rules/region/{region}", rulesHandler.GetRulesByRegion, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/rules/key/{key}", rulesHandler.GetRulesByKey, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/rule/{group}/{id}", rulesHandler.GetRuleByGroupAndID, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/rule", rulesHandler.SetRule, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rule/{group}/{id}", rulesHandler.DeleteRuleByGroup, setMethods(http.MethodDelete), setAuditBackend(localLog))

	registerFunc(clusterRouter, "/config/rule_group/{id}", rulesHandler.GetGroupConfig, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/rule_group", rulesHandler.SetGroupConfig, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rule_group/{id}", rulesHandler.DeleteGroupConfig, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/rule_groups", rulesHandler.GetAllGroupConfigs, setMethods(http.MethodGet))

	registerFunc(clusterRouter, "/config/placement-rule", rulesHandler.GetPlacementRules, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/placement-rule", rulesHandler.SetPlacementRules, setMethods(http.MethodPost), setAuditBackend(localLog))
	// {group} can be a regular expression, we should enable path encode to
	// support special characters.
	registerFunc(clusterRouter, "/config/placement-rule/{group}", rulesHandler.GetPlacementRuleByGroup, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/placement-rule/{group}", rulesHandler.SetPlacementRuleByGroup, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(escapeRouter, "/config/placement-rule/{group}", rulesHandler.DeletePlacementRuleByGroup, setMethods(http.MethodDelete), setAuditBackend(localLog))

	regionLabelHandler := newRegionLabelHandler(svr, rd)
	registerFunc(clusterRouter, "/config/region-label/rules", regionLabelHandler.GetAllRegionLabelRules, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/config/region-label/rules/ids", regionLabelHandler.GetRegionLabelRulesByIDs, setMethods(http.MethodGet))
	// {id} can be a string with special characters, we should enable path encode to support it.
	registerFunc(escapeRouter, "/config/region-label/rule/{id}", regionLabelHandler.GetRegionLabelRuleByID, setMethods(http.MethodGet))
	registerFunc(escapeRouter, "/config/region-label/rule/{id}", regionLabelHandler.DeleteRegionLabelRule, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/region-label/rule", regionLabelHandler.SetRegionLabelRule, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/config/region-label/rules", regionLabelHandler.PatchRegionLabelRules, setMethods(http.MethodPatch), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/region/id/{id}/label/{key}", regionLabelHandler.GetRegionLabelByKey, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/region/id/{id}/labels", regionLabelHandler.GetRegionLabels, setMethods(http.MethodGet))

	storeHandler := newStoreHandler(handler, rd)
	registerFunc(clusterRouter, "/store/{id}", storeHandler.GetStore, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/store/{id}", storeHandler.DeleteStore, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/state", storeHandler.SetStoreState, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/label", storeHandler.SetStoreLabel, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/weight", storeHandler.SetStoreWeight, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/store/{id}/limit", storeHandler.SetStoreLimit, setMethods(http.MethodPost), setAuditBackend(localLog))

	storesHandler := newStoresHandler(handler, rd)
	registerFunc(clusterRouter, "/stores", storesHandler.GetStores, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/stores/remove-tombstone", storesHandler.RemoveTombStone, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/stores/limit", storesHandler.GetAllStoresLimit, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/stores/limit", storesHandler.SetAllStoresLimit, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/stores/limit/scene", storesHandler.SetStoreLimitScene, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/stores/limit/scene", storesHandler.GetStoreLimitScene, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/stores/progress", storesHandler.GetStoresProgress, setMethods(http.MethodGet))

	labelsHandler := newLabelsHandler(svr, rd)
	registerFunc(clusterRouter, "/labels", labelsHandler.GetLabels, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/labels/stores", labelsHandler.GetStoresByLabel, setMethods(http.MethodGet))

	hotStatusHandler := newHotStatusHandler(handler, rd)
	registerFunc(apiRouter, "/hotspot/regions/write", hotStatusHandler.GetHotWriteRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/regions/read", hotStatusHandler.GetHotReadRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/regions/history", hotStatusHandler.GetHistoryHotRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/observe/{id}", hotStatusHandler.ObserveRegionStats, setMethods(http.MethodPost), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/stores", hotStatusHandler.GetHotStores, setMethods(http.MethodGet), setAuditBackend(prometheus))

	regionHandler := newRegionHandler(svr, rd)
	registerFunc(clusterRouter, "/region/id/{id}", regionHandler.GetRegionByID, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter.UseEncodedPath(), "/region/key/{key}", regionHandler.GetRegion, setMethods(http.MethodGet), setAuditBackend(prometheus))

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	registerFunc(clusterRouter, "/regions", regionsAllHandler.GetRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))

	regionsHandler := newRegionsHandler(svr, rd)
	registerFunc(clusterRouter, "/regions/key", regionsHandler.ScanRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/count", regionsHandler.GetRegionCount, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/store/{id}", regionsHandler.GetStoreRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/writeflow", regionsHandler.GetTopWriteFlowRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/readflow", regionsHandler.GetTopReadFlowRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/confver", regionsHandler.GetTopConfVerRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/version", regionsHandler.GetTopVersionRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/size", regionsHandler.GetTopSizeRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/keys", regionsHandler.GetTopKeysRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/miss-peer", regionsHandler.GetMissPeerRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/extra-peer", regionsHandler.GetExtraPeerRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/pending-peer", regionsHandler.GetPendingPeerRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/down-peer", regionsHandler.GetDownPeerRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/learner-peer", regionsHandler.GetLearnerPeerRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/empty-region", regionsHandler.GetEmptyRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/offline-peer", regionsHandler.GetOfflinePeerRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/oversized-region", regionsHandler.GetOverSizedRegions, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/undersized-region", regionsHandler.GetUndersizedRegions, setMethods(http.MethodGet))

	registerFunc(clusterRouter, "/regions/check/hist-size", regionsHandler.GetSizeHistogram, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/check/hist-keys", regionsHandler.GetKeysHistogram, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/sibling/{id}", regionsHandler.GetRegionSiblings, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/accelerate-schedule", regionsHandler.AccelerateRegionsScheduleInRange, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/scatter", regionsHandler.ScatterRegions, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/split", regionsHandler.SplitRegions, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/regions/range-holes", regionsHandler.GetRangeHoles, setMethods(http.MethodGet))
	registerFunc(clusterRouter, "/regions/replicated", regionsHandler.CheckRegionsReplicated, setMethods(http.MethodGet), setQueries("startKey", "{startKey}", "endKey", "{endKey}"))

	registerFunc(apiRouter, "/version", newVersionHandler(rd).GetVersion, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/status", newStatusHandler(svr, rd).GetPDStatus, setMethods(http.MethodGet))

	memberHandler := newMemberHandler(svr, rd)
	registerFunc(apiRouter, "/members", memberHandler.GetMembers, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/members/name/{name}", memberHandler.DeleteMemberByName, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(apiRouter, "/members/id/{id}", memberHandler.DeleteMemberByID, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(apiRouter, "/members/name/{name}", memberHandler.SetMemberPropertyByName, setMethods(http.MethodPost), setAuditBackend(localLog))

	leaderHandler := newLeaderHandler(svr, rd)
	registerFunc(apiRouter, "/leader", leaderHandler.GetLeader, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/leader/resign", leaderHandler.ResignLeader, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(apiRouter, "/leader/transfer/{next_leader}", leaderHandler.TransferLeader, setMethods(http.MethodPost), setAuditBackend(localLog))

	statsHandler := newStatsHandler(svr, rd)
	registerFunc(clusterRouter, "/stats/region", statsHandler.GetRegionStatus, setMethods(http.MethodGet))

	trendHandler := newTrendHandler(svr, rd)
	registerFunc(apiRouter, "/trend", trendHandler.GetTrend, setMethods(http.MethodGet), setAuditBackend(prometheus))

	adminHandler := newAdminHandler(svr, rd)
	registerFunc(clusterRouter, "/admin/cache/region/{id}", adminHandler.DeleteRegionCache, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/admin/cache/regions", adminHandler.DeleteAllRegionCache, setMethods(http.MethodDelete), setAuditBackend(localLog))
	registerFunc(clusterRouter, "/admin/reset-ts", adminHandler.ResetTS, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(apiRouter, "/admin/persist-file/{file_name}", adminHandler.SavePersistFile, setMethods(http.MethodPost), setAuditBackend(localLog))

	serviceMiddlewareHandler := newServiceMiddlewareHandler(svr, rd)
	registerFunc(apiRouter, "/service-middleware/config", serviceMiddlewareHandler.GetServiceMiddlewareConfig, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/service-middleware/config", serviceMiddlewareHandler.SetServiceMiddlewareConfig, setMethods(http.MethodPost), setAuditBackend(localLog))
	registerFunc(apiRouter, "/service-middleware/config/rate-limit", serviceMiddlewareHandler.SetRatelimitConfig, setMethods(http.MethodPost), setAuditBackend(localLog), setRateLimitAllowList())

	logHandler := newLogHandler(svr, rd)
	registerFunc(apiRouter, "/admin/log", logHandler.SetLogLevel, setMethods(http.MethodPost), setAuditBackend(localLog))
	replicationModeHandler := newReplicationModeHandler(svr, rd)
	registerFunc(clusterRouter, "/replication_mode/status", replicationModeHandler.GetReplicationModeStatus)

	pluginHandler := newPluginHandler(handler, rd)
	registerFunc(apiRouter, "/plugin", pluginHandler.LoadPlugin, setMethods(http.MethodPost))
	registerFunc(apiRouter, "/plugin", pluginHandler.UnloadPlugin, setMethods(http.MethodDelete))

	healthHandler := newHealthHandler(svr, rd)
	registerFunc(apiRouter, "/health", healthHandler.GetHealthStatus, setMethods(http.MethodGet))
	registerFunc(apiRouter, "/ping", healthHandler.Ping, setMethods(http.MethodGet))

	// metric query use to query metric data, the protocol is compatible with prometheus.
	registerFunc(apiRouter, "/metric/query", newQueryMetric(svr).QueryMetric, setMethods(http.MethodGet, http.MethodPost))
	registerFunc(apiRouter, "/metric/query_range", newQueryMetric(svr).QueryMetric, setMethods(http.MethodGet, http.MethodPost))

	// tso API
	tsoHandler := newTSOHandler(svr, rd)
	registerFunc(apiRouter, "/tso/allocator/transfer/{name}", tsoHandler.TransferLocalTSOAllocator, setMethods(http.MethodPost), setAuditBackend(localLog))

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
	registerFunc(apiRouter, "/gc/safepoint", serviceGCSafepointHandler.GetGCSafePoint, setMethods(http.MethodGet), setAuditBackend(localLog))
	registerFunc(apiRouter, "/gc/safepoint/{service_id}", serviceGCSafepointHandler.DeleteGCSafePoint, setMethods(http.MethodDelete), setAuditBackend(localLog))

	// min resolved ts API
	minResolvedTSHandler := newMinResolvedTSHandler(svr, rd)
	registerFunc(clusterRouter, "/min-resolved-ts", minResolvedTSHandler.GetMinResolvedTS, setMethods(http.MethodGet))

	// unsafe admin operation API
	unsafeOperationHandler := newUnsafeOperationHandler(svr, rd)
	registerFunc(clusterRouter, "/admin/unsafe/remove-failed-stores",
		unsafeOperationHandler.RemoveFailedStores, setMethods(http.MethodPost))
	registerFunc(clusterRouter, "/admin/unsafe/remove-failed-stores/show",
		unsafeOperationHandler.GetFailedStoresRemovalStatus, setMethods(http.MethodGet))

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
	rootRouter.HandleFunc("/health", healthHandler.GetHealthStatus).Methods(http.MethodGet)
	// Deprecated: use /pd/api/v1/ping instead.
	rootRouter.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {}).Methods(http.MethodGet)

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
