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
	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

// enableFailPointAPI enable fail point API handler.
var enableFailPointAPI bool

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

func getFunctionName(f any) string {
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
	registerPrefix := func(router *mux.Router, prefixPath, name string,
		handleFunc func(http.ResponseWriter, *http.Request), opts ...createRouteOption) {
		routeCreateFunc(router.PathPrefix(prefixPath), serviceMiddle.createHandler(handleFunc),
			name, opts...)
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

	// localLog should be used in modifying the configuration or admin operations.
	localLog := audit.LocalLogLabel
	// prometheus will be used in all API.
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
	clusterRouter.Use(newClusterMiddleware(svr).middleware)

	escapeRouter := clusterRouter.NewRoute().Subrouter().UseEncodedPath()

	operatorHandler := newOperatorHandler(handler, rd)
	registerFunc(apiRouter, "/operators", operatorHandler.GetOperators, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/operators", operatorHandler.CreateOperator, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/operators", operatorHandler.DeleteOperators, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/operators/records", operatorHandler.GetOperatorRecords, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/operators/{region_id}", operatorHandler.GetOperatorsByRegion, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/operators/{region_id}", operatorHandler.DeleteOperatorByRegion, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))

	checkerHandler := newCheckerHandler(svr, rd)
	registerFunc(apiRouter, "/checker/{name}", checkerHandler.PauseOrResumeChecker, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/checker/{name}", checkerHandler.GetCheckerStatus, setMethods(http.MethodGet), setAuditBackend(prometheus))

	schedulerHandler := newSchedulerHandler(svr, rd)
	registerFunc(apiRouter, "/schedulers", schedulerHandler.GetSchedulers, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/schedulers", schedulerHandler.CreateScheduler, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/schedulers/{name}", schedulerHandler.DeleteScheduler, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/schedulers/{name}", schedulerHandler.PauseOrResumeScheduler, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))

	diagnosticHandler := newDiagnosticHandler(svr, rd)
	registerFunc(clusterRouter, "/schedulers/diagnostic/{name}", diagnosticHandler.getDiagnosticResult, setMethods(http.MethodGet), setAuditBackend(prometheus))

	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	registerPrefix(apiRouter, "/scheduler-config", "HandleSchedulerConfig", schedulerConfigHandler.handleSchedulerConfig, setMethods(http.MethodPost, http.MethodDelete, http.MethodPut, http.MethodPatch), setAuditBackend(localLog, prometheus))
	registerPrefix(apiRouter, "/scheduler-config", "GetSchedulerConfig", schedulerConfigHandler.handleSchedulerConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))

	clusterHandler := newClusterHandler(svr, rd)
	registerFunc(apiRouter, "/cluster", clusterHandler.GetCluster, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/cluster/status", clusterHandler.GetClusterStatus, setAuditBackend(prometheus))

	confHandler := newConfHandler(svr, rd)
	registerFunc(apiRouter, "/config", confHandler.GetConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config", confHandler.SetConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/config/default", confHandler.GetDefaultConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config/schedule", confHandler.GetScheduleConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config/schedule", confHandler.SetScheduleConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/config/pd-server", confHandler.GetPDServerConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config/replicate", confHandler.GetReplicationConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config/replicate", confHandler.SetReplicationConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/config/label-property", confHandler.GetLabelPropertyConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config/label-property", confHandler.SetLabelPropertyConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/config/cluster-version", confHandler.GetClusterVersion, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config/cluster-version", confHandler.SetClusterVersion, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/config/replication-mode", confHandler.GetReplicationModeConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/config/replication-mode", confHandler.SetReplicationModeConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))

	rulesHandler := newRulesHandler(svr, rd)
	ruleRouter := clusterRouter.NewRoute().Subrouter()
	ruleRouter.Use(newRuleMiddleware(svr, rd).middleware)
	registerFunc(ruleRouter, "/config/rules", rulesHandler.GetAllRules, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/rules", rulesHandler.SetAllRules, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(ruleRouter, "/config/rules/batch", rulesHandler.BatchRules, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(ruleRouter, "/config/rules/group/{group}", rulesHandler.GetRuleByGroup, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/rules/region/{region}", rulesHandler.GetRulesByRegion, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/rules/region/{region}/detail", rulesHandler.CheckRegionPlacementRule, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/rules/key/{key}", rulesHandler.GetRulesByKey, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/rule/{group}/{id}", rulesHandler.GetRuleByGroupAndID, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/rule", rulesHandler.SetRule, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(ruleRouter, "/config/rule/{group}/{id}", rulesHandler.DeleteRuleByGroup, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))

	registerFunc(ruleRouter, "/config/rule_group/{id}", rulesHandler.GetGroupConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/rule_group", rulesHandler.SetGroupConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(ruleRouter, "/config/rule_group/{id}", rulesHandler.DeleteGroupConfig, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(ruleRouter, "/config/rule_groups", rulesHandler.GetAllGroupConfigs, setMethods(http.MethodGet), setAuditBackend(prometheus))

	registerFunc(ruleRouter, "/config/placement-rule", rulesHandler.GetPlacementRules, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/placement-rule", rulesHandler.SetPlacementRules, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	// {group} can be a regular expression, we should enable path encode to
	// support special characters.
	registerFunc(ruleRouter, "/config/placement-rule/{group}", rulesHandler.GetPlacementRuleByGroup, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(ruleRouter, "/config/placement-rule/{group}", rulesHandler.SetPlacementRuleByGroup, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(ruleRouter, "/config/placement-rule/{group}", rulesHandler.DeletePlacementRuleByGroup, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))

	regionLabelHandler := newRegionLabelHandler(svr, rd)
	registerFunc(clusterRouter, "/config/region-label/rules", regionLabelHandler.GetAllRegionLabelRules, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/config/region-label/rules/ids", regionLabelHandler.GetRegionLabelRulesByIDs, setMethods(http.MethodGet), setAuditBackend(prometheus))
	// {id} can be a string with special characters, we should enable path encode to support it.
	registerFunc(escapeRouter, "/config/region-label/rule/{id}", regionLabelHandler.GetRegionLabelRuleByID, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(escapeRouter, "/config/region-label/rule/{id}", regionLabelHandler.DeleteRegionLabelRule, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/config/region-label/rule", regionLabelHandler.SetRegionLabelRule, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/config/region-label/rules", regionLabelHandler.PatchRegionLabelRules, setMethods(http.MethodPatch), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/region/id/{id}/label/{key}", regionLabelHandler.GetRegionLabelByKey, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/region/id/{id}/labels", regionLabelHandler.GetRegionLabels, setMethods(http.MethodGet), setAuditBackend(prometheus))

	storeHandler := newStoreHandler(handler, rd)
	registerFunc(clusterRouter, "/store/{id}", storeHandler.GetStore, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/store/{id}", storeHandler.DeleteStore, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/store/{id}/state", storeHandler.SetStoreState, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/store/{id}/label", storeHandler.SetStoreLabel, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/store/{id}/label", storeHandler.DeleteStoreLabel, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/store/{id}/weight", storeHandler.SetStoreWeight, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/store/{id}/limit", storeHandler.SetStoreLimit, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))

	storesHandler := newStoresHandler(handler, rd)
	registerFunc(clusterRouter, "/stores", storesHandler.GetAllStores, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/stores/remove-tombstone", storesHandler.RemoveTombStone, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/stores/limit", storesHandler.GetAllStoresLimit, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/stores/limit", storesHandler.SetAllStoresLimit, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/stores/limit/scene", storesHandler.SetStoreLimitScene, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/stores/limit/scene", storesHandler.GetStoreLimitScene, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/stores/progress", storesHandler.GetStoresProgress, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/stores/check", storesHandler.GetStoresByState, setMethods(http.MethodGet), setAuditBackend(prometheus))

	labelsHandler := newLabelsHandler(svr, rd)
	registerFunc(clusterRouter, "/labels", labelsHandler.GetLabels, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/labels/stores", labelsHandler.GetStoresByLabel, setMethods(http.MethodGet), setAuditBackend(prometheus))

	hotStatusHandler := newHotStatusHandler(handler, rd)
	registerFunc(apiRouter, "/hotspot/regions/write", hotStatusHandler.GetHotWriteRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/regions/read", hotStatusHandler.GetHotReadRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/regions/history", hotStatusHandler.GetHistoryHotRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/stores", hotStatusHandler.GetHotStores, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/hotspot/buckets", hotStatusHandler.GetHotBuckets, setMethods(http.MethodGet), setAuditBackend(prometheus))

	regionHandler := newRegionHandler(svr, rd)
	registerFunc(clusterRouter, "/region/id/{id}", regionHandler.GetRegionByID, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter.UseEncodedPath(), "/region/key/{key}", regionHandler.GetRegion, setMethods(http.MethodGet), setAuditBackend(prometheus))

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	registerFunc(clusterRouter, "/regions", regionsAllHandler.GetRegions, setMethods(http.MethodGet), setAuditBackend(localLog, prometheus))

	regionsHandler := newRegionsHandler(svr, rd)
	registerFunc(clusterRouter, "/regions/key", regionsHandler.ScanRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/count", regionsHandler.GetRegionCount, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/store/{id}", regionsHandler.GetStoreRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/keyspace/id/{id}", regionsHandler.GetKeyspaceRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/writeflow", regionsHandler.GetTopWriteFlowRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/writequery", regionsHandler.GetTopWriteQueryRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/readflow", regionsHandler.GetTopReadFlowRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/readquery", regionsHandler.GetTopReadQueryRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/confver", regionsHandler.GetTopConfVerRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/version", regionsHandler.GetTopVersionRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/size", regionsHandler.GetTopSizeRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/keys", regionsHandler.GetTopKeysRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/cpu", regionsHandler.GetTopCPURegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/miss-peer", regionsHandler.GetMissPeerRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/extra-peer", regionsHandler.GetExtraPeerRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/pending-peer", regionsHandler.GetPendingPeerRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/down-peer", regionsHandler.GetDownPeerRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/learner-peer", regionsHandler.GetLearnerPeerRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/empty-region", regionsHandler.GetEmptyRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/offline-peer", regionsHandler.GetOfflinePeerRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/oversized-region", regionsHandler.GetOverSizedRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/undersized-region", regionsHandler.GetUndersizedRegions, setMethods(http.MethodGet), setAuditBackend(prometheus))

	registerFunc(clusterRouter, "/regions/check/hist-size", regionsHandler.GetSizeHistogram, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/check/hist-keys", regionsHandler.GetKeysHistogram, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/sibling/{id}", regionsHandler.GetRegionSiblings, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/accelerate-schedule", regionsHandler.AccelerateRegionsScheduleInRange, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/accelerate-schedule/batch", regionsHandler.AccelerateRegionsScheduleInRanges, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/scatter", regionsHandler.ScatterRegions, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/split", regionsHandler.SplitRegions, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/regions/range-holes", regionsHandler.GetRangeHoles, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/regions/replicated", regionsHandler.CheckRegionsReplicated, setMethods(http.MethodGet), setQueries("startKey", "{startKey}", "endKey", "{endKey}"), setAuditBackend(prometheus))

	registerFunc(apiRouter, "/version", newVersionHandler(rd).GetVersion, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/status", newStatusHandler(svr, rd).GetPDStatus, setMethods(http.MethodGet), setAuditBackend(prometheus))

	memberHandler := newMemberHandler(svr, rd)
	registerFunc(apiRouter, "/members", memberHandler.GetMembers, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/members/name/{name}", memberHandler.DeleteMemberByName, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/members/id/{id}", memberHandler.DeleteMemberByID, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/members/name/{name}", memberHandler.SetMemberPropertyByName, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))

	leaderHandler := newLeaderHandler(svr, rd)
	registerFunc(apiRouter, "/leader", leaderHandler.GetLeader, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/leader/resign", leaderHandler.ResignLeader, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/leader/transfer/{next_leader}", leaderHandler.TransferLeader, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))

	statsHandler := newStatsHandler(svr, rd)
	registerFunc(clusterRouter, "/stats/region", statsHandler.GetRegionStatus, setMethods(http.MethodGet), setAuditBackend(prometheus))

	trendHandler := newTrendHandler(svr, rd)
	registerFunc(apiRouter, "/trend", trendHandler.GetTrend, setMethods(http.MethodGet), setAuditBackend(prometheus))

	adminHandler := newAdminHandler(svr, rd)
	registerFunc(clusterRouter, "/admin/cache/region/{id}", adminHandler.DeleteRegionCache, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/admin/storage/region/{id}", adminHandler.DeleteRegionStorage, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/admin/cache/regions", adminHandler.DeleteAllRegionCache, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/admin/persist-file/{file_name}", adminHandler.SavePersistFile, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/admin/cluster/markers/snapshot-recovering", adminHandler.isSnapshotRecovering, setMethods(http.MethodGet), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/admin/cluster/markers/snapshot-recovering", adminHandler.markSnapshotRecovering, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/admin/cluster/markers/snapshot-recovering", adminHandler.unmarkSnapshotRecovering, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/admin/base-alloc-id", adminHandler.recoverAllocID, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))

	serviceMiddlewareHandler := newServiceMiddlewareHandler(svr, rd)
	registerFunc(apiRouter, "/service-middleware/config", serviceMiddlewareHandler.GetServiceMiddlewareConfig, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/service-middleware/config", serviceMiddlewareHandler.SetServiceMiddlewareConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(apiRouter, "/service-middleware/config/rate-limit", serviceMiddlewareHandler.SetRateLimitConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus), setRateLimitAllowList())
	registerFunc(apiRouter, "/service-middleware/config/grpc-rate-limit", serviceMiddlewareHandler.SetGRPCRateLimitConfig, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus), setRateLimitAllowList())

	logHandler := newLogHandler(svr, rd)
	registerFunc(apiRouter, "/admin/log", logHandler.SetLogLevel, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	replicationModeHandler := newReplicationModeHandler(svr, rd)
	registerFunc(clusterRouter, "/replication_mode/status", replicationModeHandler.GetReplicationModeStatus, setAuditBackend(prometheus))

	pluginHandler := newPluginHandler(handler, rd)
	registerFunc(apiRouter, "/plugin", pluginHandler.loadPlugin, setMethods(http.MethodPost), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/plugin", pluginHandler.unloadPlugin, setMethods(http.MethodDelete), setAuditBackend(prometheus))

	healthHandler := newHealthHandler(svr, rd)
	registerFunc(apiRouter, "/health", healthHandler.GetHealthStatus, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/ping", healthHandler.Ping, setMethods(http.MethodGet), setAuditBackend(prometheus))

	// metric query use to query metric data, the protocol is compatible with prometheus.
	registerFunc(apiRouter, "/metric/query", newqueryMetric(svr).queryMetric, setMethods(http.MethodGet, http.MethodPost), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/metric/query_range", newqueryMetric(svr).queryMetric, setMethods(http.MethodGet, http.MethodPost), setAuditBackend(prometheus))

	pprofHandler := newPprofHandler(svr, rd)
	// profile API
	registerFunc(apiRouter, "/debug/pprof/profile", pprof.Profile, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/trace", pprof.Trace, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/symbol", pprof.Symbol, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/heap", pprofHandler.PProfHeap, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/mutex", pprofHandler.PProfMutex, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/allocs", pprofHandler.PProfAllocs, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/block", pprofHandler.PProfBlock, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/goroutine", pprofHandler.PProfGoroutine, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/threadcreate", pprofHandler.PProfThreadcreate, setAuditBackend(localLog))
	registerFunc(apiRouter, "/debug/pprof/zip", pprofHandler.PProfZip, setAuditBackend(localLog))

	// service GC safepoint API
	serviceGCSafepointHandler := newServiceGCSafepointHandler(svr, rd)
	registerFunc(apiRouter, "/gc/safepoint", serviceGCSafepointHandler.GetGCSafePoint, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(apiRouter, "/gc/safepoint/{service_id}", serviceGCSafepointHandler.DeleteGCSafePoint, setMethods(http.MethodDelete), setAuditBackend(localLog, prometheus))

	// min resolved ts API
	minResolvedTSHandler := newMinResolvedTSHandler(svr, rd)
	registerFunc(clusterRouter, "/min-resolved-ts", minResolvedTSHandler.GetMinResolvedTS, setMethods(http.MethodGet), setAuditBackend(prometheus))
	registerFunc(clusterRouter, "/min-resolved-ts/{store_id}", minResolvedTSHandler.GetStoreMinResolvedTS, setMethods(http.MethodGet), setAuditBackend(prometheus))

	// unsafe admin operation API
	unsafeOperationHandler := newUnsafeOperationHandler(svr, rd)
	registerFunc(clusterRouter, "/admin/unsafe/remove-failed-stores",
		unsafeOperationHandler.RemoveFailedStores, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	registerFunc(clusterRouter, "/admin/unsafe/remove-failed-stores/show",
		unsafeOperationHandler.GetFailedStoresRemovalStatus, setMethods(http.MethodGet), setAuditBackend(prometheus))

	// tso API
	tsoHandler := newTSOHandler(svr, rd)
	registerFunc(apiRouter, "/tso/allocator/transfer/{name}", tsoHandler.TransferLocalTSOAllocator, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))
	tsoAdminHandler := tso.NewAdminHandler(svr.GetHandler(), rd)
	// br ebs restore phase 1 will reset ts, but at that time the cluster hasn't bootstrapped, so cannot use clusterRouter
	registerFunc(apiRouter, "/admin/reset-ts", tsoAdminHandler.ResetTS, setMethods(http.MethodPost), setAuditBackend(localLog, prometheus))

	// API to set or unset failpoints
	if enableFailPointAPI {
		registerPrefix(apiRouter, "/fail", "FailPoint", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The HTTP handler of failpoint requires the full path to be the failpoint path.
			r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix+apiPrefix+"/fail")
			new(failpoint.HttpHandler).ServeHTTP(w, r)
		}), setAuditBackend(localLog))
	}
	// Deprecated: use /pd/api/v1/health instead.
	rootRouter.HandleFunc("/health", healthHandler.GetHealthStatus).Methods(http.MethodGet)
	// Deprecated: use /pd/api/v1/ping instead.
	rootRouter.HandleFunc("/ping", func(http.ResponseWriter, *http.Request) {}).Methods(http.MethodGet)

	_ = rootRouter.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
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
