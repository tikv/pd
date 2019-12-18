// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"net/http"
	"net/http/pprof"

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
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

func createRouter(prefix string, svr *server.Server) *mux.Router {
	rd := createIndentRender()

	rootRouter := mux.NewRouter().PathPrefix(prefix).Subrouter()
	handler := svr.GetHandler()

	clusterRouter := rootRouter.NewRoute().Subrouter()
	clusterRouter.Use(newClusterMiddleware(svr).Middleware)

	operatorHandler := newOperatorHandler(handler, rd)
	rootRouter.HandleFunc("/api/v1/operators", operatorHandler.List).Methods("GET")
	rootRouter.HandleFunc("/api/v1/operators", operatorHandler.Post).Methods("POST")
	rootRouter.HandleFunc("/api/v1/operators/{region_id}", operatorHandler.Get).Methods("GET")
	rootRouter.HandleFunc("/api/v1/operators/{region_id}", operatorHandler.Delete).Methods("DELETE")

	schedulerHandler := newSchedulerHandler(handler, rd)
	rootRouter.HandleFunc("/api/v1/schedulers", schedulerHandler.List).Methods("GET")
	rootRouter.HandleFunc("/api/v1/schedulers", schedulerHandler.Post).Methods("POST")
	rootRouter.HandleFunc("/api/v1/schedulers/{name}", schedulerHandler.Delete).Methods("DELETE")
	rootRouter.HandleFunc("/api/v1/schedulers/{name}", schedulerHandler.PauseOrResume).Methods("POST")
	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	rootRouter.PathPrefix(server.SchedulerConfigHandlerPath).Handler(schedulerConfigHandler)

	clusterHandler := newClusterHandler(svr, rd)
	rootRouter.Handle("/api/v1/cluster", clusterHandler).Methods("GET")
	rootRouter.HandleFunc("/api/v1/cluster/status", clusterHandler.GetClusterStatus).Methods("GET")

	confHandler := newConfHandler(svr, rd)
	rootRouter.HandleFunc("/api/v1/config", confHandler.Get).Methods("GET")
	rootRouter.HandleFunc("/api/v1/config", confHandler.Post).Methods("POST")
	rootRouter.HandleFunc("/api/v1/config/schedule", confHandler.SetSchedule).Methods("POST")
	rootRouter.HandleFunc("/api/v1/config/schedule", confHandler.GetSchedule).Methods("GET")
	rootRouter.HandleFunc("/api/v1/config/replicate", confHandler.SetReplication).Methods("POST")
	rootRouter.HandleFunc("/api/v1/config/replicate", confHandler.GetReplication).Methods("GET")
	rootRouter.HandleFunc("/api/v1/config/label-property", confHandler.GetLabelProperty).Methods("GET")
	rootRouter.HandleFunc("/api/v1/config/label-property", confHandler.SetLabelProperty).Methods("POST")
	rootRouter.HandleFunc("/api/v1/config/cluster-version", confHandler.GetClusterVersion).Methods("GET")
	rootRouter.HandleFunc("/api/v1/config/cluster-version", confHandler.SetClusterVersion).Methods("POST")

	rulesHandler := newRulesHandler(svr, rd)
	clusterRouter.HandleFunc("/api/v1/config/rules", rulesHandler.GetAll).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/config/rules/group/{group}", rulesHandler.GetAllByGroup).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/config/rules/region/{region}", rulesHandler.GetAllByRegion).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/config/rules/key/{key}", rulesHandler.GetAllByKey).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/config/rule/{group}/{id}", rulesHandler.Get).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/config/rule", rulesHandler.Set).Methods("POST")
	clusterRouter.HandleFunc("/api/v1/config/rule/{group}/{id}", rulesHandler.Delete).Methods("DELETE")

	storeHandler := newStoreHandler(handler, rd)
	clusterRouter.HandleFunc("/api/v1/store/{id}", storeHandler.Get).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/store/{id}", storeHandler.Delete).Methods("DELETE")
	clusterRouter.HandleFunc("/api/v1/store/{id}/state", storeHandler.SetState).Methods("POST")
	clusterRouter.HandleFunc("/api/v1/store/{id}/label", storeHandler.SetLabels).Methods("POST")
	clusterRouter.HandleFunc("/api/v1/store/{id}/weight", storeHandler.SetWeight).Methods("POST")
	clusterRouter.HandleFunc("/api/v1/store/{id}/limit", storeHandler.SetLimit).Methods("POST")
	storesHandler := newStoresHandler(handler, rd)
	clusterRouter.Handle("/api/v1/stores", storesHandler).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/stores/remove-tombstone", storesHandler.RemoveTombStone).Methods("DELETE")
	clusterRouter.HandleFunc("/api/v1/stores/limit", storesHandler.GetAllLimit).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/stores/limit", storesHandler.SetAllLimit).Methods("POST")

	labelsHandler := newLabelsHandler(svr, rd)
	clusterRouter.HandleFunc("/api/v1/labels", labelsHandler.Get).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/labels/stores", labelsHandler.GetStores).Methods("GET")

	hotStatusHandler := newHotStatusHandler(handler, rd)
	rootRouter.HandleFunc("/api/v1/hotspot/regions/write", hotStatusHandler.GetHotWriteRegions).Methods("GET")
	rootRouter.HandleFunc("/api/v1/hotspot/regions/read", hotStatusHandler.GetHotReadRegions).Methods("GET")
	rootRouter.HandleFunc("/api/v1/hotspot/stores", hotStatusHandler.GetHotStores).Methods("GET")

	regionHandler := newRegionHandler(svr, rd)
	clusterRouter.HandleFunc("/api/v1/region/id/{id}", regionHandler.GetRegionByID).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/region/key/{key}", regionHandler.GetRegionByKey).Methods("GET")

	srd := createStreamingRender()
	regionsAllHandler := newRegionsHandler(svr, srd)
	clusterRouter.HandleFunc("/api/v1/regions", regionsAllHandler.GetAll).Methods("GET")

	regionsHandler := newRegionsHandler(svr, rd)
	clusterRouter.HandleFunc("/api/v1/regions/key", regionsHandler.ScanRegions).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/count", regionsHandler.GetRegionCount).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/store/{id}", regionsHandler.GetStoreRegions).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/writeflow", regionsHandler.GetTopWriteFlow).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/readflow", regionsHandler.GetTopReadFlow).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/confver", regionsHandler.GetTopConfVer).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/version", regionsHandler.GetTopVersion).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/size", regionsHandler.GetTopSize).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/miss-peer", regionsHandler.GetMissPeerRegions).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/extra-peer", regionsHandler.GetExtraPeerRegions).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/pending-peer", regionsHandler.GetPendingPeerRegions).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/down-peer", regionsHandler.GetDownPeerRegions).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/offline-peer", regionsHandler.GetOfflinePeer).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/empty-region", regionsHandler.GetEmptyRegion).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/hist-size", regionsHandler.GetSizeHistogram).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/check/hist-keys", regionsHandler.GetKeysHistogram).Methods("GET")
	clusterRouter.HandleFunc("/api/v1/regions/sibling/{id}", regionsHandler.GetRegionSiblings).Methods("GET")

	rootRouter.Handle("/api/v1/version", newVersionHandler(rd)).Methods("GET")
	rootRouter.Handle("/api/v1/status", newStatusHandler(rd)).Methods("GET")

	memberHandler := newMemberHandler(svr, rd)
	rootRouter.HandleFunc("/api/v1/members", memberHandler.ListMembers).Methods("GET")
	rootRouter.HandleFunc("/api/v1/members/name/{name}", memberHandler.DeleteByName).Methods("DELETE")
	rootRouter.HandleFunc("/api/v1/members/id/{id}", memberHandler.DeleteByID).Methods("DELETE")
	rootRouter.HandleFunc("/api/v1/members/name/{name}", memberHandler.SetMemberPropertyByName).Methods("POST")

	leaderHandler := newLeaderHandler(svr, rd)
	rootRouter.HandleFunc("/api/v1/leader", leaderHandler.Get).Methods("GET")
	rootRouter.HandleFunc("/api/v1/leader/resign", leaderHandler.Resign).Methods("POST")
	rootRouter.HandleFunc("/api/v1/leader/transfer/{next_leader}", leaderHandler.Transfer).Methods("POST")

	statsHandler := newStatsHandler(svr, rd)
	clusterRouter.HandleFunc("/api/v1/stats/region", statsHandler.Region).Methods("GET")

	trendHandler := newTrendHandler(svr, rd)
	rootRouter.HandleFunc("/api/v1/trend", trendHandler.Handle).Methods("GET")

	adminHandler := newAdminHandler(svr, rd)
	clusterRouter.HandleFunc("/api/v1/admin/cache/region/{id}", adminHandler.HandleDropCacheRegion).Methods("DELETE")
	clusterRouter.HandleFunc("/api/v1/admin/reset-ts", adminHandler.ResetTS).Methods("POST")

	logHandler := newlogHandler(svr, rd)
	rootRouter.HandleFunc("/api/v1/admin/log", logHandler.Handle).Methods("POST")

	pluginHandler := newPluginHandler(handler, rd)
	rootRouter.HandleFunc("/api/v1/plugin", pluginHandler.LoadPlugin).Methods("POST")
	rootRouter.HandleFunc("/api/v1/plugin", pluginHandler.UnloadPlugin).Methods("DELETE")

	rootRouter.Handle("/api/v1/health", newHealthHandler(svr, rd)).Methods("GET")
	rootRouter.Handle("/api/v1/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	rootRouter.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")
	// metric query use to query metric data, the protocol is compatible with prometheus.
	rootRouter.Handle("/api/v1/metric/query", newQueryMetric(svr)).Methods("GET", "POST")
	rootRouter.Handle("/api/v1/metric/query_range", newQueryMetric(svr)).Methods("GET", "POST")

	// profile API
	rootRouter.HandleFunc("/api/v1/debug/pprof/profile", pprof.Profile)
	rootRouter.Handle("/api/v1/debug/pprof/heap", pprof.Handler("heap"))
	rootRouter.Handle("/api/v1/debug/pprof/mutex", pprof.Handler("mutex"))
	rootRouter.Handle("/api/v1/debug/pprof/allocs", pprof.Handler("allocs"))
	rootRouter.Handle("/api/v1/debug/pprof/block", pprof.Handler("block"))
	rootRouter.Handle("/api/v1/debug/pprof/goroutine", pprof.Handler("goroutine"))

	// Deprecated
	rootRouter.Handle("/health", newHealthHandler(svr, rd)).Methods("GET")
	// Deprecated
	rootRouter.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")
	// Deprecated
	rootRouter.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")

	return rootRouter
}
