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
	"path"

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

const pingAPI = "/ping"

const (
	Operators                 = "/api/v1/operators"
	OperatorsByID             = "/api/v1/operators/{region_id}"
	Schedulers                = "/api/v1/schedulers"
	SchedulersByName          = "/api/v1/schedulers/{name}"
	Cluster                   = "/api/v1/cluster"
	ClusterStatus             = "/api/v1/cluster/status"
	Config                    = "/api/v1/config"
	Schedule                  = "/api/v1/config/schedule"
	Replication               = "/api/v1/config/replicate"
	Namespace                 = "/api/v1/config/namespace/{name}"
	LabelProperty             = "/api/v1/config/label-property"
	ClusterVersion            = "/api/v1/config/cluster-version"
	StoreByID                 = "/api/v1/store/{id}"
	StoreState                = "/api/v1/store/{id}/state"
	StoreLabels               = "/api/v1/store/{id}/label"
	StoreWeight               = "/api/v1/store/{id}/weight"
	StoreLimit                = "/api/v1/store/{id}/limit"
	Stores                    = "/api/v1/stores"
	RemoveTombStone           = "/api/v1/stores/remove-tombstone"
	AllLimit                  = "/api/v1/stores/limit"
	Labels                    = "/api/v1/labels"
	LabelAllStores            = "/api/v1/labels/stores"
	HotWriteRegions           = "/api/v1/hotspot/regions/write"
	HotReadRegions            = "/api/v1/hotspot/regions/read"
	HotStores                 = "/api/v1/hotspot/stores"
	RegionByID                = "/api/v1/region/id/{id}"
	RegionByKey               = "/api/v1/region/key/{key}"
	Regions                   = "/api/v1/regions"
	ScanRegions               = "/api/v1/regions/key"
	StoreRegions              = "/api/v1/regions/store/{id}"
	TopWriteFlow              = "/api/v1/regions/writeflow"
	TopReadFlow               = "/api/v1/regions/readflow"
	TopConfVer                = "/api/v1/regions/confver"
	TopVersion                = "/api/v1/regions/version"
	TopSize                   = "/api/v1/regions/size"
	MissPeerRegions           = "/api/v1/regions/check/miss-peer"
	ExtraPeerRegions          = "/api/v1/regions/check/extra-peer"
	PendingPeerRegions        = "/api/v1/regions/check/pending-peer"
	DownPeerRegions           = "/api/v1/regions/check/down-peer"
	OfflinePeer               = "/api/v1/regions/check/offline-peer"
	EmptyRegion               = "/api/v1/regions/check/empty-region"
	RegionSiblings            = "/api/v1/regions/sibling/{id}"
	IncorrectNamespaceRegions = "/api/v1/regions/check/incorrect-ns"
	Version                   = "/api/v1/version"
	Status                    = "/api/v1/status"
	Members                   = "/api/v1/members"
	MembersByName             = "/api/v1/members/name/{name}"
	MembersByID               = "/api/v1/members/id/{id}"
	Leader                    = "/api/v1/leader"
	LeaderResign              = "/api/v1/leader/resign"
	LeaderTransfer            = "/api/v1/leader/transfer/{next_leader}"
	StatsRegion               = "/api/v1/stats/region"
	Trends                    = "/api/v1/trend"
	HandleDropCacheRegion     = "/api/v1/admin/cache/region/{id}"
	AdminLog                  = "/api/v1/admin/log"
	Healths                   = "/api/v1/health"
	Diagnose                  = "/api/v1/diagnose"
	DeprecatedHealths         = "/health"
	DeprecatedDiagnose        = "/diagnose"
	Classifier                = "/api/v1/classifier/"
)

func createRouter(prefix string, svr *server.Server) *mux.Router {
	rd := render.New(render.Options{
		IndentJSON: true,
	})

	router := mux.NewRouter().PathPrefix(prefix).Subrouter()
	handler := svr.GetHandler()

	operatorHandler := newOperatorHandler(handler, rd)
	router.HandleFunc(Operators, operatorHandler.List).Methods("GET")
	router.HandleFunc(Operators, operatorHandler.Post).Methods("POST")
	router.HandleFunc(OperatorsByID, operatorHandler.Get).Methods("GET")
	router.HandleFunc(OperatorsByID, operatorHandler.Delete).Methods("DELETE")

	schedulerHandler := newSchedulerHandler(handler, rd)
	router.HandleFunc(Schedulers, schedulerHandler.List).Methods("GET")
	router.HandleFunc(Schedulers, schedulerHandler.Post).Methods("POST")
	router.HandleFunc(SchedulersByName, schedulerHandler.Delete).Methods("DELETE")

	clusterHandler := newClusterHandler(svr, rd)
	router.Handle(Cluster, clusterHandler).Methods("GET")
	router.HandleFunc(ClusterStatus, clusterHandler.GetClusterStatus).Methods("GET")

	confHandler := newConfHandler(svr, rd)
	router.HandleFunc(Config, confHandler.Get).Methods("GET")
	router.HandleFunc(Config, confHandler.Post).Methods("POST")
	router.HandleFunc(Schedule, confHandler.SetSchedule).Methods("POST")
	router.HandleFunc(Schedule, confHandler.GetSchedule).Methods("GET")
	router.HandleFunc(Replication, confHandler.SetReplication).Methods("POST")
	router.HandleFunc(Replication, confHandler.GetReplication).Methods("GET")
	router.HandleFunc(Namespace, confHandler.GetNamespace).Methods("GET")
	router.HandleFunc(Namespace, confHandler.SetNamespace).Methods("POST")
	router.HandleFunc(Namespace, confHandler.DeleteNamespace).Methods("DELETE")
	router.HandleFunc(LabelProperty, confHandler.GetLabelProperty).Methods("GET")
	router.HandleFunc(LabelProperty, confHandler.SetLabelProperty).Methods("POST")
	router.HandleFunc(ClusterVersion, confHandler.GetClusterVersion).Methods("GET")
	router.HandleFunc(ClusterVersion, confHandler.SetClusterVersion).Methods("POST")

	storeHandler := newStoreHandler(handler, rd)
	router.HandleFunc(StoreByID, storeHandler.Get).Methods("GET")
	router.HandleFunc(StoreByID, storeHandler.Delete).Methods("DELETE")
	router.HandleFunc(StoreState, storeHandler.SetState).Methods("POST")
	router.HandleFunc(StoreLabels, storeHandler.SetLabels).Methods("POST")
	router.HandleFunc(StoreWeight, storeHandler.SetWeight).Methods("POST")
	router.HandleFunc(StoreLimit, storeHandler.SetLimit).Methods("POST")
	storesHandler := newStoresHandler(handler, rd)
	router.Handle(Stores, storesHandler).Methods("GET")
	router.HandleFunc(RemoveTombStone, storesHandler.RemoveTombStone).Methods("DELETE")
	router.HandleFunc(AllLimit, storesHandler.GetAllLimit).Methods("GET")
	router.HandleFunc(AllLimit, storesHandler.SetAllLimit).Methods("POST")

	labelsHandler := newLabelsHandler(svr, rd)
	router.HandleFunc(Labels, labelsHandler.Get).Methods("GET")
	router.HandleFunc(LabelAllStores, labelsHandler.GetStores).Methods("GET")

	hotStatusHandler := newHotStatusHandler(handler, rd)
	router.HandleFunc(HotWriteRegions, hotStatusHandler.GetHotWriteRegions).Methods("GET")
	router.HandleFunc(HotReadRegions, hotStatusHandler.GetHotReadRegions).Methods("GET")
	router.HandleFunc(HotStores, hotStatusHandler.GetHotStores).Methods("GET")

	regionHandler := newRegionHandler(svr, rd)
	router.HandleFunc(RegionByID, regionHandler.GetRegionByID).Methods("GET")
	router.HandleFunc(RegionByKey, regionHandler.GetRegionByKey).Methods("GET")

	regionsHandler := newRegionsHandler(svr, rd)
	router.HandleFunc(Regions, regionsHandler.GetAll).Methods("GET")
	router.HandleFunc(ScanRegions, regionsHandler.ScanRegions).Methods("GET")
	router.HandleFunc(StoreRegions, regionsHandler.GetStoreRegions).Methods("GET")
	router.HandleFunc(TopWriteFlow, regionsHandler.GetTopWriteFlow).Methods("GET")
	router.HandleFunc(TopReadFlow, regionsHandler.GetTopReadFlow).Methods("GET")
	router.HandleFunc(TopConfVer, regionsHandler.GetTopConfVer).Methods("GET")
	router.HandleFunc(TopVersion, regionsHandler.GetTopVersion).Methods("GET")
	router.HandleFunc(TopSize, regionsHandler.GetTopSize).Methods("GET")
	router.HandleFunc(MissPeerRegions, regionsHandler.GetMissPeerRegions).Methods("GET")
	router.HandleFunc(ExtraPeerRegions, regionsHandler.GetExtraPeerRegions).Methods("GET")
	router.HandleFunc(PendingPeerRegions, regionsHandler.GetPendingPeerRegions).Methods("GET")
	router.HandleFunc(DownPeerRegions, regionsHandler.GetDownPeerRegions).Methods("GET")
	router.HandleFunc(OfflinePeer, regionsHandler.GetOfflinePeer).Methods("GET")
	router.HandleFunc(EmptyRegion, regionsHandler.GetEmptyRegion).Methods("GET")
	router.HandleFunc(RegionSiblings, regionsHandler.GetRegionSiblings).Methods("GET")
	router.HandleFunc(IncorrectNamespaceRegions, regionsHandler.GetIncorrectNamespaceRegions).Methods("GET")

	router.Handle(Version, newVersionHandler(rd)).Methods("GET")
	router.Handle(Status, newStatusHandler(rd)).Methods("GET")

	memberHandler := newMemberHandler(svr, rd)
	router.HandleFunc(Members, memberHandler.ListMembers).Methods("GET")
	router.HandleFunc(MembersByName, memberHandler.DeleteByName).Methods("DELETE")
	router.HandleFunc(MembersByID, memberHandler.DeleteByID).Methods("DELETE")
	router.HandleFunc(MembersByName, memberHandler.SetMemberPropertyByName).Methods("POST")

	leaderHandler := newLeaderHandler(svr, rd)
	router.HandleFunc(Leader, leaderHandler.Get).Methods("GET")
	router.HandleFunc(LeaderResign, leaderHandler.Resign).Methods("POST")
	router.HandleFunc(LeaderTransfer, leaderHandler.Transfer).Methods("POST")

	classifierPrefix := path.Join(prefix, Classifier)
	classifierHandler := newClassifierHandler(svr, rd, classifierPrefix)
	router.PathPrefix(Classifier).Handler(classifierHandler)

	statsHandler := newStatsHandler(svr, rd)
	router.HandleFunc(StatsRegion, statsHandler.Region).Methods("GET")

	trendHandler := newTrendHandler(svr, rd)
	router.HandleFunc(Trends, trendHandler.Handle).Methods("GET")

	adminHandler := newAdminHandler(svr, rd)
	router.HandleFunc(HandleDropCacheRegion, adminHandler.HandleDropCacheRegion).Methods("DELETE")

	logHanler := newlogHandler(svr, rd)
	router.HandleFunc(AdminLog, logHanler.Handle).Methods("POST")

	router.Handle(Healths, newHealthHandler(svr, rd)).Methods("GET")
	router.Handle(Diagnose, newDiagnoseHandler(svr, rd)).Methods("GET")

	// Deprecated
	router.Handle(DeprecatedHealths, newHealthHandler(svr, rd)).Methods("GET")
	// Deprecated
	router.Handle(DeprecatedDiagnose, newDiagnoseHandler(svr, rd)).Methods("GET")

	router.HandleFunc(pingAPI, func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")

	return router
}
