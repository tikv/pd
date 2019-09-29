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
	//Operators is the api that may GET and POST.
	Operators = "/api/v1/operators"
	//OperatorsByID is the api that may GET and DELETE.
	OperatorsByID = "/api/v1/operators/{region_id}"
	//Schedulers is the api that may GET and POST.
	Schedulers = "/api/v1/schedulers"
	//SchedulersByName is the api that may DELETE.
	SchedulersByName = "/api/v1/schedulers/{name}"
	//Cluster is the api that may GET.
	Cluster = "/api/v1/cluster"
	//ClusterStatus is the api that may GET.
	ClusterStatus = "/api/v1/cluster/status"
	//Config is the api that may GET and POST.
	Config = "/api/v1/config"
	//Schedule is the api that may POST and GET.
	Schedule = "/api/v1/config/schedule"
	//Replication is the api that may POST and GET.
	Replication = "/api/v1/config/replicate"
	//Namespace is the api that may GET, POST and DELETE.
	Namespace = "/api/v1/config/namespace/{name}"
	//LabelProperty is the api that may GET and POST.
	LabelProperty = "/api/v1/config/label-property"
	//ClusterVersion is the api that may GET and POST.
	ClusterVersion = "/api/v1/config/cluster-version"
	//StoreByID is the api that may GET and DELETE.
	StoreByID = "/api/v1/store/{id}"
	//StoreState is the api that may POST.
	StoreState = "/api/v1/store/{id}/state"
	//StoreLabels is the api that may POST.
	StoreLabels = "/api/v1/store/{id}/label"
	//StoreWeight is the api that may POST.
	StoreWeight = "/api/v1/store/{id}/weight"
	//StoreLimit is the api that may POST.
	StoreLimit = "/api/v1/store/{id}/limit"
	//Stores is the api that may GET.
	Stores = "/api/v1/stores"
	//RemoveTombStone is the api that may DELETE.
	RemoveTombStone = "/api/v1/stores/remove-tombstone"
	//AllLimit is the api that may GET and POST.
	AllLimit = "/api/v1/stores/limit"
	//Labels is the api that may GET.
	Labels = "/api/v1/labels"
	//LabelAllStores is the api that may GET.
	LabelAllStores = "/api/v1/labels/stores"
	//HotWriteRegions is the api that may GET.
	HotWriteRegions = "/api/v1/hotspot/regions/write"
	//HotReadRegions is the api that may GET.
	HotReadRegions = "/api/v1/hotspot/regions/read"
	//HotStores is the api that may GET.
	HotStores = "/api/v1/hotspot/stores"
	//RegionByID is the api that may GET.
	RegionByID = "/api/v1/region/id/{id}"
	//RegionByKey is the api that may GET.
	RegionByKey = "/api/v1/region/key/{key}"
	//Regions is the api that may GET.
	Regions = "/api/v1/regions"
	//ScanRegions is the api that may GET.
	ScanRegions = "/api/v1/regions/key"
	//StoreRegions is the api that may GET.
	StoreRegions = "/api/v1/regions/store/{id}"
	//TopWriteFlow is the api that may GET.
	TopWriteFlow = "/api/v1/regions/writeflow"
	//TopReadFlow is the api that may GET.
	TopReadFlow = "/api/v1/regions/readflow"
	//TopConfVer is the api that may GET.
	TopConfVer = "/api/v1/regions/confver"
	//TopVersion is the api that may GET.
	TopVersion = "/api/v1/regions/version"
	//TopSize is the api that may GET.
	TopSize = "/api/v1/regions/size"
	//MissPeerRegions is the api that may GET.
	MissPeerRegions = "/api/v1/regions/check/miss-peer"
	//ExtraPeerRegions is the api that may GET.
	ExtraPeerRegions = "/api/v1/regions/check/extra-peer"
	//PendingPeerRegions is the api that may GET.
	PendingPeerRegions = "/api/v1/regions/check/pending-peer"
	//DownPeerRegions is the api that may GET.
	DownPeerRegions = "/api/v1/regions/check/down-peer"
	//OfflinePeer is the api that may GET.
	OfflinePeer = "/api/v1/regions/check/offline-peer"
	//EmptyRegion is the api that may GET.
	EmptyRegion = "/api/v1/regions/check/empty-region"
	//RegionSiblings is the api that may GET.
	RegionSiblings = "/api/v1/regions/sibling/{id}"
	//IncorrectNamespaceRegions is the api that may GET.
	IncorrectNamespaceRegions = "/api/v1/regions/check/incorrect-ns"
	//Version is the api that may GET.
	Version = "/api/v1/version"
	//Status is the api that may GET.
	Status = "/api/v1/status"
	//Members is the api that may GET.
	Members = "/api/v1/members"
	//MembersByName is the api that may DELETE and POST.
	MembersByName = "/api/v1/members/name/{name}"
	//MembersByID is the api that may DELETE.
	MembersByID = "/api/v1/members/id/{id}"
	//Leader is the api that may GET.
	Leader = "/api/v1/leader"
	//LeaderResign is the api that may POST.
	LeaderResign = "/api/v1/leader/resign"
	//LeaderTransfer is the api that may POST.
	LeaderTransfer = "/api/v1/leader/transfer/{next_leader}"
	//StatsRegion is the api that may GET.
	StatsRegion = "/api/v1/stats/region"
	//Trends is the api that may GET.
	Trends = "/api/v1/trend"
	//HandleDropCacheRegion is the api that may DELETE.
	HandleDropCacheRegion = "/api/v1/admin/cache/region/{id}"
	//AdminLog is the api that may POST.
	AdminLog = "/api/v1/admin/log"
	//Healths is the api that may GET.
	Healths = "/api/v1/health"
	//Diagnose is the api that may GET.
	Diagnose = "/api/v1/diagnose"
	//DeprecatedHealths is the api that may GET.
	DeprecatedHealths = "/health"
	//DeprecatedDiagnose is the api that may GET.
	DeprecatedDiagnose = "/diagnose"
	//Classifier is the prefix.
	Classifier = "/api/v1/classifier/"
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
