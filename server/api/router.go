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
	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

func createRouter(prefix string, svr *server.Server) *mux.Router {
	rd := render.New(render.Options{
		Directory:  "templates",
		Extensions: []string{".html"},
		Asset:      Asset,
		AssetNames: func() []string {
			return []string{"templates/index.html"}
		},
		IndentJSON: true,
		Delims:     render.Delims{"[[", "]]"},
	})

	router := mux.NewRouter().PathPrefix(prefix).Subrouter()

	handler := svr.GetHandler()
	schedulerHandler := newSchedulerHandler(handler, rd)
	router.HandleFunc("/api/v1/schedulers", schedulerHandler.List).Methods("GET")
	router.HandleFunc("/api/v1/schedulers", schedulerHandler.Post).Methods("POST")
	router.HandleFunc("/api/v1/schedulers/{name}", schedulerHandler.Delete).Methods("DELETE")

	router.Handle("/api/v1/cluster", newClusterHandler(svr, rd)).Methods("GET")

	confHandler := newConfHandler(svr, rd)
	router.HandleFunc("/api/v1/config", confHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/config", confHandler.Post).Methods("POST")

	storeHandler := newStoreHandler(svr, rd)
	router.HandleFunc("/api/v1/store/{id}", storeHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/store/{id}", storeHandler.Delete).Methods("DELETE")
	router.Handle("/api/v1/stores", newStoresHandler(svr, rd)).Methods("GET")

	router.Handle("/api/v1/events", newEventsHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/feed", newFeedHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/history/operators", newHistoryOperatorHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/region/{id}", newRegionHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/regions", newRegionsHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/version", newVersionHandler(rd)).Methods("GET")

	router.Handle("/api/v1/members", newMemberListHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/members/{name}", newMemberDeleteHandler(svr, rd)).Methods("DELETE")
	router.Handle("/api/v1/leader", newLeaderHandler(svr, rd)).Methods("GET")

	balancerHandler := newBalancerHandler(svr, rd)
	router.HandleFunc("/api/v1/balancers", balancerHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/balancers", balancerHandler.Post).Methods("POST")

	router.Handle("/", newHomeHandler(rd)).Methods("GET")
	router.Handle("/ws", newWSHandler(svr))

	return router
}
