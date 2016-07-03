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

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/unrolled/render"
)

func createRouter(svr *server.Server) *mux.Router {
	rd := render.New(render.Options{
		Directory:  "templates",
		Extensions: []string{".html"},
		IndentJSON: true,
		Delims:     render.Delims{"[[", "]]"},
	})

	router := mux.NewRouter()
	router.Handle("/api/v1/balancers", newBalancerHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/cluster", newClusterHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/events", newEventsHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/feed", newFeedHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/history/operators", newHistoryOperatorHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/metrics", prometheus.Handler()).Methods("GET")
	router.Handle("/api/v1/store/{id}", newStoreHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/stores", newStoresHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/region/{id}", newRegionHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/regions", newRegionsHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/version", newVersionHandler(rd)).Methods("GET")

	router.Handle("/", newHomeHandler(rd)).Methods("GET")
	router.Handle("/ws", newWSHandler(svr))

	router.PathPrefix("/").Handler(http.FileServer(http.Dir("templates/static/")))

	return router
}
