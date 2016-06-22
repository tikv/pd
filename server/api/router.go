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

func createRouter(srv *server.Server) *mux.Router {
	rd := render.New(render.Options{
		IndentJSON: true,
	})

	router := mux.NewRouter()
	router.Handle("/api/v1/balancers", newBalancerHandler(srv, rd)).Methods("GET")
	router.Handle("/api/v1/cluster", newClusterHandler(srv, rd)).Methods("GET")
	router.Handle("/api/v1/store/{id}", newStoreHandler(srv, rd)).Methods("GET")
	router.Handle("/api/v1/stores", newStoresHandler(srv, rd)).Methods("GET")
	router.Handle("/api/v1/region/{id}", newRegionHandler(srv, rd)).Methods("GET")
	router.Handle("/api/v1/regions", newRegionsHandler(srv, rd)).Methods("GET")
	router.Handle("/api/v1/version", newVersionHandler(rd)).Methods("GET")

	return router
}
