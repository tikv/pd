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
)

func buildNewRouter() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/balancers", getBalancers).Methods("GET")
	router.HandleFunc("/api/v1/cluster", getCluster).Methods("GET")
	router.HandleFunc("/api/v1/store/{id}", getStore).Methods("GET")
	router.HandleFunc("/api/v1/stores", getStores).Methods("GET")
	router.HandleFunc("/api/v1/region/{id}", getRegion).Methods("GET")
	router.HandleFunc("/api/v1/regions", getRegions).Methods("GET")
	router.HandleFunc("/api/v1/version", getVersion).Methods("GET")

	return router
}
