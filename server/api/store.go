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
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
)

type storeController struct {
	baseController
}

type storeInfo struct {
	Store  *metapb.Store       `json:"store"`
	Status *server.StoreStatus `json:"status"`
}

type storesInfo struct {
	Count  int          `json:"count"`
	Stores []*storeInfo `json:"stores"`
}

func (sc *storeController) GetStore() {
	cluster, err := server.PdServer.GetRaftCluster()
	if err != nil {
		sc.serveError(500, err)
		return
	}
	if cluster == nil {
		sc.ServeJSON()
		return
	}

	storeIDStr := sc.Ctx.Input.Param(":storeID")
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		sc.serveError(500, err)
		return
	}

	store, status, err := cluster.GetStore(storeID)
	if err != nil {
		sc.serveError(500, err)
		return
	}

	storeInfo := &storeInfo{
		Store:  store,
		Status: status,
	}
	storeInfo.Status.Score = cluster.GetScore(storeInfo.Store, storeInfo.Status)

	sc.Data["json"] = storeInfo
	sc.ServeJSON()
}

func (sc *storeController) GetStores() {
	cluster, err := server.PdServer.GetRaftCluster()
	if err != nil {
		sc.serveError(500, err)
		return
	}
	if cluster == nil {
		sc.ServeJSON()
		return
	}

	stores := cluster.GetStores()
	storesInfo := &storesInfo{
		Count:  len(stores),
		Stores: make([]*storeInfo, 0, len(stores)),
	}

	for _, s := range stores {
		store, status, err := cluster.GetStore(s.GetId())
		if err != nil {
			sc.serveError(500, err)
			return
		}

		storeInfo := &storeInfo{
			Store:  store,
			Status: status,
		}
		storeInfo.Status.Score = cluster.GetScore(storeInfo.Store, storeInfo.Status)
		storesInfo.Stores = append(storesInfo.Stores, storeInfo)
	}

	sc.Data["json"] = storesInfo
	sc.ServeJSON()
}
