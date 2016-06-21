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
	Count  int             `json:"count"`
	Stores []*metapb.Store `json:"stores"`
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

	store, err := cluster.GetStore(storeID)
	if err != nil {
		sc.serveError(500, err)
		return
	}

	sc.Data["json"] = store
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
	sc.Data["json"] = &storeInfo{
		Count:  len(stores),
		Stores: stores,
	}
	sc.ServeJSON()
}
