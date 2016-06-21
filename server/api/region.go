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

	"github.com/pingcap/pd/server"
)

type regionController struct {
	baseController
}

func (rc *regionController) GetRegion() {
	cluster, err := server.PdServer.GetRaftCluster()
	if err != nil {
		rc.serveError(500, err)
		return
	}
	if cluster == nil {
		rc.ServeJSON()
		return
	}

	regionIDStr := rc.Ctx.Input.Param(":regionID")
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		rc.serveError(500, err)
		return
	}

	region, err := cluster.GetRegionByID(regionID)
	if err != nil {
		rc.serveError(500, err)
		return
	}

	rc.Data["json"] = region
	rc.ServeJSON()
}

func (rc *regionController) GetRegions() {
}
