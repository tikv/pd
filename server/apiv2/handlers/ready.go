// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// ReadyStatus reflects the cluster's ready status.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReadyStatus struct {
	RegionLoaded bool `json:"region_loaded"`
}

// @Summary  It will return whether pd follower is ready to became leader.
// @Router   /ready [get]
// @Param    verbose query  bool    false  "Whether to return details."
// @Success  200
// @Failure  500
func Ready(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	s := svr.GetStorage()
	regionLoaded := storage.AreRegionsLoaded(s)
	if regionLoaded {
		c.Status(http.StatusOK)
	} else {
		c.Status(http.StatusInternalServerError)
	}

	if _, ok := c.GetQuery("verbose"); !ok {
		return
	}
	resp := &ReadyStatus{
		RegionLoaded: regionLoaded,
	}
	if regionLoaded {
		c.IndentedJSON(http.StatusOK, resp)
	} else {
		c.AbortWithStatusJSON(http.StatusInternalServerError, resp)
	}
}
