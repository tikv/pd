// Copyright 2025 TiKV Project Authors.
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
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterSafePoint register gc safe point related handlers to router paths.
func RegisterSafePoint(r *gin.RouterGroup) {
	router := r.Group("gc/safepoint")
	router.Use(middlewares.BootstrapChecker())
	router.GET("/:keyspaceID", LoadGCSafePoint)
}

// GCSafePoint is the response structure for gc safe point.
type GCSafePoint struct {
	*endpoint.GCSafePointV2
}

// LoadGCSafePoint returns target keyspace gc safe point.
//
// @Tags     safepoint
// @Summary  Get gc safe point.
// @Param    keyspaceId  path  uint32  true  "Keyspace ID"
// @Produce  json
// @Success  200  {object}  GCSafePoint
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /gc/safepoint/{keyspaceID} [get]
func LoadGCSafePoint(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetSafePointV2Manager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	keyspaceIDStr := c.Param("keyspaceID")
	keyspaceID, err := strconv.ParseUint(keyspaceIDStr, 10, 24)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace ID: "+keyspaceIDStr)
		return
	}
	resp, err := manager.LoadGCSafePoint(uint32(keyspaceID))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &GCSafePoint{resp})
}
