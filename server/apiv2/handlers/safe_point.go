// Copyright 2026 TiKV Project Authors.
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

	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterSafePoint registers GC safe point related handlers to router paths.
func RegisterSafePoint(r *gin.RouterGroup) {
	router := r.Group("gc/safepoint")
	router.Use(middlewares.BootstrapChecker())
	router.GET("/:keyspaceID", LoadGCSafePoint)
}

// GCSafePoint is the response structure for GC safe point.
type GCSafePoint struct {
	KeyspaceID uint32 `json:"keyspace_id"`
	SafePoint  uint64 `json:"safe_point"`
}

// LoadGCSafePoint returns the GC safe point of the target keyspace.
//
//	@Tags		safepoint
//	@Summary	Get GC safe point.
//	@Param		keyspaceID	path	uint32	true	"Keyspace ID"
//	@Produce	json
//	@Success	200	{object}	GCSafePoint
//	@Failure	400	{string}	string	"The input is invalid."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/gc/safepoint/{keyspaceID} [get]
func LoadGCSafePoint(c *gin.Context) {
	keyspaceIDStr := c.Param("keyspaceID")
	keyspaceID, err := strconv.ParseUint(keyspaceIDStr, 10, 32)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace ID: "+keyspaceIDStr)
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetGCStateManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	gcState, err := manager.GetGCState(uint32(keyspaceID), true)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &GCSafePoint{
		KeyspaceID: uint32(keyspaceID),
		SafePoint:  gcState.GCSafePoint,
	})
}
