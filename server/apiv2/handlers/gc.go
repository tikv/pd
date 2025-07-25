// Copyright 2023 TiKV Project Authors.
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
	// "encoding/json"
	"net/http"
	"strconv"
	// "strings"
	// "time"

	"github.com/gin-gonic/gin"

	// "github.com/pingcap/errors"
	// "github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace/constant"
	// "github.com/tikv/pd/pkg/errs"
	// "github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterGC register keyspace related handlers to router paths.
func RegisterGC(r *gin.RouterGroup) {
	router := r.Group("gc")
	router.Use(middlewares.BootstrapChecker())
	router.GET("/:name", GetGCState)
	router.GET("/id/:id", GetGCStateByID)
}

// GetGCState returns target keyspace.
//
// @Tags     GC
// @Summary  Get GC info.
// @Param    name  path  string  true  "Keyspace Name"
// @Produce  json
// @Success  200  {object}  GCState
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /gc/{name} [get]
func GetGCState(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}

	var keyspaceID uint32
	name := c.Param("name")
	if name == "" {
		keyspaceID  = constant.NullKeyspaceID
	} else {
		meta, err := manager.LoadKeyspace(name)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		keyspaceID = meta.Id
	}

	m := svr.GetGCStateManager()
	state, err := m.GetGCState(keyspaceID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, state)
}

// GetGCStateByID returns target keyspace.
//
// @Tags     GC
// @Summary  Get GC info
// @Param    id  path  string  true  "Keyspace id"
// @Produce  json
// @Success  200  {object}  GCState
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /gc/id/{id} [get]
func GetGCStateByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "invalid keyspace id")
		return
	}
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetGCStateManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	state, err := manager.GetGCState(uint32(id))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, state)
}
