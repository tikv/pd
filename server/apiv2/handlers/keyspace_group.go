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
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterTSOKeyspaceGroup registers keyspace group handlers to the server.
func RegisterTSOKeyspaceGroup(r *gin.RouterGroup) {
	router := r.Group("tso/keyspace-group")
	router.Use(middlewares.BootstrapChecker())
	router.POST("", CreateKeyspaceGroups)
	router.GET("", GetKeyspaceGroups)
	router.GET("/:id", GetKeyspaceGroupByID)
	router.DELETE("/:id", DeleteKeyspaceGroupByID)
}

// CreateKeyspaceGroupParams is the params for creating keyspace groups.
type CreateKeyspaceGroupParams struct {
	KeyspaceGroups []*endpoint.KeyspaceGroup `json:"keyspace-groups"`
}

// CreateKeyspaceGroups creates keyspace groups.
func CreateKeyspaceGroups(c *gin.Context) {
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	createParams := &CreateKeyspaceGroupParams{}
	err := c.BindJSON(createParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}

	for _, keyspaceGroup := range createParams.KeyspaceGroups {
		if err := validateKeyspaceGroupID(keyspaceGroup.ID); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
			return
		}
	}

	err = manager.CreateKeyspaces(createParams.KeyspaceGroups)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// GetKeyspaceGroups gets all keyspace groups.
func GetKeyspaceGroups(c *gin.Context) {
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	keyspaceGroups, err := manager.GetKeyspaceGroups()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	c.IndentedJSON(http.StatusOK, keyspaceGroups)
}

// GetKeyspaceGroupByID gets keyspace group by id.
func GetKeyspaceGroupByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	if err := validateKeyspaceGroupID(uint32(id)); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	kg, err := manager.GetKeyspaceGroupByID(uint32(id))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	c.IndentedJSON(http.StatusOK, kg)
}

// DeleteKeyspaceGroupByID deletes keyspace group by id.
func DeleteKeyspaceGroupByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	if err := validateKeyspaceGroupID(uint32(id)); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	svr := c.MustGet("server").(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	err = manager.DeleteKeyspaceGroupByID(uint32(id))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

func validateKeyspaceGroupID(id uint32) error {
	// TODO: check if id is in the valid range.
	return nil
}
