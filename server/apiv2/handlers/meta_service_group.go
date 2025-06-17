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
	"fmt"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

const (
	metaServiceGroupUninitializedErr = "meta-service groups manager is not initialized"
	metaServiceGroupAlreadyExistsErr = "meta-service group %s already exists"
)

// RegisterMetaServiceGroup registers meta-service group related handlers to router paths.
func RegisterMetaServiceGroup(r *gin.RouterGroup) {
	router := r.Group("meta-service-groups")
	router.Use(middlewares.BootstrapChecker())
	router.GET("", GetMetaServiceGroups)
	router.POST("", AddMetaServiceGroups)
}

// MetaServiceGroupStatus represents the status of a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatus struct {
	ID                string `json:"id"`
	Addresses         string `json:"addresses"`
	AssignedKeyspaces int    `json:"assigned_keyspaces"`
}

// AddMetaServiceGroupRequest represents a request to add a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type AddMetaServiceGroupRequest struct {
	ID        string `json:"id"`
	Addresses string `json:"addresses"`
}

// AddMetaServiceGroups adds one or more meta-service groups as specified in the request.
//
// @Tags     meta-service-groups
// @Summary  Adds new meta-service groups.
// @Param    body  body  []AddMetaServiceGroupRequest  true  "List of meta-service groups to add"
// @Produce  json
// @Success  200  {object}  []MetaServiceGroupStatus  "List of newly added plus existing groups"
// @Failure  400  {string}  string                    "Bad request (invalid JSON or duplicate group)"
// @Failure  500  {string}  string                    "Internal server error"
// @Router   /meta-service-groups [post]
func AddMetaServiceGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetMetaServiceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, metaServiceGroupUninitializedErr)
		return
	}

	var requests []AddMetaServiceGroupRequest
	err := c.BindJSON(&requests)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	// Constructs new meta-service groups.
	currentGroups := manager.GetGroups()
	newGroups := make(map[string]string)
	for _, request := range requests {
		// Update existing newGroups is not allowed via the post-method.
		if _, exists := currentGroups[request.ID]; exists {
			c.AbortWithStatusJSON(http.StatusBadRequest, fmt.Errorf(metaServiceGroupAlreadyExistsErr, request.ID))
			return
		}
		newGroups[request.ID] = request.Addresses
	}
	for id, addresses := range currentGroups {
		newGroups[id] = addresses
	}
	// Update persisted pd config.
	keyspaceCfg := svr.GetConfig().Keyspace
	keyspaceCfg.MetaServiceGroups = newGroups
	if err = svr.SetKeyspaceConfig(keyspaceCfg); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	assignmentCounts, err := manager.GetAssignmentCounts()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	response := make([]MetaServiceGroupStatus, 0, len(newGroups))
	for id, addresses := range newGroups {
		response = append(response, MetaServiceGroupStatus{
			ID:                id,
			Addresses:         addresses,
			AssignedKeyspaces: assignmentCounts[id],
		})
	}
	c.IndentedJSON(http.StatusOK, response)
}

// GetMetaServiceGroups returns a list of all meta-service groups and their assignment counts.
//
// @Tags     meta-service-groups
// @Summary  Get meta-service groups.
// @Produce  json
// @Success  200  {object}  []MetaServiceGroupStatus  "List of all meta-service groups"
// @Failure  500  {string}  string                    "Internal server error"
// @Router   /meta-service-groups [get]
func GetMetaServiceGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetMetaServiceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, metaServiceGroupUninitializedErr)
		return
	}

	groups := manager.GetGroups()
	assignmentCounts, err := manager.GetAssignmentCounts()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	response := make([]MetaServiceGroupStatus, 0, len(groups))
	for id, addresses := range groups {
		response = append(response, MetaServiceGroupStatus{
			ID:                id,
			Addresses:         addresses,
			AssignedKeyspaces: assignmentCounts[id],
		})
	}
	// sort for deterministic output
	sort.Slice(response, func(i, j int) bool {
		return response[i].ID < response[j].ID
	})
	c.IndentedJSON(http.StatusOK, response)
}
