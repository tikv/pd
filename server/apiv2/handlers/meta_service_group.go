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
	"maps"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

const (
	metaServiceGroupUninitializedErr = "meta-service groups manager is not initialized"
)

// RegisterMetaServiceGroup registers meta-service group related handlers to router paths.
func RegisterMetaServiceGroup(r *gin.RouterGroup) {
	router := r.Group("meta-service-groups")
	router.Use(middlewares.BootstrapChecker())
	router.GET("", GetMetaServiceGroups)
	router.PATCH("", PatchMetaServiceGroups)
}

// MetaServiceGroupStatus represents the status of a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatus struct {
	// ID is the unique identifier of the meta-service group.
	ID string `json:"id"`
	// Addresses is a comma-separated list of addresses for the meta-service group.
	Addresses string `json:"addresses"`
	// AssignedKeyspaces is the number of keyspaces assigned to this meta-service group.
	AssignedKeyspaces int `json:"assigned_keyspaces"`
}

// PatchMetaServiceGroups applies a JSON Merge Patch to the meta-service groups.
//
// @Tags     meta-service-groups
// @Summary  Patch meta-service groups using JSON Merge Patch.
// @Param    body  body  object  true  "JSON Merge Patch for meta-service groups (string values for add/update, null for delete)"
// @Produce  json
// @Success  200  {object}  []MetaServiceGroupStatus  "List of all meta-service groups after patch"
// @Failure  400  {string}  string                    "Bad request (invalid JSON or invalid operation)"
// @Failure  500  {string}  string                    "Internal server error"
// @Router   /meta-service-groups [patch]
func PatchMetaServiceGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetMetaServiceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, metaServiceGroupUninitializedErr)
		return
	}

	var patch map[string]*string
	if err := c.BindJSON(&patch); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}

	currentGroups := manager.GetGroups()
	newGroups := make(map[string]string)
	maps.Copy(newGroups, currentGroups)

	for id, addresses := range patch {
		if addresses == nil {
			// Remove operation
			delete(newGroups, id)
		} else {
			// Add or update operation
			newGroups[id] = *addresses
		}
	}

	keyspaceCfg := svr.GetConfig().Keyspace
	keyspaceCfg.MetaServiceGroups = newGroups
	if err := svr.SetKeyspaceConfig(keyspaceCfg); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	status, err := buildMetaServiceGroupStatus(manager)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, status)
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

	status, err := buildMetaServiceGroupStatus(manager)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, status)
}

func buildMetaServiceGroupStatus(manager *keyspace.MetaServiceGroupManager) ([]MetaServiceGroupStatus, error) {
	currentGroups := manager.GetGroups()
	assignmentCounts, err := manager.GetAssignmentCounts()
	if err != nil {
		return nil, err
	}

	status := make([]MetaServiceGroupStatus, 0, len(currentGroups))
	for id, addresses := range currentGroups {
		status = append(status, MetaServiceGroupStatus{
			ID:                id,
			Addresses:         addresses,
			AssignedKeyspaces: assignmentCounts[id],
		})
	}
	// sort for deterministic output
	sort.Slice(status, func(i, j int) bool {
		return status[i].ID < status[j].ID
	})
	return status, nil
}
