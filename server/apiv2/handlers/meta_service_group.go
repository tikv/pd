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
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
	"github.com/tikv/pd/server/config"
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
	router.PATCH("/:id/status", PatchMetaServiceGroupStatus)
}

// MetaServiceGroupStatus represents the status of a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatus struct {
	// ID is the unique identifier of the meta-service group.
	ID string `json:"id"`
	// Addresses is a comma-separated list of addresses for the meta-service group.
	Addresses string `json:"addresses"`
	// Status is the persisted status (assignment count and enabled state).
	Status *endpoint.MetaServiceGroupStatus `json:"status,omitempty"`
	// AssignedKeyspaces is kept for backward compatibility with existing
	// clients. It mirrors Status.AssignmentCount; prefer Status going forward.
	AssignedKeyspaces int `json:"assigned_keyspaces"`
}

// PatchMetaServiceGroups applies a JSON Merge Patch to the meta-service groups.
//
// @Tags     meta-service-groups
// @Summary  Patch meta-service groups using JSON Merge Patch.
// @Param    body  body  object  true  "JSON Merge Patch for meta-service groups (string values for add/update, null for delete)"
// @Produce  json
// @Success  200  {array}   MetaServiceGroupStatus    "List of all meta-service groups after patch"
// @Failure  400  {string}  string                    "Bad request (invalid JSON or invalid operation)"
// @Failure  409  {string}  string                    "Conflicting concurrent update, retryable"
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
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	// A top-level JSON `null` decodes into a nil map, which would otherwise be
	// treated as a successful no-op. The request body must be a JSON object.
	if patch == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "request body must be a JSON object")
		return
	}
	normalizedPatch := make(map[string]*string, len(patch))
	for id, addresses := range patch {
		trimmedID := strings.TrimSpace(id)
		if trimmedID == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, "group ID cannot be empty or whitespace-only")
			return
		}
		if _, exists := normalizedPatch[trimmedID]; exists {
			c.AbortWithStatusJSON(http.StatusBadRequest, "duplicate meta-service group ID after trimming")
			return
		}
		// A null value (nil pointer) means delete the group. A non-null value
		// must trim to a non-empty address; an empty or whitespace-only address
		// is rejected.
		if addresses == nil {
			normalizedPatch[trimmedID] = nil
			continue
		}
		// Reject IDs that are not URL-safe on add/update: a '/' would make the
		// group unpatchable via /meta-service-groups/{id}/status. Deletes (nil
		// above) are still allowed so any legacy bad group can be removed.
		if !config.IsValidMetaServiceGroupID(trimmedID) {
			c.AbortWithStatusJSON(http.StatusBadRequest, "group ID cannot contain '/'")
			return
		}
		trimmedAddresses := strings.TrimSpace(*addresses)
		if trimmedAddresses == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, "group addresses cannot be empty or whitespace-only")
			return
		}
		normalizedPatch[trimmedID] = &trimmedAddresses
	}
	oldCfg := svr.GetPersistOptions().GetKeyspaceConfig()
	newCfg := oldCfg.Clone()
	newGroups := oldCfg.GetMetaServiceGroups()
	deletedGroups := make([]string, 0)
	for id, addresses := range normalizedPatch {
		if addresses == nil {
			// Remove operation
			delete(newGroups, id)
			deletedGroups = append(deletedGroups, id)
		} else {
			// Add or update operation
			newGroups[id] = *addresses
		}
	}
	newCfg.MetaServiceGroups = newGroups
	if err := manager.UpdateGroupsSafely(c.Request.Context(), newGroups, deletedGroups, func() error {
		return svr.SetKeyspaceConfigWithoutKeyspaceManagerUpdate(oldCfg, newCfg)
	}, func() {
		svr.UpdateKeyspaceConfig(newCfg)
	}); err != nil {
		if errors.Is(err, keyspace.ErrGroupHasAssignedKeyspaces) {
			c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, errs.ErrEtcdTxnConflict) {
			// A concurrent group update or assignment lost the etcd txn compare;
			// this is a retryable conflict rather than an internal error.
			c.AbortWithStatusJSON(http.StatusConflict, err.Error())
			return
		}
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	status, err := buildMetaServiceGroupStatus(c, manager)
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
// @Success  200  {array}   MetaServiceGroupStatus    "List of all meta-service groups"
// @Failure  500  {string}  string                    "Internal server error"
// @Router   /meta-service-groups [get]
func GetMetaServiceGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetMetaServiceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, metaServiceGroupUninitializedErr)
		return
	}

	status, err := buildMetaServiceGroupStatus(c, manager)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, status)
}

// PatchMetaServiceGroupStatus patches the status of a specific meta-service group.
//
// @Tags     meta-service-groups
// @Summary  Patch meta-service groups status.
// @Param    id    path  string  true  "Meta-service group ID"
// @Param    body  body  object  true  "Patch for meta-service status"
// @Produce  json
// @Success  200  {array}   MetaServiceGroupStatus    "List of all meta-service groups after patch"
// @Failure  400  {string}  string                    "Bad request (invalid JSON or invalid operation)"
// @Failure  404  {string}  string                    "The meta-service group does not exist"
// @Failure  409  {string}  string                    "Conflicting concurrent update, retryable"
// @Failure  500  {string}  string                    "Internal server error"
// @Router   /meta-service-groups/{id}/status [patch]
func PatchMetaServiceGroupStatus(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetMetaServiceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, metaServiceGroupUninitializedErr)
		return
	}
	groupID := c.Param("id")
	patch := &keyspace.MetaServiceGroupStatusPatch{}
	if err := c.ShouldBindJSON(patch); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	if err := manager.PatchStatus(c.Request.Context(), groupID, patch); err != nil {
		if errors.Is(err, keyspace.ErrUnknownMetaServiceGroup) {
			c.AbortWithStatusJSON(http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, keyspace.ErrInvalidAssignmentCount) {
			c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, errs.ErrEtcdTxnConflict) {
			// A concurrent status patch or assignment lost the etcd txn compare;
			// this is a retryable conflict rather than an internal error.
			c.AbortWithStatusJSON(http.StatusConflict, err.Error())
			return
		}
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	status, err := buildMetaServiceGroupStatus(c, manager)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, status)
}

func buildMetaServiceGroupStatus(c *gin.Context, manager *keyspace.MetaServiceGroupManager) ([]MetaServiceGroupStatus, error) {
	currentGroups := manager.GetGroups()
	statuses, err := manager.GetStatus(c.Request.Context())
	if err != nil {
		return nil, err
	}

	status := make([]MetaServiceGroupStatus, 0, len(currentGroups))
	for id, addresses := range currentGroups {
		// GetGroups and GetStatus take the lock separately, so a freshly added
		// group may be missing from statuses. Synthesize a default status so the
		// enabled state and assignment count stay consistent instead of emitting
		// a partial object.
		s := statuses[id]
		if s == nil {
			s = &endpoint.MetaServiceGroupStatus{}
		}
		status = append(status, MetaServiceGroupStatus{
			ID:                id,
			Addresses:         addresses,
			Status:            s,
			AssignedKeyspaces: s.AssignmentCount,
		})
	}
	// sort for deterministic output
	sort.Slice(status, func(i, j int) bool {
		return status[i].ID < status[j].ID
	})
	return status, nil
}
