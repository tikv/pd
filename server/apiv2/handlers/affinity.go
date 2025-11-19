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
	"encoding/hex"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterAffinity registers affinity group related handlers to router paths.
func RegisterAffinity(r *gin.RouterGroup) {
	router := r.Group("affinity-groups")
	router.Use(middlewares.BootstrapChecker())
	router.POST("", CreateAffinityGroups)
	router.DELETE("/:group_id", DeleteAffinityGroup)
	router.GET("", GetAllAffinityGroups)
	router.GET("/:group_id", GetAffinityGroup)
}

// --- API Structures ---

// CreateAffinityGroupInput defines the input for a single group in the creation request.
type CreateAffinityGroupInput struct {
	Ranges []keyutil.KeyRange `json:"ranges"`
}

// CreateAffinityGroupsRequest defines the body for the POST request.
type CreateAffinityGroupsRequest struct {
	AffinityGroups map[string]CreateAffinityGroupInput `json:"affinity_groups"`
	DataLayout     string                              `json:"data_layout,omitempty"`
	TableGroup     string                              `json:"table_group,omitempty"`
}

// AffinityGroupsResponse defines the success response for the POST request.
type AffinityGroupsResponse struct {
	AffinityGroups map[string]*affinity.GroupState `json:"affinity_groups"`
}

// --- Handlers ---

// CreateAffinityGroups automatically creates and configures affinity groups.
// @Tags     affinity-groups
// @Summary  Automatically create and configure affinity groups based on key ranges.
// @Param    body  body  CreateAffinityGroupsRequest  true  "Parameters to auto-create affinity groups"
// @Produce  json
// @Success  200  {object}  CreateAffinityGroupsResponse
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /affinity-groups [post]
func CreateAffinityGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	req := &CreateAffinityGroupsRequest{}
	if err := c.BindJSON(req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	// TODO: validate DataLayout and TableGroup if necessary
	if len(req.AffinityGroups) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrAffinityGroupContent.GenWithStackByArgs("no affinity groups provided"))
		return
	}

	groupsWithRanges := make([]affinity.GroupWithRanges, 0, len(req.AffinityGroups))
	
	// Prepare key ranges for validation
	var allNewRanges []affinity.KeyRangeInput
	
	for groupID, input := range req.AffinityGroups {
		if err := validateGroupID(groupID); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
			return
		}
		if manager.IsGroupExist(groupID) {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrAffinityGroupExist.GenWithStackByArgs(groupID))
			return
		}
		if len(input.Ranges) == 0 {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrAffinityGroupContent.GenWithStackByArgs("no key ranges provided for group "+groupID))
			return
		}
		
		// Convert KeyRange to labeler format (hex-encoded strings)
		var keyRanges []any
		for _, kr := range input.Ranges {
			keyRanges = append(keyRanges, map[string]any{
				"start_key": hex.EncodeToString(kr.StartKey),
				"end_key":   hex.EncodeToString(kr.EndKey),
			})
			
			// Collect key ranges for overlap validation
			allNewRanges = append(allNewRanges, affinity.KeyRangeInput{
				StartKey: kr.StartKey,
				EndKey:   kr.EndKey,
				GroupID:  groupID,
			})
		}
		
		// TODO: use a zero LeaderStoreID and empty VoterStoreIDs to indicate
		groupsWithRanges = append(groupsWithRanges, affinity.GroupWithRanges{
			Group: &affinity.Group{
				ID:            groupID,
				LeaderStoreID: 0,
				VoterStoreIDs: make([]uint64, 0, len(input.Ranges)),
			},
			KeyRanges: keyRanges,
		})
	}
	
	// Validate that key ranges don't overlap
	if err := manager.ValidateKeyRanges(allNewRanges); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}

	// Save affinity groups with their key ranges
	// The manager will handle storage persistence, label creation, and in-memory updates atomically
	if err := manager.SaveAffinityGroups(groupsWithRanges); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	// Convert internal manager output to API output.
	resp := AffinityGroupsResponse{
		AffinityGroups: make(map[string]*affinity.GroupState, len(req.AffinityGroups)),
	}
	for _, gwr := range groupsWithRanges {
		state := manager.GetAffinityGroupState(gwr.Group.ID)
		if state == nil {
			state = &affinity.GroupState{}
		}
		resp.AffinityGroups[gwr.Group.ID] = state
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// TODO: add more tests for CreateAffinityGroups and DeleteAffinityGroup
// after AllocAffinityGroup is ready.

// DeleteAffinityGroup deletes a specific affinity group by group id.
// @Tags     affinity-groups
// @Summary  Delete an affinity group by group id.
// @Param    group_id  path  string  true  "The group id of the affinity group"
// @Produce  json
// @Success  200  {string}  string  "Affinity group deleted successfully."
// @Failure  404  {string}  string  "Affinity group not found."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /affinity-groups/{group_id} [delete]
func DeleteAffinityGroup(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	groupID := c.Param("group_id")

	if !manager.IsGroupExist(groupID) {
		c.AbortWithStatusJSON(http.StatusNotFound, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID))
		return
	}

	// Delete the affinity group from manager
	err = manager.DeleteAffinityGroup(groupID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	// Note: Label deletion is now handled inside the DeleteAffinityGroup method

	c.JSON(http.StatusOK, "Affinity group deleted successfully.")
}

// GetAllAffinityGroups lists affinity groups, with optional range details.
// @Tags     affinity-groups
// @Summary  List all affinity groups.
// @Produce  json
// @Success  200  {object}  GetAllAffinityGroupsResponse
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /affinity-groups [get]
func GetAllAffinityGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	allGroupInfos := manager.GetAllAffinityGroupStates()
	response := AffinityGroupsResponse{
		AffinityGroups: make(map[string]*affinity.GroupState, len(allGroupInfos)),
	}
	for _, info := range allGroupInfos {
		response.AffinityGroups[info.ID] = info
	}
	c.IndentedJSON(http.StatusOK, response)
}

// GetAffinityGroup gets a specific affinity group by group id, with optional range details.
// @Tags     affinity-groups
// @Summary  Get an affinity group by group id.
// @Param    group_id    path  string  true   "The group id of the affinity group"
// @Produce  json
// @Success  200  {object}  *affinity.GroupState
// @Failure  404  {string}  string  "Affinity group not found."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /affinity-groups/{group_id} [get]
func GetAffinityGroup(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	groupID := c.Param("group_id")
	groupInfo := manager.GetAffinityGroupState(groupID)
	if groupInfo == nil {
		c.AbortWithStatusJSON(http.StatusNotFound, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID))
		return
	}
	c.IndentedJSON(http.StatusOK, groupInfo)
}

const (
	// idPattern is a regex that specifies acceptable characters of the id.
	// Valid id must be non-empty and 64 characters or fewer and consist only of letters (a-z, A-Z),
	// numbers (0-9), hyphens (-), and underscores (_).
	idPattern = "^[-A-Za-z0-9_]{1,64}$"
)

// validateGroupID check if user provided id is legal.
// It throws error when id contains illegal character.
func validateGroupID(id string) error {
	isValid, err := regexp.MatchString(idPattern, id)
	if err != nil {
		return err
	}
	if !isValid {
		return errors.Errorf("illegal id %s, should contain only alphanumerical and underline", id)
	}
	return nil
}
