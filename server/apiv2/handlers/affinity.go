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
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterAffinity registers affinity group related handlers to router paths.
func RegisterAffinity(r *gin.RouterGroup) {
	router := r.Group("affinity-groups")
	router.Use(middlewares.BootstrapChecker())
	router.POST("", PostAffinityGroups)
	router.PATCH("", BatchModifyAffinityGroups)
	router.PUT("/:group_id", UpdateAffinityGroupPeers)
	router.DELETE("/:group_id", DeleteAffinityGroup)
	router.GET("", GetAllAffinityGroups)
	router.GET("/:group_id", GetAffinityGroup)
}

// --- API Structures ---

// AffinityKeyRange represents a key range in affinity group API requests.
// It uses underscore naming for JSON fields to maintain consistency with other API structures.
// For Classic custom groups, an empty start or end key leaves that endpoint unbounded.
type AffinityKeyRange struct {
	StartKey []byte `json:"start_key"`
	EndKey   []byte `json:"end_key"`
}

// toKeyutilKeyRange converts AffinityKeyRange to keyutil.KeyRange.
func (r AffinityKeyRange) toKeyutilKeyRange() keyutil.KeyRange {
	return keyutil.KeyRange{
		StartKey: r.StartKey,
		EndKey:   r.EndKey,
	}
}

// CreateAffinityGroupInput defines the input for a single group in the creation request.
type CreateAffinityGroupInput struct {
	Ranges []AffinityKeyRange `json:"ranges"`
}

// CreateAffinityGroupsRequest defines the body for the POST request.
type CreateAffinityGroupsRequest struct {
	AffinityGroups map[string]CreateAffinityGroupInput `json:"affinity_groups"`
}

// AffinityGroupsResponse defines the success response for the POST request.
type AffinityGroupsResponse struct {
	AffinityGroups map[string]*affinity.GroupState `json:"affinity_groups"`
}

// BatchDeleteAffinityGroupsRequest defines the body for batch delete request.
type BatchDeleteAffinityGroupsRequest struct {
	IDs   []string `json:"ids"`
	Force bool     `json:"force,omitempty"`
}

// GroupRangesModification defines add or remove operations for a specific group.
type GroupRangesModification struct {
	ID     string             `json:"id"`
	Ranges []AffinityKeyRange `json:"ranges"`
}

// BatchModifyAffinityGroupsRequest defines the body for batch modify request.
type BatchModifyAffinityGroupsRequest struct {
	Add    []GroupRangesModification `json:"add,omitempty"`
	Remove []GroupRangesModification `json:"remove,omitempty"`
}

// UpdateAffinityGroupPeersRequest defines the body for updating peer distribution of an affinity group.
type UpdateAffinityGroupPeersRequest struct {
	LeaderStoreID uint64   `json:"leader_store_id"`
	VoterStoreIDs []uint64 `json:"voter_store_ids"`
}

// --- Handlers ---

// PostAffinityGroups handles POST requests for affinity groups.
//
//	@Tags			affinity-groups
//	@Summary		Create or delete affinity groups.
//	@Description	Create affinity groups (default), or delete multiple groups if ?delete query parameter is present. When delete parameter is absent, expects CreateAffinityGroupsRequest in body. When delete parameter is present, expects BatchDeleteAffinityGroupsRequest in body.
//	@Param			delete				query	string	false	"If present, triggers batch deletion instead of creation"
//	@Param			skip_exist_check	query	bool	false	"If true, skip existing groups and create the rest"
//	@Param			body				body	object	true	"Request body (type depends on delete parameter)"
//	@Produce		json
//	@Success		200	{object}	AffinityGroupsResponse	"Response for create or delete operation"
//	@Failure		400	{string}	string					"The input is invalid."
//	@Failure		500	{string}	string					"PD server failed to proceed the request."
//	@Router			/affinity-groups [post]
func PostAffinityGroups(c *gin.Context) {
	// Check if this is a delete operation via ?delete query parameter
	// Use POST /?delete to delete multiple objects
	// Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-multiple-objects.html
	if _, hasDelete := c.GetQuery("delete"); hasDelete {
		deleteAffinityGroups(c)
		return
	}
	// Default: create operation
	createAffinityGroups(c)
}

// createAffinityGroups automatically creates and configures affinity groups.
func createAffinityGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	req := &CreateAffinityGroupsRequest{}
	// Reject duplicate names before Unmarshal can merge map entries or struct fields.
	body, err := jsontext.NewDecoder(c.Request.Body).ReadValue()
	if errors.Is(err, jsontext.ErrDuplicateName) {
		handleAffinityError(c, errs.ErrAffinityGroupContent.FastGenByArgs(err.Error()))
		return
	}
	if err == nil {
		err = json.Unmarshal(body, req)
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	if len(req.AffinityGroups) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrAffinityGroupContent.GenWithStackByArgs("no affinity groups provided").Error())
		return
	}

	skipExistCheck := false
	if rawValue, ok := c.GetQuery("skip_exist_check"); ok {
		parsed, err := strconv.ParseBool(rawValue)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrInvalidArgument.GenWithStackByArgs("skip_exist_check", err).Error())
			return
		}
		skipExistCheck = parsed
	}

	groups := make([]affinity.GroupKeyRanges, 0, len(req.AffinityGroups))
	for groupID, input := range req.AffinityGroups {
		if err := affinity.ValidateGroupID(groupID); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
			return
		}
		if manager.IsGroupExist(groupID) {
			if skipExistCheck {
				continue
			}
			c.AbortWithStatusJSON(http.StatusConflict, errs.ErrAffinityGroupExist.GenWithStackByArgs(groupID).Error())
			return
		}
		if len(input.Ranges) == 0 {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrAffinityGroupContent.GenWithStackByArgs("no key ranges provided for group "+groupID).Error())
			return
		}

		// Convert AffinityKeyRange to keyutil.KeyRange
		var keyRanges []keyutil.KeyRange
		for _, kr := range input.Ranges {
			// Validate key range
			if err := validateKeyRange(groupID, kr.StartKey, kr.EndKey, false); err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
				return
			}
			keyRanges = append(keyRanges, kr.toKeyutilKeyRange())
		}

		groups = append(groups, affinity.GroupKeyRanges{
			GroupID:   groupID,
			KeyRanges: keyRanges,
		})
	}

	// Create affinity groups with their key ranges
	// The manager will handle storage persistence, label creation, and in-memory updates atomically
	if skipExistCheck {
		err = manager.CreateAffinityGroupsIfNotExists(groups)
	} else {
		err = manager.CreateAffinityGroups(groups)
	}
	if handleAffinityError(c, err) {
		return
	}

	// Convert internal manager output to API output.
	resp := AffinityGroupsResponse{
		AffinityGroups: make(map[string]*affinity.GroupState, len(req.AffinityGroups)),
	}
	for _, group := range groups {
		state := manager.GetAffinityGroupState(group.GroupID)
		if state == nil {
			state = &affinity.GroupState{}
		}
		resp.AffinityGroups[group.GroupID] = state
	}
	if skipExistCheck {
		for groupID := range req.AffinityGroups {
			if _, ok := resp.AffinityGroups[groupID]; ok {
				continue
			}
			state := manager.GetAffinityGroupState(groupID)
			if state == nil {
				state = &affinity.GroupState{}
			}
			resp.AffinityGroups[groupID] = state
		}
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// deleteAffinityGroups deletes multiple affinity groups in batch.
func deleteAffinityGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	req := &BatchDeleteAffinityGroupsRequest{}
	if err := c.BindJSON(req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}

	if len(req.IDs) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrEmptyRequest.GenWithStackByArgs("no group ids provided").Error())
		return
	}

	// Validate all group IDs
	for _, id := range req.IDs {
		if err := affinity.ValidateExistingGroupID(id); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
			return
		}
	}

	// Semantics are enforced in manager:
	// - force=false: missing IDs will cause an error
	// - force=true: missing IDs are ignored while existing groups are deleted
	err = manager.DeleteAffinityGroups(req.IDs, req.Force)
	if handleAffinityError(c, err) {
		return
	}

	c.IndentedJSON(http.StatusOK, map[string]any{
		"deleted": req.IDs,
	})
}

// BatchModifyAffinityGroups batch modifies key ranges for multiple affinity groups.
//
//	@Tags		affinity-groups
//	@Summary	Batch modify key ranges for affinity groups. Remove operations are executed before add operations.
//	@Param		body	body	BatchModifyAffinityGroupsRequest	true	"Batch modify request with add and remove operations"
//	@Produce	json
//	@Success	200	{object}	AffinityGroupsResponse	"Updated affinity groups"
//	@Failure	400	{string}	string					"The input is invalid."
//	@Failure	500	{string}	string					"PD server failed to proceed the request."
//	@Router		/affinity-groups [patch]
func BatchModifyAffinityGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	req := &BatchModifyAffinityGroupsRequest{}
	if err := c.BindJSON(req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}

	if len(req.Add) == 0 && len(req.Remove) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrEmptyRequest.GenWithStackByArgs("no add or remove operations provided").Error())
		return
	}

	// Check if any group appears in both add and remove operations.
	addGroups := make(map[string]bool)
	for _, op := range req.Add {
		addGroups[op.ID] = true
	}
	for _, op := range req.Remove {
		if addGroups[op.ID] {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrAffinityGroupConflict.GenWithStackByArgs(op.ID).Error())
			return
		}
	}

	// Validate and convert operations in one pass
	affectedGroups := make(map[string]bool)
	addOps, err := convertAndValidateRangeOps(req.Add, manager, affectedGroups, false)
	if handleAffinityError(c, err) {
		return
	}
	removeOps, err := convertAndValidateRangeOps(req.Remove, manager, affectedGroups, true)
	if handleAffinityError(c, err) {
		return
	}

	// Call manager to perform batch modify
	err = manager.UpdateAffinityGroupKeyRanges(addOps, removeOps)
	if handleAffinityError(c, err) {
		return
	}

	// Build response with all affected groups
	resp := AffinityGroupsResponse{
		AffinityGroups: make(map[string]*affinity.GroupState, len(affectedGroups)),
	}

	for groupID := range affectedGroups {
		state := manager.GetAffinityGroupState(groupID)
		if state != nil {
			resp.AffinityGroups[groupID] = state
		}
	}

	c.IndentedJSON(http.StatusOK, resp)
}

// UpdateAffinityGroupPeers updates leader and voter stores of an affinity group.
//
//	@Tags		affinity-groups
//	@Summary	Update leader and voter stores of an affinity group.
//	@Param		group_id	path	string							true	"The group id of the affinity group"
//	@Param		body		body	UpdateAffinityGroupPeersRequest	true	"New leader and voter store IDs"
//	@Produce	json
//	@Success	200	{object}	affinity.GroupState	"Updated affinity group state"
//	@Failure	400	{string}	string				"The input is invalid."
//	@Failure	404	{string}	string				"Affinity group not found."
//	@Failure	500	{string}	string				"PD server failed to proceed the request."
//	@Router		/affinity-groups/{group_id} [put]
func UpdateAffinityGroupPeers(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	groupID := c.Param("group_id")
	req := &UpdateAffinityGroupPeersRequest{}
	if err := c.BindJSON(req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	if err := affinity.ValidateGroupID(groupID); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}
	// TODO: check whether leader store ID and voter store IDs are healthy and fit the group.
	if req.LeaderStoreID == 0 || len(req.VoterStoreIDs) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrAffinityGroupContent.GenWithStackByArgs("leader_store_id and voter_store_ids are required").Error())
		return
	}

	// Note: Duplicate store ID and leader-in-voters validation is performed by AdjustGroup in the manager layer
	state, err := manager.UpdateAffinityGroupPeers(groupID, req.LeaderStoreID, req.VoterStoreIDs)
	if handleAffinityError(c, err) {
		return
	}

	c.IndentedJSON(http.StatusOK, state)
}

// DeleteAffinityGroup deletes a specific affinity group by group id.
//
//	@Tags			affinity-groups
//	@Summary		Delete an affinity group by group id.
//	@Param			group_id	path	string	true	"The group id of the affinity group"
//	@Param			force		query	bool	false	"Force delete even if the group has key ranges"
//	@Produce		json
//	@Success		200	{string}	string	"Affinity group deleted successfully."
//	@Failure		400	{string}	string	"The input is invalid or group has ranges without force flag."
//	@Failure		404	{string}	string	"Affinity group not found."
//	@Failure		500	{string}	string	"PD server failed to proceed the request."
//	@Router			/affinity-groups/{group_id} [delete]
//	@Description	force=false: missing group returns 404; force=true: the group and its label rule are deleted if they exist, so a label rule left without its group is deleted too.
func DeleteAffinityGroup(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	groupID := c.Param("group_id")
	if err := affinity.ValidateExistingGroupID(groupID); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}

	// Read force parameter from query string, default to false
	var queryParams struct {
		Force bool `form:"force"`
	}
	if err := c.ShouldBindQuery(&queryParams); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	force := queryParams.Force

	if !force && !manager.IsGroupExist(groupID) {
		c.AbortWithStatusJSON(http.StatusNotFound, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID).Error())
		return
	}

	// A forced delete also runs when the group does not exist, so that it
	// removes a label rule left without its group, for example by a creation
	// request that failed after some of its transactions committed.
	err = manager.DeleteAffinityGroups([]string{groupID}, force)
	if handleAffinityError(c, err) {
		return
	}

	c.JSON(http.StatusOK, "Affinity group deleted successfully.")
}

// GetAllAffinityGroups lists affinity groups, with optional range details.
//
//	@Tags		affinity-groups
//	@Summary	List all affinity groups.
//	@Param		ids		query	[]string	false	"Optional affinity group IDs. Repeat as ids=a&ids=b."
//	@Param		prefix	query	string		false	"Optional prefix that the IDs of the listed groups start with."
//	@Produce	json
//	@Success	200	{object}	AffinityGroupsResponse
//	@Failure	400	{string}	string	"The input is invalid."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/affinity-groups [get]
func GetAllAffinityGroups(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	groupStates, err := affinity.CollectGroupStatesByQuery(manager, c.QueryArray("ids"), c.Query("prefix"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}

	response := AffinityGroupsResponse{
		AffinityGroups: groupStates,
	}
	c.IndentedJSON(http.StatusOK, response)
}

// GetAffinityGroup gets a specific affinity group by group id, with optional range details.
//
//	@Tags		affinity-groups
//	@Summary	Get an affinity group by group id.
//	@Param		group_id	path	string	true	"The group id of the affinity group"
//	@Produce	json
//	@Success	200	{object}	*affinity.GroupState
//	@Failure	400	{string}	string	"Invalid affinity group ID."
//	@Failure	404	{string}	string	"Affinity group not found."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/affinity-groups/{group_id} [get]
func GetAffinityGroup(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	groupID := c.Param("group_id")
	groupState, err := manager.CheckAndGetAffinityGroupState(groupID)
	if handleAffinityError(c, err) {
		return
	}
	c.IndentedJSON(http.StatusOK, groupState)
}

// handleAffinityError maps affinity-related errors to HTTP status codes and writes the response.
// Returns true if the error has been handled and a response has been written.
func handleAffinityError(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errs.ErrAffinityGroupNotFound.Equal(err):
		c.AbortWithStatusJSON(http.StatusNotFound, err.Error())
	case errs.ErrAffinityGroupContent.Equal(err),
		errs.ErrInvalidGroupID.Equal(err),
		errs.ErrEmptyRequest.Equal(err),
		errs.ErrAffinityGroupConflict.Equal(err),
		errs.ErrInvalidKeyFormat.Equal(err):
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
	case errs.ErrAffinityGroupExist.Equal(err):
		c.AbortWithStatusJSON(http.StatusConflict, err.Error())
	default:
		// such as ErrAffinityInternal
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
	}
	return true
}

// validateKeyRange checks the endpoints of a range in a create, add, or remove request.
func validateKeyRange(groupID string, startKey, endKey []byte, remove bool) error {
	// Both empty represents the entire key space.
	if len(startKey) == 0 && len(endKey) == 0 {
		return nil
	}
	if len(startKey) == 0 || len(endKey) == 0 {
		// Removal must match persisted ranges even if creation would reject them.
		if remove || (!kerneltype.IsNextGen() && !strings.HasPrefix(groupID, "_tidb_")) {
			return nil
		}
		return errs.ErrAffinityGroupContent.FastGenByArgs("key range must have both start_key and end_key, or both empty for entire key space")
	}
	if bytes.Compare(startKey, endKey) >= 0 {
		return errs.ErrAffinityGroupContent.FastGenByArgs("start_key must be less than end_key")
	}
	return nil
}

// convertAndValidateRangeOps validates group operations and converts them to GroupKeyRanges format.
// It updates affectedGroups with all groups encountered.
// Multiple operations for the same group are automatically merged.
func convertAndValidateRangeOps(ops []GroupRangesModification, manager *affinity.Manager, affectedGroups map[string]bool, remove bool) ([]affinity.GroupKeyRanges, error) {
	grouped := make(map[string][]keyutil.KeyRange)
	order := make([]string, 0, len(ops))
	validateID := affinity.ValidateGroupID
	if remove {
		validateID = affinity.ValidateExistingGroupID
	}

	for _, op := range ops {
		if err := validateID(op.ID); err != nil {
			return nil, err
		}
		if !manager.IsGroupExist(op.ID) {
			return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(op.ID)
		}
		affectedGroups[op.ID] = true
		if len(op.Ranges) == 0 {
			return nil, errs.ErrAffinityGroupContent.FastGenByArgs("no key ranges provided")
		}

		if _, ok := grouped[op.ID]; !ok {
			order = append(order, op.ID)
		}
		// Validate and merge ranges for this group
		for _, kr := range op.Ranges {
			if err := validateKeyRange(op.ID, kr.StartKey, kr.EndKey, remove); err != nil {
				return nil, err
			}
			grouped[op.ID] = append(grouped[op.ID], keyutil.KeyRange{
				StartKey: kr.StartKey,
				EndKey:   kr.EndKey,
			})
		}
	}

	// Preserve the first occurrence of each group in the request.
	result := make([]affinity.GroupKeyRanges, 0, len(grouped))
	for _, groupID := range order {
		result = append(result, affinity.GroupKeyRanges{
			GroupID:   groupID,
			KeyRanges: grouped[groupID],
		})
	}

	return result, nil
}
