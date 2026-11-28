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
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterAffinity registers affinity group related handlers to router paths.
func RegisterAffinity(r *gin.RouterGroup) {
	router := r.Group("affinity-groups")
	bootstrap := middlewares.BootstrapChecker()
	createAudit := middlewares.Audit("CreateAffinityGroups", audit.LocalLogLabel, audit.PrometheusHistogram)
	deleteAudit := middlewares.Audit("DeleteAffinityGroups", audit.LocalLogLabel, audit.PrometheusHistogram)
	router.POST("", bootstrap, func(c *gin.Context) {
		if _, hasDelete := c.GetQuery("delete"); hasDelete {
			deleteAudit(c)
		} else {
			createAudit(c)
		}
	}, PostAffinityGroups)
	router.POST("/targets", bootstrap, middlewares.Audit("UpdateAffinityGroupTargets", audit.LocalLogLabel, audit.PrometheusHistogram), UpdateAffinityGroupTargets)
	router.PATCH("/policies", bootstrap, middlewares.Audit("UpdateAffinityGroupPolicies", audit.LocalLogLabel, audit.PrometheusHistogram), UpdateAffinityGroupPolicies)
	router.PATCH("", bootstrap, middlewares.Audit("BatchModifyAffinityGroups", audit.LocalLogLabel, audit.PrometheusHistogram), BatchModifyAffinityGroups)
	router.PUT("/:group_id", bootstrap, middlewares.Audit("UpdateAffinityGroupPeers", audit.LocalLogLabel, audit.PrometheusHistogram), UpdateAffinityGroupPeers)
	router.PATCH("/:group_id/policy", bootstrap, middlewares.Audit("UpdateAffinityGroupPolicy", audit.LocalLogLabel, audit.PrometheusHistogram), UpdateAffinityGroupPolicy)
	router.DELETE("/:group_id", bootstrap, middlewares.Audit("DeleteAffinityGroup", audit.LocalLogLabel, audit.PrometheusHistogram), DeleteAffinityGroup)
	// Count reads forwarded to scheduling as well as reads handled locally.
	affinityRedirect := middlewares.AffinityMicroserviceRedirector()
	router.GET("", middlewares.Audit("GetAllAffinityGroups", audit.PrometheusHistogram), affinityRedirect, bootstrap, GetAllAffinityGroups)
	router.GET("/:group_id", middlewares.Audit("GetAffinityGroup", audit.PrometheusHistogram), affinityRedirect, bootstrap, GetAffinityGroup)
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
	// InheritPolicyFrom names a group whose balance policy the new group takes,
	// such as the group that a TRUNCATE replaces. It is ignored if that group
	// does not exist, or if the new group already exists.
	InheritPolicyFrom string `json:"inherit_policy_from,omitempty"`
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

// UpdateAffinityGroupPolicyRequest defines the body for updating an affinity group's balance policy.
type UpdateAffinityGroupPolicyRequest struct {
	BalancePolicy affinity.BalancePolicy `json:"balance_policy"`
}

// UpdateAffinityGroupPoliciesRequest defines the body for updating the balance policy of several affinity groups.
type UpdateAffinityGroupPoliciesRequest struct {
	IDs           []string               `json:"ids"`
	BalancePolicy affinity.BalancePolicy `json:"balance_policy"`
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
		keyRanges := make([]keyutil.KeyRange, 0, len(input.Ranges))
		for _, kr := range input.Ranges {
			keyRanges = append(keyRanges, kr.toKeyutilKeyRange())
		}
		groups = append(groups, affinity.GroupKeyRanges{
			GroupID:           groupID,
			KeyRanges:         keyRanges,
			InheritPolicyFrom: input.InheritPolicyFrom,
		})
	}
	states, err := manager.CreateAffinityGroupsWithContext(c.Request.Context(), groups, skipExistCheck)
	if handleAffinityError(c, err) {
		return
	}
	c.IndentedJSON(http.StatusOK, AffinityGroupsResponse{AffinityGroups: states})
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

	// Semantics are enforced in manager:
	// - force=false: missing IDs will cause an error
	// - force=true: missing IDs are ignored while existing groups are deleted
	err = manager.DeleteAffinityGroupsWithContext(c.Request.Context(), req.IDs, req.Force)
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
//	@Summary	Batch modify key ranges for affinity groups. Add operations are persisted before remove operations; a partial failure keeps the committed prefix in this order.
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
	addOps, err := convertAndValidateRangeOps(req.Add, manager, affectedGroups)
	if handleAffinityError(c, err) {
		return
	}
	removeOps, err := convertAndValidateRangeOps(req.Remove, manager, affectedGroups)
	if handleAffinityError(c, err) {
		return
	}

	// Call manager to perform batch modify
	err = manager.UpdateAffinityGroupKeyRangesWithContext(c.Request.Context(), addOps, removeOps)
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
	state, err := manager.UpdateAffinityGroupPeersWithContext(c.Request.Context(), groupID, req.LeaderStoreID, req.VoterStoreIDs)
	if handleAffinityError(c, err) {
		return
	}

	c.IndentedJSON(http.StatusOK, state)
}

// UpdateAffinityGroupPolicy updates the balance policy of an affinity group.
//
//	@Tags		affinity-groups
//	@Summary	Update the balance policy of an affinity group.
//	@Param		group_id	path	string								true	"The group id of the affinity group"
//	@Param		body		body	UpdateAffinityGroupPolicyRequest	true	"New balance policy: auto or fixed"
//	@Produce	json
//	@Success	200	{object}	affinity.GroupState	"Updated affinity group state"
//	@Failure	400	{string}	string				"The input is invalid."
//	@Failure	404	{string}	string				"Affinity group not found."
//	@Failure	500	{string}	string				"PD server failed to proceed the request."
//	@Router		/affinity-groups/{group_id}/policy [patch]
func UpdateAffinityGroupPolicy(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	groupID := c.Param("group_id")
	req := &UpdateAffinityGroupPolicyRequest{}
	if err := c.BindJSON(req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	state, err := manager.UpdateAffinityGroupPolicyWithContext(c.Request.Context(), groupID, req.BalancePolicy)
	if handleAffinityError(c, err) {
		return
	}

	c.IndentedJSON(http.StatusOK, state)
}

// UpdateAffinityGroupPolicies updates the balance policy of several affinity groups.
// If prevalidation finds a missing group, nothing is written. Each chunk uses
// one etcd transaction. A storage error or stale state detected during execution
// leaves previously acknowledged chunks in place. Repeating the request is safe.
//
//	@Tags		affinity-groups
//	@Summary	Update the balance policy of several affinity groups.
//	@Param		body	body	UpdateAffinityGroupPoliciesRequest	true	"Group IDs and the new balance policy: auto or fixed"
//	@Produce	json
//	@Success	200	{object}	AffinityGroupsResponse	"Updated affinity group states"
//	@Failure	400	{string}	string					"The input is invalid."
//	@Failure	404	{string}	string					"An affinity group is not found."
//	@Failure	500	{string}	string					"PD server failed to proceed the request."
//	@Router		/affinity-groups/policies [patch]
func UpdateAffinityGroupPolicies(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	req := &UpdateAffinityGroupPoliciesRequest{}
	if err := c.BindJSON(req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	states, err := manager.UpdateAffinityGroupPoliciesWithContext(c.Request.Context(), req.IDs, req.BalancePolicy)
	if handleAffinityError(c, err) {
		return
	}

	resp := AffinityGroupsResponse{
		AffinityGroups: make(map[string]*affinity.GroupState, len(states)),
	}
	for _, state := range states {
		resp.AffinityGroups[state.ID] = state
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// UpdateAffinityGroupTargets writes leader and voter targets for several affinity groups.
// A target is written only if the stored group matches the expected contents and any supplied modification revision.
// The scheduling service uses it to store the targets it decides.
// Targets commit independently. Canceling the request stops further work and preserves commits already made.
//
//	@Tags			affinity-groups
//	@Summary		Write leader and voter targets for affinity groups whose stored groups are unchanged.
//	@Description	Each target commits independently, so an HTTP 500 on a later target leaves earlier commits in place.
//	@Param			body	body	affinity.GroupTargetsRequest	true	"Targets with the groups they were decided from"
//	@Produce		json
//	@Success		200	{object}	affinity.GroupTargetsResponse	"IDs of groups whose targets were newly written or already stored with the requested peers"
//	@Failure		400	{string}	string							"The input is invalid."
//	@Failure		500	{string}	string							"PD server failed to proceed the request."
//	@Router			/affinity-groups/targets [post]
func UpdateAffinityGroupTargets(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	req := &affinity.GroupTargetsRequest{}
	if err := c.BindJSON(req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause().Error())
		return
	}
	resp, err := manager.UpdateAffinityGroupTargets(c.Request.Context(), req.Targets)
	if handleAffinityError(c, err) {
		return
	}

	c.IndentedJSON(http.StatusOK, &resp)
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
	if err := affinity.ValidateGroupID(groupID); err != nil {
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
	err = manager.DeleteAffinityGroupsWithContext(c.Request.Context(), []string{groupID}, force)
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

// convertAndValidateRangeOps validates group operations and converts them to GroupKeyRanges format.
// It updates affectedGroups with all groups encountered.
// Multiple operations for the same group are automatically merged.
func convertAndValidateRangeOps(ops []GroupRangesModification, manager *affinity.Manager, affectedGroups map[string]bool) ([]affinity.GroupKeyRanges, error) {
	grouped := make(map[string][]keyutil.KeyRange)
	order := make([]string, 0, len(ops))
	for _, op := range ops {
		if err := affinity.ValidateGroupID(op.ID); err != nil {
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
			if err := affinity.ValidateKeyRange(op.ID, kr.StartKey, kr.EndKey); err != nil {
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
