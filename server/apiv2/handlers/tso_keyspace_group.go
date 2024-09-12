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
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// GroupManagerUninitializedErr is the error message for uninitialized keyspace group manager.
const GroupManagerUninitializedErr = "keyspace group manager is not initialized"

// RegisterTSOKeyspaceGroup registers keyspace group handlers to the server.
func RegisterTSOKeyspaceGroup(r *gin.RouterGroup) {
	router := r.Group("tso/keyspace-groups")
	router.Use(middlewares.BootstrapChecker())
	router.POST("", CreateKeyspaceGroups)
	router.GET("", GetKeyspaceGroups)
	router.GET("/:id", GetKeyspaceGroupByID)
	router.DELETE("/:id", DeleteKeyspaceGroupByID)
	router.PATCH("/:id", SetNodesForKeyspaceGroup)          // only to support set nodes
	router.PATCH("/:id/*node", SetPriorityForKeyspaceGroup) // only to support set priority
	router.POST("/:id/alloc", AllocNodesForKeyspaceGroup)
	router.POST("/:id/split", SplitKeyspaceGroupByID)
	router.DELETE("/:id/split", FinishSplitKeyspaceByID)
	router.POST("/:id/merge", MergeKeyspaceGroups)
	router.DELETE("/:id/merge", FinishMergeKeyspaceByID)
}

// CreateKeyspaceGroupParams defines the params for creating keyspace groups.
type CreateKeyspaceGroupParams struct {
	KeyspaceGroups []*endpoint.KeyspaceGroup `json:"keyspace-groups"`
}

// CreateKeyspaceGroups creates keyspace groups.
func CreateKeyspaceGroups(c *gin.Context) {
	createParams := &CreateKeyspaceGroupParams{}
	err := c.BindJSON(createParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	for _, keyspaceGroup := range createParams.KeyspaceGroups {
		if !isValid(keyspaceGroup.ID) {
			c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
			return
		}
		if keyspaceGroup.UserKind == "" {
			keyspaceGroup.UserKind = endpoint.Basic.String()
		} else if !endpoint.IsUserKindValid(keyspaceGroup.UserKind) {
			c.AbortWithStatusJSON(http.StatusBadRequest, "invalid user kind")
			return
		}
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	err = manager.CreateKeyspaceGroups(createParams.KeyspaceGroups)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// GetKeyspaceGroups gets keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func GetKeyspaceGroups(c *gin.Context) {
	scanStart, scanLimit, err := parseLoadAllQuery(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	keyspaceGroups, err := manager.GetKeyspaceGroups(scanStart, scanLimit)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	var kgs []*endpoint.KeyspaceGroup
	state, set := c.GetQuery("state")
	if set {
		state := strings.ToLower(state)
		switch state {
		case "merge":
			for _, keyspaceGroup := range keyspaceGroups {
				if keyspaceGroup.MergeState != nil {
					kgs = append(kgs, keyspaceGroup)
				}
			}
		case "split":
			for _, keyspaceGroup := range keyspaceGroups {
				if keyspaceGroup.SplitState != nil {
					kgs = append(kgs, keyspaceGroup)
				}
			}
		default:
		}
	} else {
		kgs = keyspaceGroups
	}

	c.IndentedJSON(http.StatusOK, kgs)
}

// GetKeyspaceGroupPrimaryResponse defines the response for getting primary node of keyspace group.
type GetKeyspaceGroupPrimaryResponse struct {
	ID      uint32 `json:"id"`
	Primary string `json:"primary"`
}

// GetKeyspaceGroupByID gets keyspace group by ID.
func GetKeyspaceGroupByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}

	fields := c.Query("fields") // split by comma if need to add more fields
	if fields == "primary" {
		primary, err := manager.GetKeyspaceGroupPrimaryByID(id)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		c.JSON(http.StatusOK, &GetKeyspaceGroupPrimaryResponse{
			ID:      id,
			Primary: primary,
		})
		return
	}

	kg, err := manager.GetKeyspaceGroupByID(id)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, kg)
}

// DeleteKeyspaceGroupByID deletes keyspace group by ID.
func DeleteKeyspaceGroupByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	kg, err := manager.DeleteKeyspaceGroupByID(id)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, kg)
}

// SplitKeyspaceGroupByIDParams defines the params for splitting a keyspace group.
type SplitKeyspaceGroupByIDParams struct {
	NewID     uint32   `json:"new-id"`
	Keyspaces []uint32 `json:"keyspaces"`
	// StartKeyspaceID and EndKeyspaceID are used to indicate the range of keyspaces to be split.
	StartKeyspaceID uint32 `json:"start-keyspace-id"`
	EndKeyspaceID   uint32 `json:"end-keyspace-id"`
}

var patrolKeyspaceAssignmentState struct {
	syncutil.RWMutex
	patrolled bool
}

// SplitKeyspaceGroupByID splits keyspace group by ID into a new keyspace group with the given new ID.
// And the keyspaces in the old keyspace group will be moved to the new keyspace group.
func SplitKeyspaceGroupByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	splitParams := &SplitKeyspaceGroupByIDParams{}
	err = c.BindJSON(splitParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	if !isValid(splitParams.NewID) {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	if len(splitParams.Keyspaces) == 0 && splitParams.StartKeyspaceID == 0 && splitParams.EndKeyspaceID == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid empty keyspaces")
		return
	}
	if splitParams.StartKeyspaceID < constant.DefaultKeyspaceID ||
		splitParams.StartKeyspaceID > splitParams.EndKeyspaceID {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid start/end keyspace id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	groupManager := svr.GetKeyspaceGroupManager()
	if groupManager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}

	patrolKeyspaceAssignmentState.Lock()
	if !patrolKeyspaceAssignmentState.patrolled {
		// Patrol keyspace assignment before splitting keyspace group.
		err = manager.PatrolKeyspaceAssignment(splitParams.StartKeyspaceID, splitParams.EndKeyspaceID)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			patrolKeyspaceAssignmentState.Unlock()
			return
		}
		patrolKeyspaceAssignmentState.patrolled = true
	}
	patrolKeyspaceAssignmentState.Unlock()

	// Split keyspace group.
	err = groupManager.SplitKeyspaceGroupByID(
		id, splitParams.NewID,
		splitParams.Keyspaces, splitParams.StartKeyspaceID, splitParams.EndKeyspaceID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// FinishSplitKeyspaceByID finishes split keyspace group by ID.
func FinishSplitKeyspaceByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	err = manager.FinishSplitKeyspaceByID(id)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// MergeKeyspaceGroupsParams defines the params for merging the keyspace groups.
type MergeKeyspaceGroupsParams struct {
	MergeList           []uint32 `json:"merge-list"`
	MergeAllIntoDefault bool     `json:"merge-all-into-default"`
}

// MergeKeyspaceGroups merges the keyspace groups in the merge list into the target keyspace group.
func MergeKeyspaceGroups(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	mergeParams := &MergeKeyspaceGroupsParams{}
	err = c.BindJSON(mergeParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	if len(mergeParams.MergeList) == 0 && !mergeParams.MergeAllIntoDefault {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid empty merge list")
		return
	}
	if len(mergeParams.MergeList) > 0 && mergeParams.MergeAllIntoDefault {
		c.AbortWithStatusJSON(http.StatusBadRequest, "non-empty merge list when merge all into default")
		return
	}
	for _, mergeID := range mergeParams.MergeList {
		if !isValid(mergeID) {
			c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
			return
		}
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	groupManager := svr.GetKeyspaceGroupManager()
	if groupManager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	// Merge keyspace group.
	if mergeParams.MergeAllIntoDefault {
		err = groupManager.MergeAllIntoDefaultKeyspaceGroup()
	} else {
		err = groupManager.MergeKeyspaceGroups(id, mergeParams.MergeList)
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// FinishMergeKeyspaceByID finishes merging keyspace group by ID.
func FinishMergeKeyspaceByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	err = manager.FinishMergeKeyspaceByID(id)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// AllocNodesForKeyspaceGroupParams defines the params for allocating nodes for keyspace groups.
type AllocNodesForKeyspaceGroupParams struct {
	Replica int `json:"replica"`
}

// AllocNodesForKeyspaceGroup allocates nodes for keyspace group.
func AllocNodesForKeyspaceGroup(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	allocParams := &AllocNodesForKeyspaceGroupParams{}
	err = c.BindJSON(allocParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	if manager.GetNodesCount() < allocParams.Replica || allocParams.Replica < constant.DefaultKeyspaceGroupReplicaCount {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid replica, should be in [2, nodes_num]")
		return
	}
	keyspaceGroup, err := manager.GetKeyspaceGroupByID(id)
	if err != nil || keyspaceGroup == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "keyspace group does not exist")
		return
	}
	if len(keyspaceGroup.Members) >= allocParams.Replica {
		c.AbortWithStatusJSON(http.StatusBadRequest, "existed replica is larger than the new replica")
		return
	}

	// check if nodes exist
	existMembers := make(map[string]struct{})
	for _, member := range keyspaceGroup.Members {
		if exist, addr := manager.IsExistNode(member.Address); exist {
			existMembers[addr] = struct{}{}
		}
	}
	// get the nodes
	nodes, err := manager.AllocNodesForKeyspaceGroup(id, existMembers, allocParams.Replica)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nodes)
}

// SetNodesForKeyspaceGroupParams defines the params for setting nodes for keyspace group.
// Notes: it should be used carefully.
type SetNodesForKeyspaceGroupParams struct {
	Nodes []string `json:"nodes"`
}

// SetNodesForKeyspaceGroup sets nodes for keyspace group.
func SetNodesForKeyspaceGroup(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	setParams := &SetNodesForKeyspaceGroupParams{}
	err = c.BindJSON(setParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	// check if keyspace group exists
	keyspaceGroup, err := manager.GetKeyspaceGroupByID(id)
	if err != nil || keyspaceGroup == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "keyspace group does not exist")
		return
	}
	// check if node exists
	for _, node := range setParams.Nodes {
		if exist, _ := manager.IsExistNode(node); !exist {
			c.AbortWithStatusJSON(http.StatusBadRequest, "node does not exist")
			return
		}
	}
	// set nodes
	err = manager.SetNodesForKeyspaceGroup(id, setParams.Nodes)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// SetPriorityForKeyspaceGroupParams defines the params for setting priority of tso node for the keyspace group.
type SetPriorityForKeyspaceGroupParams struct {
	Priority int `json:"priority"`
}

// SetPriorityForKeyspaceGroup sets priority of tso node for the keyspace group.
func SetPriorityForKeyspaceGroup(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	node, err := parseNodeAddress(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid node address")
		return
	}
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
		return
	}
	setParams := &SetPriorityForKeyspaceGroupParams{}
	err = c.BindJSON(setParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	// check if keyspace group exists
	kg, err := manager.GetKeyspaceGroupByID(id)
	if err != nil || kg == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "keyspace group does not exist")
		return
	}
	// check if node exists
	members := kg.Members
	if slice.NoneOf(members, func(i int) bool {
		return members[i].IsAddressEquivalent(node)
	}) {
		c.AbortWithStatusJSON(http.StatusBadRequest, "tso node does not exist in the keyspace group")
	}
	// set priority
	err = manager.SetPriorityForKeyspaceGroup(id, node, setParams.Priority)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

func validateKeyspaceGroupID(c *gin.Context) (uint32, error) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return 0, err
	}
	if !isValid(uint32(id)) {
		return 0, errors.Errorf("invalid keyspace group id: %d", id)
	}
	return uint32(id), nil
}

func parseNodeAddress(c *gin.Context) (string, error) {
	node := c.Param("node")
	if node == "" {
		return "", errors.New("invalid node address")
	}
	// In pd-ctl, we use url.PathEscape to escape the node address and replace the % to \%.
	// But in the gin framework, it will unescape the node address automatically.
	// So we need to replace the \/ to /.
	node = strings.ReplaceAll(node, "\\/", "/")
	node = strings.TrimPrefix(node, "/")
	return node, nil
}

func isValid(id uint32) bool {
	return id >= constant.DefaultKeyspaceGroupID && id <= constant.MaxKeyspaceGroupCountInUse
}
