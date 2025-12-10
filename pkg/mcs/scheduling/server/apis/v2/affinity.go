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

package apis

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/errs"
	scheserver "github.com/tikv/pd/pkg/mcs/scheduling/server"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
)

// APIPathPrefix is the prefix of the scheduling v2 API path.
const APIPathPrefix = "/scheduling/api/v2"

// AffinityGroupsResponse defines the response payload for listing affinity groups.
type AffinityGroupsResponse struct {
	AffinityGroups map[string]*affinity.GroupState `json:"affinity_groups"`
}

// RegisterV2Router registers v2 affinity routes to the given engine.
func RegisterV2Router(engine *gin.Engine) {
	root := engine.Group(APIPathPrefix)
	redirector := multiservicesapi.ServiceRedirector()
	root.Use(affinitySchedulingEnabledChecker())
	root.GET("/affinity-groups", redirector, getAllAffinityGroups)
	root.GET("/affinity-groups/:group_id", redirector, getAffinityGroup)
}

func getAffinityManager(c *gin.Context) (*affinity.Manager, bool) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	if svr.IsClosed() {
		c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrServerNotStarted.FastGenByArgs().Error())
		return nil, false
	}
	cluster := svr.GetCluster()
	if cluster == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrNotBootstrapped.FastGenByArgs().Error())
		return nil, false
	}
	manager := cluster.GetAffinityManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, errs.ErrAffinityDisabled.FastGenByArgs().Error())
		return nil, false
	}
	return manager, true
}

func affinitySchedulingEnabledChecker() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
		if svr.GetPersistConfig().IsAffinitySchedulingEnabled() {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, errs.ErrAffinityDisabled.FastGenByArgs().Error())
	}
}

func getAllAffinityGroups(c *gin.Context) {
	manager, ok := getAffinityManager(c)
	if !ok {
		return
	}
	allGroupStates := manager.GetAllAffinityGroupStates()
	resp := AffinityGroupsResponse{
		AffinityGroups: make(map[string]*affinity.GroupState, len(allGroupStates)),
	}
	for _, state := range allGroupStates {
		resp.AffinityGroups[state.ID] = state
	}
	c.IndentedJSON(http.StatusOK, resp)
}

func getAffinityGroup(c *gin.Context) {
	manager, ok := getAffinityManager(c)
	if !ok {
		return
	}
	groupID := c.Param("group_id")
	groupState, err := manager.CheckAndGetAffinityGroupState(groupID)
	if err != nil {
		switch {
		case errs.ErrInvalidGroupID.Equal(err):
			c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		case errs.ErrAffinityGroupNotFound.Equal(err):
			c.AbortWithStatusJSON(http.StatusNotFound, err.Error())
		default:
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		}
		return
	}
	c.IndentedJSON(http.StatusOK, groupState)
}
