// Copyright 2022 TiKV Project Authors.
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
	"errors"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	rmserver "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/resource-manager/api/v1/"

var (
	apiServiceGroup = apiutil.APIServiceGroup{
		Name:       "resource-manager",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

func init() {
	rmserver.SetUpRestHandler = func(srv *rmserver.Service) (http.Handler, apiutil.APIServiceGroup) {
		s := NewService(srv)
		return s.handler(), apiServiceGroup
	}
}

// Service is the resource group service.
type Service struct {
	apiHandlerEngine *gin.Engine
	baseEndpoint     *gin.RouterGroup

	manager *rmserver.Manager
}

// NewService returns a new Service.
func NewService(srv *rmserver.Service) *Service {
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.Default())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	manager := srv.GetManager()
	apiHandlerEngine.Use(func(c *gin.Context) {
		// manager implements the interface of basicserver.Service.
		c.Set("service", manager.GetBasicServer())
		c.Next()
	})
	apiHandlerEngine.Use(multiservicesapi.ServiceRedirector())
	endpoint := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		manager:          manager,
		apiHandlerEngine: apiHandlerEngine,
		baseEndpoint:     endpoint,
	}
	s.RegisterRouter()
	return s
}

// RegisterRouter registers the router of the service.
func (s *Service) RegisterRouter() {
	configEndpoint := s.baseEndpoint.Group("/config")
	configEndpoint.POST("/group", s.postResourceGroup)
	configEndpoint.PUT("/group", s.putResourceGroup)
	configEndpoint.GET("/group/:name", s.getResourceGroup)
	configEndpoint.GET("/groups", s.getResourceGroupList)
	configEndpoint.DELETE("/group/:name", s.deleteResourceGroup)
}

func (s *Service) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.apiHandlerEngine.ServeHTTP(w, r)
	})
}

// postResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	Add a resource group
//	@Param		groupInfo	body		object	true	"json params, rmpb.ResourceGroup"
//	@Success	200			{string}	string	"Success"
//	@Failure	400			{string}	error
//	@Failure	500			{string}	error
//	@Router		/config/group [POST]
func (s *Service) postResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.manager.AddResourceGroup(&group); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, "Success!")
}

// putResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	updates an exists resource group
//	@Param		groupInfo	body	object	true	"json params, rmpb.ResourceGroup"
//	@Success	200			"Success"
//	@Failure	400			{string}	error
//	@Failure	500			{string}	error
//	@Router		/config/group [PUT]
func (s *Service) putResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.manager.ModifyResourceGroup(&group); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, "Success!")
}

// getResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	Get resource group by name.
//	@Success	200		{string}	json	format	of	rmpb.ResourceGroup
//	@Failure	404		{string}	error
//	@Param		name	path		string	true	"groupName"
//	@Router		/config/group/{name} [GET]
func (s *Service) getResourceGroup(c *gin.Context) {
	group := s.manager.GetResourceGroup(c.Param("name"))
	if group == nil {
		c.String(http.StatusNotFound, errors.New("resource group not found").Error())
	}
	c.JSON(http.StatusOK, group)
}

// getResourceGroupList
//
//	@Tags		ResourceManager
//	@Summary	get all resource group with a list.
//	@Success	200	{string}	json	format	of	[]rmpb.ResourceGroup
//	@Failure	404	{string}	error
//	@Router		/config/groups [GET]
func (s *Service) getResourceGroupList(c *gin.Context) {
	groups := s.manager.GetResourceGroupList()
	c.JSON(http.StatusOK, groups)
}

// deleteResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	delete resource group by name.
//	@Param		name	path		string	true	"Name of the resource group to be deleted"
//	@Success	200		{string}	string	"Success!"
//	@Failure	404		{string}	error
//	@Router		/config/group/{name} [DELETE]
func (s *Service) deleteResourceGroup(c *gin.Context) {
	if err := s.manager.DeleteResourceGroup(c.Param("name")); err != nil {
		c.String(http.StatusNotFound, err.Error())
	}
	c.JSON(http.StatusOK, "Success!")
}
