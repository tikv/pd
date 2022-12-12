// Copyright 2020 TiKV Project Authors.
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

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errcode"
	cors "github.com/rs/cors/wrapper/gin"
	rmserver "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/server"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/resource-groups/api/v1/"

var (
	apiServiceGroup = server.ServiceGroup{
		Name:       "resource-groups",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

func init() {
	rmserver.SetUpRestService = func(srv *rmserver.Service) (http.Handler, server.ServiceGroup) {
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
	apiHandlerEngine.Use(cors.AllowAll())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	endpoint := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		manager:          srv.GetManager(),
		apiHandlerEngine: apiHandlerEngine,
		baseEndpoint:     endpoint,
	}
	s.RegisterRouter()
	return s
}

// RegisterRouter registers the router of the service.
func (s *Service) RegisterRouter() {
	configEndpoint := s.baseEndpoint.Group("/config")
	configEndpoint.POST("/group", s.putResourceGroup)
	configEndpoint.GET("/group/:name", s.getResourceGroup)
	configEndpoint.GET("/groups", s.getResourceGroupList)
	configEndpoint.DELETE("/group/:name", s.deleteResourceGroup)
}

func (s *Service) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.apiHandlerEngine.ServeHTTP(w, r)
	})
}

// @Summary add a resource group
// @Param address path string true "ip:port"
// @Success 200 "added successfully"
// @Failure 401 {object} rest.ErrorResponse
// @Router /config/group/ [post]
func (s *Service) putResourceGroup(c *gin.Context) {
	var group rmserver.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}
	if err := s.manager.PutResourceGroup(&group); err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, "success")
}

// @ID getResourceGroup
// @Summary Get current alert count from AlertManager
// @Success 200 {object} int
// @Param name string true "groupName"
// @Router /config/group/{name} [get]
// @Failure 404 {object} rest.ErrorResponse
func (s *Service) getResourceGroup(c *gin.Context) {
	group := s.manager.GetResourceGroup(c.Param("name"))
	if group == nil {
		c.JSON(http.StatusNotFound, errcode.NewNotFoundErr(errors.New("resource group not found")))
	}
	c.JSON(http.StatusOK, group)
}

// @ID getResourceGroupList
// @Summary Get current alert count from AlertManager
// @Success 200 {array} ResourceGroup
// @Router /config/groups [get]
func (s *Service) getResourceGroupList(c *gin.Context) {
	groups := s.manager.GetResourceGroupList()
	c.JSON(http.StatusOK, groups)
}

// @ID getResourceGroup
// @Summary Get current alert count from AlertManager
// @Success 200 "deleted successfully"
// @Param name string true "groupName"
// @Router /config/group/{name} [get]
// @Failure 404 {object} error
func (s *Service) deleteResourceGroup(c *gin.Context) {
	//TODO: Implement deleteResourceGroup
}
