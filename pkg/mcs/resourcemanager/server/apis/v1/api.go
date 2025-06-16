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
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/reflectutil"
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
	root             *gin.RouterGroup

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
		c.Set(multiservicesapi.ServiceContextKey, manager.GetBasicServer())
		c.Next()
	})
	apiHandlerEngine.GET("metrics", utils.PromHandler())
	apiHandlerEngine.GET("status", utils.StatusHandler)
	pprof.Register(apiHandlerEngine)
	endpoint := apiHandlerEngine.Group(APIPathPrefix)
	endpoint.Use(multiservicesapi.ServiceRedirector())
	s := &Service{
		manager:          manager,
		apiHandlerEngine: apiHandlerEngine,
		root:             endpoint,
	}
	s.RegisterRouter()
	return s
}

// RegisterRouter registers the router of the service.
func (s *Service) RegisterRouter() {
	configEndpoint := s.root.Group("/config")
	configEndpoint.POST("/group", s.postResourceGroup)
	configEndpoint.PUT("/group", s.putResourceGroup)
	configEndpoint.GET("/group/:name", s.getResourceGroup)
	configEndpoint.GET("/groups", s.getResourceGroupList)
	configEndpoint.DELETE("/group/:name", s.deleteResourceGroup)
	configEndpoint.GET("/controller", s.getControllerConfig)
	configEndpoint.POST("/controller", s.setControllerConfig)
	// Without keyspace name, it will get/set the service limit of the null keyspace.
	configEndpoint.POST("/keyspace/service-limit", s.setKeyspaceServiceLimit)
	configEndpoint.GET("/keyspace/service-limit", s.getKeyspaceServiceLimit)
	// With keyspace name, it will get/set the service limit of the given keyspace.
	configEndpoint.POST("/keyspace/service-limit/:keyspace_name", s.setKeyspaceServiceLimit)
	configEndpoint.GET("/keyspace/service-limit/:keyspace_name", s.getKeyspaceServiceLimit)
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
//	@Router		/config/group [post]
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
	c.String(http.StatusOK, "Success!")
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
	c.String(http.StatusOK, "Success!")
}

// getResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	Get resource group by name.
//	@Success	200		    {string}	json	format	of	rmserver.ResourceGroup
//	@Failure	404		    {string}	error
//	@Param		name	    path		string	true	"groupName"
//	@Param		with_stats	query		bool	false	"whether to return statistics data."
//	@Param		keyspace_name		path	string	true	"Keyspace name"
//	@Router		/config/group/{name} [get]
func (s *Service) getResourceGroup(c *gin.Context) {
	withStats := strings.EqualFold(c.Query("with_stats"), "true")
	keyspaceName := c.Query("keyspace_name")
	keyspaceIDValue, err := s.manager.GetKeyspaceIDByName(c, keyspaceName)
	if err != nil {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(keyspaceIDValue)
	group := s.manager.GetResourceGroup(keyspaceID, c.Param("name"), withStats)
	if group == nil {
		c.String(http.StatusNotFound, errors.New("resource group not found").Error())
	}
	c.IndentedJSON(http.StatusOK, group)
}

// getResourceGroupList
//
//	@Tags		ResourceManager
//	@Summary	get all resource group with a list.
//	@Success	200	{string}	json	format	of	[]rmserver.ResourceGroup
//	@Failure	404	{string}	error
//	@Param		with_stats		query	bool	false	"whether to return statistics data."
//	@Param		keyspace_name		path	string	true	"Keyspace name"
//	@Router		/config/groups [get]
func (s *Service) getResourceGroupList(c *gin.Context) {
	withStats := strings.EqualFold(c.Query("with_stats"), "true")
	keyspaceName := c.Query("keyspace_name")
	keyspaceIDValue, err := s.manager.GetKeyspaceIDByName(c, keyspaceName)
	if err != nil {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(keyspaceIDValue)
	groups := s.manager.GetResourceGroupList(keyspaceID, withStats)
	c.IndentedJSON(http.StatusOK, groups)
}

// deleteResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	delete resource group by name.
//	@Param		name	path		string	true	"Name of the resource group to be deleted"
//	@Param		keyspace_name		path	string	true	"Keyspace name"
//	@Success	200		{string}	string	"Success!"
//	@Failure	404		{string}	error
//	@Router		/config/group/{name} [delete]
func (s *Service) deleteResourceGroup(c *gin.Context) {
	keyspaceName := c.Query("keyspace_name")
	keyspaceIDValue, err := s.manager.GetKeyspaceIDByName(c, keyspaceName)
	if err != nil {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(keyspaceIDValue)
	if err := s.manager.DeleteResourceGroup(keyspaceID, c.Param("name")); err != nil {
		c.String(http.StatusNotFound, err.Error())
	}
	c.String(http.StatusOK, "Success!")
}

// GetControllerConfig
//
//	@Tags		ResourceManager
//	@Summary	Get the resource controller config.
//	@Success	200		{string}	json	format	of	rmserver.ControllerConfig
//	@Failure	400 	{string}	error
//	@Router		/config/controller [get]
func (s *Service) getControllerConfig(c *gin.Context) {
	config := s.manager.GetControllerConfig()
	c.IndentedJSON(http.StatusOK, config)
}

// SetControllerConfig
//
//	@Tags		ResourceManager
//	@Summary	Set the resource controller config.
//	@Param		config	body	object	true	"json params, rmserver.ControllerConfig"
//	@Success	200		{string}	string	"Success!"
//	@Failure	400 	{string}	error
//	@Router		/config/controller [post]
func (s *Service) setControllerConfig(c *gin.Context) {
	conf := make(map[string]any)
	if err := c.ShouldBindJSON(&conf); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	for k, v := range conf {
		key := reflectutil.FindJSONFullTagByChildTag(reflect.TypeOf(rmserver.ControllerConfig{}), k)
		if key == "" {
			c.String(http.StatusBadRequest, fmt.Sprintf("config item %s not found", k))
			return
		}
		if err := s.manager.UpdateControllerConfigItem(key, v); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
	}
	c.String(http.StatusOK, "Success!")
}

// KeyspaceServiceLimitRequest is the request body for setting the service limit of the keyspace.
type KeyspaceServiceLimitRequest struct {
	ServiceLimit float64 `json:"service_limit"`
}

// SetKeyspaceServiceLimit
//
//	@Tags		ResourceManager
//	@Summary	Set the service limit of the keyspace. If the keyspace is valid, the service limit will be set.
//	@Param		keyspace_name		path	string	true	"Keyspace name"
//	@Param		service_limit	body		object	true	"json params, keyspaceServiceLimitRequest"
//	@Success	200				{string}	string	"Success!"
//	@Failure	400				{string}	error
//	@Router		/config/keyspace/service-limit/{keyspace_name} [post]
func (s *Service) setKeyspaceServiceLimit(c *gin.Context) {
	keyspaceName := c.Param("keyspace_name")
	keyspaceIDValue, err := s.manager.GetKeyspaceIDByName(c, keyspaceName)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	var req KeyspaceServiceLimitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if req.ServiceLimit < 0 {
		c.String(http.StatusBadRequest, "service_limit must be non-negative")
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(keyspaceIDValue)
	s.manager.SetKeyspaceServiceLimit(keyspaceID, req.ServiceLimit)
	c.String(http.StatusOK, "Success!")
}

// GetKeyspaceServiceLimit
//
//	@Tags		ResourceManager
//	@Summary	Get the service limit of the keyspace. If the keyspace name is empty, it will return the service limit of the null keyspace.
//	@Param		keyspace_name	path		string	true	"Keyspace name"
//	@Success	200				{string}	json	format	of	rmserver.serviceLimiter
//	@Failure	400				{string}	error
//	@Failure	404				{string}	error
//	@Router		/config/keyspace/service-limit/{keyspace_name} [get]
func (s *Service) getKeyspaceServiceLimit(c *gin.Context) {
	keyspaceName := c.Param("keyspace_name")
	keyspaceIDValue, err := s.manager.GetKeyspaceIDByName(c, keyspaceName)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(keyspaceIDValue)
	limiter := s.manager.GetKeyspaceServiceLimiter(keyspaceID)
	if limiter == nil {
		c.String(http.StatusNotFound, fmt.Sprintf("keyspace manager not found with keyspace name: %s, id: %d", keyspaceName, keyspaceID))
		return
	}
	c.IndentedJSON(http.StatusOK, limiter)
}
