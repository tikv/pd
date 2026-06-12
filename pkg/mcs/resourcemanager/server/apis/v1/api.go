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
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/metadataapi"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/logutil"
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

	manager       *rmserver.Manager
	configService *metadataapi.ConfigService
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
	apiHandlerEngine.GET("health", getHealth)
	pprof.Register(apiHandlerEngine)
	endpoint := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		manager:          manager,
		configService:    metadataapi.NewConfigService(metadataapi.NewManagerStore(manager)),
		apiHandlerEngine: apiHandlerEngine,
		root:             endpoint,
	}
	s.RegisterAdminRouter()
	s.RegisterRouter()
	s.RegisterPrimaryRouter()
	return s
}

// getHealth returns the health status of the Resource Manager service.
func getHealth(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*rmserver.Server)
	if svr.IsClosed() {
		c.String(http.StatusServiceUnavailable, errs.ErrServerNotStarted.GenWithStackByArgs().Error())
		return
	}
	if svr.GetParticipant().IsPrimaryElected() {
		c.String(http.StatusOK, "ok")
		return
	}

	c.String(http.StatusInternalServerError, "no primary elected")
}

// RegisterAdminRouter registers the router of the TSO admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	router.PUT("/log", changeLogLevel)
}

// RegisterRouter registers the router of the service.
func (s *Service) RegisterRouter() {
	configEndpoint := s.root.Group("/config")
	configEndpoint.GET("", getConfig)
	configEndpoint.Use(multiservicesapi.ServiceRedirector())
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
	// RU version endpoint — sets per-keyspace RU version in controller config.
	configEndpoint.POST("/controller/ru-version/:keyspace_name", s.setKeyspaceRUVersion)
}

// RegisterPrimaryRouter registers the router of the primary handler.
func (s *Service) RegisterPrimaryRouter() {
	redirector := multiservicesapi.ServiceRedirector()
	router := s.root.Group("primary")
	router.Use(redirector)
	router.POST("transfer", transferPrimary)
}

func (s *Service) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.apiHandlerEngine.ServeHTTP(w, r)
	})
}

func changeLogLevel(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*rmserver.Server)
	var level string
	if err := c.Bind(&level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if err := svr.SetLogLevel(level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))
	c.String(http.StatusOK, "The log level is updated.")
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
	s.configService.PostResourceGroup(c)
}

// putResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	updates an exists resource group
//	@Param		groupInfo	body	object	true	"json params, rmpb.ResourceGroup"
//	@Success	200			"Success"
//	@Failure	400			{string}	error
//	@Failure	404			{string}	error
//	@Failure	500			{string}	error
//	@Router		/config/group [PUT]
func (s *Service) putResourceGroup(c *gin.Context) {
	s.configService.PutResourceGroup(c)
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
	s.configService.GetResourceGroup(c)
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
	s.configService.GetResourceGroupList(c)
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
	s.configService.DeleteResourceGroup(c)
}

// GetControllerConfig
//
//	@Tags		ResourceManager
//	@Summary	Get the resource controller config.
//	@Success	200		{string}	json	format	of	rmserver.ControllerConfig
//	@Failure	400 	{string}	error
//	@Router		/config/controller [get]
func (s *Service) getControllerConfig(c *gin.Context) {
	s.configService.GetControllerConfig(c)
}

// SetControllerConfig
//
//	@Tags		ResourceManager
//	@Summary	Set the resource controller config.
//	@Param		config	body	object	true	"json params, rmserver.ControllerConfig"
//	@Success	200		{string}	string	"Success!"
//	@Failure	400 	{string}	error
//	@Failure	403 	{string}	error
//	@Failure	500 	{string}	error
//	@Router		/config/controller [post]
func (s *Service) setControllerConfig(c *gin.Context) {
	s.configService.SetControllerConfig(c)
}

// KeyspaceServiceLimitRequest is the request body for setting the service limit of the keyspace.
type KeyspaceServiceLimitRequest = metadataapi.KeyspaceServiceLimitRequest

// SetKeyspaceRUVersionRequest is the request body for setting the RU version of a keyspace.
type SetKeyspaceRUVersionRequest struct {
	RUVersion int32 `json:"ru_version"`
}

// SetKeyspaceServiceLimit
//
//	@Tags		ResourceManager
//	@Summary	Set the service limit of the keyspace. If the keyspace is valid, the service limit will be set.
//	@Param		keyspace_name		path	string	true	"Keyspace name"
//	@Param		service_limit	body		object	true	"json params, keyspaceServiceLimitRequest"
//	@Success	200				{string}	string	"Success!"
//	@Failure	400				{string}	error
//	@Failure	403				{string}	error
//	@Failure	404				{string}	error
//	@Failure	500				{string}	error
//	@Router		/config/keyspace/service-limit/{keyspace_name} [post]
func (s *Service) setKeyspaceServiceLimit(c *gin.Context) {
	s.configService.SetKeyspaceServiceLimit(c)
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
	s.configService.GetKeyspaceServiceLimit(c)
}

// setKeyspaceRUVersion sets the RU version for the given keyspace in the controller config.
//
//	@Tags		ResourceManager
//	@Summary	Set the RU version of the keyspace.
//	@Param		keyspace_name	path	string	true	"Keyspace name"
//	@Param		config			body	object	true	"json params, SetKeyspaceRUVersionRequest"
//	@Success	200				{string}	string	"Success!"
//	@Failure	400				{string}	error
//	@Failure	403				{string}	error
//	@Failure	500				{string}	error
//	@Router		/config/controller/ru-version/{keyspace_name} [post]
func (s *Service) setKeyspaceRUVersion(c *gin.Context) {
	keyspaceName := c.Param("keyspace_name")
	keyspaceIDValue, err := s.manager.GetKeyspaceIDByName(c, keyspaceName)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	var req SetKeyspaceRUVersionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if req.RUVersion <= 0 {
		c.String(http.StatusBadRequest, "ru_version must be positive")
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(keyspaceIDValue)
	if err := s.manager.SetKeyspaceRUVersion(keyspaceID, req.RUVersion); err != nil {
		if rmserver.IsMetadataWriteDisabledError(err) {
			c.String(http.StatusForbidden, err.Error())
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

// GetConfig
//
// @Tags		ResourceManager
// @Summary	Get the resource manager config.
// @Success	200		{string}	json	format	of	rmserver.Config
func getConfig(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*rmserver.Server)
	config := svr.GetConfig()
	c.IndentedJSON(http.StatusOK, config)
}

// transferPrimary transfers the primary member to `new_primary`.
// @Tags     primary
// @Summary  Transfer the primary member to `new_primary`.
// @Produce  json
// @Param    new_primary body   string  false "new primary name"
// @Success  200  string  string
// @Router   /primary/transfer [post]
func transferPrimary(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*rmserver.Server)
	var input map[string]string
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	newPrimary := ""
	if v, ok := input["new_primary"]; ok {
		newPrimary = v
	}

	if err := utils.TransferPrimary(svr.GetClient(), svr.GetParticipant().GetExpectedPrimaryLease(),
		constant.ResourceManagerServiceName, svr.Name(), newPrimary, 0, nil); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "success")
}
