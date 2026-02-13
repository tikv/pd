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

package redirector

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/errs"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	apis "github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/utils/reflectutil"
)

type pdMetadataStore interface {
	AddResourceGroup(*rmpb.ResourceGroup) error
	ModifyResourceGroup(*rmpb.ResourceGroup) error
	GetResourceGroup(uint32, string, bool) (*rmserver.ResourceGroup, error)
	GetResourceGroupList(uint32, bool) ([]*rmserver.ResourceGroup, error)
	DeleteResourceGroup(uint32, string) error
	GetControllerConfig() *rmserver.ControllerConfig
	UpdateControllerConfigItem(string, any) error
	SetKeyspaceServiceLimit(uint32, float64) error
	lookupKeyspaceID(context.Context, string) (uint32, error)
	lookupKeyspaceServiceLimit(uint32) (any, bool)
}

type pdMetadataManagerAdapter struct {
	*rmserver.Manager
}

func (a *pdMetadataManagerAdapter) lookupKeyspaceID(ctx context.Context, name string) (uint32, error) {
	keyspaceIDValue, err := a.GetKeyspaceIDByName(ctx, name)
	if err != nil {
		return 0, err
	}
	return rmserver.ExtractKeyspaceID(keyspaceIDValue), nil
}

func (a *pdMetadataManagerAdapter) lookupKeyspaceServiceLimit(keyspaceID uint32) (any, bool) {
	limiter := a.GetKeyspaceServiceLimiter(keyspaceID)
	if limiter == nil {
		return nil, false
	}
	return limiter, true
}

type pdMetadataHandler struct {
	store  pdMetadataStore
	engine *gin.Engine
}

func newPDMetadataHandler(manager *rmserver.Manager) http.Handler {
	return newPDMetadataHandlerWithStore(&pdMetadataManagerAdapter{Manager: manager})
}

func newPDMetadataHandlerWithStore(store pdMetadataStore) http.Handler {
	handler := &pdMetadataHandler{
		store:  store,
		engine: gin.New(),
	}
	handler.engine.Use(gin.Recovery())
	handler.registerRouter()
	return handler
}

func (h *pdMetadataHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.engine.ServeHTTP(w, r)
}

func (h *pdMetadataHandler) registerRouter() {
	root := h.engine.Group(apis.APIPathPrefix)
	configEndpoint := root.Group("/config")
	configEndpoint.POST("/group", h.postResourceGroup)
	configEndpoint.PUT("/group", h.putResourceGroup)
	configEndpoint.GET("/group/:name", h.getResourceGroup)
	configEndpoint.GET("/groups", h.getResourceGroupList)
	configEndpoint.DELETE("/group/:name", h.deleteResourceGroup)
	configEndpoint.GET("/controller", h.getControllerConfig)
	configEndpoint.POST("/controller", h.setControllerConfig)
	configEndpoint.POST("/keyspace/service-limit", h.setKeyspaceServiceLimit)
	configEndpoint.GET("/keyspace/service-limit", h.getKeyspaceServiceLimit)
	configEndpoint.POST("/keyspace/service-limit/:keyspace_name", h.setKeyspaceServiceLimit)
	configEndpoint.GET("/keyspace/service-limit/:keyspace_name", h.getKeyspaceServiceLimit)
}

func (h *pdMetadataHandler) postResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := h.store.AddResourceGroup(&group); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

func (h *pdMetadataHandler) putResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := h.store.ModifyResourceGroup(&group); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

func (h *pdMetadataHandler) getResourceGroup(c *gin.Context) {
	withStats := strings.EqualFold(c.Query("with_stats"), "true")
	keyspaceID, err := h.getKeyspaceIDByName(c, c.Query("keyspace_name"))
	if err != nil {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	groupName := c.Param("name")
	group, err := h.store.GetResourceGroup(keyspaceID, groupName, withStats)
	if err != nil {
		if errs.ErrResourceGroupNotExists.Equal(err) || errs.ErrKeyspaceNotExists.Equal(err) {
			c.String(http.StatusNotFound, err.Error())
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if group == nil {
		c.String(http.StatusNotFound, errs.ErrResourceGroupNotExists.FastGenByArgs(groupName).Error())
		return
	}
	c.IndentedJSON(http.StatusOK, group)
}

func (h *pdMetadataHandler) getResourceGroupList(c *gin.Context) {
	withStats := strings.EqualFold(c.Query("with_stats"), "true")
	keyspaceID, err := h.getKeyspaceIDByName(c, c.Query("keyspace_name"))
	if err != nil {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	groups, err := h.store.GetResourceGroupList(keyspaceID, withStats)
	if err != nil {
		if errs.ErrKeyspaceNotExists.Equal(err) {
			c.String(http.StatusNotFound, err.Error())
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, groups)
}

func (h *pdMetadataHandler) deleteResourceGroup(c *gin.Context) {
	keyspaceID, err := h.getKeyspaceIDByName(c, c.Query("keyspace_name"))
	if err != nil {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	groupName := c.Param("name")
	if err := h.store.DeleteResourceGroup(keyspaceID, groupName); err != nil {
		if errs.ErrResourceGroupNotExists.Equal(err) || errs.ErrKeyspaceNotExists.Equal(err) {
			c.String(http.StatusNotFound, err.Error())
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

func (h *pdMetadataHandler) getControllerConfig(c *gin.Context) {
	config := h.store.GetControllerConfig()
	c.IndentedJSON(http.StatusOK, config)
}

func (h *pdMetadataHandler) setControllerConfig(c *gin.Context) {
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
		if err := h.store.UpdateControllerConfigItem(key, v); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
	}
	c.String(http.StatusOK, "Success!")
}

func (h *pdMetadataHandler) setKeyspaceServiceLimit(c *gin.Context) {
	keyspaceID, err := h.getKeyspaceIDByName(c, getServiceLimitTargetKeyspace(c))
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	var req apis.KeyspaceServiceLimitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if req.ServiceLimit < 0 {
		c.String(http.StatusBadRequest, "service_limit must be non-negative")
		return
	}
	if err := h.store.SetKeyspaceServiceLimit(keyspaceID, req.ServiceLimit); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

func (h *pdMetadataHandler) getKeyspaceServiceLimit(c *gin.Context) {
	keyspaceName := getServiceLimitTargetKeyspace(c)
	keyspaceID, err := h.getKeyspaceIDByName(c, keyspaceName)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	limiter, ok := h.store.lookupKeyspaceServiceLimit(keyspaceID)
	if !ok {
		c.String(http.StatusNotFound,
			fmt.Sprintf("keyspace manager not found with keyspace name: %s, id: %d", keyspaceName, keyspaceID))
		return
	}
	c.IndentedJSON(http.StatusOK, limiter)
}

func getServiceLimitTargetKeyspace(c *gin.Context) string {
	keyspaceName := c.Param("keyspace_name")
	if keyspaceName != "" {
		return keyspaceName
	}
	return c.Query("keyspace_name")
}

func (h *pdMetadataHandler) getKeyspaceIDByName(c *gin.Context, keyspaceName string) (uint32, error) {
	return h.store.lookupKeyspaceID(c, keyspaceName)
}
