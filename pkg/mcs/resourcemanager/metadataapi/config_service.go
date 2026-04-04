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

package metadataapi

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
	"github.com/tikv/pd/pkg/utils/reflectutil"
)

// KeyspaceServiceLimitRequest is the request body for setting the service limit of a keyspace.
type KeyspaceServiceLimitRequest struct {
	ServiceLimit float64 `json:"service_limit"`
}

// ConfigStore abstracts metadata operations for config APIs.
type ConfigStore interface {
	AddResourceGroup(*rmpb.ResourceGroup) error
	ModifyResourceGroup(*rmpb.ResourceGroup) error
	GetResourceGroup(uint32, string, bool) (*rmserver.ResourceGroup, error)
	GetResourceGroupList(uint32, bool) ([]*rmserver.ResourceGroup, error)
	DeleteResourceGroup(uint32, string) error
	GetControllerConfig() *rmserver.ControllerConfig
	UpdateControllerConfigItems(map[string]any) error
	UpdateControllerConfigItem(string, any) error
	SetKeyspaceServiceLimit(uint32, float64) error
	LookupKeyspaceID(context.Context, string) (uint32, error)
	LookupKeyspaceServiceLimit(uint32) (any, bool)
}

// ManagerStore adapts rmserver.Manager to ConfigStore.
type ManagerStore struct {
	manager *rmserver.Manager
}

// NewManagerStore builds a ConfigStore from rmserver.Manager.
func NewManagerStore(manager *rmserver.Manager) *ManagerStore {
	return &ManagerStore{manager: manager}
}

// AddResourceGroup adds a resource group.
func (s *ManagerStore) AddResourceGroup(group *rmpb.ResourceGroup) error {
	return s.manager.AddResourceGroup(group)
}

// ModifyResourceGroup modifies a resource group.
func (s *ManagerStore) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	return s.manager.ModifyResourceGroup(group)
}

// GetResourceGroup gets one resource group.
func (s *ManagerStore) GetResourceGroup(keyspaceID uint32, groupName string, withStats bool) (*rmserver.ResourceGroup, error) {
	return s.manager.GetResourceGroup(keyspaceID, groupName, withStats)
}

// GetResourceGroupList gets all resource groups.
func (s *ManagerStore) GetResourceGroupList(keyspaceID uint32, withStats bool) ([]*rmserver.ResourceGroup, error) {
	return s.manager.GetResourceGroupList(keyspaceID, withStats)
}

// DeleteResourceGroup deletes a resource group.
func (s *ManagerStore) DeleteResourceGroup(keyspaceID uint32, groupName string) error {
	return s.manager.DeleteResourceGroup(keyspaceID, groupName)
}

// GetControllerConfig gets controller config.
func (s *ManagerStore) GetControllerConfig() *rmserver.ControllerConfig {
	return s.manager.GetControllerConfig()
}

// UpdateControllerConfigItem updates one controller config item.
func (s *ManagerStore) UpdateControllerConfigItem(key string, value any) error {
	return s.manager.UpdateControllerConfigItem(key, value)
}

// UpdateControllerConfigItems updates controller config items atomically.
func (s *ManagerStore) UpdateControllerConfigItems(items map[string]any) error {
	return s.manager.UpdateControllerConfigItems(items)
}

// SetKeyspaceServiceLimit sets keyspace service limit.
func (s *ManagerStore) SetKeyspaceServiceLimit(keyspaceID uint32, limit float64) error {
	return s.manager.SetKeyspaceServiceLimit(keyspaceID, limit)
}

// LookupKeyspaceID resolves keyspace name to ID.
func (s *ManagerStore) LookupKeyspaceID(ctx context.Context, keyspaceName string) (uint32, error) {
	keyspaceIDValue, err := s.manager.GetKeyspaceIDByName(ctx, keyspaceName)
	if err != nil {
		return 0, err
	}
	return rmserver.ExtractKeyspaceID(keyspaceIDValue), nil
}

// LookupKeyspaceServiceLimit gets the keyspace limiter snapshot.
func (s *ManagerStore) LookupKeyspaceServiceLimit(keyspaceID uint32) (any, bool) {
	limiter := s.manager.GetKeyspaceServiceLimiter(keyspaceID)
	if limiter == nil {
		return nil, false
	}
	return limiter, true
}

// ConfigService serves resource-manager /config metadata APIs.
type ConfigService struct {
	configStore ConfigStore
}

// NewConfigService creates a metadata config service.
func NewConfigService(configStore ConfigStore) *ConfigService {
	return &ConfigService{configStore: configStore}
}

// Register mounts /config routes onto the provided router group.
func (s *ConfigService) Register(configEndpoint *gin.RouterGroup) {
	configEndpoint.POST("/group", s.PostResourceGroup)
	configEndpoint.PUT("/group", s.PutResourceGroup)
	configEndpoint.GET("/group/:name", s.GetResourceGroup)
	configEndpoint.GET("/groups", s.GetResourceGroupList)
	configEndpoint.DELETE("/group/:name", s.DeleteResourceGroup)
	configEndpoint.GET("/controller", s.GetControllerConfig)
	configEndpoint.POST("/controller", s.SetControllerConfig)
	configEndpoint.POST("/keyspace/service-limit", s.SetKeyspaceServiceLimit)
	configEndpoint.GET("/keyspace/service-limit", s.GetKeyspaceServiceLimit)
	configEndpoint.POST("/keyspace/service-limit/:keyspace_name", s.SetKeyspaceServiceLimit)
	configEndpoint.GET("/keyspace/service-limit/:keyspace_name", s.GetKeyspaceServiceLimit)
}

// PostResourceGroup handles POST /config/group.
func (s *ConfigService) PostResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.configStore.AddResourceGroup(&group); err != nil {
		s.respondStoreWriteError(c, err)
		return
	}
	c.String(http.StatusOK, "Success!")
}

// PutResourceGroup handles PUT /config/group.
func (s *ConfigService) PutResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.configStore.ModifyResourceGroup(&group); err != nil {
		s.respondStoreWriteError(c, err)
		return
	}
	c.String(http.StatusOK, "Success!")
}

// GetResourceGroup handles GET /config/group/:name.
func (s *ConfigService) GetResourceGroup(c *gin.Context) {
	withStats := strings.EqualFold(c.Query("with_stats"), "true")
	keyspaceID, err := s.configStore.LookupKeyspaceID(c, c.Query("keyspace_name"))
	if err != nil {
		s.respondKeyspaceLookupError(c, err)
		return
	}
	groupName := c.Param("name")
	group, err := s.configStore.GetResourceGroup(keyspaceID, groupName, withStats)
	if err != nil {
		s.respondStoreReadError(c, err)
		return
	}
	if group == nil {
		c.String(http.StatusNotFound, errs.ErrResourceGroupNotExists.FastGenByArgs(groupName).Error())
		return
	}
	c.IndentedJSON(http.StatusOK, group)
}

// GetResourceGroupList handles GET /config/groups.
func (s *ConfigService) GetResourceGroupList(c *gin.Context) {
	withStats := strings.EqualFold(c.Query("with_stats"), "true")
	keyspaceID, err := s.configStore.LookupKeyspaceID(c, c.Query("keyspace_name"))
	if err != nil {
		s.respondKeyspaceLookupError(c, err)
		return
	}
	groups, err := s.configStore.GetResourceGroupList(keyspaceID, withStats)
	if err != nil {
		s.respondStoreReadError(c, err)
		return
	}
	c.IndentedJSON(http.StatusOK, groups)
}

// DeleteResourceGroup handles DELETE /config/group/:name.
func (s *ConfigService) DeleteResourceGroup(c *gin.Context) {
	keyspaceID, err := s.configStore.LookupKeyspaceID(c, c.Query("keyspace_name"))
	if err != nil {
		s.respondKeyspaceLookupError(c, err)
		return
	}
	if err := s.configStore.DeleteResourceGroup(keyspaceID, c.Param("name")); err != nil {
		s.respondStoreWriteError(c, err)
		return
	}
	c.String(http.StatusOK, "Success!")
}

// GetControllerConfig handles GET /config/controller.
func (s *ConfigService) GetControllerConfig(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, s.configStore.GetControllerConfig())
}

// SetControllerConfig handles POST /config/controller.
func (s *ConfigService) SetControllerConfig(c *gin.Context) {
	conf := make(map[string]any)
	if err := c.ShouldBindJSON(&conf); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	resolvedConf := make(map[string]any, len(conf))
	for k, v := range conf {
		key := reflectutil.FindJSONFullTagByChildTag(reflect.TypeOf(rmserver.ControllerConfig{}), k)
		if key == "" {
			c.String(http.StatusBadRequest, fmt.Sprintf("config item %s not found", k))
			return
		}
		resolvedConf[key] = v
	}
	if err := s.configStore.UpdateControllerConfigItems(resolvedConf); err != nil {
		if rmserver.IsMetadataWriteDisabledError(err) {
			c.String(http.StatusForbidden, err.Error())
			return
		}
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

// SetKeyspaceServiceLimit handles POST /config/keyspace/service-limit*.
func (s *ConfigService) SetKeyspaceServiceLimit(c *gin.Context) {
	keyspaceName := c.Param("keyspace_name")
	keyspaceID, err := s.configStore.LookupKeyspaceID(c, keyspaceName)
	if err != nil {
		s.respondKeyspaceLookupError(c, err)
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
	if err := s.configStore.SetKeyspaceServiceLimit(keyspaceID, req.ServiceLimit); err != nil {
		s.respondStoreWriteError(c, err)
		return
	}
	c.String(http.StatusOK, "Success!")
}

// GetKeyspaceServiceLimit handles GET /config/keyspace/service-limit*.
func (s *ConfigService) GetKeyspaceServiceLimit(c *gin.Context) {
	keyspaceName := c.Param("keyspace_name")
	keyspaceID, err := s.configStore.LookupKeyspaceID(c, keyspaceName)
	if err != nil {
		s.respondKeyspaceLookupError(c, err)
		return
	}
	limiter, ok := s.configStore.LookupKeyspaceServiceLimit(keyspaceID)
	if !ok {
		c.String(http.StatusNotFound,
			fmt.Sprintf("keyspace manager not found with keyspace name: %s, id: %d", keyspaceName, keyspaceID))
		return
	}
	c.IndentedJSON(http.StatusOK, limiter)
}

func (*ConfigService) respondKeyspaceLookupError(c *gin.Context, err error) {
	if errs.ErrKeyspaceNotExists.Equal(err) || errs.ErrKeyspaceNotExistsByName.Equal(err) {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	c.String(http.StatusInternalServerError, err.Error())
}

func (*ConfigService) respondStoreReadError(c *gin.Context, err error) {
	if errs.ErrResourceGroupNotExists.Equal(err) || errs.ErrKeyspaceNotExists.Equal(err) {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	c.String(http.StatusInternalServerError, err.Error())
}

func (*ConfigService) respondStoreWriteError(c *gin.Context, err error) {
	if rmserver.IsMetadataWriteDisabledError(err) {
		c.String(http.StatusForbidden, err.Error())
		return
	}
	if errs.ErrResourceGroupNotExists.Equal(err) || errs.ErrKeyspaceNotExists.Equal(err) {
		c.String(http.StatusNotFound, err.Error())
		return
	}
	c.String(http.StatusInternalServerError, err.Error())
}
