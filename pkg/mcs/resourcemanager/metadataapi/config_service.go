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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
	//nolint:staticcheck // kvproto is generated against the legacy protobuf runtime.
	"github.com/golang/protobuf/jsonpb"

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
	UpdateControllerConfigItem(string, any) error
	SetKeyspaceServiceLimit(uint32, float64) error
	LookupKeyspaceID(context.Context, string) (uint32, error)
	LookupKeyspaceServiceLimit(uint32) (any, bool)
}

// Resource groups are small configuration objects. Keep malformed or
// malicious requests from consuming unbounded memory during JSON decoding.
const maxResourceGroupRequestBytes int64 = 1 << 20

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
	if err := decodeResourceGroup(c, &group); err != nil {
		respondResourceGroupDecodeError(c, err)
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
	if err := decodeResourceGroup(c, &group); err != nil {
		respondResourceGroupDecodeError(c, err)
		return
	}
	if err := s.configStore.ModifyResourceGroup(&group); err != nil {
		s.respondStoreWriteError(c, err)
		return
	}
	c.String(http.StatusOK, "Success!")
}

func respondResourceGroupDecodeError(c *gin.Context, err error) {
	status := http.StatusBadRequest
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		status = http.StatusRequestEntityTooLarge
	}
	c.String(status, err.Error())
}

func decodeResourceGroup(c *gin.Context, group *rmpb.ResourceGroup) error {
	body := http.MaxBytesReader(c.Writer, c.Request.Body, maxResourceGroupRequestBytes)
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	legacyJSON, rawKeyspaceID, err := splitResourceGroupJSON(data)
	if err != nil {
		return err
	}
	// Keep the legacy encoding/json behavior for all existing ResourceGroup
	// fields. In particular, it matches JSON field names case-insensitively.
	if err := json.Unmarshal(legacyJSON, group); err != nil {
		// The updated ResourceGroup contains a protobuf oneof, so clients may
		// serialize the whole message as protobuf JSON. Retry strictly to
		// accept enum names and quoted 64-bit integers without silently
		// dropping fields that belong to neither JSON dialect.
		*group = rmpb.ResourceGroup{}
		if protoErr := (&jsonpb.Unmarshaler{}).Unmarshal(bytes.NewReader(data), group); protoErr != nil {
			return fmt.Errorf("invalid resource group JSON: legacy JSON: %v; protobuf JSON: %w", err, protoErr)
		}
		return validateResourceGroupKeyspaceID(group, rawKeyspaceID)
	}
	if rawKeyspaceID != nil {
		keyspaceID, err := decodeKeyspaceIDJSON(rawKeyspaceID)
		if err != nil {
			return err
		}
		group.KeyspaceId = keyspaceID
	}
	return validateResourceGroupKeyspaceID(group, rawKeyspaceID)
}

func splitResourceGroupJSON(data []byte) ([]byte, json.RawMessage, error) {
	if isJSONNull(data) {
		return data, nil, nil
	}
	// KeyspaceIDValue became a protobuf oneof, which encoding/json cannot decode.
	// Remove it from the legacy payload and decode it separately with jsonpb.
	fields, err := decodeJSONObjectFields(data)
	if err != nil {
		return nil, nil, err
	}
	legacyFields := make([]jsonObjectField, 0, len(fields))
	var rawKeyspaceID json.RawMessage
	for _, field := range fields {
		if !isKeyspaceIDJSONField(field.name) {
			legacyFields = append(legacyFields, field)
			continue
		}
		if rawKeyspaceID != nil {
			return nil, nil, errors.New("keyspace_id must be set only once")
		}
		rawKeyspaceID, err = normalizeKeyspaceIDJSON(field.value)
		if err != nil {
			return nil, nil, err
		}
	}
	return marshalJSONObjectFields(legacyFields), rawKeyspaceID, nil
}

type jsonObjectField struct {
	name  string
	value json.RawMessage
}

func marshalJSONObjectFields(fields []jsonObjectField) []byte {
	var buffer bytes.Buffer
	buffer.WriteByte('{')
	for i, field := range fields {
		if i > 0 {
			buffer.WriteByte(',')
		}
		name, _ := json.Marshal(field.name)
		buffer.Write(name)
		buffer.WriteByte(':')
		buffer.Write(field.value)
	}
	buffer.WriteByte('}')
	return buffer.Bytes()
}

func decodeJSONObjectFields(data []byte) ([]jsonObjectField, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, errors.New("expected a JSON object")
	}

	// A map loses duplicate names, so scan the object tokens as well.
	decoder := json.NewDecoder(bytes.NewReader(data))
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	fields := make([]jsonObjectField, 0, len(object))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, errors.New("expected a JSON object field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields = append(fields, jsonObjectField{name: name, value: value})
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return fields, nil
}

func isKeyspaceIDJSONField(name string) bool {
	return strings.EqualFold(name, "keyspace_id") || strings.EqualFold(name, "keyspaceId")
}

func isJSONNull(data []byte) bool {
	return bytes.Equal(bytes.TrimSpace(data), []byte("null"))
}

func normalizeKeyspaceIDJSON(data []byte) (json.RawMessage, error) {
	if isJSONNull(data) {
		return data, nil
	}
	fields, err := decodeJSONObjectFields(data)
	if err != nil {
		return nil, err
	}
	normalized := make(map[string]json.RawMessage, len(fields))
	seenKnownFields := make(map[string]struct{}, 2)
	for _, field := range fields {
		name := field.name
		switch {
		case strings.EqualFold(name, "value"):
			name = "value"
		case strings.EqualFold(name, "keyspace_identity") ||
			strings.EqualFold(name, "keyspaceIdentity"):
			name = "keyspace_identity"
		}
		if name == "value" || name == "keyspace_identity" {
			if _, ok := seenKnownFields[name]; ok {
				return nil, fmt.Errorf("keyspace_id %s must be set only once", name)
			}
			seenKnownFields[name] = struct{}{}
		}
		normalized[name] = field.value
	}
	return json.Marshal(normalized)
}

func decodeKeyspaceIDJSON(rawKeyspaceID json.RawMessage) (*rmpb.KeyspaceIDValue, error) {
	data, err := json.Marshal(map[string]json.RawMessage{"keyspace_id": rawKeyspaceID})
	if err != nil {
		return nil, err
	}
	var group rmpb.ResourceGroup
	if err := (&jsonpb.Unmarshaler{AllowUnknownFields: true}).Unmarshal(bytes.NewReader(data), &group); err != nil {
		return nil, err
	}
	return group.GetKeyspaceId(), nil
}

func validateResourceGroupKeyspaceID(group *rmpb.ResourceGroup, rawKeyspaceID json.RawMessage) error {
	keyspaceID := group.GetKeyspaceId()
	if keyspaceID == nil {
		return nil
	}
	if _, ok := keyspaceID.GetKeyspace().(*rmpb.KeyspaceIDValue_Value); ok {
		return nil
	}
	// Legacy KeyspaceIDValue encoded value 0 as an empty JSON object.
	var fields map[string]json.RawMessage
	if json.Unmarshal(rawKeyspaceID, &fields) == nil && fields != nil && len(fields) == 0 {
		keyspaceID.Keyspace = &rmpb.KeyspaceIDValue_Value{Value: 0}
		return nil
	}
	return errors.New("keyspace_id must contain a legacy value")
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
	for key, v := range resolvedConf {
		if err := s.configStore.UpdateControllerConfigItem(key, v); err != nil {
			if rmserver.IsMetadataWriteDisabledError(err) {
				c.String(http.StatusForbidden, err.Error())
				return
			}
			c.String(http.StatusBadRequest, err.Error())
			return
		}
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
	// Resource groups are still being loaded asynchronously: the request can
	// succeed once loading completes, so report it as retryable rather than as
	// an internal error.
	if errs.ErrResourceGroupsLoading.Equal(err) {
		c.String(http.StatusServiceUnavailable, err.Error())
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
	if errs.ErrResourceGroupsLoading.Equal(err) {
		c.String(http.StatusServiceUnavailable, err.Error())
		return
	}
	c.String(http.StatusInternalServerError, err.Error())
}
