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
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

const managerUninitializedErr = "keyspace manager is not initialized"

// RegisterKeyspace register keyspace related handlers to router paths.
func RegisterKeyspace(r *gin.RouterGroup) {
	router := r.Group("keyspaces")
	router.Use(middlewares.BootstrapChecker())
	router.POST("", CreateKeyspace)
	router.GET("", LoadAllKeyspaces)
	router.GET("/:name", LoadKeyspace)
	router.PATCH("/:name/config", UpdateKeyspaceConfig)
	router.PUT("/:name/state", UpdateKeyspaceState)
	router.GET("/id/:id", LoadKeyspaceByID)
}

// CreateKeyspaceParams represents parameters needed when creating a new keyspace.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type CreateKeyspaceParams struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

// CreateKeyspace creates keyspace according to given input.
//
// @Tags     keyspaces
// @Summary  Create new keyspace.
// @Param    body  body  CreateKeyspaceParams  true  "Create keyspace parameters"
// @Produce  json
// @Success  200  {object}  KeyspaceMeta
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /keyspaces [post]
func CreateKeyspace(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	createParams := &CreateKeyspaceParams{}
	err := c.BindJSON(createParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	req := &keyspace.CreateKeyspaceRequest{
		Name:       createParams.Name,
		Config:     createParams.Config,
		CreateTime: time.Now().Unix(),
		IsPreAlloc: false,
	}
	meta, err := manager.CreateKeyspace(req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

// LoadKeyspace returns target keyspace.
//
// @Tags     keyspaces
// @Summary  Get keyspace info.
// @Param    name  path  string  true  "Keyspace Name"
// @Produce  json
// @Success  200  {object}  KeyspaceMeta
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /keyspaces/{name} [get]
func LoadKeyspace(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	name := c.Param("name")
	meta, err := manager.LoadKeyspace(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	if value, ok := c.GetQuery("force_refresh_group_id"); ok && value == "true" {
		groupManager := svr.GetKeyspaceGroupManager()
		if groupManager == nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, GroupManagerUninitializedErr)
			return
		}
		// keyspace has been checked in LoadKeyspace, so no need to check again.
		groupID, err := groupManager.GetGroupByKeyspaceID(meta.GetId())
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		meta.Config[keyspace.TSOKeyspaceGroupIDKey] = strconv.FormatUint(uint64(groupID), 10)
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

// LoadKeyspaceByID returns target keyspace.
//
// @Tags     keyspaces
// @Summary  Get keyspace info.
// @Param    id  path  string  true  "Keyspace id"
// @Produce  json
// @Success  200  {object}  KeyspaceMeta
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /keyspaces/id/{id} [get]
func LoadKeyspaceByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "invalid keyspace id")
		return
	}
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	meta, err := manager.LoadKeyspaceByID(uint32(id))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

// parseLoadAllQuery parses LoadAllKeyspaces'/GetKeyspaceGroups' query parameters.
// page_token:
// The keyspace/keyspace group id of the scan start. If not set, scan from keyspace/keyspace group with id 0.
// It's string of ID of the previous scan result's last element (next_page_token).
// limit:
// The maximum number of keyspace metas/keyspace groups to return. If not set, no limit is posed.
// Every scan scans limit + 1 keyspaces/keyspace groups (if limit != 0), the extra scanned keyspace/keyspace group
// is to check if there's more, and used to set next_page_token in response.
func parseLoadAllQuery(c *gin.Context) (scanStart uint32, scanLimit int, err error) {
	pageToken, set := c.GetQuery("page_token")
	if !set || pageToken == "" {
		// If pageToken is empty or unset, then scan from ID of 0.
		scanStart = 0
	} else {
		scanStart64, err := strconv.ParseUint(pageToken, 10, 32)
		if err != nil {
			return 0, 0, err
		}
		scanStart = uint32(scanStart64)
	}

	limitStr, set := c.GetQuery("limit")
	if !set || limitStr == "" || limitStr == "0" {
		// If limit is unset or empty or 0, then no limit is posed for scan.
		scanLimit = 0
	} else {
		scanLimit64, err := strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			return 0, 0, err
		}
		// Scan an extra element for next_page_token.
		scanLimit = int(scanLimit64) + 1
	}

	return scanStart, scanLimit, nil
}

// LoadAllKeyspacesResponse represents response given when loading all keyspaces.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type LoadAllKeyspacesResponse struct {
	Keyspaces []*KeyspaceMeta `json:"keyspaces"`
	// Token that can be used to read immediate next page.
	// If it's empty, then end has been reached.
	NextPageToken string `json:"next_page_token"`
}

// LoadAllKeyspaces loads range of keyspaces.
//
// @Tags     keyspaces
// @Summary  list keyspaces.
// @Param    page_token  query  string  false  "page token"
// @Param    limit       query  string  false  "maximum number of results to return"
// @Produce  json
// @Success  200  {object}  LoadAllKeyspacesResponse
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /keyspaces [get]
func LoadAllKeyspaces(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	scanStart, scanLimit, err := parseLoadAllQuery(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}
	scanned, err := manager.LoadRangeKeyspace(scanStart, scanLimit)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	resp := &LoadAllKeyspacesResponse{}
	// If scanned 0 keyspaces, return result immediately.
	if len(scanned) == 0 {
		c.IndentedJSON(http.StatusOK, resp)
		return
	}
	var resultKeyspaces []*KeyspaceMeta
	if scanLimit == 0 || len(scanned) < scanLimit {
		// No next page, all scanned are results.
		resultKeyspaces = make([]*KeyspaceMeta, len(scanned))
		for i, meta := range scanned {
			resultKeyspaces[i] = &KeyspaceMeta{meta}
		}
	} else {
		// Scanned limit + 1 keyspaces, there is next page, all but last are results.
		resultKeyspaces = make([]*KeyspaceMeta, len(scanned)-1)
		for i := range resultKeyspaces {
			resultKeyspaces[i] = &KeyspaceMeta{scanned[i]}
		}
		// Also set next_page_token here.
		resp.NextPageToken = strconv.Itoa(int(scanned[len(scanned)-1].Id))
	}
	resp.Keyspaces = resultKeyspaces
	c.IndentedJSON(http.StatusOK, resp)
}

// UpdateConfigParams represents parameters needed to modify target keyspace's configs.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// A Map of string to string pointer is used to differentiate between json null and "",
// which will both be set to "" if value type is string during binding.
type UpdateConfigParams struct {
	Config map[string]*string `json:"config"`
}

// UpdateKeyspaceConfig updates target keyspace's config.
// This api uses PATCH semantic and supports JSON Merge Patch.
// format and processing rules.
//
// @Tags     keyspaces
// @Summary  Update keyspace config.
// @Param    name  path  string              true  "Keyspace Name"
// @Param    body  body  UpdateConfigParams  true  "Update keyspace parameters"
// @Produce  json
// @Success  200  {object}  KeyspaceMeta
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
//
// Router /keyspaces/{name}/config [patch]
func UpdateKeyspaceConfig(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}

	name := c.Param("name")
	configParams := &UpdateConfigParams{}
	err := c.BindJSON(configParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	mutations := getMutations(configParams.Config)

	// Check if the update is supported.
	for _, mutation := range mutations {
		if mutation.Key == keyspace.GCManagementType && mutation.Value == keyspace.KeyspaceLevelGC {
			err = keyspace.ErrUnsupportedOperationInKeyspace
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
	}

	meta, err := manager.UpdateKeyspaceConfig(name, mutations)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

// getMutations converts a given JSON merge patch to a series of keyspace config mutations.
func getMutations(patch map[string]*string) []*keyspace.Mutation {
	mutations := make([]*keyspace.Mutation, 0, len(patch))
	for k, v := range patch {
		if v == nil {
			mutations = append(mutations, &keyspace.Mutation{
				Op:  keyspace.OpDel,
				Key: k,
			})
		} else {
			mutations = append(mutations, &keyspace.Mutation{
				Op:    keyspace.OpPut,
				Key:   k,
				Value: *v,
			})
		}
	}
	return mutations
}

// UpdateStateParam represents parameters needed to modify target keyspace's state.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type UpdateStateParam struct {
	State string `json:"state"`
}

// UpdateKeyspaceState update the target keyspace's state.
//
// @Tags     keyspaces
// @Summary  Update keyspace state.
// @Param    name  path  string            true  "Keyspace Name"
// @Param    body  body  UpdateStateParam  true  "New state for the keyspace"
// @Produce  json
// @Success  200  {object}  KeyspaceMeta
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
//
// Router /keyspaces/{name}/state [put]
func UpdateKeyspaceState(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceManager()
	if manager == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, managerUninitializedErr)
		return
	}
	name := c.Param("name")
	param := &UpdateStateParam{}
	err := c.BindJSON(param)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	targetState, ok := keyspacepb.KeyspaceState_value[strings.ToUpper(param.State)]
	if !ok {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.Errorf("unknown target state: %s", param.State))
		return
	}
	meta, err := manager.UpdateKeyspaceState(name, keyspacepb.KeyspaceState(targetState), time.Now().Unix())
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &KeyspaceMeta{meta})
}

// KeyspaceMeta wraps keyspacepb.KeyspaceMeta to provide custom JSON marshal.
type KeyspaceMeta struct {
	*keyspacepb.KeyspaceMeta
}

// MarshalJSON creates custom marshal of KeyspaceMeta with the following:
// 1. Keyspace State are marshaled to their corresponding name for better readability.
func (meta *KeyspaceMeta) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID             uint32            `json:"id"`
		Name           string            `json:"name,omitempty"`
		State          string            `json:"state,omitempty"`
		CreatedAt      int64             `json:"created_at,omitempty"`
		StateChangedAt int64             `json:"state_changed_at,omitempty"`
		Config         map[string]string `json:"config,omitempty"`
	}{
		meta.Id,
		meta.Name,
		meta.State.String(),
		meta.CreatedAt,
		meta.StateChangedAt,
		meta.Config,
	})
}

// UnmarshalJSON reverse KeyspaceMeta's the Custom JSON marshal.
func (meta *KeyspaceMeta) UnmarshalJSON(data []byte) error {
	aux := &struct {
		ID             uint32            `json:"id"`
		Name           string            `json:"name,omitempty"`
		State          string            `json:"state,omitempty"`
		CreatedAt      int64             `json:"created_at,omitempty"`
		StateChangedAt int64             `json:"state_changed_at,omitempty"`
		Config         map[string]string `json:"config,omitempty"`
	}{}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	pbMeta := &keyspacepb.KeyspaceMeta{
		Id:             aux.ID,
		Name:           aux.Name,
		State:          keyspacepb.KeyspaceState(keyspacepb.KeyspaceState_value[aux.State]),
		CreatedAt:      aux.CreatedAt,
		StateChangedAt: aux.StateChangedAt,
		Config:         aux.Config,
	}
	meta.KeyspaceMeta = pbMeta
	return nil
}
