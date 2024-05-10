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

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterMicroService registers microservice handler to the router.
func RegisterMicroService(r *gin.RouterGroup) {
	router := r.Group("ms")
	router.GET("members/:service", GetMembers)
	router.GET("primary/:service", GetPrimary)
	router.POST("primary/transfer/:service", TransferPrimary)
}

// GetMembers gets all members of the cluster for the specified service.
// @Tags     members
// @Summary  Get all members of the cluster for the specified service.
// @Produce  json
// @Success  200  {object}  []discovery.ServiceRegistryEntry
// @Router   /ms/members/{service} [get]
func GetMembers(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	if !svr.IsAPIServiceMode() {
		c.AbortWithStatusJSON(http.StatusNotFound, "not support micro service")
		return
	}

	if service := c.Param("service"); len(service) > 0 {
		entries, err := discovery.GetMSMembers(service, svr.GetClient())
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, entries)
		return
	}

	c.AbortWithStatusJSON(http.StatusInternalServerError, "please specify service")
}

// GetPrimary gets the primary member of the specified service.
// @Tags     primary
// @Summary  Get the primary member of the specified service.
// @Produce  json
// @Success  200  {object}  string
// @Router   /ms/primary/{service} [get]
func GetPrimary(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	if !svr.IsAPIServiceMode() {
		c.AbortWithStatusJSON(http.StatusNotFound, "not support micro service")
		return
	}

	if service := c.Param("service"); len(service) > 0 {
		addr, _ := svr.GetServicePrimaryAddr(c.Request.Context(), service)
		c.IndentedJSON(http.StatusOK, addr)
		return
	}

	c.AbortWithStatusJSON(http.StatusInternalServerError, "please specify service")
}

// TransferPrimary transfers the primary member of the specified service.
// @Tags     primary
// @Summary  Transfer the primary member of the specified service.
// @Produce  json
// @Param    service     path    string  true  "service name"
// @Param    new_primary body   string  false "new primary address"
// @Success  200  string  string
// @Router   /ms/primary/transfer/{service} [post]
func TransferPrimary(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	if !svr.IsAPIServiceMode() {
		c.AbortWithStatusJSON(http.StatusNotFound, "not support micro service")
		return
	}

	if service := c.Param("service"); len(service) > 0 {
		var input map[string]string
		if err := c.BindJSON(&input); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}

		newPrimary, keyspaceGroupID := "", utils.DefaultKeyspaceGroupID
		if v, ok := input["new_primary"]; ok {
			newPrimary = v
		}

		if v, ok := input["keyspace_group_id"]; ok {
			keyspaceGroupIDRaw, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
				return
			}
			keyspaceGroupID = uint32(keyspaceGroupIDRaw)
		}
		oldPrimary, _ := svr.GetServicePrimaryAddr(c.Request.Context(), service)
		if oldPrimary == newPrimary {
			c.AbortWithStatusJSON(http.StatusInternalServerError, "new primary is the same as the old one")
			return
		}
		if err := discovery.TransferPrimary(svr.GetClient(), service, oldPrimary, newPrimary, keyspaceGroupID); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, "success")
		return
	}

	c.AbortWithStatusJSON(http.StatusInternalServerError, "please specify service")
}
