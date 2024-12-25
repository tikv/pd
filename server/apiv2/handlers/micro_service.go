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

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterMicroService registers microservice handler to the router.
func RegisterMicroService(r *gin.RouterGroup) {
	router := r.Group("ms")
	router.GET("members/:service", GetMembers)
	router.GET("primary/:service", GetPrimary)
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
