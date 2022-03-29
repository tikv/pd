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

package apiv2

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

var group = server.ServiceGroup{
	Name:       "core",
	IsCore:     true,
	Version:    "v2",
	PathPrefix: apiV2Prefix,
}

const apiV2Prefix = "/pd/api/v2/"

// NewV2Handler creates a HTTP handler for API.
func NewV2Handler(_ context.Context, svr *server.Server) (http.Handler, server.ServiceGroup, error) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("server", svr)
		c.Next()
	})
	router.Use(middlewares.Redirector())
	_ = router.Group(apiV2Prefix)

	return router, group, nil
}
