// Copyright 2025 TiKV Project Authors.
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
	"github.com/unrolled/render"

	routerserver "github.com/tikv/pd/pkg/mcs/router/server"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/router/api/v1"

var (
	apiServiceGroup = apiutil.APIServiceGroup{
		Name:       "router",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

func init() {
	routerserver.SetUpRestHandler = func(srv *routerserver.Service) (http.Handler, apiutil.APIServiceGroup) {
		s := NewService(srv)
		return s.apiHandlerEngine, apiServiceGroup
	}
}

// Service is the router service.
type Service struct {
	apiHandlerEngine *gin.Engine
	root             *gin.RouterGroup

	srv *routerserver.Service
	rd  *render.Render
}

func createIndentRender() *render.Render {
	return render.New(render.Options{
		IndentJSON: true,
	})
}

// NewService returns a new Service.
func NewService(srv *routerserver.Service) *Service {
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.Default())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	apiHandlerEngine.Use(func(c *gin.Context) {
		c.Set(multiservicesapi.ServiceContextKey, srv.Server)
		c.Next()
	})
	apiHandlerEngine.GET("metrics", mcsutils.PromHandler())
	apiHandlerEngine.GET("status", mcsutils.StatusHandler)
	pprof.Register(apiHandlerEngine)
	root := apiHandlerEngine.Group(APIPathPrefix)
	root.Use(multiservicesapi.ServiceRedirector())
	s := &Service{
		srv:              srv,
		apiHandlerEngine: apiHandlerEngine,
		root:             root,
		rd:               createIndentRender(),
	}
	return s
}
