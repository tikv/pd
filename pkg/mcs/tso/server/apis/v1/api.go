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

package apis

import (
	"net/http"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	tsoserver "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/unrolled/render"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/tso/api/v1"

var (
	once            sync.Once
	apiServiceGroup = apiutil.APIServiceGroup{
		Name:       "tso",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

func init() {
	tsoserver.SetUpRestHandler = func(srv *tsoserver.Service) (http.Handler, apiutil.APIServiceGroup) {
		s := NewService(srv)
		return s.handler(), apiServiceGroup
	}
}

// Service is the tso service.
type Service struct {
	apiHandlerEngine *gin.Engine
	baseEndpoint     *gin.RouterGroup

	srv *tsoserver.Service
	rd  *render.Render
}

func createIndentRender() *render.Render {
	return render.New(render.Options{
		IndentJSON: true,
	})
}

// NewService returns a new Service.
func NewService(srv *tsoserver.Service) *Service {
	once.Do(func() {
		// These global modification will be effective only for the first invoke.
		_ = godotenv.Load()
		gin.SetMode(gin.ReleaseMode)
	})
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.Default())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	apiHandlerEngine.Use(func(c *gin.Context) {
		c.Set("service", srv)
		c.Next()
	})
	apiHandlerEngine.Use(multiservicesapi.ServiceRedirector())
	apiHandlerEngine.GET("metrics", utils.PromHandler(promhttp.Handler()))
	endpoint := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		srv:              srv,
		apiHandlerEngine: apiHandlerEngine,
		baseEndpoint:     endpoint,
		rd:               createIndentRender(),
	}
	s.RegisterRouter()
	return s
}

// RegisterRouter registers the router of the service.
func (s *Service) RegisterRouter() {
	tsoAdminHandler := tso.NewAdminHandler(s.srv.GetHandler(), s.rd)
	s.baseEndpoint.POST("/admin/reset-ts", gin.WrapF(tsoAdminHandler.ResetTS))
}

func (s *Service) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.apiHandlerEngine.ServeHTTP(w, r)
	})
}
