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
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	tsoserver "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/"

var (
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
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.Default())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
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
	configEndpoint := s.baseEndpoint.Group("/")
	configEndpoint.POST("/admin/reset-ts", gin.WrapF(s.ResetTS))
}

func (s *Service) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.apiHandlerEngine.ServeHTTP(w, r)
	})
}

// ResetTS
// FIXME: details of input json body params
// @Tags     admin
// @Summary  Reset the ts.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Reset ts successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  403  {string}  string  "Reset ts is forbidden."
// @Failure  500  {string}  string  "TSO server failed to proceed the request."
// @Router   /admin/reset-ts [post]
// if force-use-larger=true:
//
//	reset ts to max(current ts, input ts).
//
// else:
//
//	reset ts to input ts if it > current ts and < upper bound, error if not in that range
//
// during EBS based restore, we call this to make sure ts of pd >= resolved_ts in backup.
func (s *Service) ResetTS(w http.ResponseWriter, r *http.Request) {
	handler := s.srv.GetHandler()
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(s.rd, w, r.Body, &input); err != nil {
		return
	}
	tsValue, ok := input["tso"].(string)
	if !ok || len(tsValue) == 0 {
		s.rd.JSON(w, http.StatusBadRequest, "invalid tso value")
		return
	}
	ts, err := strconv.ParseUint(tsValue, 10, 64)
	if err != nil {
		s.rd.JSON(w, http.StatusBadRequest, "invalid tso value")
		return
	}

	forceUseLarger := false
	forceUseLargerVal, contains := input["force-use-larger"]
	if contains {
		if forceUseLarger, ok = forceUseLargerVal.(bool); !ok {
			s.rd.JSON(w, http.StatusBadRequest, "invalid force-use-larger value")
			return
		}
	}
	var ignoreSmaller, skipUpperBoundCheck bool
	if forceUseLarger {
		ignoreSmaller, skipUpperBoundCheck = true, true
	}

	if err = handler.ResetTS(ts, ignoreSmaller, skipUpperBoundCheck); err != nil {
		if err == server.ErrServerNotStarted {
			s.rd.JSON(w, http.StatusInternalServerError, err.Error())
		} else {
			s.rd.JSON(w, http.StatusForbidden, err.Error())
		}
		return
	}
	s.rd.JSON(w, http.StatusOK, "Reset ts successfully.")
}
