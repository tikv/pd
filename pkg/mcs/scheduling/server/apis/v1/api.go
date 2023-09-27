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
	"strconv"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	scheserver "github.com/tikv/pd/pkg/mcs/scheduling/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/schedule"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/unrolled/render"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/scheduling/api/v1"
const handlerKey = "handler"

var (
	once            sync.Once
	apiServiceGroup = apiutil.APIServiceGroup{
		Name:       "scheduling",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

func init() {
	scheserver.SetUpRestHandler = func(srv *scheserver.Service) (http.Handler, apiutil.APIServiceGroup) {
		s := NewService(srv)
		return s.apiHandlerEngine, apiServiceGroup
	}
}

// Service is the tso service.
type Service struct {
	apiHandlerEngine *gin.Engine
	root             *gin.RouterGroup

	srv *scheserver.Service
	rd  *render.Render
}

type server struct {
	server *scheserver.Server
}

func (s *server) GetCoordinator() *schedule.Coordinator {
	return s.server.GetCoordinator()
}

func (s *server) GetCluster() sche.SharedCluster {
	return s.server.GetCluster()
}

func createIndentRender() *render.Render {
	return render.New(render.Options{
		IndentJSON: true,
	})
}

// NewService returns a new Service.
func NewService(srv *scheserver.Service) *Service {
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
		c.Set(multiservicesapi.ServiceContextKey, srv.Server)
		c.Set(handlerKey, handler.NewHandler(&server{server: srv.Server}))
		c.Next()
	})
	apiHandlerEngine.Use(multiservicesapi.ServiceRedirector())
	apiHandlerEngine.GET("metrics", utils.PromHandler())
	pprof.Register(apiHandlerEngine)
	root := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		srv:              srv,
		apiHandlerEngine: apiHandlerEngine,
		root:             root,
		rd:               createIndentRender(),
	}
	s.RegisterOperatorsRouter()
	s.RegisterSchedulersRouter()
	s.RegisterCheckersRouter()
	return s
}

// RegisterSchedulersRouter registers the router of the schedulers handler.
func (s *Service) RegisterSchedulersRouter() {
	router := s.root.Group("schedulers")
	router.GET("", getSchedulers)
	router.GET("/diagnostic/:name", getDiagnosticResult)
	router.POST("/:name", pauseOrResumeScheduler)
}

// RegisterCheckersRouter registers the router of the checkers handler.
func (s *Service) RegisterCheckersRouter() {
	router := s.root.Group("checkers")
	router.GET("/:name", getCheckerByName)
	router.POST("/:name", pauseOrResumeChecker)
}

// RegisterOperatorsRouter registers the router of the operators handler.
func (s *Service) RegisterOperatorsRouter() {
	router := s.root.Group("operators")
	router.GET("", getOperators)
	router.POST("", createOperator)
	router.GET("/:id", getOperatorByRegion)
	router.DELETE("/:id", deleteOperatorByRegion)
	router.GET("/records", getOperatorRecords)
}

// @Tags     operators
// @Summary  Get an operator by ID.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {object}  operator.OpWithStatus
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{id} [GET]
func getOperatorByRegion(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	id := c.Param("id")

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	op, err := handler.GetOperatorStatus(regionID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.IndentedJSON(http.StatusOK, op)
}

// @Tags     operators
// @Summary  List operators.
// @Param    kind  query  string  false  "Specify the operator kind."  Enums(admin, leader, region, waiting)
// @Produce  json
// @Success  200  {array}   operator.Operator
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [GET]
func getOperators(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	var (
		results []*operator.Operator
		err     error
	)

	kinds := c.QueryArray("kind")
	if len(kinds) == 0 {
		results, err = handler.GetOperators()
	} else {
		results, err = handler.GetOperatorsByKinds(kinds)
	}

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, results)
}

// @Tags     operator
// @Summary  Cancel a Region's pending operator.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {string}  string  "The pending operator is canceled."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{region_id} [delete]
func deleteOperatorByRegion(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	id := c.Param("id")

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if err = handler.RemoveOperator(regionID); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.String(http.StatusOK, "The pending operator is canceled.")
}

// @Tags     operator
// @Summary  lists the finished operators since the given timestamp in second.
// @Param    from  query  integer  false  "From Unix timestamp"
// @Produce  json
// @Success  200  {object}  []operator.OpRecord
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/records [get]
func getOperatorRecords(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	from, err := apiutil.ParseTime(c.Query("from"))
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	records, err := handler.GetRecords(from)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, records)
}

// FIXME: details of input json body params
// @Tags     operator
// @Summary  Create an operator.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The operator is created."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [post]
func createOperator(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	var input map[string]interface{}
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	statusCode, result, err := handler.HandleOperatorCreation(input)
	if err != nil {
		c.String(statusCode, err.Error())
		return
	}
	if statusCode == http.StatusOK && result == nil {
		c.String(http.StatusOK, "The operator is created.")
		return
	}
	c.IndentedJSON(statusCode, result)
}

// @Tags     checkers
// @Summary  Get checker by name
// @Param    name  path  string  true  "The name of the checker."
// @Produce  json
// @Success  200  {string}  string  "The checker's status."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /checkers/{name} [get]
func getCheckerByName(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	name := c.Param("name")
	output, err := handler.GetCheckerStatus(name)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, output)
}

// FIXME: details of input json body params
// @Tags     checker
// @Summary  Pause or resume region merge.
// @Accept   json
// @Param    name  path  string  true  "The name of the checker."
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Pause or resume the scheduler successfully."
// @Failure  400  {string}  string  "Bad format request."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /checker/{name} [post]
func pauseOrResumeChecker(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	var input map[string]int
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	name := c.Param("name")
	t, ok := input["delay"]
	if !ok {
		c.String(http.StatusBadRequest, "missing pause time")
		return
	}
	if t < 0 {
		c.String(http.StatusBadRequest, "delay cannot be negative")
		return
	}
	if err := handler.PauseOrResumeChecker(name, int64(t)); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if t == 0 {
		c.String(http.StatusOK, "Resume the checker successfully.")
	} else {
		c.String(http.StatusOK, "Pause the checker successfully.")
	}
}

// @Tags     schedulers
// @Summary  List all created schedulers by status.
// @Produce  json
// @Success  200  {array}   string
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers [get]
func getSchedulers(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	status := c.Query("status")
	_, needTS := c.GetQuery("timestamp")
	output, err := handler.GetSchedulerByStatus(status, needTS)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, output)
}

// @Tags     schedulers
// @Summary  List schedulers diagnostic result.
// @Produce  json
// @Success  200  {array}   string
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/diagnostic/{name} [get]
func getDiagnosticResult(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	name := c.Param("name")
	result, err := handler.GetDiagnosticResult(name)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, result)
}

// FIXME: details of input json body params
// @Tags     scheduler
// @Summary  Pause or resume a scheduler.
// @Accept   json
// @Param    name  path  string  true  "The name of the scheduler."
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Pause or resume the scheduler successfully."
// @Failure  400  {string}  string  "Bad format request."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/{name} [post]
func pauseOrResumeScheduler(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]int64
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	name := c.Param("name")
	t, ok := input["delay"]
	if !ok {
		c.String(http.StatusBadRequest, "missing pause time")
		return
	}
	if err := handler.PauseOrResumeScheduler(name, t); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Pause or resume the scheduler successfully.")
}
