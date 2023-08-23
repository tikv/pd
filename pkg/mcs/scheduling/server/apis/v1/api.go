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
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	scheserver "github.com/tikv/pd/pkg/mcs/scheduling/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/unrolled/render"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/scheduling/api/v1/"

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
}

// RegisterCheckersRouter registers the router of the checkers handler.
func (s *Service) RegisterCheckersRouter() {
	router := s.root.Group("checkers")
	router.GET("/:name", getCheckerByName)
}

// RegisterOperatorsRouter registers the router of the operators handler.
func (s *Service) RegisterOperatorsRouter() {
	router := s.root.Group("operators")
	router.GET("", getOperators)
	router.GET("/:id", getOperatorByID)
}

// @Tags     operators
// @Summary  Get an operator by ID.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {object}  operator.OpWithStatus
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{id} [GET]
func getOperatorByID(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	id := c.Param("id")

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	opController := svr.GetCoordinator().GetOperatorController()
	if opController == nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, opController.GetOperatorStatus(regionID))
}

// @Tags     operators
// @Summary  List operators.
// @Param    kind  query  string  false  "Specify the operator kind."  Enums(admin, leader, region, waiting)
// @Produce  json
// @Success  200  {array}   operator.Operator
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [GET]
func getOperators(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	var (
		results []*operator.Operator
		ops     []*operator.Operator
		err     error
	)

	opController := svr.GetCoordinator().GetOperatorController()
	if opController == nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	kinds := c.QueryArray("kind")
	if len(kinds) == 0 {
		results = opController.GetOperators()
	} else {
		for _, kind := range kinds {
			switch kind {
			case "admin":
				ops = opController.GetOperatorsOfKind(operator.OpAdmin)
			case "leader":
				ops = opController.GetOperatorsOfKind(operator.OpLeader)
			case "region":
				ops = opController.GetOperatorsOfKind(operator.OpRegion)
			case "waiting":
				ops = opController.GetWaitingOperators()
			}
			results = append(results, ops...)
		}
	}

	c.JSON(http.StatusOK, results)
}

// @Tags     checkers
// @Summary  Get checker by name
// @Param    name  path  string  true  "The name of the checker."
// @Produce  json
// @Success  200  {string}  string  "The checker's status."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /checkers/{name} [get]
func getCheckerByName(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	name := c.Param("name")
	co := svr.GetCoordinator()
	isPaused, err := co.IsCheckerPaused(name)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	output := map[string]bool{
		"paused": isPaused,
	}
	c.JSON(http.StatusOK, output)
}

type schedulerPausedPeriod struct {
	Name     string    `json:"name"`
	PausedAt time.Time `json:"paused_at"`
	ResumeAt time.Time `json:"resume_at"`
}

// @Tags     schedulers
// @Summary  List all created schedulers by status.
// @Produce  json
// @Success  200  {array}   string
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers [get]
func getSchedulers(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	co := svr.GetCoordinator()
	sc := co.GetSchedulersController()
	schedulers := sc.GetSchedulerNames()

	status := c.Query("status")
	_, needTS := c.GetQuery("timestamp")
	switch status {
	case "paused":
		var pausedSchedulers []string
		pausedPeriods := []schedulerPausedPeriod{}
		for _, scheduler := range schedulers {
			paused, err := sc.IsSchedulerPaused(scheduler)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}

			if paused {
				if needTS {
					s := schedulerPausedPeriod{
						Name:     scheduler,
						PausedAt: time.Time{},
						ResumeAt: time.Time{},
					}
					pausedAt, err := sc.GetPausedSchedulerDelayAt(scheduler)
					if err != nil {
						c.String(http.StatusInternalServerError, err.Error())
						return
					}
					s.PausedAt = time.Unix(pausedAt, 0)
					resumeAt, err := sc.GetPausedSchedulerDelayUntil(scheduler)
					if err != nil {
						c.String(http.StatusInternalServerError, err.Error())
						return
					}
					s.ResumeAt = time.Unix(resumeAt, 0)
					pausedPeriods = append(pausedPeriods, s)
				} else {
					pausedSchedulers = append(pausedSchedulers, scheduler)
				}
			}
		}
		if needTS {
			c.JSON(http.StatusOK, pausedPeriods)
		} else {
			c.JSON(http.StatusOK, pausedSchedulers)
		}
		return
	case "disabled":
		var disabledSchedulers []string
		for _, scheduler := range schedulers {
			disabled, err := sc.IsSchedulerDisabled(scheduler)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}

			if disabled {
				disabledSchedulers = append(disabledSchedulers, scheduler)
			}
		}
		c.JSON(http.StatusOK, disabledSchedulers)
	default:
		c.JSON(http.StatusOK, schedulers)
	}
}
