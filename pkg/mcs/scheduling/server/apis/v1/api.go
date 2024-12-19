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
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/unrolled/render"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	scheserver "github.com/tikv/pd/pkg/mcs/scheduling/server"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/response"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/scheduling/api/v1"
const handlerKey = "handler"

var (
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
	*scheserver.Server
}

// GetCluster returns the cluster.
func (s *server) GetCluster() sche.SchedulerCluster {
	return s.Server.GetCluster()
}

func createIndentRender() *render.Render {
	return render.New(render.Options{
		IndentJSON: true,
	})
}

// NewService returns a new Service.
func NewService(srv *scheserver.Service) *Service {
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.Default())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	apiHandlerEngine.Use(func(c *gin.Context) {
		c.Set(multiservicesapi.ServiceContextKey, srv.Server)
		c.Set(handlerKey, handler.NewHandler(&server{srv.Server}))
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
	s.RegisterAdminRouter()
	s.RegisterConfigRouter()
	s.RegisterOperatorsRouter()
	s.RegisterSchedulersRouter()
	s.RegisterCheckersRouter()
	s.RegisterHotspotRouter()
	s.RegisterRegionsRouter()
	s.RegisterStoresRouter()
	s.RegisterPrimaryRouter()
	return s
}

// RegisterAdminRouter registers the router of the admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	router.PUT("/log", changeLogLevel)
	router.DELETE("cache/regions", deleteAllRegionCache)
	router.DELETE("cache/regions/:id", deleteRegionCacheByID)
}

// RegisterSchedulersRouter registers the router of the schedulers handler.
func (s *Service) RegisterSchedulersRouter() {
	router := s.root.Group("schedulers")
	router.GET("", getSchedulers)
	router.GET("/diagnostic/:name", getDiagnosticResult)
	router.GET("/config", getSchedulerConfig)
	router.GET("/config/:name/list", getSchedulerConfigByName)
	// TODO: in the future, we should split pauseOrResumeScheduler to two different APIs.
	// And we need to do one-to-two forwarding in the API middleware.
	router.POST("/:name", pauseOrResumeScheduler)
}

// RegisterCheckersRouter registers the router of the checkers handler.
func (s *Service) RegisterCheckersRouter() {
	router := s.root.Group("checkers")
	router.GET("/:name", getCheckerByName)
	router.POST("/:name", pauseOrResumeChecker)
}

// RegisterHotspotRouter registers the router of the hotspot handler.
func (s *Service) RegisterHotspotRouter() {
	router := s.root.Group("hotspot")
	router.GET("/regions/write", getHotWriteRegions)
	router.GET("/regions/read", getHotReadRegions)
	router.GET("/regions/history", getHistoryHotRegions)
	router.GET("/stores", getHotStores)
	router.GET("/buckets", getHotBuckets)
}

// RegisterOperatorsRouter registers the router of the operators handler.
func (s *Service) RegisterOperatorsRouter() {
	router := s.root.Group("operators")
	router.GET("", getOperators)
	router.POST("", createOperator)
	router.DELETE("", deleteOperators)
	router.GET("/:id", getOperatorByRegion)
	router.DELETE("/:id", deleteOperatorByRegion)
	router.GET("/records", getOperatorRecords)
}

// RegisterStoresRouter registers the router of the stores handler.
func (s *Service) RegisterStoresRouter() {
	router := s.root.Group("stores")
	router.GET("", getAllStores)
	router.GET("/:id", getStoreByID)
}

// RegisterRegionsRouter registers the router of the regions handler.
func (s *Service) RegisterRegionsRouter() {
	router := s.root.Group("regions")
	router.GET("", getAllRegions)
	router.GET("/:id", getRegionByID)
	router.GET("/count", getRegionCount)
	router.POST("/accelerate-schedule", accelerateRegionsScheduleInRange)
	router.POST("/accelerate-schedule/batch", accelerateRegionsScheduleInRanges)
	router.POST("/scatter", scatterRegions)
	router.POST("/split", splitRegions)
	router.GET("/replicated", checkRegionsReplicated)
}

// RegisterConfigRouter registers the router of the config handler.
func (s *Service) RegisterConfigRouter() {
	router := s.root.Group("config")
	router.GET("", getConfig)

	rules := router.Group("rules")
	rules.GET("", getAllRules)
	rules.GET("/group/:group", getRuleByGroup)
	rules.GET("/region/:region", getRulesByRegion)
	rules.GET("/region/:region/detail", checkRegionPlacementRule)
	rules.GET("/key/:key", getRulesByKey)

	// We cannot merge `/rule` and `/rules`, because we allow `group_id` to be "group",
	// which is the same as the prefix of `/rules/group/:group`.
	rule := router.Group("rule")
	rule.GET("/:group/:id", getRuleByGroupAndID)

	groups := router.Group("rule_groups")
	groups.GET("", getAllGroupConfigs)
	groups.GET("/:id", getRuleGroupConfig)

	placementRule := router.Group("placement-rule")
	placementRule.GET("", getPlacementRules)
	placementRule.GET("/:group", getPlacementRuleByGroup)

	regionLabel := router.Group("region-label")
	regionLabel.GET("/rules", getAllRegionLabelRules)
	regionLabel.GET("/rules/ids", getRegionLabelRulesByIDs)
	regionLabel.GET("/rules/:id", getRegionLabelRuleByID)

	regions := router.Group("regions")
	regions.GET("/:id/label/:key", getRegionLabelByKey)
	regions.GET("/:id/labels", getRegionLabels)
}

// RegisterPrimaryRouter registers the router of the primary handler.
func (s *Service) RegisterPrimaryRouter() {
	router := s.root.Group("primary")
	router.POST("transfer", transferPrimary)
}

// @Tags     admin
// @Summary  Change the log level.
// @Produce  json
// @Success  200  {string}  string  "The log level is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /admin/log [put]
func changeLogLevel(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	var level string
	if err := c.Bind(&level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if err := svr.SetLogLevel(level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))
	c.String(http.StatusOK, "The log level is updated.")
}

// @Tags     config
// @Summary  Get full config.
// @Produce  json
// @Success  200  {object}  config.Config
// @Router   /config [get]
func getConfig(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	cfg := svr.GetConfig()
	cfg.Schedule.MaxMergeRegionKeys = cfg.Schedule.GetMaxMergeRegionKeys()
	c.IndentedJSON(http.StatusOK, cfg)
}

// @Tags     admin
// @Summary  Drop all regions from cache.
// @Produce  json
// @Success  200  {string}  string  "All regions are removed from server cache."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /admin/cache/regions [delete]
func deleteAllRegionCache(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	cluster := svr.GetCluster()
	if cluster == nil {
		c.String(http.StatusInternalServerError, errs.ErrNotBootstrapped.GenWithStackByArgs().Error())
		return
	}
	cluster.ResetRegionCache()
	c.String(http.StatusOK, "All regions are removed from server cache.")
}

// @Tags     admin
// @Summary  Drop a specific region from cache.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {string}  string  "The region is removed from server cache."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /admin/cache/regions/{id} [delete]
func deleteRegionCacheByID(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	cluster := svr.GetCluster()
	if cluster == nil {
		c.String(http.StatusInternalServerError, errs.ErrNotBootstrapped.GenWithStackByArgs().Error())
		return
	}
	regionIDStr := c.Param("id")
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	cluster.RemoveRegionIfExist(regionID)
	c.String(http.StatusOK, "The region is removed from server cache.")
}

// @Tags     operators
// @Summary  Get an operator by ID.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {object}  operator.OpWithStatus
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{id} [get]
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
// @Param    kind   query  string  false  "Specify the operator kind."  Enums(admin, leader, region, waiting)
// @Param    object query  bool    false  "Whether to return as JSON object."
// @Produce  json
// @Success  200  {array}   operator.Operator
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [get]
func getOperators(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	var (
		results []*operator.Operator
		err     error
	)

	kinds := c.QueryArray("kind")
	_, objectFlag := c.GetQuery("object")
	if len(kinds) == 0 {
		results, err = handler.GetOperators()
	} else {
		results, err = handler.GetOperatorsByKinds(kinds)
	}

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if objectFlag {
		objResults := make([]*operator.OpObject, len(results))
		for i, op := range results {
			objResults[i] = op.ToJSONObject()
		}
		c.IndentedJSON(http.StatusOK, objResults)
	} else {
		c.IndentedJSON(http.StatusOK, results)
	}
}

// @Tags     operators
// @Summary  Delete operators.
// @Produce  json
// @Success  200  {string}  string  "All pending operator are canceled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [delete]
func deleteOperators(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	if err := handler.RemoveOperators(); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.String(http.StatusOK, "All pending operator are canceled.")
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
	var input map[string]any
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
// @Summary  List all scheduler configs.
// @Produce  json
// @Success  200  {object}  map[string]any
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/config/ [get]
func getSchedulerConfig(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	sc, err := handler.GetSchedulersController()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	sches, configs, err := sc.GetAllSchedulerConfigs()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, schedulers.ToPayload(sches, configs))
}

// @Tags     schedulers
// @Summary  List scheduler config by name.
// @Produce  json
// @Success  200  {object}  map[string]any
// @Failure  404  {string}  string  scheduler not found
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/config/{name}/list [get]
func getSchedulerConfigByName(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	sc, err := handler.GetSchedulersController()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	handlers := sc.GetSchedulerHandlers()
	name := c.Param("name")
	if _, ok := handlers[name]; !ok {
		c.String(http.StatusNotFound, errs.ErrSchedulerNotFound.GenWithStackByArgs().Error())
		return
	}
	isDisabled, err := sc.IsSchedulerDisabled(name)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if isDisabled {
		c.String(http.StatusNotFound, errs.ErrSchedulerNotFound.GenWithStackByArgs().Error())
		return
	}
	c.Request.URL.Path = "/list"
	handlers[name].ServeHTTP(c.Writer, c.Request)
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

// @Tags     hotspot
// @Summary  List the hot write regions.
// @Produce  json
// @Success  200  {object}  statistics.StoreHotPeersInfos
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/write [get]
func getHotWriteRegions(c *gin.Context) {
	getHotRegions(utils.Write, c)
}

// @Tags     hotspot
// @Summary  List the hot read regions.
// @Produce  json
// @Success  200  {object}  statistics.StoreHotPeersInfos
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/read [get]
func getHotReadRegions(c *gin.Context) {
	getHotRegions(utils.Read, c)
}

func getHotRegions(typ utils.RWType, c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	storeIDs := c.QueryArray("store_id")
	if len(storeIDs) < 1 {
		hotRegions, err := handler.GetHotRegions(typ)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, hotRegions)
		return
	}

	var ids []uint64
	for _, storeID := range storeIDs {
		id, err := strconv.ParseUint(storeID, 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, errs.ErrInvalidStoreID.FastGenByArgs(storeID).Error())
			return
		}
		_, err = handler.GetStore(id)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		ids = append(ids, id)
	}

	hotRegions, err := handler.GetHotRegions(typ, ids...)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, hotRegions)
}

// @Tags     hotspot
// @Summary  List the hot stores.
// @Produce  json
// @Success  200  {object}  handler.HotStoreStats
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/stores [get]
func getHotStores(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	stores, err := handler.GetHotStores()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, stores)
}

// @Tags     hotspot
// @Summary  List the hot buckets.
// @Produce  json
// @Success  200  {object}  handler.HotBucketsResponse
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/buckets [get]
func getHotBuckets(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	regionIDs := c.QueryArray("region_id")
	ids := make([]uint64, len(regionIDs))
	for i, regionID := range regionIDs {
		if id, err := strconv.ParseUint(regionID, 10, 64); err == nil {
			ids[i] = id
		}
	}
	ret, err := handler.GetHotBuckets(ids...)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, ret)
}

// @Tags     hotspot
// @Summary  List the history hot regions.
// @Accept   json
// @Produce  json
// @Success  200  {object}  storage.HistoryHotRegions
// @Router   /hotspot/regions/history [get]
func getHistoryHotRegions(c *gin.Context) {
	// TODO: support history hotspot in scheduling server with stateless in the future.
	// Ref: https://github.com/tikv/pd/pull/7183
	var res storage.HistoryHotRegions
	c.IndentedJSON(http.StatusOK, res)
}

// @Tags     rule
// @Summary  List all rules of cluster.
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules [get]
func getAllRules(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	rules := manager.GetAllRules()
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     rule
// @Summary  List all rules of cluster by group.
// @Param    group  path  string  true  "The name of group"
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/group/{group} [get]
func getRuleByGroup(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	group := c.Param("group")
	rules := manager.GetRulesByGroup(group)
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     rule
// @Summary  List all rules of cluster by region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/region/{region} [get]
func getRulesByRegion(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	regionStr := c.Param("region")
	region, code, err := handler.PreCheckForRegion(regionStr)
	if err != nil {
		c.String(code, err.Error())
		return
	}
	rules := manager.GetRulesForApplyRegion(region)
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     rule
// @Summary  List rules and matched peers related to the given region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {object}  placement.RegionFit
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/region/{region}/detail [get]
func checkRegionPlacementRule(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	regionStr := c.Param("region")
	region, code, err := handler.PreCheckForRegion(regionStr)
	if err != nil {
		c.String(code, err.Error())
		return
	}
	regionFit, err := handler.CheckRegionPlacementRule(region)
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, regionFit)
}

// @Tags     rule
// @Summary  List all rules of cluster by key.
// @Param    key  path  string  true  "The name of key"
// @Produce  json
// @Success  200  {array}   placement.Rule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rules/key/{key} [get]
func getRulesByKey(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	keyHex := c.Param("key")
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		c.String(http.StatusBadRequest, errs.ErrKeyFormat.Error())
		return
	}
	rules := manager.GetRulesByKey(key)
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     rule
// @Summary  Get rule of cluster by group and id.
// @Param    group  path  string  true  "The name of group"
// @Param    id     path  string  true  "Rule Id"
// @Produce  json
// @Success  200  {object}  placement.Rule
// @Failure  404  {string}  string  "The rule does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Router   /config/rule/{group}/{id} [get]
func getRuleByGroupAndID(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	group, id := c.Param("group"), c.Param("id")
	rule := manager.GetRule(group, id)
	if rule == nil {
		c.String(http.StatusNotFound, errs.ErrRuleNotFound.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, rule)
}

// @Tags     rule
// @Summary  List all rule group configs.
// @Produce  json
// @Success  200  {array}   placement.RuleGroup
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule_groups [get]
func getAllGroupConfigs(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	ruleGroups := manager.GetRuleGroups()
	c.IndentedJSON(http.StatusOK, ruleGroups)
}

// @Tags     rule
// @Summary  Get rule group config by group id.
// @Param    id  path  string  true  "Group Id"
// @Produce  json
// @Success  200  {object}  placement.RuleGroup
// @Failure  404  {string}  string  "The RuleGroup does not exist."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/rule_groups/{id} [get]
func getRuleGroupConfig(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	id := c.Param("id")
	group := manager.GetRuleGroup(id)
	if group == nil {
		c.String(http.StatusNotFound, errs.ErrRuleNotFound.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, group)
}

// @Tags     rule
// @Summary  List all rules and groups configuration.
// @Produce  json
// @Success  200  {array}   placement.GroupBundle
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/placement-rules [get]
func getPlacementRules(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	bundles := manager.GetAllGroupBundles()
	c.IndentedJSON(http.StatusOK, bundles)
}

// @Tags     rule
// @Summary  Get group config and all rules belong to the group.
// @Param    group  path  string  true  "The name of group"
// @Produce  json
// @Success  200  {object}  placement.GroupBundle
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/placement-rules/{group} [get]
func getPlacementRuleByGroup(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	manager, err := handler.GetRuleManager()
	if err == errs.ErrPlacementDisabled {
		c.String(http.StatusPreconditionFailed, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	g := c.Param("group")
	group := manager.GetGroupBundle(g)
	c.IndentedJSON(http.StatusOK, group)
}

// @Tags     region_label
// @Summary  Get label of a region.
// @Param    id   path  integer  true  "Region Id"
// @Param    key  path  string   true  "Label key"
// @Produce  json
// @Success  200  {string}  string
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/regions/{id}/label/{key} [get]
func getRegionLabelByKey(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	idStr := c.Param("id")
	labelKey := c.Param("key") // TODO: test https://github.com/tikv/pd/pull/4004

	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	region, err := handler.GetRegion(id)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if region == nil {
		c.String(http.StatusNotFound, errs.ErrRegionNotFound.FastGenByArgs().Error())
		return
	}

	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	labelValue := l.GetRegionLabel(region, labelKey)
	c.IndentedJSON(http.StatusOK, labelValue)
}

// @Tags     region_label
// @Summary  Get labels of a region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {string}  string
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Router   /config/regions/{id}/labels [get]
func getRegionLabels(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	region, err := handler.GetRegion(id)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if region == nil {
		c.String(http.StatusNotFound, errs.ErrRegionNotFound.FastGenByArgs().Error())
		return
	}
	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	labels := l.GetRegionLabels(region)
	c.IndentedJSON(http.StatusOK, labels)
}

// @Tags     region_label
// @Summary  List all label rules of cluster.
// @Produce  json
// @Success  200  {array}  labeler.LabelRule
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rules [get]
func getAllRegionLabelRules(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	rules := l.GetAllLabelRules()
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     region_label
// @Summary  Get label rules of cluster by ids.
// @Param    body  body  []string  true  "IDs of query rules"
// @Produce  json
// @Success  200  {array}   labeler.LabelRule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rules/ids [get]
func getRegionLabelRulesByIDs(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	var ids []string
	if err := c.BindJSON(&ids); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	rules, err := l.GetLabelRules(ids)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     region_label
// @Summary  Get label rule of cluster by id.
// @Param    id  path  string  true  "Rule Id"
// @Produce  json
// @Success  200  {object}  labeler.LabelRule
// @Failure  404  {string}  string  "The rule does not exist."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rules/{id} [get]
func getRegionLabelRuleByID(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	id, err := url.PathUnescape(c.Param("id"))
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	rule := l.GetLabelRule(id)
	if rule == nil {
		c.String(http.StatusNotFound, errs.ErrRegionRuleNotFound.FastGenByArgs().Error())
		return
	}
	c.IndentedJSON(http.StatusOK, rule)
}

// @Tags     region
// @Summary  Accelerate regions scheduling a in given range, only receive hex format for keys
// @Accept   json
// @Param    body   body   object   true   "json params"
// @Param    limit  query  integer  false  "Limit count"  default(256)
// @Produce  json
// @Success  200  {string}  string  "Accelerate regions scheduling in a given range [startKey, endKey)"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/accelerate-schedule [post]
func accelerateRegionsScheduleInRange(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]any
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	rawStartKey, ok1 := input["start_key"].(string)
	rawEndKey, ok2 := input["end_key"].(string)
	if !ok1 || !ok2 {
		c.String(http.StatusBadRequest, "start_key or end_key is not string")
		return
	}

	limitStr, _ := c.GetQuery("limit")
	limit, err := handler.AdjustLimit(limitStr, 256 /*default limit*/)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	err = handler.AccelerateRegionsScheduleInRange(rawStartKey, rawEndKey, limit)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, fmt.Sprintf("Accelerate regions scheduling in a given range [%s,%s)", rawStartKey, rawEndKey))
}

// @Tags     region
// @Summary  Accelerate regions scheduling in given ranges, only receive hex format for keys
// @Accept   json
// @Param    body   body   object   true   "json params"
// @Param    limit  query  integer  false  "Limit count"  default(256)
// @Produce  json
// @Success  200  {string}  string  "Accelerate regions scheduling in given ranges [startKey1, endKey1), [startKey2, endKey2), ..."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/accelerate-schedule/batch [post]
func accelerateRegionsScheduleInRanges(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input []map[string]any
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	limitStr, _ := c.GetQuery("limit")
	limit, err := handler.AdjustLimit(limitStr, 256 /*default limit*/)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	var msgBuilder strings.Builder
	msgBuilder.Grow(128)
	msgBuilder.WriteString("Accelerate regions scheduling in given ranges: ")
	var startKeys, endKeys [][]byte
	for _, rg := range input {
		startKey, rawStartKey, err := apiutil.ParseKey("start_key", rg)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		endKey, rawEndKey, err := apiutil.ParseKey("end_key", rg)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		startKeys = append(startKeys, startKey)
		endKeys = append(endKeys, endKey)
		msgBuilder.WriteString(fmt.Sprintf("[%s,%s), ", rawStartKey, rawEndKey))
	}
	err = handler.AccelerateRegionsScheduleInRanges(startKeys, endKeys, limit)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, msgBuilder.String())
}

// @Tags     region
// @Summary  Scatter regions by given key ranges or regions id distributed by given group with given retry limit
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Scatter regions by given key ranges or regions id distributed by given group with given retry limit"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/scatter [post]
func scatterRegions(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]any
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	rawStartKey, ok1 := input["start_key"].(string)
	rawEndKey, ok2 := input["end_key"].(string)
	group, _ := input["group"].(string)
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
	}

	opsCount, failures, err := func() (int, map[uint64]error, error) {
		if ok1 && ok2 {
			return handler.ScatterRegionsByRange(rawStartKey, rawEndKey, group, retryLimit)
		}
		ids, ok := typeutil.JSONToUint64Slice(input["regions_id"])
		if !ok {
			return 0, nil, errors.New("regions_id is invalid")
		}
		return handler.ScatterRegionsByID(ids, group, retryLimit)
	}()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	s := handler.BuildScatterRegionsResp(opsCount, failures)
	c.IndentedJSON(http.StatusOK, &s)
}

// @Tags     region
// @Summary  Split regions with given split keys
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Split regions with given split keys"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /regions/split [post]
func splitRegions(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]any
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	s, ok := input["split_keys"]
	if !ok {
		c.String(http.StatusBadRequest, "split_keys should be provided.")
		return
	}
	rawSplitKeys := s.([]any)
	if len(rawSplitKeys) < 1 {
		c.String(http.StatusBadRequest, "empty split keys.")
		return
	}
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
	}
	s, err := handler.SplitRegions(c.Request.Context(), rawSplitKeys, retryLimit)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &s)
}

// @Tags     region
// @Summary  Check if regions in the given key ranges are replicated. Returns 'REPLICATED', 'INPROGRESS', or 'PENDING'. 'PENDING' means that there is at least one region pending for scheduling. Similarly, 'INPROGRESS' means there is at least one region in scheduling.
// @Param    startKey  query  string  true  "Regions start key, hex encoded"
// @Param    endKey    query  string  true  "Regions end key, hex encoded"
// @Produce  plain
// @Success  200  {string}  string  "INPROGRESS"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/replicated [get]
func checkRegionsReplicated(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	rawStartKey, ok1 := c.GetQuery("startKey")
	rawEndKey, ok2 := c.GetQuery("endKey")
	if !ok1 || !ok2 {
		c.String(http.StatusBadRequest, "there is no start_key or end_key")
		return
	}

	state, err := handler.CheckRegionsReplicated(rawStartKey, rawEndKey)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, state)
}

// @Tags        store
// @Summary     Get a store's information.
// @Param       id path integer true "Store Id"
// @Produce     json
// @Success     200 {object} response.StoreInfo
// @Failure     400 {string} string "The input is invalid."
// @Failure     404 {string} string "The store does not exist."
// @Failure     500 {string} string "PD server failed to proceed the request."
// @Router      /stores/{id} [get]
func getStoreByID(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	idStr := c.Param("id")
	storeID, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	store := svr.GetBasicCluster().GetStore(storeID)
	if store == nil {
		c.String(http.StatusNotFound, errs.ErrStoreNotFound.FastGenByArgs(storeID).Error())
		return
	}

	storeInfo := response.BuildStoreInfo(&svr.GetConfig().Schedule, store)
	c.IndentedJSON(http.StatusOK, storeInfo)
}

// @Tags        store
// @Summary     Get all stores in the cluster.
// @Produce     json
// @Success     200 {object} response.StoresInfo
// @Failure     500 {string} string "PD server failed to proceed the request."
// @Router      /stores [get]
func getAllStores(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	stores := svr.GetBasicCluster().GetMetaStores()
	StoresInfo := &response.StoresInfo{
		Stores: make([]*response.StoreInfo, 0, len(stores)),
	}

	for _, s := range stores {
		storeID := s.GetId()
		store := svr.GetBasicCluster().GetStore(storeID)
		if store == nil {
			c.String(http.StatusInternalServerError, errs.ErrStoreNotFound.FastGenByArgs(storeID).Error())
			return
		}
		if store.GetMeta().State == metapb.StoreState_Tombstone {
			continue
		}
		storeInfo := response.BuildStoreInfo(&svr.GetConfig().Schedule, store)
		StoresInfo.Stores = append(StoresInfo.Stores, storeInfo)
	}
	StoresInfo.Count = len(StoresInfo.Stores)
	c.IndentedJSON(http.StatusOK, StoresInfo)
}

// @Tags     region
// @Summary  List all regions in the cluster.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Router   /regions [get]
func getAllRegions(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	regions := svr.GetBasicCluster().GetRegions()
	b, err := response.MarshalRegionsInfoJSON(c.Request.Context(), regions)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.Data(http.StatusOK, "application/json", b)
}

// @Tags     region
// @Summary  Get count of regions.
// @Produce  json
// @Success  200  {object}  response.RegionsInfo
// @Router   /regions/count [get]
func getRegionCount(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	count := svr.GetBasicCluster().GetTotalRegionCount()
	c.IndentedJSON(http.StatusOK, &response.RegionsInfo{Count: count})
}

// @Tags     region
// @Summary  Search for a region by region ID.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {object}  response.RegionInfo
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/{id} [get]
func getRegionByID(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	idStr := c.Param("id")
	regionID, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	regionInfo := svr.GetBasicCluster().GetRegion(regionID)
	if regionInfo == nil {
		c.String(http.StatusNotFound, errs.ErrRegionNotFound.FastGenByArgs(regionID).Error())
		return
	}
	b, err := response.MarshalRegionInfoJSON(c.Request.Context(), regionInfo)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.Data(http.StatusOK, "application/json", b)
}

// TransferPrimary transfers the primary member to `new_primary`.
// @Tags     primary
// @Summary  Transfer the primary member to `new_primary`.
// @Produce  json
// @Param    new_primary body   string  false "new primary name"
// @Success  200  string  string
// @Router   /primary/transfer [post]
func transferPrimary(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	var input map[string]string
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	newPrimary := ""
	if v, ok := input["new_primary"]; ok {
		newPrimary = v
	}

	if err := mcsutils.TransferPrimary(svr.GetClient(), svr.GetParticipant().GetExpectedPrimaryLease(),
		constant.SchedulingServiceName, svr.Name(), newPrimary, 0, nil); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, "success")
}
