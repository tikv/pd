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
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	scheserver "github.com/tikv/pd/pkg/mcs/scheduling/server"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/logutil"
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
	*scheserver.Server
}

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
		c.Set(handlerKey, handler.NewHandler(&server{srv.Server}))
		c.Next()
	})
	apiHandlerEngine.Use(multiservicesapi.ServiceRedirector())
	apiHandlerEngine.GET("metrics", mcsutils.PromHandler())
	pprof.Register(apiHandlerEngine)
	root := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		srv:              srv,
		apiHandlerEngine: apiHandlerEngine,
		root:             root,
		rd:               createIndentRender(),
	}
	s.RegisterAdminRouter()
	s.RegisterOperatorsRouter()
	s.RegisterSchedulersRouter()
	s.RegisterCheckersRouter()
	s.RegisterHotspotRouter()
	s.RegisterConfigRouter()
	return s
}

// RegisterAdminRouter registers the router of the admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	router.PUT("/log", changeLogLevel)
}

// RegisterSchedulersRouter registers the router of the schedulers handler.
func (s *Service) RegisterSchedulersRouter() {
	router := s.root.Group("schedulers")
	router.GET("", getSchedulers)
	router.GET("/diagnostic/:name", getDiagnosticResult)
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
	router.GET("/:id", getOperatorByRegion)
	router.DELETE("/:id", deleteOperatorByRegion)
	router.GET("/records", getOperatorRecords)
}

// RegisterConfigRouter registers the router of the config handler.
func (s *Service) RegisterConfigRouter() {
	router := s.root.Group("config")

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
	groups.GET("/:id", getGroupConfig)

	placementRule := router.Group("placement-rule")
	placementRule.GET("", getPlacementRules)
	placementRule.GET("/:group", getPlacementRuleByGroup)
}

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
			c.String(http.StatusBadRequest, fmt.Sprintf("invalid store id: %s", storeID))
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
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
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
func getGroupConfig(c *gin.Context) {
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
