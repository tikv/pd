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
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	tsoserver "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// APIPathPrefix is the prefix of the API path.
	APIPathPrefix = "/tso/api/v1"
)

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
		return s.apiHandlerEngine, apiServiceGroup
	}
}

// Service is the tso service.
type Service struct {
	apiHandlerEngine *gin.Engine
	root             *gin.RouterGroup

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
	apiHandlerEngine.Use(func(c *gin.Context) {
		c.Set(multiservicesapi.ServiceContextKey, srv)
		c.Next()
	})
	apiHandlerEngine.GET("metrics", utils.PromHandler())
	apiHandlerEngine.GET("status", utils.StatusHandler)
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
	s.RegisterKeyspaceGroupRouter()
	s.RegisterHealthRouter()
	s.RegisterConfigRouter()
	s.RegisterPrimaryRouter()
	return s
}

// RegisterAdminRouter registers the router of the TSO admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	router.POST("/reset-ts", ResetTS)
	router.PUT("/log", changeLogLevel)
}

// RegisterKeyspaceGroupRouter registers the router of the TSO keyspace group handler.
func (s *Service) RegisterKeyspaceGroupRouter() {
	router := s.root.Group("keyspace-groups")
	router.GET("/members", GetKeyspaceGroupMembers)
}

// RegisterHealthRouter registers the router of the health handler.
func (s *Service) RegisterHealthRouter() {
	router := s.root.Group("health")
	router.GET("", GetHealth)
}

// RegisterConfigRouter registers the router of the config handler.
func (s *Service) RegisterConfigRouter() {
	router := s.root.Group("config")
	router.GET("", getConfig)
}

// RegisterPrimaryRouter registers the router of the primary handler.
func (s *Service) RegisterPrimaryRouter() {
	router := s.root.Group("primary")
	router.POST("transfer", transferPrimary)
}

func changeLogLevel(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
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

// ResetTSParams is the input json body params of ResetTS
type ResetTSParams struct {
	TSO           string `json:"tso"`
	ForceUseLarge bool   `json:"force-use-larger"`
}

// ResetTS is the http.HandlerFunc of ResetTS
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
// @Failure  503  {string}  string  "It's a temporary failure, please retry."
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
func ResetTS(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	var param ResetTSParams
	if err := c.ShouldBindJSON(&param); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if len(param.TSO) == 0 {
		c.String(http.StatusBadRequest, "invalid tso value")
		return
	}
	ts, err := strconv.ParseUint(param.TSO, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, "invalid tso value")
		return
	}

	var ignoreSmaller, skipUpperBoundCheck bool
	if param.ForceUseLarge {
		ignoreSmaller, skipUpperBoundCheck = true, true
	}

	if err = svr.ResetTS(ts, ignoreSmaller, skipUpperBoundCheck, 0); err != nil {
		if err == errs.ErrServerNotStarted {
			c.String(http.StatusInternalServerError, err.Error())
		} else if err == errs.ErrEtcdTxnConflict {
			// If the error is ErrEtcdTxnConflict, it means there is a temporary failure.
			// Return 503 to let the client retry.
			// Ref: https://datatracker.ietf.org/doc/html/rfc7231#section-6.6.4
			c.String(http.StatusServiceUnavailable,
				fmt.Sprintf("It's a temporary failure with error %s, please retry.", err.Error()))
		} else {
			c.String(http.StatusForbidden, err.Error())
		}
		return
	}
	c.String(http.StatusOK, "Reset ts successfully.")
}

// GetHealth returns the health status of the TSO service.
func GetHealth(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	am, err := svr.GetKeyspaceGroupManager().GetAllocatorManager(constant.DefaultKeyspaceGroupID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if am.GetMember().IsLeaderElected() {
		c.IndentedJSON(http.StatusOK, "ok")
		return
	}

	c.String(http.StatusInternalServerError, "no leader elected")
}

// KeyspaceGroupMember contains the keyspace group and its member information.
type KeyspaceGroupMember struct {
	Group     *endpoint.KeyspaceGroup
	Member    *tsopb.Participant
	IsPrimary bool   `json:"is_primary"`
	PrimaryID uint64 `json:"primary_id"`
}

// GetKeyspaceGroupMembers gets the keyspace group members that the TSO service is serving.
func GetKeyspaceGroupMembers(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	kgm := svr.GetKeyspaceGroupManager()
	keyspaceGroups := kgm.GetKeyspaceGroups()
	members := make(map[uint32]*KeyspaceGroupMember, len(keyspaceGroups))
	for id, group := range keyspaceGroups {
		am, err := kgm.GetAllocatorManager(id)
		if err != nil {
			log.Error("failed to get allocator manager",
				zap.Uint32("keyspace-group-id", id), zap.Error(err))
			continue
		}
		member := am.GetMember()
		members[id] = &KeyspaceGroupMember{
			Group:     group,
			Member:    member.GetMember().(*tsopb.Participant),
			IsPrimary: member.IsLeader(),
			PrimaryID: member.GetLeaderID(),
		}
	}
	c.IndentedJSON(http.StatusOK, members)
}

// @Tags     config
// @Summary  Get full config.
// @Produce  json
// @Success  200  {object}  config.Config
// @Router   /config [get]
func getConfig(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	c.IndentedJSON(http.StatusOK, svr.GetConfig())
}

// TransferPrimary transfers the primary member to `new_primary`.
// @Tags     primary
// @Summary  Transfer the primary member to `new_primary`.
// @Produce  json
// @Param    new_primary body   string  false "new primary name"
// @Success  200  string  string
// @Router   /primary/transfer [post]
func transferPrimary(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	var input map[string]string
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	// We only support default keyspace group now.
	newPrimary, keyspaceGroupID := "", constant.DefaultKeyspaceGroupID
	if v, ok := input["new_primary"]; ok {
		newPrimary = v
	}

	allocator, err := svr.GetTSOAllocatorManager(keyspaceGroupID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	globalAllocator, err := allocator.GetAllocator(tso.GlobalDCLocation)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	// only members of specific group are valid primary candidates.
	group := svr.GetKeyspaceGroupManager().GetKeyspaceGroups()[keyspaceGroupID]
	memberMap := make(map[string]bool, len(group.Members))
	for _, member := range group.Members {
		memberMap[member.Address] = true
	}

	if err := utils.TransferPrimary(svr.GetClient(), globalAllocator.(*tso.GlobalTSOAllocator).GetExpectedPrimaryLease(),
		constant.TSOServiceName, svr.Name(), newPrimary, keyspaceGroupID, memberMap); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, "success")
}
