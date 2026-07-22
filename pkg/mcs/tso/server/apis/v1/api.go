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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	tsoserver "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/logutil"
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
	apiHandlerEngine.GET("health", getHealth)
	pprof.Register(apiHandlerEngine)
	root := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		srv:              srv,
		apiHandlerEngine: apiHandlerEngine,
		root:             root,
		rd:               createIndentRender(),
	}
	s.RegisterAdminRouter()
	s.RegisterKeyspaceGroupRouter()
	// Deprecated, kept for compatibility.
	// Use /health instead.
	s.RegisterHealthRouter()
	s.RegisterConfigRouter()
	s.RegisterPrimaryRouter()
	return s
}

// RegisterAdminRouter registers the router of the TSO admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	// reset-ts needs to be forwarded to the primary.
	router.POST("/reset-ts", multiservicesapi.ServiceRedirector(), resetTS)
	router.PUT("/log", changeLogLevel)
}

// RegisterKeyspaceGroupRouter registers the router of the TSO keyspace group handler.
func (s *Service) RegisterKeyspaceGroupRouter() {
	router := s.root.Group("keyspace-groups")
	router.GET("/members", getKeyspaceGroupMembers)
}

// RegisterHealthRouter registers the router of the health handler.
func (s *Service) RegisterHealthRouter() {
	router := s.root.Group("health")
	router.GET("", getHealth)
}

// RegisterConfigRouter registers the router of the config handler.
func (s *Service) RegisterConfigRouter() {
	router := s.root.Group("config")
	router.GET("", getConfig)
}

// RegisterPrimaryRouter registers the router of the primary handler.
func (s *Service) RegisterPrimaryRouter() {
	router := s.root.Group("primary")
	// We do not use the ServiceRedirector middleware here because it only knows
	// group 0's primary, which is wrong for non-default keyspace groups. Instead,
	// transferPrimary forwards the request to the primary of the target keyspace
	// group itself (see forwardToGroupPrimary).
	router.POST("transfer", transferPrimary)
	// evictPrimary transfers away every keyspace group primary currently held by
	// this node. It only acts on groups this node is serving as primary, so there
	// is nothing to forward.
	router.POST("evict", evictPrimary)
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
func resetTS(c *gin.Context) {
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
		switch err {
		case errs.ErrServerNotStarted:
			c.String(http.StatusInternalServerError, err.Error())
		case errs.ErrEtcdTxnConflict:
			// If the error is ErrEtcdTxnConflict, it means there is a temporary failure.
			// Return 503 to let the client retry.
			// Ref: https://datatracker.ietf.org/doc/html/rfc7231#section-6.6.4
			c.String(http.StatusServiceUnavailable,
				fmt.Sprintf("It's a temporary failure with error %s, please retry.", err.Error()))
		default:
			c.String(http.StatusForbidden, err.Error())
		}
		return
	}
	c.String(http.StatusOK, "Reset ts successfully.")
}

// getHealth returns the health status of the TSO service.
func getHealth(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	if svr.IsClosed() {
		c.String(http.StatusServiceUnavailable, errs.ErrServerNotStarted.GenWithStackByArgs().Error())
		return
	}
	allocator, err := svr.GetKeyspaceGroupManager().GetAllocator(constant.DefaultKeyspaceGroupID)
	if err != nil {
		// The allocator is absent on this node, e.g. it has watched the keyspace group meta
		// but does not serve the allocator for the default keyspace group.
		if errs.ErrGetAllocator.Equal(err) {
			c.String(http.StatusNotFound, "allocator not found")
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if allocator.GetMember().(*member.Participant).IsPrimaryElected() {
		c.String(http.StatusOK, "ok")
		return
	}

	c.String(http.StatusInternalServerError, "no primary elected")
}

// KeyspaceGroupMember contains the keyspace group and its member information.
type KeyspaceGroupMember struct {
	Group     *endpoint.KeyspaceGroup
	Member    *tsopb.Participant
	IsPrimary bool   `json:"is_primary"`
	PrimaryID uint64 `json:"primary_id"`
}

// getKeyspaceGroupMembers gets the keyspace group members that the TSO service is serving.
func getKeyspaceGroupMembers(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	kgm := svr.GetKeyspaceGroupManager()
	keyspaceGroups := kgm.GetServingKeyspaceGroups()
	members := make(map[uint32]*KeyspaceGroupMember, len(keyspaceGroups))
	for id, group := range keyspaceGroups {
		allocator, err := kgm.GetAllocator(id)
		if err != nil {
			log.Error("failed to get tso allocator",
				zap.Uint32("keyspace-group-id", id), zap.Error(err))
			continue
		}
		m := allocator.GetMember().(*member.Participant)
		members[id] = &KeyspaceGroupMember{
			Group:     group,
			Member:    m.GetMember().(*tsopb.Participant),
			IsPrimary: m.IsServing(),
			PrimaryID: m.GetPrimaryID(),
		}
	}
	c.IndentedJSON(http.StatusOK, members)
}

// @Tags     config
// @Summary  Get full config.
// @Produce  json
// @Success  200  {object}  tsoserver.Config
// @Router   /config [get]
func getConfig(c *gin.Context) {
	var config *tsoserver.Config
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	config = svr.GetConfig()
	c.IndentedJSON(http.StatusOK, config)
}

// transferPrimary transfers the primary member to `new_primary`.
// @Tags     primary
// @Summary  Transfer the primary member to `new_primary`.
// @Produce  json
// @Param    new_primary        body  string  false  "new primary name"
// @Param    keyspace_group_id  body  integer false  "keyspace group ID (default: 0)"
// @Success  200  string  string
// @Router   /primary/transfer [post]
func transferPrimary(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	// Read the raw body first so that it can be replayed when the request needs
	// to be forwarded to the primary of the target keyspace group.
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	// Reject an empty body explicitly. Since this endpoint has side effects, we
	// must not silently fall back to a random transfer on the default keyspace
	// group when no body is provided.
	if len(body) == 0 {
		c.String(http.StatusBadRequest, "request body is required")
		return
	}
	var input struct {
		NewPrimary      string  `json:"new_primary"`
		KeyspaceGroupID *uint32 `json:"keyspace_group_id"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	keyspaceGroupID := constant.DefaultKeyspaceGroupID
	if input.KeyspaceGroupID != nil {
		keyspaceGroupID = *input.KeyspaceGroupID
	}

	allocator, err := svr.GetTSOAllocator(keyspaceGroupID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest,
			fmt.Sprintf("keyspace group %d not found on this tso node", keyspaceGroupID))
		return
	}

	group := svr.GetKeyspaceGroupManager().GetKeyspaceGroupByID(keyspaceGroupID)
	if group == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest,
			fmt.Sprintf("keyspace group %d not found on this tso node", keyspaceGroupID))
		return
	}

	// The transfer must be executed on the current primary of the target keyspace
	// group. We cannot rely on ServiceRedirector here because it only knows the
	// default group (0) primary. If this node is not the primary of the group,
	// forward the request to the group's actual primary. This keeps the original
	// redirect-on-follower behavior for the default group and extends it to
	// non-default groups.
	if !allocator.GetMember().IsServing() {
		forwardToGroupPrimary(c, svr, keyspaceGroupID, allocator.GetPrimaryAddr(), body)
		return
	}
	participant, ok := allocator.GetMember().(*member.Participant)
	if !ok {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "the tso member is not a participant")
		return
	}

	// only members of specific group are valid primary candidates.
	memberMap := make(map[string]bool, len(group.Members))
	for _, member := range group.Members {
		memberMap[member.Address] = true
	}

	if err := utils.TransferPrimary(svr.GetClient(), participant,
		mcs.TSOServiceName, svr.Name(), input.NewPrimary, keyspaceGroupID, memberMap); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, "success")
}

// evictPrimary transfers away every keyspace group primary currently held by this
// node, moving each to another member of the same group. It is a best-effort,
// node-local operation: groups that this node is not serving as primary are
// skipped, and a failure on one group does not stop the others.
//
// The request is rejected as a whole if this node is the primary of any
// splitting keyspace group: a split target must campaign on the same TSO node as
// its split source, but eviction transfers each group to an independently chosen
// member, which could break that invariant and leave the target keyspaces
// without a primary. A single TransferPrimary is not atomic and the groups are
// iterated in no particular order, so the split state is checked for every
// candidate group up front before transferring anything; otherwise a normal
// group could be moved before a later splitting group aborts the loop, leaving
// partial side effects. Since splitting is transient, the caller should retry
// once it finishes.
// @Tags     primary
// @Summary  Evict all keyspace group primaries held by this node.
// @Produce  json
// @Success  200  {object}  map[string]string
// @Failure  500  {object}  map[string]string
// @Router   /primary/evict [post]
func evictPrimary(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*tsoserver.Service)
	kgm := svr.GetKeyspaceGroupManager()

	// Collect the keyspace groups this node is currently the primary of. There is
	// nothing to transfer away for the others.
	primaryGroupIDs := make([]uint32, 0, len(kgm.GetServingKeyspaceGroups()))
	for keyspaceGroupID := range kgm.GetServingKeyspaceGroups() {
		allocator, err := svr.GetTSOAllocator(keyspaceGroupID)
		if err != nil || !allocator.GetMember().IsServing() {
			continue
		}
		primaryGroupIDs = append(primaryGroupIDs, keyspaceGroupID)
	}

	// Pre-check every candidate group before transferring anything so the
	// operation is all-or-nothing: reject the whole request if any of them is
	// splitting, instead of moving some groups first and only then aborting.
	for _, keyspaceGroupID := range primaryGroupIDs {
		if group := kgm.GetKeyspaceGroupByID(keyspaceGroupID); group != nil && group.IsSplitting() {
			c.AbortWithStatusJSON(http.StatusInternalServerError,
				errs.ErrKeyspaceGroupInSplit.FastGenByArgs(keyspaceGroupID).Error())
			return
		}
	}

	// results maps a keyspace group ID to the transfer outcome ("success" or the
	// error message), so the caller can see the per-group result.
	results := make(map[uint32]string)
	failed := false
	for _, keyspaceGroupID := range primaryGroupIDs {
		allocator, err := svr.GetTSOAllocator(keyspaceGroupID)
		if err != nil {
			continue
		}
		// Re-check the split state against the latest meta right before the
		// transfer: a split can still start after the pre-check above. This
		// narrows, but cannot fully close, the window.
		group := kgm.GetKeyspaceGroupByID(keyspaceGroupID)
		if group == nil {
			continue
		}
		if group.IsSplitting() {
			c.AbortWithStatusJSON(http.StatusInternalServerError,
				errs.ErrKeyspaceGroupInSplit.FastGenByArgs(keyspaceGroupID).Error())
			return
		}

		// only members of the specific group are valid primary candidates.
		memberMap := make(map[string]bool, len(group.Members))
		for _, member := range group.Members {
			memberMap[member.Address] = true
		}

		participant, ok := allocator.GetMember().(*member.Participant)
		if !ok {
			continue
		}
		// TODO: consider priority here. This is a one-shot transfer; if this node
		// has a higher priority for the group, the priority checker will move the
		// primary back to it, so the eviction does not durably drain the node.
		// Priority handling is being reworked, so revisit this when needed.
		// An empty new primary lets TransferPrimary pick a random other member.
		if err := utils.TransferPrimary(svr.GetClient(), participant,
			mcs.TSOServiceName, svr.Name(), "", keyspaceGroupID, memberMap); err != nil {
			log.Warn("failed to evict keyspace group primary",
				zap.Uint32("keyspace-group-id", keyspaceGroupID), errs.ZapError(err))
			results[keyspaceGroupID] = err.Error()
			failed = true
			continue
		}
		results[keyspaceGroupID] = "success"
	}

	if failed {
		c.IndentedJSON(http.StatusInternalServerError, results)
		return
	}
	c.IndentedJSON(http.StatusOK, results)
}

// forwardToGroupPrimary forwards the transfer primary request to the primary of
// the given keyspace group, replaying the original request body. The request is
// fully handled (forwarded or aborted with an error) when this returns.
func forwardToGroupPrimary(c *gin.Context, svr *tsoserver.Service, keyspaceGroupID uint32, primaryAddr string, body []byte) {
	// Prevent more than one redirection.
	if name := c.Request.Header.Get(multiservicesapi.ServiceRedirectorHeader); len(name) != 0 {
		log.Error("redirect but server is not the primary of the keyspace group",
			zap.String("from", name), zap.String("server", svr.Name()),
			zap.Uint32("keyspace-group-id", keyspaceGroupID), errs.ZapError(errs.ErrRedirectToNotPrimary))
		c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrRedirectToNotPrimary.FastGenByArgs().Error())
		return
	}
	if primaryAddr == "" {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable,
			fmt.Sprintf("the primary of keyspace group %d is not elected yet", keyspaceGroupID))
		return
	}
	u, err := url.Parse(primaryAddr)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrURLParse.Wrap(err).GenWithStackByCause().Error())
		return
	}
	c.Request.Header.Set(multiservicesapi.ServiceRedirectorHeader, svr.Name())
	// Replay the original body for the forwarded request.
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	apiutil.NewCustomReverseProxies(svr.GetHTTPClient(), []url.URL{*u}).ServeHTTP(c.Writer, c.Request)
	c.Abort()
}
