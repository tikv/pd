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
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	tsoserver "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// APIPathPrefix is the prefix of the API path.
	APIPathPrefix = "/tso/api/v1"
)

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
		c.Set(multiservicesapi.ServiceContextKey, srv)
		c.Next()
	})
	apiHandlerEngine.Use(multiservicesapi.ServiceRedirector())
	apiHandlerEngine.GET("metrics", utils.PromHandler())
	root := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		srv:              srv,
		apiHandlerEngine: apiHandlerEngine,
		root:             root,
		rd:               createIndentRender(),
	}
	s.RegisterAdminRouter()
	s.RegisterKeyspaceGroupRouter()
	return s
}

// RegisterAdminRouter registers the router of the TSO admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	tsoAdminHandler := tso.NewAdminHandler(s.srv.GetHandler(), s.rd)
	router.POST("/reset-ts", gin.WrapF(tsoAdminHandler.ResetTS))
}

// RegisterKeyspaceGroupRouter registers the router of the TSO keyspace group handler.
func (s *Service) RegisterKeyspaceGroupRouter() {
	router := s.root.Group("keyspace-groups")
	router.GET("/members", GetKeyspaceGroupMembers)
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
