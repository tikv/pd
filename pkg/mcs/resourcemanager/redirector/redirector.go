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

package redirector

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/urfave/negroni/v3"
	"go.uber.org/zap"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/server"
)

// NewHandler creates a new redirector handler for resource manager.
func NewHandler(_ context.Context, svr *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
	// Write requests must stay on pd-server.
	// To keep read requests served by the deployed resource-manager microservice,
	// we only forward GET requests to that microservice.
	pdWriteHandler := newPDWriteHandler(svr)
	return negroni.New(
			serverapi.NewRedirector(svr,
				// Exception: primary transfer is a microservice control-plane operation.
				// Keep it forwarded to the resource-manager service.
				serverapi.MicroserviceRedirectRule(
					apis.APIPathPrefix+"primary/transfer",
					strings.TrimRight(apis.APIPathPrefix, "/")+"/primary/transfer",
					constant.ResourceManagerServiceName,
					[]string{http.MethodPost}),
				serverapi.MicroserviceRedirectRule(
					apis.APIPathPrefix,
					strings.TrimRight(apis.APIPathPrefix, "/"),
					constant.ResourceManagerServiceName,
					[]string{http.MethodGet}),
			),
			negroni.Wrap(pdWriteHandler),
		), apiutil.APIServiceGroup{
			Name:       "resource-manager",
			Version:    "v1",
			IsCore:     false,
			PathPrefix: apis.APIPathPrefix,
		}, nil
}

type pdWriteService struct {
	svr *server.Server
}

func newPDWriteHandler(svr *server.Server) http.Handler {
	// gin is used here to keep the same request/response semantics as the
	// resource-manager microservice handlers, while implementing writes in pd-server.
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	s := &pdWriteService{
		svr: svr,
	}

	// Only register write endpoints; reads are forwarded by the redirector.
	root := r.Group(apis.APIPathPrefix)
	config := root.Group("/config")
	config.POST("/group", s.postResourceGroup)
	config.PUT("/group", s.putResourceGroup)
	config.DELETE("/group/:name", s.deleteResourceGroup)
	config.POST("/controller", s.setControllerConfig)
	config.POST("/keyspace/service-limit", s.setKeyspaceServiceLimit)
	config.POST("/keyspace/service-limit/:keyspace_name", s.setKeyspaceServiceLimit)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.ServeHTTP(w, req)
	})
}

func (s *pdWriteService) postResourceGroup(c *gin.Context) {
	st := s.svr.GetStorage()
	if st == nil {
		c.String(http.StatusInternalServerError, "storage is nil")
		return
	}
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := rmserver.ValidateResourceGroupForWrite(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(group.GetKeyspaceId())
	if err := rmserver.EnsureDefaultResourceGroupExists(st, keyspaceID); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if err := rmserver.PersistResourceGroupSettingsAndStates(st, keyspaceID, &group); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

func (s *pdWriteService) putResourceGroup(c *gin.Context) {
	st := s.svr.GetStorage()
	if st == nil {
		c.String(http.StatusInternalServerError, "storage is nil")
		return
	}
	var patch rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&patch); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := rmserver.ValidateResourceGroupForWrite(&patch); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	keyspaceID := rmserver.ExtractKeyspaceID(patch.GetKeyspaceId())

	// Load the current settings and apply patch semantics, then persist back.
	key := keypath.KeyspaceResourceGroupSettingPath(keyspaceID, patch.GetName())
	raw, err := st.Load(key)
	if err != nil || raw == "" {
		if err == nil {
			err = errs.ErrResourceGroupNotExists.FastGenByArgs(patch.GetName())
		}
		// Keep consistent with microservice handler behavior (it returns 500 for modify failures).
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	curPB := &rmpb.ResourceGroup{}
	if err := proto.Unmarshal([]byte(raw), curPB); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	cur := rmserver.FromProtoResourceGroup(curPB)
	if err := cur.PatchSettings(&patch); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	updated := cur.IntoProtoResourceGroup(keyspaceID)
	if err := st.SaveResourceGroupSetting(keyspaceID, updated.GetName(), updated); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

func (s *pdWriteService) deleteResourceGroup(c *gin.Context) {
	st := s.svr.GetStorage()
	if st == nil {
		c.String(http.StatusInternalServerError, "storage is nil")
		return
	}
	name := c.Param("name")
	if name == "" {
		c.String(http.StatusBadRequest, errs.ErrInvalidGroup.Error())
		return
	}
	if name == rmserver.DefaultResourceGroupName {
		c.String(http.StatusBadRequest, errs.ErrDeleteReservedGroup.Error())
		return
	}
	// Keep compatibility: keyspace is encoded in query for delete in some clients.
	keyspaceName := c.Query("keyspace_name")
	keyspaceID := uint32(0)
	if keyspaceName != "" {
		meta, err := s.svr.GetKeyspaceManager().LoadKeyspace(keyspaceName)
		if err != nil {
			c.String(http.StatusNotFound, err.Error())
			return
		}
		keyspaceID = meta.GetId()
	}
	key := keypath.KeyspaceResourceGroupSettingPath(keyspaceID, name)
	raw, err := st.Load(key)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if raw == "" {
		c.String(http.StatusNotFound, errs.ErrResourceGroupNotExists.FastGenByArgs(name).Error())
		return
	}
	if err := st.DeleteResourceGroupSetting(keyspaceID, name); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

func (s *pdWriteService) setControllerConfig(c *gin.Context) {
	st := s.svr.GetStorage()
	if st == nil {
		c.String(http.StatusInternalServerError, "storage is nil")
		return
	}
	conf := make(map[string]any)
	if err := c.ShouldBindJSON(&conf); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	// Load current config from storage (fallback to PD default if not found).
	var cfg rmserver.ControllerConfig
	if raw, err := st.LoadControllerConfig(); err == nil && raw != "" {
		if uerr := json.Unmarshal([]byte(raw), &cfg); uerr != nil {
			log.Warn("failed to unmarshal controller config from storage, fallback to default",
				zap.Error(uerr), zap.String("raw", raw))
			cfg = *s.svr.GetControllerConfig()
		}
	} else {
		cfg = *s.svr.GetControllerConfig()
	}

	for k, v := range conf {
		key := reflectutil.FindJSONFullTagByChildTag(reflect.TypeOf(rmserver.ControllerConfig{}), k)
		if key == "" {
			c.String(http.StatusBadRequest, "config item "+k+" not found")
			return
		}
		kp := strings.Split(key, ".")
		if len(kp) == 0 {
			c.String(http.StatusBadRequest, "invalid key "+key)
			return
		}
		var target any
		switch kp[0] {
		case "request-unit":
			target = &cfg.RequestUnit
		default:
			target = &cfg
		}
		updated, found, err := jsonutil.AddKeyValue(target, kp[len(kp)-1], v)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		if !found {
			c.String(http.StatusBadRequest, "config item "+key+" not found")
			return
		}
		if updated {
			if err := st.SaveControllerConfig(&cfg); err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
		}
	}
	c.String(http.StatusOK, "Success!")
}

func (s *pdWriteService) setKeyspaceServiceLimit(c *gin.Context) {
	st := s.svr.GetStorage()
	if st == nil {
		c.String(http.StatusInternalServerError, "storage is nil")
		return
	}
	keyspaceName := c.Param("keyspace_name")
	// Without keyspace name, it will get/set the service limit of the null keyspace.
	keyspaceID := uint32(0)
	if keyspaceName != "" {
		meta, err := s.svr.GetKeyspaceManager().LoadKeyspace(keyspaceName)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		keyspaceID = meta.GetId()
	}
	var req apis.KeyspaceServiceLimitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if req.ServiceLimit < 0 {
		c.String(http.StatusBadRequest, "service_limit must be non-negative")
		return
	}
	if err := st.SaveServiceLimit(keyspaceID, req.ServiceLimit); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}
