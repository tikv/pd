// Copyright 2026 TiKV Project Authors.
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
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/mcs/resourcemanager/metadataapi"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	apis "github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
)

type rmMetadataHandler struct {
	configService *metadataapi.ConfigService
	engine        *gin.Engine
}

func newRMMetadataHandler(provider *rmMetadataManagerProvider) (http.Handler, error) {
	manager, err := rmserver.NewMetadataOnlyManager[*rmMetadataManagerProvider](provider)
	if err != nil {
		return nil, err
	}
	return newRMMetadataHandlerWithConfigStore(metadataapi.NewManagerStore(manager)), nil
}

func newRMMetadataHandlerWithConfigStore(configStore metadataapi.ConfigStore) http.Handler {
	handler := &rmMetadataHandler{
		configService: metadataapi.NewConfigService(configStore),
		engine:        gin.New(),
	}
	handler.engine.Use(gin.Recovery())
	handler.registerRouter()
	return handler
}

func (h *rmMetadataHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.engine.ServeHTTP(w, r)
}

func (h *rmMetadataHandler) registerRouter() {
	root := h.engine.Group(apis.APIPathPrefix)
	configEndpoint := root.Group("/config")
	h.configService.Register(configEndpoint)
}
