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
	"net/http"
	"strings"

	"github.com/urfave/negroni/v3"

	rm_server "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/server"
)

type pdMetadataManagerProvider struct {
	*server.Server
}

// GetResourceGroupWriteRole returns the write role used by the local PD metadata manager.
func (*pdMetadataManagerProvider) GetResourceGroupWriteRole() rm_server.ResourceGroupWriteRole {
	return rm_server.ResourceGroupWriteRolePDMetaOnly
}

// NewHandler creates a new redirector handler for resource manager.
func NewHandler(_ context.Context, svr *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
	manager := rm_server.NewManager[*pdMetadataManagerProvider](&pdMetadataManagerProvider{Server: svr})
	localHandler := newPDMetadataHandler(manager)
	redirector := negroni.New(
		serverapi.NewRedirector(svr,
			serverapi.MicroserviceRedirectRule(
				"/resource-manager/",
				"/resource-manager",
				constant.ResourceManagerServiceName,
				[]string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
				func(r *http.Request) bool {
					return !shouldHandlePDMetadataLocally(r)
				}),
		),
	)
	redirector.UseHandler(localHandler)
	return redirector, apiutil.APIServiceGroup{
		Name:       "resource-manager",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: apis.APIPathPrefix,
	}, nil
}

func shouldHandlePDMetadataLocally(r *http.Request) bool {
	path := strings.TrimRight(r.URL.Path, "/")
	if len(path) == 0 {
		path = "/"
	}
	prefix := strings.TrimRight(apis.APIPathPrefix, "/")
	switch r.Method {
	case http.MethodPost:
		return path == prefix+"/config/group" ||
			path == prefix+"/config/controller" ||
			path == prefix+"/config/keyspace/service-limit" ||
			strings.HasPrefix(path, prefix+"/config/keyspace/service-limit/")
	case http.MethodPut:
		return path == prefix+"/config/group"
	case http.MethodDelete:
		return strings.HasPrefix(path, prefix+"/config/group/")
	case http.MethodGet:
		return path == prefix+"/config/groups" ||
			strings.HasPrefix(path, prefix+"/config/group/") ||
			path == prefix+"/config/controller" ||
			path == prefix+"/config/keyspace/service-limit" ||
			strings.HasPrefix(path, prefix+"/config/keyspace/service-limit/")
	default:
		return false
	}
}
