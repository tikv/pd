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
	redirector.UseHandler(newPDMetadataFallbackHandler(localHandler))
	return redirector, apiutil.APIServiceGroup{
		Name:       "resource-manager",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: apis.APIPathPrefix,
	}, nil
}

func shouldHandlePDMetadataLocally(r *http.Request) bool {
	// Keep metadata APIs redirected to RM until PD<->RM metadata sync is in place.
	_ = r
	return false
}

func newPDMetadataFallbackHandler(localHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Prevent bypassing redirect rules via an injected "forbidden forward" header.
		if r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) == "true" {
			http.NotFound(w, r)
			return
		}
		if !shouldHandlePDMetadataLocally(r) {
			http.NotFound(w, r)
			return
		}
		localHandler.ServeHTTP(w, r)
	})
}
