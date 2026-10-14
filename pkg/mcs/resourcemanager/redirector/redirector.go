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
	"sync"

	"github.com/urfave/negroni/v3"

	rm_server "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/server"
)

type rmMetadataManagerProvider struct {
	*server.Server
}

// GetResourceGroupWriteRole returns the write role used by the local RM metadata fallback manager.
func (*rmMetadataManagerProvider) GetResourceGroupWriteRole() rm_server.ResourceGroupWriteRole {
	return rm_server.ResourceGroupWriteRolePDMetaOnly
}

// NewHandler creates a new redirector handler for resource manager.
func NewHandler(_ context.Context, svr *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
	redirector := negroni.New(
		serverapi.NewRedirector(svr,
			serverapi.MicroserviceRedirectRule(
				"/resource-manager/",
				"/resource-manager",
				constant.ResourceManagerServiceName,
				[]string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
				func(r *http.Request) bool {
					return !shouldHandleRMMetadataLocally(r)
				}),
		),
	)
	// Build the local RM metadata handler lazily so standalone mode does not
	// register RM manager lifecycle callbacks during PD startup.
	redirector.UseHandler(newRMMetadataFallbackHandler(
		shouldHandleRMMetadataLocally,
		func() (http.Handler, error) {
			return newRMMetadataHandler(&rmMetadataManagerProvider{Server: svr})
		},
	))
	return redirector, apiutil.APIServiceGroup{
		Name:       "resource-manager",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: apis.APIPathPrefix,
	}, nil
}

// TODO: Implement a real local handling policy for RM metadata fallback.
func shouldHandleRMMetadataLocally(r *http.Request) bool {
	// Keep metadata APIs redirected to RM until PD<->RM metadata sync is in place.
	_ = r
	return false
}

func newRMMetadataFallbackHandler(
	shouldHandle func(*http.Request) bool,
	localHandlerFactory func() (http.Handler, error),
) http.Handler {
	var (
		initOnce     sync.Once
		initErr      error
		localHandler http.Handler
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Prevent bypassing RM metadata fallback guard via an injected "forbidden forward" header.
		if r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) == "true" {
			http.NotFound(w, r)
			return
		}
		if !shouldHandle(r) {
			http.NotFound(w, r)
			return
		}
		initOnce.Do(func() {
			localHandler, initErr = localHandlerFactory()
		})
		if initErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(initErr.Error()))
			return
		}
		localHandler.ServeHTTP(w, r)
	})
}
