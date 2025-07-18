// Copyright 2025 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redirector

import (
	"context"
	"net/http"

	"github.com/tikv/pd/pkg/mcs/resourcemanager/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/server"
	"github.com/urfave/negroni/v3"
)

// NewHandler creates a new redirector handler for resource manager.
func NewHandler(_ context.Context, svr *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
	return negroni.New(
			serverapi.NewRedirector(svr,
				serverapi.MicroserviceRedirectRule(
					"/resource-manager/",
					"/resource-manager",
					constant.ResourceManagerServiceName,
					[]string{http.MethodGet, http.MethodPost, http.MethodDelete}),
			),
		), apiutil.APIServiceGroup{
			Name:       "resource-manager",
			Version:    "v1",
			IsCore:     false,
			PathPrefix: apis.APIPathPrefix,
		}, nil
}
