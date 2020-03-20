// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dashboard

import (
	"context"
	"net/http"

	"github.com/pingcap-incubator/tidb-dashboard/pkg/apiserver"

	"github.com/pingcap/pd/v4/pkg/dashboard/uiserver"
	"github.com/pingcap/pd/v4/server"
)

var (
	apiServiceGroup = server.ServiceGroup{
		Name:       "dashboard-api",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: "/dashboard/api/",
	}

	uiServiceGroup = server.ServiceGroup{
		Name:       "dashboard-ui",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: "/dashboard/",
	}
)

// GetServiceBuilders returns all ServiceBuilders required by Dashboard
func GetServiceBuilders() []server.HandlerBuilder {
	var s *apiserver.Service
	redirector := NewRedirector()

	return []server.HandlerBuilder{
		// Dashboard API Service
		func(ctx context.Context, srv *server.Server) (http.Handler, server.ServiceGroup, error) {
			var err error
			if s, err = newAPIService(srv, redirector); err != nil {
				return nil, apiServiceGroup, err
			}

			m := NewManager(srv, s, redirector)
			srv.AddStartCallback(m.start)
			srv.AddCloseCallback(m.stop)

			return apiserver.Handler(s), apiServiceGroup, nil
		},
		// Dashboard UI
		func(context.Context, *server.Server) (http.Handler, server.ServiceGroup, error) {
			handler := s.NewStatusAwareHandler(http.StripPrefix(uiServiceGroup.PathPrefix, uiserver.Handler()))
			return handler, uiServiceGroup, nil
		},
	}
}
