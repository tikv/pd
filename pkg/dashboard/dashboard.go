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

// +build !without_dashboard

package dashboard

import (
	"context"
	"net/http"
	"time"

	"github.com/pingcap-incubator/tidb-dashboard/pkg/apiserver"

	"github.com/pingcap/pd/v4/pkg/dashboard/adapter"
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

// SetCheckInterval changes adapter.CheckInterval
func SetCheckInterval(d time.Duration) {
	adapter.CheckInterval = d
}

// GetServiceBuilders returns all ServiceBuilders required by Dashboard
func GetServiceBuilders() []server.HandlerBuilder {
	var s *apiserver.Service
	var redirector *adapter.Redirector

	return []server.HandlerBuilder{
		// Dashboard API Service
		func(ctx context.Context, srv *server.Server) (http.Handler, server.ServiceGroup, error) {
			redirector = adapter.NewRedirector(srv.Name())

			var err error
			if s, err = adapter.NewAPIService(srv, http.HandlerFunc(redirector.ReverseProxy)); err != nil {
				return nil, apiServiceGroup, err
			}

			m := adapter.NewManager(srv, s, redirector)
			srv.AddStartCallback(m.Start)
			srv.AddCloseCallback(m.Stop)

			return apiserver.Handler(s), apiServiceGroup, nil
		},
		// Dashboard UI
		func(context.Context, *server.Server) (http.Handler, server.ServiceGroup, error) {
			handler := s.NewStatusAwareHandler(
				http.StripPrefix(uiServiceGroup.PathPrefix, uiserver.Handler()),
				http.HandlerFunc(redirector.TemporaryRedirect),
			)
			return handler, uiServiceGroup, nil
		},
	}
}
