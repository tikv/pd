// Copyright 2020 TiKV Project Authors.
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

//go:build !without_dashboard
// +build !without_dashboard

package dashboard

import (
	"context"
	"net/http"
	"time"

	"github.com/pingcap/tidb-dashboard/pkg/apiserver"
	"github.com/pingcap/tidb-dashboard/pkg/config"
	"github.com/pingcap/tidb-dashboard/pkg/uiserver"

	"github.com/tikv/pd/pkg/dashboard/adapter"
	"github.com/tikv/pd/pkg/dashboard/distroutil"
	"github.com/tikv/pd/pkg/dashboard/keyvisual"
	ui "github.com/tikv/pd/pkg/dashboard/uiserver"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
)

var (
	apiServiceGroup = apiutil.APIServiceGroup{
		Name:       "dashboard-api",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: config.APIPathPrefix,
	}

	uiServiceGroup = apiutil.APIServiceGroup{
		Name:       "dashboard-ui",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: config.UIPathPrefix,
	}
)

// SetCheckInterval changes adapter.CheckInterval
func SetCheckInterval(d time.Duration) {
	adapter.CheckInterval = d
}

// GetServiceBuilders returns all ServiceBuilders required by Dashboard
func GetServiceBuilders() []server.HandlerBuilder {
	var (
		err           error
		cfg           *config.Config
		internalProxy bool
		redirector    *adapter.Redirector
		assets        http.FileSystem
		s             *apiserver.Service
	)

	// The order of execution must be sequential.
	return []server.HandlerBuilder{
		// Dashboard API Service
		func(_ context.Context, srv *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
			distroutil.MustLoadAndReplaceStrings()

			if cfg, err = adapter.GenDashboardConfig(srv); err != nil {
				return nil, apiServiceGroup, err
			}
			internalProxy = srv.GetConfig().Dashboard.InternalProxy
			redirector = adapter.NewRedirector(srv.Name(), cfg.ClusterTLSConfig)
			assets = ui.Assets(cfg)

			var stoppedHandler http.Handler
			if internalProxy {
				stoppedHandler = http.HandlerFunc(redirector.ReverseProxy)
			} else {
				stoppedHandler = http.HandlerFunc(redirector.TemporaryRedirect)
			}
			s = apiserver.NewService(
				cfg,
				stoppedHandler,
				assets,
				keyvisual.GenCustomDataProvider(srv),
			)

			m := adapter.NewManager(srv, s, redirector)
			srv.AddStartCallback(m.Start)
			srv.AddCloseCallback(m.Stop)

			return apiserver.Handler(s), apiServiceGroup, nil
		},
		// Dashboard UI
		func(context.Context, *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
			if err != nil {
				return nil, uiServiceGroup, err
			}

			var handler http.Handler
			uiHandler := http.StripPrefix(uiServiceGroup.PathPrefix, uiserver.Handler(assets))
			if internalProxy {
				handler = redirector.NewStatusAwareHandler(uiHandler)
			} else {
				handler = s.NewStatusAwareHandler(uiHandler, http.HandlerFunc(redirector.TemporaryRedirect))
			}

			return handler, uiServiceGroup, nil
		},
	}
}
