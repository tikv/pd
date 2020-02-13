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

package apiserver

import (
	"context"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/apiserver"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/keyvisual"
	"net/http"
	"sync"

	"github.com/pingcap-incubator/tidb-dashboard/pkg/config"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/dbstore"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/keyvisual/region"
	"github.com/pingcap/pd/pkg/dashboard/keyvisual/input"
	"github.com/pingcap/pd/server"
)

var serviceGroup = server.ServiceGroup{
	Name:       "dashboard-api",
	Version:    "v1",
	IsCore:     false,
	PathPrefix: "/dashboard/api",
}

// NewService returns an http.Handler that serves the dashboard API
func NewService(ctx context.Context, srv *server.Server) (http.Handler, server.ServiceGroup) {
	cfg := srv.GetConfig()
	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		panic(err)
	}
	dashboardCfg := &config.Config{
		Version:    server.PDReleaseVersion,
		DataDir:    cfg.DataDir,
		PDEndPoint: etcdCfg.ACUrls[0].String(),
	}
	dashboardCfg.TLSConfig, err = cfg.Security.ToTLSConfig()
	if err != nil {
		panic(err)
	}

	// key visual
	dashboardCtx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	store := dbstore.MustOpenDBStore(dashboardCfg)
	localDataProvider := &region.PDDataProvider{
		PeriodicGetter: input.NewCorePeriodicGetter(srv),
		GetEtcdClient:  srv.GetClient,
		Store:          store,
	}
	keyvisualService := keyvisual.NewService(dashboardCtx, wg, dashboardCfg, localDataProvider)
	// api server
	services := &apiserver.Services{
		Store:     store,
		KeyVisual: keyvisualService,
	}
	handler := apiserver.Handler(serviceGroup.PathPrefix, dashboardCfg, services)

	// callback
	srv.AddStartCallback(func() error {
		keyvisualService.Start()
		return nil
	}, "unreachable")
	srv.AddCloseCallback(func() error {
		cancel()
		return nil
	}, "unreachable")
	srv.AddCloseCallback(func() error {
		wg.Wait()
		return nil
	}, "unreachable")
	srv.AddCloseCallback(store.Close, "close dashboard dbstore error")

	return handler, serviceGroup
}
