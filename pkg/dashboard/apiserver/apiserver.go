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
	"net/http"

	"github.com/pingcap-incubator/tidb-dashboard/pkg/apiserver"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/config"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/dbstore"
	dashboardhttp "github.com/pingcap-incubator/tidb-dashboard/pkg/http"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/keyvisual"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/keyvisual/region"
	dashboardpd "github.com/pingcap-incubator/tidb-dashboard/pkg/pd"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/tidb"
	"github.com/pingcap/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/pd/v4/pkg/dashboard/keyvisual/input"
	"github.com/pingcap/pd/v4/server"
)

var _ dashboardpd.EtcdProvider = (*PdEtcdProvider)(nil)

var serviceGroup = server.ServiceGroup{
	Name:       "dashboard-api",
	Version:    "v1",
	IsCore:     false,
	PathPrefix: "/dashboard/api/",
}

// PdEtcdProvider provides etcd client from PD server
type PdEtcdProvider struct {
	srv *server.Server
}

// GetEtcdClient gets etcd client
func (p *PdEtcdProvider) GetEtcdClient() *clientv3.Client {
	return p.srv.GetClient()
}

// NewService returns an http.Handler that serves the dashboard API
func NewService(ctx context.Context, srv *server.Server) (http.Handler, server.ServiceGroup) {
	cfg := srv.GetConfig()
	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		panic(err)
	}

	dashboardCfg := &config.Config{
		DataDir:    cfg.DataDir,
		PDEndPoint: etcdCfg.ACUrls[0].String(),
	}
	dashboardCfg.TLSConfig, err = cfg.Security.ToTLSConfig()
	if err != nil {
		panic(err)
	}

	etcdProvider := &PdEtcdProvider{srv: srv}
	tidbForwarder := tidb.NewForwarder(tidb.NewForwarderConfig(), etcdProvider)
	httpClient := dashboardhttp.NewHTTPClientWithConf(dashboardCfg)
	store := dbstore.MustOpenDBStore(dashboardCfg)
	// key visual
	localDataProvider := &region.PDDataProvider{
		PeriodicGetter: input.NewCorePeriodicGetter(srv),
		EtcdProvider:   etcdProvider,
		Store:          store,
	}
	keyvisualService := keyvisual.NewService(ctx, dashboardCfg, localDataProvider)
	// api server
	services := &apiserver.Services{
		Store:         store,
		KeyVisual:     keyvisualService,
		TiDBForwarder: tidbForwarder,
		EtcdProvider:  etcdProvider,
		HTTPClient:    httpClient,
	}
	handler := apiserver.Handler(serviceGroup.PathPrefix, dashboardCfg, services)

	// callback
	srv.AddStartCallback(
		func() {
			// FIXME: Handle open error
			_ = tidbForwarder.Open()
		},
		keyvisualService.Start,
	)
	srv.AddCloseCallback(
		keyvisualService.Close,
		func() {
			_ = tidbForwarder.Close()
			_ = store.Close()
		},
	)

	log.Info("Enabled Dashboard API", zap.String("path", serviceGroup.PathPrefix))
	return handler, serviceGroup
}
