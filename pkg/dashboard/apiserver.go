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
	"net/http"

	"github.com/pingcap-incubator/tidb-dashboard/pkg/apiserver"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/config"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/keyvisual/region"
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/pd/v4/pkg/dashboard/keyvisual/input"
	"github.com/pingcap/pd/v4/server"
)

func newAPIService(srv *server.Server) (*apiserver.Service, error) {
	cfg := srv.GetConfig()

	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}

	dashboardCfg := &config.Config{
		DataDir:    cfg.DataDir,
		PDEndPoint: etcdCfg.ACUrls[0].String(),
	}
	dashboardCfg.ClusterTLSConfig, err = cfg.Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	dashboardCfg.TiDBTLSConfig, err = cfg.Dashboard.ToTiDBTLSConfig()
	if err != nil {
		return nil, err
	}

	s := apiserver.NewService(
		dashboardCfg,
		apiserver.StoppedHandler,
		func(c *config.Config, httpClient *http.Client, etcdClient *clientv3.Client) *region.PDDataProvider {
			return &region.PDDataProvider{
				EtcdClient:     etcdClient,
				PeriodicGetter: input.NewCorePeriodicGetter(srv),
			}
		},
	)

	return s, nil
}
