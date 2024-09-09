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

package adapter

import (
	"os"
	"strings"

	"github.com/pingcap/tidb-dashboard/pkg/config"

	"github.com/tikv/pd/server"
)

const (
	envTidbDashboardDisableCustomPromAddr = "TIDB_DASHBOARD_DISABLE_CUSTOM_PROM_ADDR"
)

// GenDashboardConfig generates a configuration for Dashboard Server.
func GenDashboardConfig(srv *server.Server) (*config.Config, error) {
	cfg := srv.GetConfig()

	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}

	dashboardCfg := config.Default()
	dashboardCfg.DataDir = cfg.DataDir
	dashboardCfg.PDEndPoint = etcdCfg.AdvertiseClientUrls[0].String()
	dashboardCfg.PublicPathPrefix = cfg.Dashboard.PublicPathPrefix
	dashboardCfg.EnableTelemetry = cfg.Dashboard.EnableTelemetry
	dashboardCfg.EnableExperimental = cfg.Dashboard.EnableExperimental
	if dashboardCfg.ClusterTLSConfig, err = cfg.Security.ToTLSConfig(); err != nil {
		return nil, err
	}
	if dashboardCfg.ClusterTLSInfo, err = cfg.Security.ToTLSInfo(); err != nil {
		return nil, err
	}
	if dashboardCfg.TiDBTLSConfig, err = cfg.Dashboard.ToTiDBTLSConfig(); err != nil {
		return nil, err
	}

	dashboardCfg.NormalizePublicPathPrefix()

	// Allow setting DisableCustomPromAddr via environment variable.
	disableCustomPromAddr := strings.ToLower(os.Getenv(envTidbDashboardDisableCustomPromAddr))
	if disableCustomPromAddr == "true" || disableCustomPromAddr == "1" {
		dashboardCfg.DisableCustomPromAddr = true
	}

	return dashboardCfg, nil
}
