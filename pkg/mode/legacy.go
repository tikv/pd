// Copyright 2023 TiKV Project Authors.
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

package mode

import (
	"context"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/autoscaling"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/swaggerserver"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/apiv2"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/join"

	// Register schedulers.
	_ "github.com/tikv/pd/server/schedulers"

	// Register Service
	_ "github.com/tikv/pd/pkg/mcs/registry"
	_ "github.com/tikv/pd/pkg/mcs/resource_manager/server/install"
	// todo init function
)

// LegacyStart starts the legacy mode, where are all services are started.
func LegacyStart(ctx context.Context, cfg *config.Config) ServiceServer {
	err := join.PrepareJoinCluster(cfg)
	if err != nil {
		log.Fatal("join meet error", errs.ZapError(err))
	}

	// Creates server.
	serviceBuilders := []server.HandlerBuilder{api.NewHandler, apiv2.NewV2Handler, swaggerserver.NewHandler, autoscaling.NewHandler}
	serviceBuilders = append(serviceBuilders, dashboard.GetServiceBuilders()...)
	svr, err := server.CreateServer(ctx, cfg, serviceBuilders...)
	if err != nil {
		log.Fatal("create server failed", errs.ZapError(err))
	}
	return svr
}
