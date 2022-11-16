// Copyright 2022 TiKV Project Authors.
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

package registry

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	// ServerGRPCServiceregistry is the global grpc service registry.
	ServerGRPCServiceregistry = make(GRPCServiceregistry)
)

// GRPCServiceLoader is a function that creates a grpc service.
type GRPCServiceLoader func(*server.Server) GRPCService

// GRPCService is the interface that should wraps the RegisterService method.
type GRPCService interface {
	RegisterService(g *grpc.Server)
}

// GRPCServiceregistry is a map that stores all registered grpc services.
type GRPCServiceregistry map[string]GRPCServiceLoader

// InstallAllServices installs all registered grpc services.
// TODO: use `uber/fx` to manage the lifecycle of grpc services.
func (r GRPCServiceregistry) InstallAllServices(srv *server.Server, g *grpc.Server) {
	for name, loader := range r {
		loader(srv).RegisterService(g)
		log.Info("grpc service registered", zap.String("service-name", name))
	}
}

// RegisterService registers a grpc service.
func (r GRPCServiceregistry) RegisterService(name string, service GRPCServiceLoader) {
	r[name] = service
}

func init() {
	server.NewGRPCServiceregistry = func() server.GRPCServiceregistry {
		return ServerGRPCServiceregistry
	}
}
