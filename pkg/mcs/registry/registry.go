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
	"net/http"

	"github.com/pingcap/log"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	// ServerServiceRegistry is the global grpc service registry.
	ServerServiceRegistry = make(ServiceRegistry)
)

// ServiceLoader is a function that creates a grpc service.
type ServiceLoader func(*server.Server) RegistrableService

// RegistrableService is the interface that should wraps the RegisterService method.
type RegistrableService interface {
	RegisterGRPCService(g *grpc.Server)
	RegisterRESTServer(userDefineHandler map[string]http.Handler)
}

// ServiceRegistry is a map that stores all registered grpc services.
type ServiceRegistry map[string]ServiceLoader

// InstallAllServices installs all registered grpc services.
// TODO: use `uber/fx` to manage the lifecycle of grpc services.
func (r ServiceRegistry) InstallAllServices(srv *server.Server, g *grpc.Server, h map[string]http.Handler) {
	for name, loader := range r {
		l := loader(srv)
		l.RegisterGRPCService(g)
		l.RegisterRESTServer(h)
		log.Info("service registered", zap.String("service-name", name))
	}
}

// RegisterService registers a grpc service.
func (r ServiceRegistry) RegisterService(name string, service ServiceLoader) {
	r[name] = service
}

func init() {
	server.NewServiceregistry = func() server.Serviceregistry {
		return ServerServiceRegistry
	}
}
