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

// Package registry is used to register the services.
// TODO: Remove the `pd/server` dependencies
// TODO: Use the `uber/fx` to manage the lifecycle of services.
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
	ServerServiceRegistry = newServiceRegistry()
)

// ServiceBuilder is a function that creates a grpc service.
type ServiceBuilder func(*server.Server) RegistrableService

// RegistrableService is the interface that should wraps the RegisterService method.
type RegistrableService interface {
	RegisterGRPCService(g *grpc.Server)
	RegisterRESTHandler(userDefineHandlers map[string]http.Handler)
}

// ServiceRegistry is a map that stores all registered grpc services.
// It implements the `Serviceregistry` interface.
type ServiceRegistry struct {
	builders map[string]ServiceBuilder
	services map[string]RegistrableService
}

func newServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		builders: make(map[string]ServiceBuilder),
		services: make(map[string]RegistrableService),
	}
}

// InstallAllGRPCServices installs all registered grpc services.
func (r *ServiceRegistry) InstallAllGRPCServices(srv *server.Server, g *grpc.Server) {
	for name, builder := range r.builders {
		if l, ok := r.services[name]; ok {
			l.RegisterGRPCService(g)
			log.Info("gRPC service already registered", zap.String("service-name", name))
			continue
		}
		l := builder(srv)
		l.RegisterGRPCService(g)
		log.Info("gRPC service register success", zap.String("service-name", name))
	}
}

// InstallAllRESTHandler installs all registered REST services.
func (r *ServiceRegistry) InstallAllRESTHandler(srv *server.Server, h map[string]http.Handler) {
	for name, builder := range r.builders {
		if l, ok := r.services[name]; ok {
			l.RegisterRESTHandler(h)
			log.Info("restful API service already registered", zap.String("service-name", name))
			continue
		}
		l := builder(srv)
		l.RegisterRESTHandler(h)
		log.Info("restful API service register success", zap.String("service-name", name))
	}
}

// RegisterService registers a grpc service.
func (r ServiceRegistry) RegisterService(name string, service ServiceBuilder) {
	r.builders[name] = service
}

func init() {
	server.NewServiceregistry = func() server.Serviceregistry {
		return ServerServiceRegistry
	}
}
