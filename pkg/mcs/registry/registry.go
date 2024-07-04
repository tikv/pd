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
// TODO: Use the `uber/fx` to manage the lifecycle of services.
package registry

import (
	"fmt"
	"net/http"

	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	// ServerServiceRegistry is the global grpc service registry.
	ServerServiceRegistry = NewServerServiceRegistry()
)

// ServiceBuilder is a function that creates a grpc service.
type ServiceBuilder func(bs.Server) RegistrableService

// RegistrableService is the interface that should wraps the RegisterService method.
type RegistrableService interface {
	RegisterGRPCService(g *grpc.Server)
	RegisterRESTHandler(userDefineHandlers map[string]http.Handler) error
}

// ServiceRegistry is a map that stores all registered grpc services.
// It implements the `ServiceRegistry` interface.
type ServiceRegistry struct {
	builders map[string]ServiceBuilder
	services map[string]RegistrableService
}

// NewServerServiceRegistry creates a new ServiceRegistry.
func NewServerServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		builders: make(map[string]ServiceBuilder),
		services: make(map[string]RegistrableService),
	}
}

func createServiceName(prefix, name string) string {
	return fmt.Sprintf("%s_%s", prefix, name)
}

// InstallAllGRPCServices installs all registered grpc services.
func (r *ServiceRegistry) InstallAllGRPCServices(srv bs.Server, g *grpc.Server) {
	prefix := srv.Name()
	for name, builder := range r.builders {
		serviceName := createServiceName(prefix, name)
		if l, ok := r.services[serviceName]; ok {
			l.RegisterGRPCService(g)
			log.Info("gRPC service already registered", zap.String("prefix", prefix), zap.String("service-name", name))
			continue
		}
		l := builder(srv)
		r.services[serviceName] = l
		l.RegisterGRPCService(g)
		log.Info("gRPC service registered successfully", zap.String("prefix", prefix), zap.String("service-name", name))
	}
}

// InstallAllRESTHandler installs all registered REST services.
func (r *ServiceRegistry) InstallAllRESTHandler(srv bs.Server, h map[string]http.Handler) {
	prefix := srv.Name()
	for name, builder := range r.builders {
		serviceName := createServiceName(prefix, name)
		if l, ok := r.services[serviceName]; ok {
			if err := l.RegisterRESTHandler(h); err != nil {
				log.Error("register restful API service failed", zap.String("prefix", prefix), zap.String("service-name", name), zap.Error(err))
			} else {
				log.Info("restful API service already registered", zap.String("prefix", prefix), zap.String("service-name", name))
			}
			continue
		}
		l := builder(srv)
		r.services[serviceName] = l
		if err := l.RegisterRESTHandler(h); err != nil {
			log.Error("register restful API service failed", zap.String("prefix", prefix), zap.String("service-name", name), zap.Error(err))
		} else {
			log.Info("restful API service registered successfully", zap.String("prefix", prefix), zap.String("service-name", name))
		}
	}
}

// RegisterService registers a grpc service.
func (r *ServiceRegistry) RegisterService(name string, service ServiceBuilder) {
	r.builders[name] = service
}
