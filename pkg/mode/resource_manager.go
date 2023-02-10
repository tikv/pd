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
	"net"
	"net/http"

	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/registry"
	rm_server "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

// ResourceManagerServer is the server for resource manager.
type ResourceManagerServer struct {
	ctx        context.Context
	name       string
	serverURL  string
	etcdClient *clientv3.Client
	httpClient *http.Client
	grpcServer *grpc.Server
	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
	// primaryCallbacks will be called after the server becomes leader.
	primaryCallbacks []func(context.Context)
}

// Context returns the context.
func (s *ResourceManagerServer) Context() context.Context {
	return s.ctx
}

// AddStartCallback adds a callback in the startServer phase.
func (s *ResourceManagerServer) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

// Name returns the name of the server.
func (s *ResourceManagerServer) Name() string {
	return s.name
}

// GetClient returns the etcd client.
func (s *ResourceManagerServer) GetClient() *clientv3.Client {
	return s.etcdClient
}

// GetHTTPClient returns builtin etcd client.
func (s *ResourceManagerServer) GetHTTPClient() *http.Client {
	return s.httpClient
}

// GetMember returns the member.
func (s *ResourceManagerServer) IsPrimary() bool {
	// TODO: implement this
	return true
}

// AddPrimaryCallback adds the callback function when the server becomes leader.
func (s *ResourceManagerServer) AddPrimaryCallback(callbacks ...func(context.Context)) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
}

// Run runs the server.
func (s *ResourceManagerServer) Run() error {
	log.Info("resource manager server is running")
	lis, err := net.Listen("tcp", s.serverURL)
	if err != nil {
		return err
	}
	s.grpcServer.Serve(lis)
	return nil
}

// Close closes the server.
func (s *ResourceManagerServer) Close() {
	log.Info("closing server")
	if s.etcdClient != nil {
		if err := s.etcdClient.Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}
	log.Info("close server")
}

// ResourceManagerStart starts the resource manager server.
func ResourceManagerStart(ctx context.Context, cfg *config.Config) bs.Server {
	// TODO: use resource manager config
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return nil
	}
	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil
	}

	// create client
	etcdClient, httpClient, err := etcdutil.CreateClients(tlsConfig, etcdCfg.ACUrls)
	if err != nil {
		return nil
	}
	// start server
	s := &ResourceManagerServer{
		serverURL:  "127.0.0.1:50051", // TODO: use resource manager config
		ctx:        ctx,
		name:       "ResourceManager",
		etcdClient: etcdClient,
		httpClient: httpClient,
		grpcServer: grpc.NewServer(),
	}
	registry.ServerServiceRegistry.RegisterService("ResourceManager", rm_server.NewService)
	registry.ServerServiceRegistry.InstallAllGRPCServices(s, s.grpcServer)
	return s
}
