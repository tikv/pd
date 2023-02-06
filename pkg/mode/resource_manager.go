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
	"time"

	// "github.com/pingcap/log"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mcs/registry"
	rm_server "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ResourceManagerServer is the server for resource manager.
type ResourceManagerServer struct {
	ctx    context.Context
	name   string
	client *clientv3.Client
	member *member.Member
}

// Context returns the context.
func (s *ResourceManagerServer) Context() context.Context {
	return s.ctx
}

// AddStartCallback adds the callback function when the server starts.
func (s *ResourceManagerServer) AddStartCallback(callbacks ...func()) {

}

// Name returns the name of the server.
func (s *ResourceManagerServer) Name() string {
	return s.name
}

// GetClient returns the etcd client.
func (s *ResourceManagerServer) GetClient() *clientv3.Client {
	return s.client
}

// GetMember returns the member.
func (s *ResourceManagerServer) GetMember() *member.Member {
	return s.member
}

// AddLeaderCallback adds the callback function when the server becomes leader.
func (s *ResourceManagerServer) AddLeaderCallback(callbacks ...func(context.Context)) {

}

// ResourceManagerStart starts the resource manager server.
func ResourceManagerStart(ctx context.Context, cfg *config.Config) ServiceServer {
	// start client
	etcdTimeout := time.Second * 3
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return nil
	}
	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil
	}

	endpoints := []string{etcdCfg.ACUrls[0].String()}
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints), zap.Reflect("cert", cfg.Security))

	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
		LogConfig:   &lgc,
	})
	if err != nil {
		return nil
	}
	// start server
	s := &ResourceManagerServer{
		ctx:    ctx,
		name:   "ResourceManager",
		client: client,
		member: nil, // todo: add member
	}
	gs := grpc.NewServer()
	registry.ServerServiceRegistry.RegisterService("ResourceManager", rm_server.NewService)
	registry.ServerServiceRegistry.InstallAllGRPCServices(s, gs)

	return nil
}
