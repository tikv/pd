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

package server

import (
	"context"
	"time"

	"github.com/pingcap/log"
	basicsvr "github.com/tikv/pd/pkg/basic_server"
	_ "github.com/tikv/pd/pkg/mcs/registry"
	tsosvr "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	_ "google.golang.org/grpc"
)

// TSOStart starts the TSO server.
func TSOStart(ctx context.Context, cfg *config.Config) basicsvr.Server {
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
	svr := tsosvr.NewServer(ctx, client)
	// TODO: wait for #5933 to check-in
	//gs := grpc.NewServer()
	//registry.ServerServiceRegistry.RegisterService("TSO", tsosvr.NewService)
	//registry.ServerServiceRegistry.InstallAllGRPCServices(svr, gs)
	return svr
}
