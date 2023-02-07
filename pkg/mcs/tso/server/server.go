// Copyright 2016 TiKV Project Authors.
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
	"flag"
	"net/http"
	"os"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	basicsvr "github.com/tikv/pd/pkg/basic_server"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"go.etcd.io/etcd/clientv3"
)

// if server doesn't implement all methods of basicsvr.Server, this line will result in
// clear error message "*MyTServerype does not implement basicsvr.Server (missing Method method)"
var _ basicsvr.Server = (*Server)(nil)

// Server is the TSO server. It implements basicsvr.Server
// nolint
type Server struct {
	ctx context.Context
}

// TODO: Implement the following methos

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return ""
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// Run runs the pd server.
func (s *Server) Run() error {
	return nil
}

// Close closes the server.
func (s *Server) Close() {
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return true
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return nil
}

// GetHTTPClient returns builtin etcd client.
func (s *Server) GetHTTPClient() *http.Client {
	return nil
}

func CreateServerWrapper(args []string) (context.Context, context.CancelFunc, basicsvr.Server) {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])

	if cfg.Version {
		printVersionInfo()
		exit(0)
	}

	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", errs.ZapError(err))
	}

	if cfg.ConfigCheck {
		printConfigCheckMsg(cfg)
		exit(0)
	}

	// TODO: Initialize logger

	// TODO: Make it configurable if it has big impact on performance.
	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	// Joins the cluster
	// ...

	// Creates the server
	// ...

	return nil, nil, nil
}

func printVersionInfo() {
}

func printConfigCheckMsg(cfg *Config) {
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
