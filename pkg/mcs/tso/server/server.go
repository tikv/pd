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
	"flag"
	"net/http"
	"os"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"go.etcd.io/etcd/clientv3"
)

// Server is the TSO server, and it implements bs.Server.
// nolint
type Server struct {
	ctx context.Context
}

// TODO: Implement the following methods defined in bs.Server

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

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return nil
}

// GetHTTPClient returns builtin http client.
func (s *Server) GetHTTPClient() *http.Client {
	return nil
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(args []string) (context.Context, context.CancelFunc, bs.Server) {
	cfg := tso.NewConfig()
	err := cfg.Parse(args)

	if cfg.Version {
		// TODO: support printing TSO server info
		// server.PrintTSOInfo()
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

	// New zap logger
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	// TODO: support printing TSO server info
	// LogTSOInfo()

	// TODO: Make it configurable if it has big impact on performance.
	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	// TODO: Create the server
	return nil, nil, nil
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
